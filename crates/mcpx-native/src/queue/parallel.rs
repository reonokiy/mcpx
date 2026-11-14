use super::notification_controls::Controls;
use super::*;
use crate::driver::EventDispatch;
use crate::stdio::{StreamCancellation, StreamWriter};
use crate::transport::{Input, Message};
use mcpx_command::ClientCommandExt;
use mcpx_core::{Action, BoundArgs, Credential, Driver, DriverContext, HostRuntime};
use std::cell::{Cell, RefCell};
use std::ops::ControlFlow;

#[derive(Default)]
struct Health {
    failure: RefCell<Option<Error>>,
    received: Cell<bool>,
}
impl Health {
    fn get(&self) -> bool {
        self.failure.borrow().is_none()
    }
    fn fail(&self, error: Error) {
        if self.get() {
            *self.failure.borrow_mut() = Some(error);
        }
    }
    fn reset(&self) {
        *self.failure.borrow_mut() = None;
    }
    fn recoverable(&self) -> bool {
        self.failure
            .borrow()
            .as_ref()
            .is_some_and(crate::reconnect::recoverable)
    }
}

struct RecoveryBudget {
    deadline: tokio::time::Instant,
    remaining: u8,
}
#[derive(Clone, Copy)]
struct RecoveryPolicy {
    max_attempts: u8,
    backoff: Duration,
    session: bool,
}
impl From<crate::StdioReconnectPolicy> for RecoveryPolicy {
    fn from(policy: crate::StdioReconnectPolicy) -> Self {
        Self {
            max_attempts: policy.max_attempts,
            backoff: policy.backoff,
            session: false,
        }
    }
}
impl From<crate::HttpSessionRecoveryPolicy> for RecoveryPolicy {
    fn from(policy: crate::HttpSessionRecoveryPolicy) -> Self {
        Self {
            max_attempts: policy.max_attempts,
            backoff: policy.backoff,
            session: true,
        }
    }
}
impl RecoveryPolicy {
    fn retry(self, error: &Error) -> bool {
        if self.session {
            crate::HttpSessionRecoveryPolicy::retryable(error)
        } else {
            crate::reconnect::recoverable(error)
        }
    }
    fn timeout(self) -> Error {
        Error::local(
            "Timeout",
            if self.session {
                "HTTP session recovery budget exhausted"
            } else {
                "stdio recovery budget exhausted"
            },
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn recover(
    remote: &mut Remote,
    policy: RecoveryPolicy,
    budget: &mut RecoveryBudget,
    pending: &mut VecDeque<Job>,
    receiver: &mut mpsc::UnboundedReceiver<Job>,
    shutdown: &CancellationToken,
    wake: &Notify,
    controls: &mut Controls,
) -> Result<()> {
    let deadline = budget.deadline;
    while budget.remaining > 0 {
        let delayed = budget.remaining < policy.max_attempts;
        budget.remaining -= 1;
        let round = async {
            if delayed {
                tokio::time::sleep(policy.backoff).await;
            }
            remote.reconnect().await
        };
        tokio::pin!(round);
        let result = loop {
            controls.prune();
            let waiting = pending
                .iter()
                .map(|job| job.deadline)
                .chain(controls.deadline())
                .min();
            tokio::select! {biased;
                _=shutdown.cancelled()=>return Err(closed_error()),
                _=tokio::time::sleep_until(deadline)=>return Err(policy.timeout()),
                result=&mut round=>break result,
                incoming=receiver.recv(),if !receiver.is_closed()=>match incoming {Some(job)=>pending.push_back(job),None=>return Err(closed_error())},
                _=controls.receive(),if controls.open()=>{},
                _=wake.notified()=>prune(pending),
                _=async {if let Some(deadline)=waiting {tokio::time::sleep_until(deadline.into()).await;}else{std::future::pending::<()>().await;} }=>prune(pending),
            }
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(policy.timeout());
        }
        match result {
            Ok(()) => return Ok(()),
            Err(error) if budget.remaining > 0 && policy.retry(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Err(Error::local(
        "ReconnectExhausted",
        if policy.session {
            "HTTP session recovery rounds exhausted"
        } else {
            "stdio recovery rounds exhausted"
        },
    ))
}

#[derive(Clone, Copy)]
enum Output {
    Outcome,
    Command,
}
impl Output {
    fn reply(self, outcome: CallOutcome) -> Reply {
        match self {
            Self::Outcome => Reply::Outcome(outcome),
            Self::Command => Reply::Execution(Execution::Outcome(outcome)),
        }
    }
}
enum Candidate {
    Call(ToolBinding, BoundArgs, u64, Arc<()>),
    Resume(Continuation, Value),
    Command(String, Vec<String>),
}
impl Candidate {
    fn output(&self) -> Output {
        match self {
            Self::Command(..) => Output::Command,
            _ => Output::Outcome,
        }
    }
    async fn admit(
        &self,
        remote: &mut Remote,
        context: DriverContext,
    ) -> Result<ControlFlow<String, u64>> {
        match self {
            Self::Call(binding, args, ..) => remote
                .admit_prepared_call(context, binding, args)
                .await
                .map(ControlFlow::Continue),
            Self::Resume(handle, responses) => remote
                .admit_prepared_resume(context, handle, responses.clone())
                .await
                .map(ControlFlow::Continue),
            Self::Command(tool, argv) => remote.admit_prepared_command(context, tool, argv).await,
        }
    }
    fn restore(self) -> Request {
        match self {
            Self::Call(_, args, id, lease) => Request::Call(id, args.value().clone(), lease),
            Self::Resume(handle, responses) => Request::Resume(handle, responses),
            Self::Command(tool, argv) => Request::Execute(tool, argv),
        }
    }
}

fn concurrent(request: &Request) -> bool {
    matches!(
        request,
        Request::Call(..) | Request::Resume(..) | Request::Execute(..)
    )
}
fn interrupted(job: &Job, shutdown: &CancellationToken) -> Option<Error> {
    if shutdown.is_cancelled() {
        Some(closed_error())
    } else if job.cancellation.is_cancelled() || job.reply.is_closed() {
        Some(Error::new(
            "Cancelled",
            "operation cancelled before sending",
        ))
    } else if job.deadline <= Instant::now() {
        Some(Error::new("Timeout", "operation expired before sending"))
    } else {
        None
    }
}

struct Active {
    output: Output,
    stream: Option<StreamCancellation>,
    job: Job,
    action_id: u64,
    action: Action,
    credential: Credential,
    http: reqwest::Client,
    task: Option<tokio::task::JoinHandle<()>>,
    response: Option<crate::response_recovery::ResponseIo>,
    completion: Option<Result<CallOutcome>>,
}
pub(super) fn finish(mut job: Job, result: Result<Reply>) {
    let result = result.map_err(|error| {
        let error = if error.operation_id.is_none() {
            error.with_context(mcpx_core::ErrorContext::not_sent())
        } else {
            error
        };
        error.with_queue_operation(job.id)
    });
    if let Some(ticket) = job.operation_event.take() {
        let outcome = match &result {
            Ok(Reply::Outcome(outcome) | Reply::Execution(Execution::Outcome(outcome))) => {
                Some(outcome)
            }
            _ => None,
        };
        ticket.finish(outcome, result.as_ref().err());
    }
    drop(job._slot);
    drop(job._bytes);
    drop(job._resume_slot.take());
    let _ = job.reply.send(result);
}
fn now(origin: Instant) -> u64 {
    origin.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

async fn http_task(
    http: reqwest::Client,
    credential: Credential,
    action: Action,
    sender: mpsc::Sender<Message>,
) {
    if let Action::Http {
        id,
        url,
        headers,
        body,
        ..
    } = action
    {
        crate::response_recovery::reader(http, credential, id, url, headers, Some(body), sender)
            .await;
    }
}
async fn cancel_http(active: &Active, _cause: &Error) {
    if let Some(request) = &active.stream {
        request.cancel().await;
        return;
    }
    let Action::Http {
        cancellation: Some(notification),
        url,
        headers,
        ..
    } = &active.action
    else {
        return;
    };
    if headers.get("mcp-protocol-version").map(String::as_str)
        != Some(mcpx_protocol::LEGACY_VERSION)
    {
        return;
    }
    let mut request = active.http.post(url).body(notification.to_string());
    for (name, value) in headers {
        request = request.header(name, value);
    }
    if let Some(token) = active.credential.token() {
        request = request.bearer_auth(token);
    }
    let _ = tokio::time::timeout(Duration::from_millis(200), request.send()).await;
}
async fn feed(
    runtime: &mut HostRuntime,
    message: Message,
    origin: Instant,
    writer: Option<&StreamWriter>,
    healthy: &Health,
    active: &mut BTreeMap<u64, Active>,
) {
    if let Err(error) = runtime.advance_time(now(origin)) {
        runtime.abort_error(error.clone());
        healthy.fail(error);
        return;
    }
    let input = match message.input {
        Input::NotificationHeaders(status, mime, deadline) => {
            if runtime.notification_status().is_some_and(|s| {
                s.resource_id == message.id && s.phase == mcpx_core::NotificationPhase::Opening
            }) && Instant::now() >= deadline
            {
                let _ = runtime.abort_notifications(message.id, true);
            } else {
                let _ = runtime.begin_notifications(message.id, status, &mime);
            }
            return;
        }
        Input::NotificationChunk(bytes, deadline) => {
            if runtime.notification_status().is_some_and(|s| {
                s.resource_id == message.id && s.phase == mcpx_core::NotificationPhase::Opening
            }) && Instant::now() >= deadline
            {
                let _ = runtime.abort_notifications(message.id, true);
            } else {
                let _ = runtime.push_notifications(message.id, &bytes);
            }
            return;
        }
        Input::NotificationEof => {
            let _ = runtime.finish_notifications(message.id);
            return;
        }
        Input::NotificationFailed(timeout) => {
            let _ = runtime.abort_notifications(message.id, timeout);
            return;
        }
        Input::Stdio(value) => {
            let result = match runtime.stream_input(value) {
                Ok(Some(reply)) => match writer {
                    Some(writer) => {
                        tokio::time::timeout(Duration::from_millis(100), writer.write(&reply))
                            .await
                            .unwrap_or_else(|_| {
                                Err(Error::new("Timeout", "stdio capability reply timed out"))
                            })
                    }
                    None => Err(Error::new(
                        "ProtocolError",
                        "stdio input arrived on an HTTP host",
                    )),
                },
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                if let Some(status) = runtime.notification_status().filter(|s| {
                    matches!(
                        s.phase,
                        mcpx_core::NotificationPhase::Opening
                            | mcpx_core::NotificationPhase::Listening
                    )
                }) {
                    let _ = runtime.abort_notifications(status.resource_id, false);
                }
                runtime.abort_error(error.clone());
                healthy.fail(error);
            }
            return;
        }
        Input::StdioFailed(error) => {
            if let Some(status) = runtime.notification_status().filter(|s| {
                matches!(
                    s.phase,
                    mcpx_core::NotificationPhase::Opening | mcpx_core::NotificationPhase::Listening
                )
            }) {
                let _ = runtime.abort_notifications(status.resource_id, false);
            }
            runtime.abort_error(error.clone());
            healthy.fail(error);
            return;
        }
        input => input,
    };
    let id = message.id;
    if !runtime.machine().has_action(id) {
        return;
    }
    if let Some(io) = active
        .values_mut()
        .find(|running| running.action_id == id)
        .and_then(|running| running.response.as_mut())
    {
        if io.external_stop() {
            return;
        }
        if let Err(error) = io.feed(runtime, input, origin)
            && runtime.machine().has_action(id)
        {
            let _ = runtime.fail_request(id, error);
        }
        return;
    }
    let result = match input {
        Input::Headers(status, headers) => runtime.begin_response(id, status, &headers).map(|_| ()),
        Input::Chunk(bytes) => runtime.push_response_for(id, &bytes).map(|_| ()),
        Input::Eof(_) => runtime.finish_response_for(id),
        Input::Failed(error, _) => runtime.fail_request(id, error),
        _ => unreachable!(),
    };
    if let Err(error) = result
        && runtime.machine().has_action(id)
    {
        let _ = runtime.fail_request(id, error);
    }
}

async fn harvest(
    runtime: &mut HostRuntime,
    active: &mut BTreeMap<u64, Active>,
    retired: &mut Vec<u64>,
    events: &EventDispatch,
    healthy: &Health,
    origin: Instant,
) {
    events.publish(runtime);
    let operations: Vec<_> = active.keys().copied().collect();
    for operation in operations {
        let running = active.get_mut(&operation).unwrap();
        if running.completion.is_none() {
            running.completion = runtime.take_completion(operation);
        }
        if let Some(io) = running.response.as_mut() {
            if let Some(result) = &running.completion {
                io.terminal(result.is_ok());
            }
            if running.completion.is_none() && (running.job.reply.is_closed() || io.external_stop())
            {
                continue;
            }
            let service = io.service(runtime, origin).await;
            running.action_id = io.id;
            if let Err(error) = service {
                if runtime.machine().has_action(io.id) {
                    let _ = runtime.fail_request(io.id, error);
                }
                if running.completion.is_none() {
                    running.completion = runtime.take_completion(operation);
                }
                io.terminal(false);
                io.close().await;
            }
            if !io.drained() {
                continue;
            }
        }
        if let Some(result) = running.completion.take() {
            if result.is_ok() {
                healthy.received.set(true);
            }
            if running.response.is_some()
                && result.as_ref().err().is_some_and(|e| {
                    matches!(
                        e.code.as_str(),
                        "Timeout" | "Cancelled" | "TransportClosed" | "ResponseRecoveryExhausted"
                    )
                })
            {
                cancel_http(running, result.as_ref().err().unwrap()).await;
            }
            let mut running = active.remove(&operation).unwrap();
            if let Some(mut io) = running.response.take() {
                io.close().await;
            }
            if let Some(task) = running.task.take() {
                task.abort();
                let _ = task.await;
            }
            retired.push(operation);
            finish(
                running.job,
                result.map(|outcome| running.output.reply(outcome)),
            );
        }
    }
}
async fn interrupt(
    runtime: &mut HostRuntime,
    active: &mut BTreeMap<u64, Active>,
    retired: &mut Vec<u64>,
    events: &EventDispatch,
    shutdown: bool,
) {
    if shutdown {
        // A final result already accepted by Core wins owner shutdown too.
        // Join its remaining reply I/O before releasing credits and providers.
        let completed: Vec<_> = active
            .iter()
            .filter_map(|(id, running)| running.completion.is_some().then_some(*id))
            .collect();
        for operation in completed {
            let mut running = active.remove(&operation).unwrap();
            if let Some(mut io) = running.response.take() {
                io.close().await;
            }
            if let Some(task) = running.task.take() {
                task.abort();
                let _ = task.await;
            }
            retired.push(operation);
            let result = running.completion.take().unwrap();
            finish(
                running.job,
                result.map(|outcome| running.output.reply(outcome)),
            );
        }
    }
    let interrupted: Vec<_> = active
        .iter()
        .filter_map(|(operation, active)| {
            if active.completion.is_some() {
                return None;
            }
            let cause = if shutdown {
                Some(closed_error())
            } else if active.job.cancellation.is_cancelled() || active.job.reply.is_closed() {
                Some(Error::new(
                    "Cancelled",
                    "operation cancelled; execution may be unknown",
                ))
            } else if active.job.deadline <= Instant::now() {
                Some(Error::new(
                    "Timeout",
                    "operation deadline exceeded; execution may be unknown",
                ))
            } else {
                None
            };
            cause.map(|cause| (*operation, cause))
        })
        .collect();
    for (operation, cause) in interrupted {
        let running = active.get_mut(&operation).unwrap();
        let cause = runtime.action_error_context(running.action_id).map_or_else(
            || cause.clone(),
            |context| cause.clone().with_context(context),
        );
        if let Some(task) = &running.task {
            task.abort();
        }
        if let Some(mut io) = running.response.take() {
            io.close().await;
        }
        cancel_http(running, &cause).await;
        if cause.code == "Cancelled" || cause.code == "TransportClosed" {
            let _ = runtime.cancel_operation(operation);
        } else {
            let _ = runtime.fail_request(running.action_id, cause.clone());
        }
        events.publish(runtime);
        let _ = runtime.take_completion(operation);
        let running = active.remove(&operation).unwrap();
        if let Some(task) = running.task {
            let _ = task.await;
        }
        retired.push(operation);
        finish(running.job, Err(cause));
    }
    // These are host cancellation directives; their HTTP streams have already been closed.
    let _ = runtime.take_actions();
}
fn retire(remote: &mut Remote, retired: &mut Vec<u64>) {
    for operation in retired.drain(..) {
        remote.driver_mut().concurrent_finished(operation);
    }
}
fn earliest(active: &BTreeMap<u64, Active>, pending: &VecDeque<Job>, fallback: Instant) -> Instant {
    active
        .values()
        .flat_map(|a| {
            (a.completion.is_none().then_some(a.job.deadline))
                .into_iter()
                .chain(a.response.as_ref().and_then(|io| io.wake_deadline()))
        })
        .chain(pending.iter().map(|j| j.deadline))
        .min()
        .unwrap_or(fallback)
}

/// A pending credential provider borrows only the driver; response processing keeps running.
#[allow(clippy::too_many_arguments)]
async fn prepare(
    remote: &mut Remote,
    job: &mut Job,
    resolved: Option<DriverContext>,
    active: &mut BTreeMap<u64, Active>,
    retired: &mut Vec<u64>,
    pending: &mut VecDeque<Job>,
    input: &mut mpsc::Receiver<Message>,
    receiver: &mut mpsc::UnboundedReceiver<Job>,
    shutdown: &CancellationToken,
    wake: &Notify,
    origin: Instant,
    events: &EventDispatch,
    writer: Option<&StreamWriter>,
    healthy: &Health,
    notifications: &mut super::notifications::Notifications,
    controls: &mut Controls,
) -> Result<DriverContext> {
    let session_recovery = writer.is_none() && remote.driver().http_session_recovery.is_some();
    let context = if let Some(context) = resolved {
        Ok(context)
    } else {
        let (runtime, driver) = remote.host_parts_mut();
        let preparation = driver.prepare();
        tokio::pin!(preparation);
        loop {
            harvest(runtime, active, retired, events, healthy, origin).await;
            interrupt(runtime, active, retired, events, false).await;
            controls.drain(runtime, notifications).await;
            controls.prune_starts(pending);
            if controls.superseded(job) {
                break Err(Controls::superseded_error());
            }
            if session_recovery && runtime.machine().http_session_expired() {
                break Err(Error::local(
                    "SessionRecoveryPending",
                    "session renewal must precede admission",
                ));
            }
            notifications
                .recover(runtime, controls.enabled, controls.resume_pending())
                .await?;
            notifications.expire_pending(runtime).await;
            let deadline = earliest(active, pending, job.deadline)
                .min(job.deadline)
                .min(
                    notifications
                        .opening_deadline(runtime)
                        .unwrap_or(job.deadline),
                )
                .min(controls.wake_deadline().unwrap_or(job.deadline))
                .min(notifications.recovery_deadline().unwrap_or(job.deadline));
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break Err(closed_error()),
                _ = job.cancellation.cancelled() => break Err(Error::new("Cancelled","operation cancelled before sending")),
                _ = tokio::time::sleep_until(job.deadline.into()) => break Err(Error::new("Timeout","operation expired before sending")),
                Some(message) = input.recv(), if !active.is_empty() || writer.is_some() || super::notifications::active(runtime) => {
                    feed(runtime,message,origin,writer,healthy,active).await;
                    harvest(runtime,active,retired,events,healthy,origin).await;
                    interrupt(runtime,active,retired,events,false).await;
                    prune(pending);
                    if !healthy.get() { break Err(closed_error()); }
                }
                result = &mut preparation => break result,
                incoming = receiver.recv(), if !receiver.is_closed() => if let Some(incoming) = incoming { pending.push_back(incoming); },
                _ = controls.receive(), if controls.open() => {},
                _ = wake.notified() => { interrupt(runtime,active,retired,events,false).await; prune(pending); }
                _ = tokio::time::sleep_until(deadline.into()) => { interrupt(runtime,active,retired,events,false).await; prune(pending); }
            }
        }
    }?;
    retire(remote, retired);
    job.deadline = job.deadline.min(remote.driver().prepared_deadline());
    // A notification, TTL or new identity may require exclusivity only after
    // the provider returned. Keep its one resolved snapshot while draining I/O.
    while !active.is_empty() && remote.prepared_needs_barrier(&context) {
        if session_recovery && remote.machine().http_session_expired() {
            return Err(Error::local(
                "SessionRecoveryPending",
                "session renewal must precede admission",
            ));
        }
        controls
            .drain(remote.host_runtime_mut(), notifications)
            .await;
        controls.prune_starts(pending);
        if controls.superseded(job) {
            return Err(Controls::superseded_error());
        }
        notifications
            .recover(
                remote.host_runtime_mut(),
                controls.enabled,
                controls.resume_pending(),
            )
            .await?;
        notifications
            .expire_pending(remote.host_runtime_mut())
            .await;
        if let Some(error) = interrupted(job, shutdown) {
            return Err(error);
        }
        let deadline = earliest(active, pending, job.deadline)
            .min(job.deadline)
            .min(
                notifications
                    .opening_deadline(remote.host_runtime_mut())
                    .unwrap_or(job.deadline),
            )
            .min(controls.wake_deadline().unwrap_or(job.deadline))
            .min(notifications.recovery_deadline().unwrap_or(job.deadline));
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Err(closed_error()),
            _ = job.cancellation.cancelled() => return Err(Error::new("Cancelled","operation cancelled before sending")),
            _ = tokio::time::sleep_until(job.deadline.into()) => return Err(Error::new("Timeout","operation expired before sending")),
            Some(message) = input.recv() => feed(remote.host_runtime_mut(),message,origin,writer,healthy,active).await,
            incoming = receiver.recv(), if !receiver.is_closed() => if let Some(incoming) = incoming { pending.push_back(incoming); },
            _ = controls.receive(), if controls.open() => {},
            _ = wake.notified() => {},
            _ = tokio::time::sleep_until(deadline.into()) => {},
        }
        harvest(
            remote.host_runtime_mut(),
            active,
            retired,
            events,
            healthy,
            origin,
        )
        .await;
        interrupt(remote.host_runtime_mut(), active, retired, events, false).await;
        retire(remote, retired);
        prune(pending);
        if !healthy.get() {
            return Err(closed_error());
        }
    }
    if let Some(error) = interrupted(job, shutdown) {
        return Err(error);
    }
    Ok(context)
}

#[allow(clippy::too_many_arguments)]
async fn serial(
    remote: &mut Remote,
    mut job: Job,
    bindings: &mut BTreeMap<u64, SavedBinding>,
    pending: &mut VecDeque<Job>,
    receiver: &mut mpsc::UnboundedReceiver<Job>,
    shutdown: &CancellationToken,
    wake: &Notify,
    controls: &mut Controls,
) {
    let request = std::mem::replace(&mut job.request, Request::Events);
    let mut interrupted = false;
    let result = {
        let future = dispatch(remote, bindings, job.id, request);
        tokio::pin!(future);
        loop {
            controls.prune();
            let deadline = pending
                .iter()
                .map(|j| j.deadline)
                .min()
                .unwrap_or(job.deadline)
                .min(controls.deadline().unwrap_or(job.deadline));
            tokio::select! {
                biased;
                result = &mut future => break result,
                _ = shutdown.cancelled() => { interrupted = true; break Err(closed_error()); }
                _ = job.cancellation.cancelled() => { interrupted = true; break Err(Error::new("Cancelled","operation cancelled; execution may be unknown")); }
                _ = tokio::time::sleep_until(job.deadline.into()) => { interrupted = true; break Err(Error::new("Timeout","operation deadline exceeded; execution may be unknown")); }
                incoming = receiver.recv(), if !receiver.is_closed() => if let Some(incoming) = incoming { pending.push_back(incoming); },
                _ = controls.receive(), if controls.open() => {},
                _ = wake.notified() => prune(pending),
                _ = tokio::time::sleep_until(deadline.into()), if !pending.is_empty() || controls.deadline().is_some() => prune(pending),
            }
        }
    };
    if interrupted {
        if let Err(error) = &result
            && error.code == "Timeout"
        {
            remote.abort_active(error.clone()).await;
        } else {
            remote.cancel_active().await;
        }
    }
    finish(job, result);
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    remote: &mut Remote,
    mut receiver: mpsc::UnboundedReceiver<Job>,
    shutdown: CancellationToken,
    wake: Arc<Notify>,
    maximum: usize,
    http_notifications: bool,
    stdio_notifications: bool,
    control_receiver: mpsc::UnboundedReceiver<Job>,
    notification_recovery: Option<NotificationRecoveryPolicy>,
) -> Result<()> {
    let (mut writer, mut sender, mut input) =
        if let Some(process) = remote.driver_mut().process.as_mut() {
            let (writer, sender, input) = process.multiplex_io()?;
            (Some(writer), sender, input)
        } else {
            let (sender, input) = mpsc::channel(16);
            (None, sender, input)
        };
    let notifications_enabled = if writer.is_some() {
        stdio_notifications
    } else {
        http_notifications
    };
    let mut controls = Controls::new(control_receiver, notifications_enabled);
    let healthy = Health::default();
    let origin = remote.driver().clock_origin();
    let events = remote.driver().event_dispatch();
    let mut pending = VecDeque::new();
    let mut active = BTreeMap::new();
    let mut bindings = BTreeMap::new();
    let mut retired = Vec::new();
    let mut recovery_budget: Option<RecoveryBudget> = None;
    let mut recovered_at = tokio::time::Instant::now();
    let http_policy = remote
        .driver()
        .http_session_recovery
        .filter(|_| writer.is_none());
    let initial_recovery = remote.driver_mut().session_recovery_state.take();
    let mut http_budget = initial_recovery.as_ref().map(|state| RecoveryBudget {
        deadline: state.deadline,
        remaining: state.remaining,
    });
    let mut http_recovered_at =
        initial_recovery.map_or_else(tokio::time::Instant::now, |state| state.healthy_since);
    let mut http_waiting = false;
    let mut notifications = super::notifications::Notifications::new(notification_recovery);
    loop {
        if !shutdown.is_cancelled() {
            controls
                .drain(remote.host_runtime_mut(), &mut notifications)
                .await;
        }
        notifications.expire(remote.host_runtime_mut());
        for _ in 0..16 {
            let Ok(message) = input.try_recv() else { break };
            feed(
                remote.host_runtime_mut(),
                message,
                origin,
                writer.as_ref(),
                &healthy,
                &mut active,
            )
            .await;
        }
        while let Ok(job) = receiver.try_recv() {
            pending.push_back(job);
        }
        controls.prune_starts(&mut pending);
        prune(&mut pending);
        harvest(
            remote.host_runtime_mut(),
            &mut active,
            &mut retired,
            &events,
            &healthy,
            origin,
        )
        .await;
        if healthy.received.replace(false) {
            recovery_budget = None;
            if !http_waiting && !remote.machine().http_session_expired() {
                http_budget = None;
            }
        }
        interrupt(
            remote.host_runtime_mut(),
            &mut active,
            &mut retired,
            &events,
            shutdown.is_cancelled(),
        )
        .await;
        retire(remote, &mut retired);
        if shutdown.is_cancelled() {
            break;
        }
        if let Some(policy) = http_policy
            && let Some(observed) = remote.machine().http_session_expired_at()
            && !http_waiting
        {
            if http_recovered_at.elapsed() >= policy.timeout {
                http_budget = None;
            }
            if http_budget.is_none() {
                http_budget = Some(RecoveryBudget {
                    deadline: policy.deadline(origin, observed)?,
                    remaining: policy.max_attempts,
                });
            }
            http_waiting = true;
            // Retire the old listener and its checkpoint without changing the
            // separately owned enable/stop preference. A detached reader must
            // not continue to appear Listening during peer drain.
            notifications.disable(remote.host_runtime_mut()).await?;
        }
        if http_waiting {
            notifications.stop().await;
            let policy = http_policy.expect("enabled session recovery");
            let budget = http_budget.as_mut().expect("session recovery window");
            if tokio::time::Instant::now() >= budget.deadline {
                break;
            }
            if active.is_empty() {
                if recover(
                    remote,
                    policy.into(),
                    budget,
                    &mut pending,
                    &mut receiver,
                    &shutdown,
                    &wake,
                    &mut controls,
                )
                .await
                .is_err()
                {
                    break;
                }
                http_waiting = false;
                http_recovered_at = tokio::time::Instant::now();
                continue;
            }
        }
        if healthy.get() && !http_waiting {
            notifications
                .sync(
                    remote,
                    &sender,
                    controls.enabled,
                    &wake,
                    writer.as_ref(),
                    false,
                )
                .await?;
            notifications
                .recover(
                    remote.host_runtime_mut(),
                    controls.enabled,
                    controls.resume_pending(),
                )
                .await?;
        } else {
            notifications.stop().await;
        }
        if !healthy.get() {
            if writer.is_some()
                && healthy.recoverable()
                && let Some(policy) = remote.driver().reconnect_policy
            {
                if recovered_at.elapsed() >= policy.timeout {
                    recovery_budget = None;
                }
                let budget = recovery_budget.get_or_insert_with(|| RecoveryBudget {
                    deadline: tokio::time::Instant::now() + policy.timeout,
                    remaining: policy.max_attempts,
                });
                install_reader(remote, &mut input);
                if recover(
                    remote,
                    policy.into(),
                    budget,
                    &mut pending,
                    &mut receiver,
                    &shutdown,
                    &wake,
                    &mut controls,
                )
                .await
                .is_ok()
                {
                    let (new_writer, new_sender, new_input) = remote
                        .driver_mut()
                        .process
                        .as_mut()
                        .expect("stdio recovery restored process")
                        .multiplex_io()?;
                    writer = Some(new_writer);
                    sender = new_sender;
                    input = new_input;
                    healthy.reset();
                    recovered_at = tokio::time::Instant::now();
                    continue;
                }
            }
            break;
        }
        let can_start = !http_waiting
            && active.len() < maximum
            && pending.front().is_some_and(|job| {
                active.is_empty() || (concurrent(&job.request) && !remote.catalog_needs_refresh())
            });
        if can_start {
            let mut job = pending.pop_front().unwrap();
            let _event_scope = remote.machine().queue_scope(job.id)?;
            if let Some(ticket) = &job.operation_event
                && let Err(error) = ticket.start()
            {
                finish(job, Err(error));
                continue;
            }
            if !concurrent(&job.request) {
                if matches!(job.request, Request::NotificationStart) {
                    if controls.superseded(&job) {
                        finish(job, Err(Controls::superseded_error()));
                        continue;
                    }
                    let context = prepare(
                        remote,
                        &mut job,
                        None,
                        &mut active,
                        &mut retired,
                        &mut pending,
                        &mut input,
                        &mut receiver,
                        &shutdown,
                        &wake,
                        origin,
                        &events,
                        writer.as_ref(),
                        &healthy,
                        &mut notifications,
                        &mut controls,
                    )
                    .await;
                    let result = match context {
                        Err(error) => {
                            remote.driver_mut().finish();
                            Err(error)
                        }
                        Ok(context) => {
                            install_reader(remote, &mut input);
                            let result = {
                                let future = remote.with_prepared_catalog(context, |_| Ok(()));
                                tokio::pin!(future);
                                loop {
                                    controls.prune();
                                    let control_deadline = controls.deadline();
                                    tokio::select! { biased;
                                        _ = shutdown.cancelled() => break Err(closed_error()),
                                        _ = job.cancellation.cancelled() => break Err(Error::local("Cancelled", "notification start cancelled")),
                                        _ = tokio::time::sleep_until(job.deadline.into()) => break Err(Error::local("Timeout", "notification start expired")),
                                        result = &mut future => break result,
                                        _ = controls.receive(), if controls.open() => {},
                                        _ = async { if let Some(deadline) = control_deadline { tokio::time::sleep_until(deadline.into()).await; } else { std::future::pending::<()>().await; } } => {},
                                    }
                                }
                            };
                            if let Err(error) = &result {
                                remote.abort_active(error.clone()).await;
                            }
                            if !reclaim_reader(remote, &mut input, writer.is_some())? {
                                healthy.fail(closed_error());
                            }
                            controls
                                .drain(remote.host_runtime_mut(), &mut notifications)
                                .await;
                            match result {
                                Err(error) => Err(error),
                                Ok(()) if controls.superseded(&job) => {
                                    Err(Controls::superseded_error())
                                }
                                Ok(()) => {
                                    controls.enabled = true;
                                    notifications
                                        .start(remote, &sender, &wake, writer.as_ref())
                                        .await
                                        .map(|()| {
                                            Reply::NotificationStatus(
                                                remote.host_runtime_mut().notification_status(),
                                            )
                                        })
                                }
                            }
                        }
                    };
                    finish(job, result);
                    continue;
                }
                let reconnecting = matches!(job.request, Request::Reconnect);
                install_reader(remote, &mut input);
                serial(
                    remote,
                    job,
                    &mut bindings,
                    &mut pending,
                    &mut receiver,
                    &shutdown,
                    &wake,
                    &mut controls,
                )
                .await;
                if reconnecting && writer.is_some() {
                    if remote.is_suspended() {
                        healthy.fail(Error::new(
                            "ReconnectFailed",
                            "explicit reconnect did not complete",
                        ));
                    } else if let Some(process) = remote.driver_mut().process.as_mut() {
                        let (new_writer, new_sender, new_input) = process.multiplex_io()?;
                        writer = Some(new_writer);
                        sender = new_sender;
                        input = new_input;
                    }
                } else if !reclaim_reader(remote, &mut input, writer.is_some())? {
                    healthy.fail(Error::new(
                        "TransportClosed",
                        "shared stdio transport is unavailable",
                    ));
                }
                continue;
            }
            bindings.retain(|_, saved| saved.lease.strong_count() > 0);
            let candidate: Result<Candidate> =
                match std::mem::replace(&mut job.request, Request::Events) {
                    Request::Call(id, value, lease) => bindings
                        .get(&id)
                        .ok_or_else(|| Error::new("InvalidHandle", "queued binding is unavailable"))
                        .and_then(|saved| {
                            Ok(Candidate::Call(
                                saved.binding.clone(),
                                saved.binding.validate_json(value)?,
                                id,
                                lease,
                            ))
                        }),
                    Request::Resume(handle, responses) => remote
                        .machine()
                        .check_continuation_owner(&handle)
                        .map(|()| Candidate::Resume(handle, responses)),
                    Request::Execute(tool, argv) => Ok(Candidate::Command(tool, argv)),
                    _ => unreachable!(),
                };
            let candidate = match candidate {
                Ok(value) => value,
                Err(error) => {
                    finish(job, Err(error));
                    continue;
                }
            };
            let output = candidate.output();
            let prepared = prepare(
                remote,
                &mut job,
                None,
                &mut active,
                &mut retired,
                &mut pending,
                &mut input,
                &mut receiver,
                &shutdown,
                &wake,
                origin,
                &events,
                writer.as_ref(),
                &healthy,
                &mut notifications,
                &mut controls,
            )
            .await;
            retire(remote, &mut retired);
            if http_policy.is_some()
                && remote.machine().http_session_expired()
                && interrupted(&job, &shutdown).is_none()
            {
                remote.driver_mut().finish();
                job.request = candidate.restore();
                pending.push_front(job);
                continue;
            }
            let mut context = match prepared {
                Ok(context) => context,
                Err(error) => {
                    remote.driver_mut().finish();
                    finish(job, Err(error));
                    continue;
                }
            };
            let admitted = loop {
                if active.is_empty() {
                    install_reader(remote, &mut input);
                }
                let admitted = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => Err(closed_error()),
                    _ = job.cancellation.cancelled() => Err(Error::new("Cancelled","operation cancelled before sending")),
                    _ = tokio::time::sleep_until(job.deadline.into()) => Err(Error::new("Timeout","operation expired before sending")),
                    result = candidate.admit(remote, context.clone()) => result,
                };
                if !reclaim_reader(remote, &mut input, writer.is_some())? {
                    healthy.fail(Error::new(
                        "TransportClosed",
                        "shared stdio transport is unavailable",
                    ));
                }
                if admitted.as_ref().is_err_and(|error| error.code == "Busy")
                    && !active.is_empty()
                    && remote.catalog_needs_refresh()
                {
                    // TTL may cross its boundary between the last check and
                    // admission. Wait with the same credential, not a new lookup.
                    match prepare(
                        remote,
                        &mut job,
                        Some(context),
                        &mut active,
                        &mut retired,
                        &mut pending,
                        &mut input,
                        &mut receiver,
                        &shutdown,
                        &wake,
                        origin,
                        &events,
                        writer.as_ref(),
                        &healthy,
                        &mut notifications,
                        &mut controls,
                    )
                    .await
                    {
                        Ok(resolved) => {
                            context = resolved;
                            continue;
                        }
                        Err(error) => break Err(error),
                    }
                }
                break admitted;
            };
            if http_policy.is_some()
                && remote.machine().http_session_expired()
                && admitted.as_ref().is_err_and(|error| {
                    error.code == "SessionExpired"
                        && error.execution_certainty == mcpx_core::ExecutionCertainty::NotSent
                })
                && interrupted(&job, &shutdown).is_none()
            {
                remote.driver_mut().finish();
                job.request = candidate.restore();
                pending.push_front(job);
                continue;
            }
            let operation = match admitted {
                Ok(ControlFlow::Continue(operation)) => operation,
                Ok(ControlFlow::Break(help)) => {
                    finish(job, Ok(Reply::Execution(Execution::Help(help))));
                    continue;
                }
                Err(error) => {
                    if active.is_empty() {
                        remote.abort_active(error.clone()).await;
                    } else {
                        remote.driver_mut().finish();
                    }
                    finish(job, Err(error));
                    continue;
                }
            };
            let (http, credential, driver_deadline) = remote.driver().http_snapshot(operation)?;
            job.deadline = job.deadline.min(driver_deadline);
            remote
                .host_runtime_mut()
                .link_queue_operation(operation, job.id)?;
            let mut actions = remote.host_runtime_mut().take_actions();
            let action = actions
                .pop()
                .ok_or_else(|| Error::new("ProtocolError", "admitted call has no host action"))?;
            let action_id = match &action {
                Action::Http { id, .. } | Action::Stream { id, .. } => *id,
                _ => return Err(Error::new("ProtocolError", "expected a host request")),
            };
            if let Some(error) = interrupted(&job, &shutdown) {
                let error = error.with_context(
                    remote
                        .host_runtime_mut()
                        .action_error_context(action_id)
                        .expect("admitted action"),
                );
                if error.code == "Cancelled" || error.code == "TransportClosed" {
                    remote.host_runtime_mut().cancel_operation(operation)?;
                    remote.host_runtime_mut().take_actions();
                } else {
                    remote
                        .host_runtime_mut()
                        .fail_request(action_id, error.clone())?;
                }
                let _ = remote
                    .take_concurrent_completion(operation)
                    .expect("failed operation retains completion");
                finish(job, Err(error));
                continue;
            }
            let response_policy = remote
                .driver()
                .response_recovery
                .filter(|_| crate::response_recovery::eligible(&action));
            let response = if let Some(policy) = response_policy {
                Some(crate::response_recovery::ResponseIo::new(
                    remote.host_runtime_mut(),
                    action.clone(),
                    http.clone(),
                    credential.clone(),
                    policy,
                    job.deadline,
                    job.cancellation.clone(),
                    shutdown.clone(),
                    sender.clone(),
                    wake.clone(),
                )?)
            } else {
                None
            };
            let (task, stream) = if response.is_some() {
                (None, None)
            } else {
                match &action {
                    Action::Http { .. } => (
                        Some(tokio::spawn(http_task(
                            http.clone(),
                            credential.clone(),
                            action.clone(),
                            sender.clone(),
                        ))),
                        None,
                    ),
                    Action::Stream {
                        body, cancellation, ..
                    } => {
                        let writer = writer
                            .as_ref()
                            .ok_or_else(|| Error::new("ProtocolError", "missing stdio writer"))?
                            .clone();
                        let cancellation =
                            writer.cancellation(cancellation.clone().ok_or_else(|| {
                                Error::new("ProtocolError", "call is missing a cancellation frame")
                            })?);
                        let task_cancel = cancellation.clone();
                        let budget = job._bytes.clone();
                        let body = body.clone();
                        let sender = sender.clone();
                        let task = tokio::spawn(async move {
                            if let Err(error) =
                                writer.write_request(&body, task_cancel, budget).await
                            {
                                let _ = sender
                                    .send(Message {
                                        id: 0,
                                        input: Input::StdioFailed(error),
                                    })
                                    .await;
                            }
                        });
                        (Some(task), Some(cancellation))
                    }
                    _ => unreachable!(),
                }
            };
            active.insert(
                operation,
                Active {
                    output,
                    stream,
                    job,
                    action_id,
                    action,
                    credential,
                    http,
                    task,
                    response,
                    completion: None,
                },
            );
            continue;
        }
        let opening_deadline = notifications.opening_deadline(remote.host_runtime_mut());
        let deadline = earliest(&active, &pending, Instant::now() + Duration::from_secs(60))
            .min(opening_deadline.unwrap_or(Instant::now() + Duration::from_secs(60)))
            .min(
                controls
                    .wake_deadline()
                    .unwrap_or(Instant::now() + Duration::from_secs(60)),
            )
            .min(
                notifications
                    .recovery_deadline()
                    .unwrap_or(Instant::now() + Duration::from_secs(60)),
            )
            .min(
                http_budget
                    .as_ref()
                    .filter(|_| http_waiting)
                    .map_or(Instant::now() + Duration::from_secs(60), |budget| {
                        budget.deadline.into_std()
                    }),
            );
        tokio::select! {
            biased;
            Some(message) = input.recv(), if !active.is_empty() || writer.is_some() || super::notifications::active(remote.host_runtime_mut()) => feed(remote.host_runtime_mut(),message,origin,writer.as_ref(),&healthy,&mut active).await,
            _ = shutdown.cancelled() => {},
            _ = wake.notified() => {},
            incoming = receiver.recv() => match incoming {Some(job)=>pending.push_back(job),None=>break},
            _ = controls.receive(), if controls.open() => {},
            _ = tokio::time::sleep_until(deadline.into()), if !active.is_empty() || !pending.is_empty() || opening_deadline.is_some() || controls.wake_deadline().is_some() || notifications.recovery_deadline().is_some() => {},
        }
    }
    receiver.close();
    controls.close();
    while let Ok(job) = receiver.try_recv() {
        pending.push_back(job);
    }
    interrupt(
        remote.host_runtime_mut(),
        &mut active,
        &mut retired,
        &events,
        true,
    )
    .await;
    retire(remote, &mut retired);
    for job in pending {
        finish(job, Err(closed_error()));
    }
    notifications.stop().await;
    remote.close().await
}

fn install_reader(remote: &mut Remote, input: &mut mpsc::Receiver<Message>) {
    if let Some(process) = remote.driver_mut().process.as_mut() {
        let placeholder = mpsc::channel(1).1;
        process.install_receiver(std::mem::replace(input, placeholder));
    }
}
fn reclaim_reader(
    remote: &mut Remote,
    input: &mut mpsc::Receiver<Message>,
    stdio: bool,
) -> Result<bool> {
    if !stdio {
        return Ok(true);
    }
    let Some(process) = remote.driver_mut().process.as_mut() else {
        return Ok(false);
    };
    // During concurrent admission the actor may already own the receiver.
    match process.take_receiver() {
        Ok(receiver) => {
            *input = receiver;
            Ok(true)
        }
        Err(error) if error.code == "TransportClosed" => Ok(true),
        Err(error) => Err(error),
    }
}
