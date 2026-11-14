use super::{
    exchange::{Exchange, Step},
    *,
};
use crate::operation::{Reply, State, WasiOperation};
use mcpx_command::{CommandAdmission, CommandRuntimeExt, CommandSpec, Completion, Execution};
use mcpx_core::{
    BoundArgs, CallOutcome, CatalogRevision, Client, Continuation, ErrorContext, EventLimits,
    ExecutionCertainty, OperationEventBatch, OperationEvents, ToolBinding,
};
use serde::Serialize;
use serde_json::Value;
use std::{cell::RefCell, collections::VecDeque, rc::Rc};
mod controls;
mod credentials;
mod notifications;
mod reactor;
mod reconnect;
use super::responses::{CallIo, Cleanup};
use controls::Control;
use credentials::{Authentication, Provider, Resolving};
pub use reactor::{WasiReactor, WasiRemote};

#[derive(Clone, Debug)]
pub struct WasiQueueOptions {
    pub capacity: usize,
    pub max_bytes: usize,
    pub max_in_flight: usize,
    pub timeout: Duration,
    pub operation_events: EventLimits,
}
impl Default for WasiQueueOptions {
    fn default() -> Self {
        Self {
            capacity: 64,
            max_bytes: 8 * 1024 * 1024,
            max_in_flight: 16,
            timeout: Duration::from_secs(60),
            operation_events: EventLimits::default(),
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize)]
pub struct WasiQueueStats {
    pub queued: usize,
    pub active: usize,
    pub input_bytes: usize,
    pub cleanup: usize,
    pub metadata: bool,
    pub resolving: bool,
    pub authenticating: bool,
}
enum Request {
    Bind(String),
    Call(ToolBinding, BoundArgs),
    Command(String, Vec<String>),
    Resume(Continuation, Value),
    Refresh(CatalogRevision),
    Control(Control),
    Complete(String, usize),
    Spec(String),
    NotificationResume(u64),
    Reconnect { reset: bool },
}
impl Request {
    fn business(&self) -> bool {
        matches!(self, Self::Call(..) | Self::Command(..) | Self::Resume(..))
    }
}
struct Job {
    id: u64,
    request: Request,
    state: Rc<RefCell<State>>,
    bytes: usize,
    created: u64,
    deadline: u64,
    started: bool,
    refreshed: Option<CatalogRevision>,
    sent: bool,
    credential: Option<Credential>,
    scoped: bool,
}
struct Active {
    job: Job,
    core: u64,
    io: Option<CallIo>,
    completion: Option<Result<CallOutcome>>,
}
struct Metadata {
    job: Job,
    core: Option<u64>,
    io: Option<CallIo>,
}

/// A single WASIp2 owner multiplexes up to 16 HTTP calls and one notification
/// resource. Submission performs no network I/O. Poll the owner while awaiting
/// its operation futures, or use `wait` to drive all submitted work together.
pub struct WasiQueue {
    client: Client<WasiDriver>,
    options: WasiQueueOptions,
    owner: Rc<()>,
    pending: VecDeque<Job>,
    active: BTreeMap<u64, Active>,
    metadata: Option<Metadata>,
    cleanup: Vec<Cleanup>,
    events: OperationEvents,
    input_bytes: usize,
    closed: bool,
    provider: Option<Provider>,
    credential_reference: Option<String>,
    resolving: Option<Resolving>,
    authentication: Option<Authentication>,
    session_dirty: bool,
    notification_resume: Option<Job>,
    notification_resume_due: Option<u64>,
    suspended: bool,
    reconnect_id: Option<u64>,
}
fn duration_ns(value: Duration) -> Result<u64> {
    u64::try_from(value.as_nanos())
        .ok()
        .filter(|ns| *ns <= 86_400_000_000_000)
        .ok_or_else(|| Error::local("ConfigurationError", "duration exceeds 24 hours"))
}
fn mismatch() -> Error {
    Error::local("InvalidHandle", "operation belongs to another WASI queue")
}
fn encoded_len(value: &impl Serialize, limit: usize) -> Result<usize> {
    struct Count {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self.bytes.saturating_add(bytes.len());
            if self.bytes > self.limit {
                return Err(std::io::Error::other("input budget"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count { bytes: 0, limit };
    serde_json::to_writer(&mut count, value).map_err(|_| {
        Error::local(
            "QueueFull",
            "serialized input exceeds available queue budget",
        )
    })?;
    Ok(count.bytes)
}
macro_rules! decode {
    ($name:ident,$variant:ident,$ty:ty) => {
        fn $name(reply: Reply) -> Result<$ty> {
            if let Reply::$variant(value) = reply {
                Ok(value)
            } else {
                Err(Error::local(
                    "ProtocolError",
                    "unexpected WASI queue result",
                ))
            }
        }
    };
}
decode!(outcome, Outcome, CallOutcome);
decode!(execution, Execution, Execution);
decode!(binding, Binding, ToolBinding);
decode!(completion, Completion, Completion);
decode!(spec, Spec, CommandSpec);
decode!(schema_stats, SchemaStats, mcpx_core::SchemaRegistryStats);
decode!(cache_stats, CacheStats, mcpx_core::ValidatorCacheStats);
decode!(notification, Notification, mcpx_core::NotificationStatus);
fn unit(reply: Reply) -> Result<()> {
    if matches!(reply, Reply::Unit) {
        Ok(())
    } else {
        Err(Error::local(
            "ProtocolError",
            "unexpected WASI queue result",
        ))
    }
}

impl WasiQueue {
    /// Construct an unconnected owner without I/O. Its first operation discovers
    /// the remote. Uses the driver's credential until a provider is installed.
    pub fn new(
        driver: WasiDriver,
        mut runtime: HostRuntime,
        options: WasiQueueOptions,
    ) -> Result<Self> {
        if !(1..=1024).contains(&options.capacity)
            || !(1..=16).contains(&options.max_in_flight)
            || !(1024..=64 * 1024 * 1024).contains(&options.max_bytes)
            || duration_ns(options.timeout)? < 1_000_000
        {
            return Err(Error::local(
                "ConfigurationError",
                "invalid WASI queue capacity, byte budget, concurrency or timeout",
            ));
        }
        if driver.closed {
            return Err(Error::local("TransportClosed", "WASI driver is closed"));
        }
        if !(runtime.endpoint().starts_with("http://")
            || runtime.endpoint().starts_with("https://"))
        {
            return Err(Error::local(
                "CapabilityUnavailable",
                "WASI queue supports HTTP only",
            ));
        }
        if runtime.busy() {
            return Err(Error::local(
                "Busy",
                "finish existing runtime operations before creating a WASI queue",
            ));
        }
        runtime.accept_authorization(driver.credential.auth_context_id())?;
        let events = OperationEvents::new(options.operation_events)?;
        Ok(Self {
            client: Client::new(driver, runtime),
            options,
            owner: Rc::new(()),
            pending: VecDeque::new(),
            active: BTreeMap::new(),
            metadata: None,
            cleanup: Vec::new(),
            events,
            input_bytes: 0,
            closed: false,
            provider: None,
            credential_reference: None,
            resolving: None,
            authentication: None,
            session_dirty: false,
            notification_resume: None,
            notification_resume_due: None,
            suspended: false,
            reconnect_id: None,
        })
    }
    pub fn connect(
        driver: WasiDriver,
        runtime: HostRuntime,
        options: WasiQueueOptions,
    ) -> Result<Self> {
        let mut owner = Self::new(driver, runtime, options)?;
        let mut connected = owner.refresh()?;
        owner.wait(&mut connected)?;
        Ok(owner)
    }
    fn open(&self) -> Result<()> {
        if self.closed {
            Err(Error::local("TransportClosed", "WASI queue is closed"))
        } else {
            Ok(())
        }
    }
    fn budget(&self) -> Result<usize> {
        self.open()?;
        if self.pending.len()
            + self.active.len()
            + usize::from(self.metadata.is_some())
            + usize::from(self.resolving.is_some())
            + usize::from(self.authentication.is_some())
            >= self.options.capacity
        {
            return Err(Error::local("QueueFull", "WASI queue count budget is full"));
        }
        self.options
            .max_bytes
            .checked_sub(self.input_bytes)
            .and_then(|remaining| remaining.checked_sub(128))
            .ok_or_else(|| Error::local("QueueFull", "WASI queue input byte budget is full"))
    }
    fn check_bytes(&self, bytes: usize) -> Result<()> {
        if bytes > self.budget()? {
            Err(Error::local(
                "QueueFull",
                "input exceeds available queue budget",
            ))
        } else {
            Ok(())
        }
    }
    fn enqueue<T>(
        &mut self,
        request: Request,
        bytes: usize,
        decode: fn(Reply) -> Result<T>,
    ) -> Result<WasiOperation<T>> {
        self.open()?;
        self.permit(&request)?;
        let bytes = bytes
            .checked_add(128)
            .ok_or_else(|| Error::local("QueueFull", "input accounting exceeded"))?;
        if self.pending.len()
            + self.active.len()
            + usize::from(self.metadata.is_some())
            + usize::from(self.resolving.is_some())
            + usize::from(self.authentication.is_some())
            >= self.options.capacity
            || bytes > self.options.max_bytes.saturating_sub(self.input_bytes)
        {
            return Err(Error::local(
                "QueueFull",
                "WASI queue count or input byte budget is full",
            ));
        }
        let id = self.events.admit()?;
        let created = monotonic_clock::now();
        let timeout = duration_ns(self.options.timeout)?.min(self.client.driver().timeout_ns);
        let state = Rc::new(RefCell::new(State::default()));
        self.pending.push_back(Job {
            id,
            request,
            state: state.clone(),
            bytes,
            created,
            deadline: created.saturating_add(timeout),
            started: false,
            refreshed: None,
            sent: false,
            credential: None,
            scoped: false,
        });
        self.input_bytes += bytes;
        Ok(WasiOperation::new(self.owner.clone(), id, state, decode))
    }
    pub fn bind(&mut self, name: &str) -> Result<WasiOperation<ToolBinding>> {
        self.check_bytes(name.len())?;
        self.enqueue(Request::Bind(name.into()), name.len(), binding)
    }
    pub fn fresh_tool(&mut self, name: &str) -> Result<ToolBinding> {
        let mut operation = self.bind(name)?;
        self.wait(&mut operation)
    }
    pub fn refresh(&mut self) -> Result<WasiOperation<()>> {
        self.enqueue(Request::Refresh(self.client.catalog_revision()), 0, unit)
    }
    pub fn submit_args(
        &mut self,
        binding: &ToolBinding,
        args: &BoundArgs,
    ) -> Result<WasiOperation<CallOutcome>> {
        let budget = self.budget()?;
        binding.check_args(args)?;
        self.client.machine().catalog().check(binding)?;
        let bytes = encoded_len(args.value(), budget)?;
        self.enqueue(Request::Call(binding.clone(), args.clone()), bytes, outcome)
    }
    pub fn submit_json(
        &mut self,
        binding: &ToolBinding,
        value: Value,
    ) -> Result<WasiOperation<CallOutcome>> {
        self.budget()?;
        let args = binding.validate_json(value)?;
        self.submit_args(binding, &args)
    }
    pub fn submit_raw(
        &mut self,
        binding: &ToolBinding,
        json: &str,
    ) -> Result<WasiOperation<CallOutcome>> {
        self.check_bytes(json.len())?;
        self.submit_json(binding, mcpx_core::parse_json(json)?)
    }
    pub fn submit_typed(
        &mut self,
        binding: &ToolBinding,
        value: impl Serialize,
    ) -> Result<WasiOperation<CallOutcome>> {
        self.budget()?;
        let args = binding.args_from(value)?;
        self.submit_args(binding, &args)
    }
    pub fn execute(&mut self, name: &str, command: &str) -> Result<WasiOperation<Execution>> {
        self.check_bytes(name.len().saturating_add(command.len()))?;
        self.execute_argv(name, &mcpx_command::tokenize(command)?)
    }
    pub fn execute_argv(
        &mut self,
        name: &str,
        argv: &[String],
    ) -> Result<WasiOperation<Execution>> {
        let budget = self
            .budget()?
            .checked_sub(name.len())
            .ok_or_else(|| Error::local("QueueFull", "command name exceeds input budget"))?;
        let bytes = name.len().saturating_add(encoded_len(&argv, budget)?);
        self.enqueue(
            Request::Command(name.into(), argv.to_vec()),
            bytes,
            execution,
        )
    }
    pub fn resume(
        &mut self,
        handle: &Continuation,
        responses: Value,
    ) -> Result<WasiOperation<CallOutcome>> {
        let budget = self.budget()?;
        self.client.machine().check_continuation_owner(handle)?;
        let bytes = encoded_len(&responses, budget)?;
        self.enqueue(Request::Resume(handle.clone(), responses), bytes, outcome)
    }
    pub fn complete_line(
        &mut self,
        line: &str,
        cursor: usize,
    ) -> Result<WasiOperation<Completion>> {
        if line.len() > mcpx_command::MAX_COMPLETION_INPUT_BYTES {
            return Err(Error::local(
                "LimitExceeded",
                "completion input exceeds 64 KiB",
            ));
        }
        if cursor > line.len() || !line.is_char_boundary(cursor) {
            return Err(Error::local(
                "InvalidCursor",
                "completion cursor must be a UTF-8 byte boundary",
            ));
        }
        self.check_bytes(line.len())?;
        self.enqueue(
            Request::Complete(line.into(), cursor),
            line.len(),
            completion,
        )
    }
    pub fn command_spec(&mut self, name: &str) -> Result<WasiOperation<CommandSpec>> {
        self.check_bytes(name.len())?;
        self.enqueue(Request::Spec(name.into()), name.len(), spec)
    }
    /// Tighten a deadline measured from submission, including queued time.
    pub fn set_timeout<T>(
        &mut self,
        operation: &WasiOperation<T>,
        timeout: Duration,
    ) -> Result<()> {
        if !Rc::ptr_eq(&self.owner, &operation.owner) {
            return Err(mismatch());
        }
        let ns = duration_ns(timeout)?;
        if ns < 1_000_000 {
            return Err(Error::local(
                "ConfigurationError",
                "timeout must be at least 1 ms",
            ));
        }
        let id = operation.id();
        let job = self
            .pending
            .iter_mut()
            .find(|job| job.id == id)
            .or_else(|| self.active.get_mut(&id).map(|active| &mut active.job))
            .or_else(|| {
                self.metadata
                    .as_mut()
                    .filter(|metadata| metadata.job.id == id)
                    .map(|metadata| &mut metadata.job)
            });
        let job = job
            .or_else(|| {
                self.resolving
                    .as_mut()
                    .filter(|r| r.job.id == id)
                    .map(|r| &mut r.job)
            })
            .or_else(|| {
                self.authentication
                    .as_mut()
                    .filter(|a| a.job.id == id)
                    .map(|a| &mut a.job)
            })
            .or_else(|| self.notification_resume.as_mut().filter(|job| job.id == id));
        let job =
            job.ok_or_else(|| Error::local("InvalidHandle", "operation has already settled"))?;
        job.deadline = job.deadline.min(job.created.saturating_add(ns));
        Ok(())
    }
    pub fn wait<T>(&mut self, operation: &mut WasiOperation<T>) -> Result<T> {
        if !Rc::ptr_eq(&self.owner, &operation.owner) {
            return Err(mismatch());
        }
        loop {
            if let Some(result) = operation.try_result() {
                return result;
            }
            self.poll(Duration::from_secs(1))?;
        }
    }
    pub fn stats(&self) -> WasiQueueStats {
        WasiQueueStats {
            queued: self.pending.len() + usize::from(self.notification_resume.is_some()),
            active: self.active.len(),
            input_bytes: self.input_bytes,
            cleanup: self.cleanup.len(),
            metadata: self.metadata.is_some(),
            resolving: self.resolving.is_some(),
            authenticating: self.authentication.is_some(),
        }
    }
    pub fn drain_operation_events(&mut self) -> OperationEventBatch {
        self.events.drain()
    }
    pub fn drain_events(&mut self) -> EventBatch {
        self.client.drain_events()
    }
    pub fn subscribe_catalog(&self) -> Result<mcpx_core::CatalogWatcher> {
        self.client.subscribe_catalog()
    }
    pub fn start_notifications(&mut self) -> Result<mcpx_core::NotificationStatus> {
        self.open()?;
        if self.suspended {
            return Err(Self::suspended_error());
        }
        if self.authentication.is_some() {
            return Err(Error::local("Busy", "credential transition is in progress"));
        }
        self.cancel_notification_resume(
            "Cancelled",
            "notification resume superseded by fresh start",
        );
        let (runtime, driver) = self.client.host_parts_mut();
        driver.start_notifications(runtime)
    }
    pub fn stop_notifications(&mut self) -> Result<()> {
        self.cancel_notification_resume("Cancelled", "notification resume superseded by stop");
        let (runtime, driver) = self.client.host_parts_mut();
        driver.stop_notifications(runtime)
    }
    pub fn notification_status(&mut self) -> Option<mcpx_core::NotificationStatus> {
        self.client.host_runtime_mut().notification_status()
    }

    fn settle(&mut self, job: Job, mut result: Result<Reply>) {
        if let Err(error) = &mut result {
            error.queue_operation_id = Some(job.id);
        }
        let journal = match &result {
            Ok(reply) => self.events.complete(job.id, reply.outcome()),
            Err(error) => self.events.fail(job.id, error),
        };
        if let Err(error) = journal {
            result = Err(error);
        }
        if matches!(job.request, Request::Reconnect { .. }) && self.reconnect_id == Some(job.id) {
            self.suspended = result.is_err();
            self.reconnect_id = None;
            self.client.driver_mut().reconnecting = self.suspended;
        }
        if matches!(job.request, Request::NotificationResume(_)) {
            self.client.driver_mut().hold_notification_resume(false);
            self.notification_resume_due = None;
        } else {
            self.input_bytes -= job.bytes;
        }
        State::settle(&job.state, result);
    }
    fn context(job: &Job, core: Option<u64>) -> ErrorContext {
        ErrorContext {
            operation_id: core,
            queue_operation_id: Some(job.id),
            execution_certainty: if job.sent {
                ExecutionCertainty::Unknown
            } else {
                ExecutionCertainty::NotSent
            },
        }
    }
    fn interrupted(job: &Job) -> Option<Error> {
        if job.state.borrow().cancelled {
            Some(Error::local("Cancelled", "operation cancelled"))
        } else if monotonic_clock::now() >= job.deadline {
            Some(Error::local("Timeout", "operation deadline exceeded"))
        } else {
            None
        }
    }
    fn collect(&mut self) -> bool {
        let mut progress = false;
        for id in self.active.keys().copied().collect::<Vec<_>>() {
            let active = self.active.get_mut(&id).unwrap();
            if active.completion.is_none() {
                active.completion = self.client.take_concurrent_completion(active.core);
                progress |= active.completion.is_some();
            }
            if active.completion.as_ref().is_some_and(|result| {
                result.is_err() || active.io.as_ref().is_none_or(CallIo::drained)
            }) {
                let mut active = self.active.remove(&id).expect("retained operation");
                let result = active.completion.take().unwrap();
                self.finish_active(active, result);
                progress = true;
            }
        }
        progress
    }
    fn finish_active(&mut self, active: Active, result: Result<CallOutcome>) {
        let command = matches!(active.job.request, Request::Command(..));
        drop(active.io);
        self.settle(
            active.job,
            result.map(|value| {
                if command {
                    Reply::Execution(Execution::Outcome(value))
                } else {
                    Reply::Outcome(value)
                }
            }),
        );
    }
    fn cancel_active(&mut self, id: u64, error: Error, wire: bool) {
        let Some(mut active) = self.active.remove(&id) else {
            return;
        };
        let completion = active
            .completion
            .take()
            .or_else(|| self.client.take_concurrent_completion(active.core));
        if let Some(result) = completion {
            self.finish_active(active, result);
            return;
        }
        let error = error.with_context(Self::context(&active.job, Some(active.core)));
        if !matches!(error.code.as_str(), "Cancelled" | "TransportClosed")
            && let Some(io) = &active.io
        {
            let _ = self
                .client
                .host_runtime_mut()
                .fail_request(io.id, error.clone());
        } else {
            let _ = self.client.host_runtime_mut().cancel_operation(active.core);
        }
        self.client.take_concurrent_completion(active.core);
        if wire
            && self.cleanup.len() < 16
            && let Some(io) = &mut active.io
            && let Ok(Some(cleanup)) = io.cancel()
        {
            self.cleanup.push(cleanup);
        }
        drop(active.io);
        self.settle(active.job, Err(error));
    }
    fn fail_metadata(&mut self, error: Error) {
        let Some(metadata) = self.metadata.take() else {
            return;
        };
        let error = error.with_context(Self::context(&metadata.job, metadata.core));
        self.client.host_runtime_mut().abort_error(error.clone());
        drop(metadata.io);
        self.settle(metadata.job, Err(error));
    }
    fn prune(&mut self) -> bool {
        let mut progress = self.prune_credentials();
        let mut keep = VecDeque::new();
        while let Some(job) = self.pending.pop_front() {
            if let Some(error) = Self::interrupted(&job) {
                self.settle(job, Err(error));
                progress = true;
            } else {
                keep.push_back(job);
            }
        }
        self.pending = keep;
        if let Some(error) = self
            .metadata
            .as_ref()
            .and_then(|metadata| Self::interrupted(&metadata.job))
        {
            self.fail_metadata(error);
            progress = true;
        }
        for id in self.active.keys().copied().collect::<Vec<_>>() {
            if self.active[&id].completion.is_none()
                && let Some(error) = Self::interrupted(&self.active[&id].job)
            {
                self.cancel_active(id, error, true);
                progress = true;
            }
        }
        progress
    }
    fn admit(&mut self) -> Result<bool> {
        if self.metadata.is_some() || self.resolving.is_some() || self.authentication.is_some() {
            return Ok(false);
        }
        let mut progress = false;
        while let Some(front) = self.pending.front() {
            if self.suspended
                && self.reconnect_id.is_none()
                && !matches!(
                    front.request,
                    Request::Reconnect { .. } | Request::Control(_)
                )
            {
                let job = self.pending.pop_front().unwrap();
                self.settle(job, Err(Self::suspended_error()));
                progress = true;
                continue;
            }
            if matches!(front.request, Request::Reconnect { reset: false }) {
                if !self.active.is_empty() || !self.cleanup.is_empty() {
                    break;
                }
                let job = self.pending.pop_front().unwrap();
                self.begin_reconnect(job);
                return Ok(true);
            }
            if matches!(front.request, Request::Control(_)) {
                if !self.active.is_empty() {
                    break;
                }
                let job = self.pending.pop_front().unwrap();
                let result = self.events.start(job.id).and_then(|()| {
                    let Request::Control(control) = &job.request else {
                        unreachable!()
                    };
                    self.apply_control(control)
                });
                self.settle(job, result);
                progress = true;
                continue;
            }
            if front.credential.is_none() {
                if self.provider.is_some() {
                    let mut job = self.pending.pop_front().unwrap();
                    if !job.started {
                        if let Err(error) = self.events.start(job.id) {
                            self.settle(job, Err(error));
                            continue;
                        }
                        job.started = true;
                    }
                    self.resolve(job);
                    return Ok(true);
                }
                self.pending.front_mut().unwrap().credential =
                    Some(self.client.driver().credential.clone());
            }
            let front = self.pending.front().unwrap();
            let scope = front.credential.as_ref().unwrap().auth_context_id();
            let status = self.client.host_runtime_mut().catalog_status(scope)?;
            if !front.scoped {
                if (status.authorization_changed || self.session_dirty) && !self.active.is_empty() {
                    break;
                }
                let mut job = self.pending.pop_front().unwrap();
                if !job.started {
                    if let Err(error) = self.events.start(job.id) {
                        self.settle(job, Err(error));
                        continue;
                    }
                    job.started = true;
                }
                if (status.authorization_changed || self.session_dirty)
                    && self.client.driver().session.is_some()
                {
                    self.begin_authentication(job);
                    return Ok(true);
                }
                self.session_dirty = false;
                if status.authorization_changed {
                    let wanted = self.client.driver().http_notifications;
                    self.stop_notifications()?;
                    self.client.driver_mut().http_notifications = wanted;
                }
                let credential = job.credential.as_ref().unwrap();
                self.client
                    .host_runtime_mut()
                    .accept_authorization(credential.auth_context_id())?;
                self.client.driver_mut().credential = credential.clone();
                job.scoped = true;
                let local = match &job.request {
                    Request::Call(binding, _) => self.client.machine().catalog().check(binding),
                    Request::Resume(handle, _) => self.client.machine().check_continuation(handle),
                    _ => Ok(()),
                };
                if let Err(error) = local {
                    self.settle(job, Err(error));
                    progress = true;
                    continue;
                }
                self.pending.push_front(job);
            }
            let front = self.pending.front().unwrap();
            let revision = self.client.catalog_revision();
            let refreshed = front.refreshed.as_ref() == Some(&revision);
            let force_refresh = if let Request::Refresh(observed) = &front.request {
                !self
                    .client
                    .catalog_revision_source()
                    .published_since(observed)?
            } else {
                false
            };
            let needs_refresh = !refreshed && (status.needs_refresh || force_refresh);
            if needs_refresh && !self.active.is_empty()
                || front.request.business() && self.active.len() >= self.options.max_in_flight
            {
                break;
            }
            let mut job = self.pending.pop_front().unwrap();
            if !job.started {
                if let Err(error) = self.events.start(job.id) {
                    self.settle(job, Err(error));
                    progress = true;
                    continue;
                }
                job.started = true;
            }
            if needs_refresh {
                let _scope = self.client.machine().queue_scope(job.id)?;
                match self.client.host_runtime_mut().refresh_catalog() {
                    Ok(()) => {
                        self.metadata = Some(Metadata {
                            job,
                            core: None,
                            io: None,
                        })
                    }
                    Err(error) => self.settle(job, Err(error)),
                }
                return Ok(true);
            }
            let _scope = self.client.machine().queue_scope(job.id)?;
            let admitted = (|| -> Result<std::result::Result<Reply, u64>> {
                let runtime = self.client.host_runtime_mut();
                Ok(match &job.request {
                    Request::Bind(name) => Ok(Reply::Binding(runtime.tool_binding(name)?)),
                    Request::Call(binding, args) => {
                        Err(runtime.call_concurrent_bound(binding, args)?)
                    }
                    Request::Command(name, argv) => match runtime.command_concurrent(name, argv)? {
                        CommandAdmission::Help(text) => Ok(Reply::Execution(Execution::Help(text))),
                        CommandAdmission::Operation(id) => Err(id),
                    },
                    Request::Resume(handle, responses) => {
                        Err(runtime.resume_concurrent_bound(handle, responses.clone())?)
                    }
                    Request::Refresh(_) => Ok(Reply::Unit),
                    Request::Reconnect { .. } => Ok(Reply::Unit),
                    Request::Control(_) | Request::NotificationResume(_) => {
                        unreachable!("local controls are handled separately")
                    }
                    Request::Complete(line, cursor) => {
                        Ok(Reply::Completion(mcpx_command::complete_line(
                            runtime.machine().catalog().tools(),
                            line,
                            *cursor,
                        )?))
                    }
                    Request::Spec(name) => {
                        let descriptor = runtime
                            .machine()
                            .catalog()
                            .tools()
                            .find(|tool| tool.name == *name)
                            .ok_or_else(|| {
                                Error::local("ToolNotFound", "tool is not in the catalog")
                            })?;
                        Ok(Reply::Spec(CommandSpec::from_descriptor(descriptor)))
                    }
                })
            })();
            match admitted {
                Ok(Ok(reply)) => self.settle(job, Ok(reply)),
                Ok(Err(core)) => {
                    self.client
                        .host_runtime_mut()
                        .link_queue_operation(core, job.id)?;
                    self.active.insert(
                        job.id,
                        Active {
                            job,
                            core,
                            io: None,
                            completion: None,
                        },
                    );
                }
                Err(error) => self.settle(job, Err(error)),
            }
            progress = true;
        }
        Ok(progress)
    }
    fn actions(&mut self) -> Result<bool> {
        let policy = self.client.driver().response_recovery;
        let actions = self.client.host_runtime_mut().take_actions();
        let progress = !actions.is_empty();
        for action in actions {
            let (id, core) = match &action {
                Action::Http {
                    id, operation_id, ..
                } => (*id, *operation_id),
                Action::Cancel { .. } => continue,
                _ => {
                    return Err(Error::local(
                        "CapabilityUnavailable",
                        "WASI queue supports HTTP only",
                    ));
                }
            };
            if let Some(metadata) = &mut self.metadata {
                metadata.core = Some(core);
                match CallIo::new(
                    action,
                    metadata.job.credential.as_ref().unwrap().clone(),
                    metadata.job.deadline,
                    self.client.host_runtime_mut(),
                    None,
                ) {
                    Ok(io) => metadata.io = Some(io),
                    Err(error) => self.fail_metadata(error),
                }
            } else if let Some(active) = self.active.values_mut().find(|active| active.core == core)
            {
                match CallIo::new(
                    action,
                    active.job.credential.as_ref().unwrap().clone(),
                    active.job.deadline,
                    self.client.host_runtime_mut(),
                    policy,
                ) {
                    Ok(io) => {
                        active.job.sent = true;
                        active.io = Some(io);
                    }
                    Err(error) => {
                        active.job.sent = error.execution_certainty != ExecutionCertainty::NotSent;
                        let error = error.with_context(Self::context(&active.job, Some(core)));
                        self.client.host_runtime_mut().fail_request(id, error)?;
                    }
                }
            } else {
                self.client.host_runtime_mut().fail_request(
                    id,
                    Error::local("ProtocolError", "HTTP action has no queue owner"),
                )?;
            }
        }
        Ok(progress)
    }
    fn cleanup_step(&mut self) -> bool {
        let mut progress = false;
        self.cleanup.retain_mut(|cleanup| {
            if monotonic_clock::now() >= cleanup.deadline {
                progress = true;
                return false;
            }
            match cleanup.exchange.step() {
                Ok(Step::Pending) => true,
                Ok(Step::Progress) => {
                    progress = true;
                    true
                }
                Ok(Step::Response(_)) | Err(_) => {
                    progress = true;
                    false
                }
            }
        });
        progress
    }
    fn service(&mut self) -> Result<bool> {
        let now = self.client.driver().now_ms();
        self.client.host_runtime_mut().advance_time(now)?;
        let mut progress = self.collect();
        progress |= self.prune();
        progress |= self.cleanup_step();
        progress |= self.poll_credentials();
        progress |= self.poll_authentication();
        self.client.flush_host_events();
        progress |= self.service_notification_resume();
        if let Some(metadata) = &mut self.metadata
            && let Some(io) = &mut metadata.io
        {
            let (runtime, driver) = self.client.host_parts_mut();
            match io.step(runtime, driver, metadata.job.deadline) {
                Ok(value) => progress |= value,
                Err(error) => {
                    self.fail_metadata(error);
                    progress = true;
                }
            }
            if self.metadata.is_some() && !self.client.host_runtime_mut().busy() {
                let mut metadata = self.metadata.take().unwrap();
                drop(metadata.io);
                let now = self.client.driver().now_ms();
                self.client.host_runtime_mut().advance_time(now)?;
                metadata.job.refreshed = Some(self.client.catalog_revision());
                self.pending.push_front(metadata.job);
                progress = true;
            }
        }
        for id in self.active.keys().copied().collect::<Vec<_>>() {
            if self.active[&id].completion.is_none()
                && let Some(error) = Self::interrupted(&self.active[&id].job)
            {
                self.cancel_active(id, error, true);
                progress = true;
                continue;
            }
            let active = self.active.get_mut(&id).unwrap();
            let mut expired = None;
            if let Some(io) = &mut active.io {
                let (runtime, driver) = self.client.host_parts_mut();
                match io.step(runtime, driver, active.job.deadline) {
                    Ok(value) => progress |= value,
                    Err(error)
                        if matches!(
                            error.code.as_str(),
                            "Timeout" | "ResponseRecoveryExhausted"
                        ) =>
                    {
                        expired = Some(error);
                    }
                    Err(error) => {
                        if runtime.machine().has_action(io.id) {
                            runtime.fail_request(
                                io.id,
                                error.with_context(Self::context(&active.job, Some(active.core))),
                            )?;
                        }
                        active.io = None;
                        progress = true;
                    }
                }
            }
            if let Some(error) = expired {
                self.cancel_active(id, error, true);
                progress = true;
            }
        }
        progress |= self.collect();
        progress |= self.prune();
        progress |= self.admit()?;
        progress |= self.actions()?;
        self.client.flush_host_events();
        Ok(progress)
    }
    fn pollables(&self) -> Vec<Pollable> {
        let mut ready = self.client.driver().notification_pollables();
        if let Some(due) = self.notification_resume_due {
            ready.push(monotonic_clock::subscribe_instant(due));
        }
        ready.extend(
            self.authentication
                .iter()
                .map(|auth| auth.exchange.subscribe()),
        );
        ready.extend(self.active.values().flat_map(|active| {
            active
                .io
                .as_ref()
                .map(CallIo::subscribe)
                .unwrap_or_default()
        }));
        ready.extend(self.metadata.iter().flat_map(|metadata| {
            metadata
                .io
                .as_ref()
                .map(CallIo::subscribe)
                .unwrap_or_default()
        }));
        ready.extend(
            self.cleanup
                .iter()
                .map(|cleanup| cleanup.exchange.subscribe()),
        );
        ready
    }
    /// One bounded service round, optionally waiting for readiness. Zero never
    /// waits for headers/body I/O, including discovery and catalog refresh.
    pub fn poll(&mut self, wait: Duration) -> Result<()> {
        self.open()?;
        let ns = duration_ns(wait)?;
        if ns == 0 {
            // Some hosts advance lazy HTTP futures only when their pollable is
            // polled. `get`/`read` alone need not drive those futures. `ready`
            // is explicitly nonblocking; visit every resource, then drop child
            // pollables before service can replace their parents.
            let ready = self.pollables();
            for resource in &ready {
                let _ = resource.ready();
            }
            drop(ready);
            self.service()?;
            return Ok(());
        }
        if self.service()? {
            return Ok(());
        }
        let mut deadline = monotonic_clock::now().saturating_add(ns);
        for job in self
            .pending
            .iter()
            .chain(
                self.active
                    .values()
                    .filter(|a| a.completion.is_none())
                    .map(|a| &a.job),
            )
            .chain(self.metadata.iter().map(|m| &m.job))
            .chain(self.resolving.iter().map(|r| &r.job))
            .chain(self.authentication.iter().map(|a| &a.job))
            .chain(self.notification_resume.iter())
        {
            deadline = deadline.min(job.deadline);
        }
        for cleanup in &self.cleanup {
            deadline = deadline.min(cleanup.deadline);
        }
        if let Some(next) = self.credentials_deadline() {
            deadline = deadline.min(next);
        }
        if let Some(auth) = &self.authentication {
            deadline = deadline.min(auth.deadline);
        }
        let mut ready = self.pollables();
        if ready.is_empty()
            && self.pending.is_empty()
            && self.active.is_empty()
            && self.metadata.is_none()
            && self.resolving.is_none()
            && self.authentication.is_none()
            && self.notification_resume.is_none()
        {
            return Ok(());
        }
        ready.push(monotonic_clock::subscribe_instant(deadline));
        poll(&ready.iter().collect::<Vec<_>>());
        drop(ready);
        self.service()?;
        Ok(())
    }
    pub fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.cancel_notification_resume("TransportClosed", "WASI queue is closed");
        self.abort_credentials("TransportClosed");
        self.provider = None;
        self.credential_reference = None;
        self.stop_notifications()?;
        self.collect();
        while let Some(job) = self.pending.pop_front() {
            self.settle(
                job,
                Err(Error::local("TransportClosed", "WASI queue is closed")),
            );
        }
        if self.metadata.is_some() {
            self.fail_metadata(Error::local("TransportClosed", "WASI queue is closed"));
        }
        for id in self.active.keys().copied().collect::<Vec<_>>() {
            self.cancel_active(
                id,
                Error::local("TransportClosed", "WASI queue is closed"),
                true,
            );
        }
        while !self.cleanup.is_empty() {
            self.cleanup_step();
            if self.cleanup.is_empty() {
                break;
            }
            let mut ready: Vec<_> = self
                .cleanup
                .iter()
                .map(|cleanup| cleanup.exchange.subscribe())
                .collect();
            ready.push(monotonic_clock::subscribe_instant(
                self.cleanup.iter().map(|c| c.deadline).min().unwrap(),
            ));
            poll(&ready.iter().collect::<Vec<_>>());
        }
        crate::run_blocking(self.client.close())
    }
}
impl Drop for WasiQueue {
    fn drop(&mut self) {
        // Destruction never starts new I/O. Explicit close performs session DELETE.
        self.cancel_notification_resume("TransportClosed", "WASI queue owner was dropped");
        self.abort_credentials("TransportClosed");
        self.provider = None;
        self.credential_reference = None;
        let _ = self.stop_notifications();
        while let Some(job) = self.pending.pop_front() {
            self.settle(
                job,
                Err(Error::local(
                    "TransportClosed",
                    "WASI queue owner was dropped",
                )),
            );
        }
        if self.metadata.is_some() {
            self.fail_metadata(Error::local(
                "TransportClosed",
                "WASI queue owner was dropped",
            ));
        }
        for id in self.active.keys().copied().collect::<Vec<_>>() {
            self.cancel_active(
                id,
                Error::local("TransportClosed", "WASI queue owner was dropped"),
                false,
            );
        }
        self.cleanup.clear();
    }
}
