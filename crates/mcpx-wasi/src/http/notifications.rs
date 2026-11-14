use super::{
    exchange::{Exchange, Step},
    *,
};
use mcpx_core::{Client, NotificationPhase, NotificationRequest, NotificationStatus};
use std::collections::VecDeque;
use wasip2::{http::types::IncomingBody, io::streams::InputStream};
mod recovery;

struct Source {
    id: u64,
    root: u64,
    credential: Credential,
    opened: Option<u64>,
    ended: Option<u64>,
}
struct Budget {
    root: u64,
    failed_id: u64,
    deadline: u64,
    next: u64,
    remaining: u8,
}
#[derive(Default)]
pub(super) struct Recovery {
    policy: Option<crate::NotificationRecoveryPolicy>,
    source: Option<Source>,
    budget: Option<Budget>,
    next: Option<u64>,
    manual: bool,
}

struct Incoming {
    stream: InputStream,
    _body: IncomingBody,
}
enum Reader {
    Opening(Exchange),
    Body(Incoming),
    Ended,
}
pub(super) struct NotificationIo {
    reader: Reader,
    writer: Option<(Exchange, u64)>,
    pending: VecDeque<Vec<u8>>,
    plan: NotificationRequest,
    credential: Credential,
    deadline: u64,
    opening: bool,
    opened: Option<u64>,
    ended: Option<u64>,
    writer_failed: bool,
}
fn active(status: &NotificationStatus) -> bool {
    matches!(
        status.phase,
        NotificationPhase::Opening | NotificationPhase::Listening
    )
}
impl NotificationIo {
    fn new(plan: NotificationRequest, credential: Credential, timeout: u64) -> Result<Self> {
        let deadline = monotonic_clock::now()
            .checked_add(timeout)
            .ok_or_else(super::timeout)?;
        let exchange = Exchange::new(
            if plan.method == "GET" {
                Method::Get
            } else {
                Method::Post
            },
            &plan.url,
            &plan.headers,
            plan.body
                .as_ref()
                .map(serde_json::to_vec)
                .transpose()
                .expect("JSON value")
                .unwrap_or_default(),
            &credential,
            deadline,
            true,
        )?;
        Ok(Self {
            reader: Reader::Opening(exchange),
            writer: None,
            pending: VecDeque::new(),
            plan,
            credential,
            deadline,
            opening: true,
            opened: None,
            ended: None,
            writer_failed: false,
        })
    }
    fn fail(&mut self, runtime: &mut HostRuntime, timed_out: bool) {
        self.ended.get_or_insert_with(monotonic_clock::now);
        let _ = runtime.abort_notifications(self.plan.resource_id, timed_out);
        self.reader = Reader::Ended;
    }
    fn collect_replies(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        let replies = match runtime.notification_replies(self.plan.resource_id) {
            Ok(replies) => replies,
            Err(error) if error.code == "InvalidHandle" => return Ok(()),
            Err(error) => return Err(error),
        };
        if self.pending.len() + replies.len() > 16 {
            return Err(Error::new(
                "LimitExceeded",
                "notification reply queue exceeded",
            ));
        }
        self.pending.extend(
            replies
                .iter()
                .map(|reply| serde_json::to_vec(reply).expect("JSON value")),
        );
        Ok(())
    }
    fn read_step(&mut self, runtime: &mut HostRuntime) -> Result<bool> {
        let id = self.plan.resource_id;
        let mut progress = false;
        if runtime
            .notification_status()
            .is_some_and(|s| s.resource_id == id && s.phase == NotificationPhase::Opening)
            && monotonic_clock::now() >= self.deadline
        {
            self.fail(runtime, true);
            progress = true;
        }
        match &mut self.reader {
            Reader::Opening(exchange) => match exchange.step()? {
                Step::Progress => progress = true,
                Step::Pending => {}
                Step::Response(response) => {
                    let mime = response
                        .headers()
                        .get("content-type")
                        .first()
                        .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
                        .unwrap_or_default();
                    runtime.begin_notifications(id, response.status(), &mime)?;
                    self.opened = Some(monotonic_clock::now());
                    let body = response.consume().map_err(|_| host_error())?;
                    self.reader = Reader::Body(Incoming {
                        stream: body.stream().map_err(|_| host_error())?,
                        _body: body,
                    });
                    progress = true;
                }
            },
            Reader::Body(incoming) => match incoming.stream.read(65536) {
                Ok(bytes) if bytes.is_empty() => {}
                Ok(bytes) => {
                    if runtime.push_notifications(id, &bytes)? {
                        self.reader = Reader::Ended;
                    }
                    progress = true;
                }
                Err(StreamError::Closed) => {
                    self.reader = Reader::Ended;
                    self.ended = Some(monotonic_clock::now());
                    // Core records the exact EOF diagnosis; accepted legacy
                    // requests remain available to the independent writer.
                    let _ = runtime.finish_notifications(id);
                    progress = true;
                }
                Err(_) => return Err(host_error()),
            },
            Reader::Ended => {}
        }
        self.opening = runtime
            .notification_status()
            .is_some_and(|s| s.phase == NotificationPhase::Opening);
        Ok(progress)
    }
    fn write_step(&mut self, runtime: &mut HostRuntime) -> Result<bool> {
        let mut progress = false;
        self.collect_replies(runtime)?;
        if self.writer.is_none()
            && let Some(body) = self.pending.pop_front()
        {
            let deadline = monotonic_clock::now().saturating_add(500_000_000);
            let mut headers = self.plan.headers.clone();
            headers.retain(|name, _| !name.eq_ignore_ascii_case("last-event-id"));
            headers.insert("content-type".into(), "application/json".into());
            headers.insert(
                "accept".into(),
                "application/json, text/event-stream".into(),
            );
            self.writer = Some((
                Exchange::new(
                    Method::Post,
                    &self.plan.url,
                    &headers,
                    body,
                    &self.credential,
                    deadline,
                    false,
                )?,
                deadline,
            ));
            progress = true;
        }
        if let Some((writer, deadline)) = &mut self.writer {
            ensure(*deadline)?;
            match writer.step()? {
                Step::Progress => progress = true,
                Step::Pending => {}
                Step::Response(response) => {
                    if response.status() != 202 {
                        return Err(host_error());
                    }
                    self.writer = None;
                    progress = true;
                }
            }
        }
        Ok(progress)
    }
    fn step(&mut self, runtime: &mut HostRuntime) -> bool {
        let reader_progress = match self.read_step(runtime) {
            Ok(progress) => progress,
            Err(error) => {
                self.fail(runtime, error.code == "Timeout");
                true
            }
        };
        match self.write_step(runtime) {
            Ok(progress) => reader_progress || progress,
            Err(error) => {
                self.writer_failed = true;
                self.fail(runtime, error.code == "Timeout");
                self.writer = None;
                self.pending.clear();
                true
            }
        }
    }
    fn subscribe(&self) -> Vec<Pollable> {
        let mut ready = Vec::with_capacity(4);
        match &self.reader {
            Reader::Opening(exchange) => ready.push(exchange.subscribe()),
            Reader::Body(incoming) => ready.push(incoming.stream.subscribe()),
            Reader::Ended => {}
        }
        if self.opening && !matches!(self.reader, Reader::Ended) {
            ready.push(monotonic_clock::subscribe_instant(self.deadline));
        }
        if self.writer.is_none() && !self.pending.is_empty() {
            ready.push(monotonic_clock::subscribe_instant(monotonic_clock::now()));
        }
        if let Some((writer, deadline)) = &self.writer {
            ready.push(writer.subscribe());
            ready.push(monotonic_clock::subscribe_instant(*deadline));
        }
        ready
    }
    fn drained(&self) -> bool {
        matches!(self.reader, Reader::Ended) && self.writer.is_none() && self.pending.is_empty()
    }
}
impl WasiDriver {
    /// Enable automatic HTTP notifications after a complete catalog is available.
    pub fn with_http_notifications(mut self, enabled: bool) -> Self {
        self.http_notifications = enabled;
        self
    }
    fn ensure_notifications(&mut self, runtime: &mut HostRuntime, force: bool) -> Result<()> {
        if self.closed || self.reconnecting {
            return Err(Error::local(
                "TransportClosed",
                "WASI session is unavailable",
            ));
        }
        if runtime
            .catalog_status(self.credential.auth_context_id())?
            .authorization_changed
        {
            return Err(Error::new(
                "BindingStale",
                "notification credential context was not accepted",
            ));
        }
        if self.notifications.is_some() {
            return Ok(());
        }
        let status = runtime.notification_status();
        if !force
            && status.is_some_and(|s| {
                s.phase != NotificationPhase::Cancelled
                    || s.error_code.as_deref() != Some("BindingStale")
            })
        {
            return Ok(());
        }
        let plan = runtime.start_notifications()?;
        self.recovery.budget = None;
        let root = plan.resource_id;
        self.open_notifications(
            runtime,
            plan,
            self.credential.clone(),
            root,
            self.timeout_ns,
        )
    }
    fn open_notifications(
        &mut self,
        runtime: &mut HostRuntime,
        plan: NotificationRequest,
        credential: Credential,
        root: u64,
        timeout: u64,
    ) -> Result<()> {
        let id = plan.resource_id;
        self.recovery.next = None;
        self.recovery.source = (plan.method == "GET").then(|| Source {
            id,
            root,
            credential: credential.clone(),
            opened: None,
            ended: None,
        });
        match NotificationIo::new(plan, credential, timeout) {
            Ok(io) => {
                self.notifications = Some(io);
                Ok(())
            }
            Err(error) => {
                let _ = runtime.abort_notifications(id, error.code == "Timeout");
                if let Some(source) = &mut self.recovery.source {
                    source.ended = Some(monotonic_clock::now());
                }
                Err(error)
            }
        }
    }
    pub fn start_notifications(&mut self, runtime: &mut HostRuntime) -> Result<NotificationStatus> {
        if self.closed || self.reconnecting {
            return Err(Error::local(
                "TransportClosed",
                "WASI session is unavailable",
            ));
        }
        if self.notifications.as_ref().is_some_and(|io| {
            runtime
                .notification_status()
                .is_some_and(|s| s.resource_id == io.plan.resource_id && active(&s))
        }) {
            self.http_notifications = true;
            return Ok(runtime.notification_status().expect("active resource"));
        }
        self.stop_notifications(runtime)?;
        self.ensure_notifications(runtime, true)?;
        self.http_notifications = true;
        Ok(runtime.notification_status().expect("started resource"))
    }
    pub fn stop_notifications(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        self.http_notifications = false;
        self.notifications = None;
        self.recovery.source = None;
        self.recovery.budget = None;
        self.recovery.next = None;
        if let Some(status) = runtime.notification_status() {
            runtime.stop_notifications(status.resource_id)?;
        }
        Ok(())
    }
    pub(super) fn service_notifications(&mut self, runtime: &mut HostRuntime) -> bool {
        if self.reconnecting {
            return false;
        }
        if self.http_notifications && !self.closed {
            let _ = self.ensure_notifications(runtime, false);
        }
        let Some(mut io) = self.notifications.take() else {
            return self.recover_notifications(runtime);
        };
        if runtime.notification_status().is_none_or(|s| {
            s.resource_id != io.plan.resource_id || s.phase == NotificationPhase::Cancelled
        }) {
            self.recovery.source = None;
            self.recovery.next = None;
            return true;
        }
        let mut progress = false;
        for _ in 0..16 {
            if runtime.advance_time(self.now_ms()).is_err() {
                io.fail(runtime, false);
                break;
            }
            if !io.step(runtime) {
                break;
            }
            progress = true;
        }
        if let Some(source) = &mut self.recovery.source {
            source.opened = io.opened;
            source.ended = io.ended;
        }
        if io.writer_failed {
            self.recovery.source = None;
        }
        if !io.drained() {
            self.notifications = Some(io);
        }
        progress |= self.recover_notifications(runtime);
        progress
    }
    pub(super) fn wait_with_notifications(
        &mut self,
        ready: &Pollable,
        deadline: u64,
        mut runtime: Option<&mut HostRuntime>,
    ) -> Result<()> {
        loop {
            ensure(deadline)?;
            if let Some(runtime) = runtime.as_deref_mut() {
                self.service_notifications(runtime);
                self.deliver_events(runtime);
            }
            let timer = monotonic_clock::subscribe_instant(deadline);
            let notifications = self.notification_pollables();
            let mut ready_list = vec![ready, &timer];
            ready_list.extend(notifications.iter());
            let signalled = poll(&ready_list);
            ensure(deadline)?;
            if signalled.contains(&0) {
                return Ok(());
            }
        }
    }
    pub(super) fn notification_pollables(&self) -> Vec<Pollable> {
        let mut ready = self
            .notifications
            .as_ref()
            .map(NotificationIo::subscribe)
            .unwrap_or_default();
        if let Some(next) = self.recovery.next {
            ready.push(monotonic_clock::subscribe_instant(next));
        }
        ready
    }
    /// Cooperatively advance notification I/O. A wait timeout is not a stream
    /// failure; the independent opening/ACK and reply deadlines still apply.
    pub fn poll_notifications(
        &mut self,
        runtime: &mut HostRuntime,
        wait: Duration,
    ) -> Result<Option<NotificationStatus>> {
        if self.closed {
            return Err(Error::new("TransportClosed", "WASI driver is closed"));
        }
        let ns = u64::try_from(wait.as_nanos())
            .ok()
            .filter(|ns| *ns <= 86_400_000_000_000)
            .ok_or_else(|| Error::new("ConfigurationError", "poll wait exceeds 24 hours"))?;
        if ns == 0 {
            let ready = self.notification_pollables();
            for resource in &ready {
                let _ = resource.ready();
            }
            drop(ready);
        }
        let progressed = self.service_notifications(runtime);
        if !progressed && ns > 0 && (self.notifications.is_some() || self.recovery.next.is_some()) {
            let timer =
                monotonic_clock::subscribe_instant(monotonic_clock::now().saturating_add(ns));
            let ready = self.notification_pollables();
            let mut refs: Vec<_> = ready.iter().collect();
            refs.push(&timer);
            poll(&refs);
            // All pollables must drop before their parent resources advance.
            drop(refs);
            drop(ready);
            drop(timer);
            self.service_notifications(runtime);
        }
        self.deliver_events(runtime);
        Ok(runtime.notification_status())
    }
}

/// Notification controls for the standalone WASIp2 client. Hosts call poll while
/// idle; ordinary HTTP waits service the same resource automatically.
pub trait WasiClientExt {
    /// Enable or restart a listener; returns before network acknowledgment.
    fn start_notifications(&mut self) -> Result<NotificationStatus>;
    /// Disable listening and drop its reader, queued replies and active writer.
    fn stop_notifications(&mut self) -> Result<()>;
    /// Read the core's latest diagnostic snapshot without advancing I/O.
    fn notification_status(&mut self) -> Option<NotificationStatus>;
    /// Resume one exact failed legacy GET using its original credentials.
    /// Waits cooperatively for retry and accepted replies, up to driver timeout.
    fn resume_notifications(&mut self, resource_id: u64) -> Result<NotificationStatus>;
    /// Bound retry/reply waiting independently; opening retains its own timeout.
    fn resume_notifications_with_timeout(
        &mut self,
        resource_id: u64,
        timeout: Duration,
    ) -> Result<NotificationStatus>;
    /// Run bounded service work, optionally waiting up to 24 hours for I/O.
    /// Zero never waits; an idle wait timeout does not fail the subscription.
    fn poll_notifications(&mut self, wait: Duration) -> Result<Option<NotificationStatus>>;
}
impl WasiClientExt for Client<WasiDriver> {
    fn start_notifications(&mut self) -> Result<NotificationStatus> {
        let (runtime, driver) = self.host_parts_mut();
        driver.start_notifications(runtime)
    }
    fn stop_notifications(&mut self) -> Result<()> {
        let (runtime, driver) = self.host_parts_mut();
        driver.stop_notifications(runtime)
    }
    fn notification_status(&mut self) -> Option<NotificationStatus> {
        self.host_parts_mut().0.notification_status()
    }
    fn resume_notifications(&mut self, resource_id: u64) -> Result<NotificationStatus> {
        let (runtime, driver) = self.host_parts_mut();
        driver.resume_notifications(runtime, resource_id)
    }
    fn resume_notifications_with_timeout(
        &mut self,
        resource_id: u64,
        timeout: Duration,
    ) -> Result<NotificationStatus> {
        let (runtime, driver) = self.host_parts_mut();
        driver.resume_notifications_with_timeout(runtime, resource_id, timeout)
    }
    fn poll_notifications(&mut self, wait: Duration) -> Result<Option<NotificationStatus>> {
        let (runtime, driver) = self.host_parts_mut();
        driver.poll_notifications(runtime, wait)
    }
}
