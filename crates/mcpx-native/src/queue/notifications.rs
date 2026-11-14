use super::*;
use crate::stdio::{StreamCancellation, StreamWriter};
use crate::transport::{Input, Message};
use futures_util::StreamExt;
use mcpx_core::Driver;
use mcpx_core::{Credential, HostRuntime, NotificationPhase, NotificationRequest};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};

pub(super) fn active(runtime: &HostRuntime) -> bool {
    runtime.notification_status().is_some_and(|s| {
        matches!(
            s.phase,
            NotificationPhase::Opening | NotificationPhase::Listening
        )
    })
}
struct Io {
    id: u64,
    opening_deadline: Instant,
    reader: tokio::task::JoinHandle<()>,
    writer: tokio::task::JoinHandle<()>,
    replies: mpsc::Sender<Value>,
    outstanding: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
    health: Arc<std::sync::Mutex<ConnectionHealth>>,
}
#[derive(Default)]
struct ConnectionHealth {
    opened: Option<Instant>,
    ended: Option<Instant>,
}
impl Drop for Io {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
    }
}
impl Io {
    async fn stop(mut self) {
        self.reader.abort();
        self.writer.abort();
        let _ = (&mut self.reader).await;
        let _ = (&mut self.writer).await;
    }
    fn open(
        plan: NotificationRequest,
        http: reqwest::Client,
        credential: Arc<Credential>,
        timeout: Duration,
        input: mpsc::Sender<Message>,
        wake: Arc<Notify>,
    ) -> Result<Self> {
        let opening_deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::local(
                "ConfigurationError",
                "notification timeout exceeds clock range",
            )
        })?;
        let id = plan.resource_id;
        let client = http.clone();
        let snapshot = credential.clone();
        let plan_copy = plan.clone();
        let incoming = input.clone();
        let health = Arc::new(std::sync::Mutex::new(ConnectionHealth::default()));
        let reader_health = health.clone();
        let reader = tokio::spawn(async move {
            let run = async {
                let mut request = client.request(
                    if plan_copy.method == "GET" {
                        reqwest::Method::GET
                    } else {
                        reqwest::Method::POST
                    },
                    &plan_copy.url,
                );
                for (name, value) in &plan_copy.headers {
                    request = request.header(name, value);
                }
                if let Some(token) = snapshot.token() {
                    request = request.bearer_auth(token);
                }
                if let Some(body) = &plan_copy.body {
                    request = request.body(body.to_string());
                }
                let response = tokio::time::timeout_at(opening_deadline.into(), request.send())
                    .await
                    .map_err(|_| true)?
                    .map_err(|_| false)?;
                let status = response.status().as_u16();
                let mime = response
                    .headers()
                    .get("content-type")
                    .and_then(|h| h.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                if status == 200
                    && mime
                        .split(';')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .eq_ignore_ascii_case("text/event-stream")
                {
                    reader_health.lock().unwrap().opened = Some(Instant::now());
                }
                incoming
                    .send(Message {
                        id,
                        input: Input::NotificationHeaders(status, mime, opening_deadline),
                    })
                    .await
                    .map_err(|_| false)?;
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|_| false)?;
                    for bytes in chunk.chunks(65536) {
                        incoming
                            .send(Message {
                                id,
                                input: Input::NotificationChunk(bytes.to_vec(), opening_deadline),
                            })
                            .await
                            .map_err(|_| false)?;
                    }
                }
                reader_health.lock().unwrap().ended = Some(Instant::now());
                let _ = incoming
                    .send(Message {
                        id,
                        input: Input::NotificationEof,
                    })
                    .await;
                Ok::<(), bool>(())
            }
            .await;
            if let Err(timeout) = run {
                reader_health.lock().unwrap().ended = Some(Instant::now());
                let _ = incoming
                    .send(Message {
                        id,
                        input: Input::NotificationFailed(timeout),
                    })
                    .await;
            }
        });
        let (replies, mut output) = mpsc::channel::<Value>(16);
        let outstanding = Arc::new(AtomicUsize::new(0));
        let pending = outstanding.clone();
        let failed = Arc::new(AtomicBool::new(false));
        let failure = failed.clone();
        let writer = tokio::spawn(async move {
            while let Some(body) = output.recv().await {
                let mut request = http.post(&plan.url).body(body.to_string());
                for (name, value) in &plan.headers {
                    if name != "last-event-id" {
                        request = request.header(name, value);
                    }
                }
                request = request
                    .header("accept", "application/json, text/event-stream")
                    .header("content-type", "application/json");
                if let Some(token) = credential.token() {
                    request = request.bearer_auth(token);
                }
                let result = tokio::time::timeout(Duration::from_millis(500), request.send()).await;
                let success =
                    matches!(result,Ok(Ok(ref response)) if response.status().as_u16()==202);
                if !success {
                    failure.store(true, Ordering::SeqCst);
                }
                pending.fetch_sub(1, Ordering::SeqCst);
                wake.notify_one();
                if !success {
                    let _ = input
                        .send(Message {
                            id,
                            input: Input::NotificationFailed(false),
                        })
                        .await;
                    break;
                }
            }
        });
        Ok(Self {
            id,
            opening_deadline,
            reader,
            writer,
            replies,
            outstanding,
            failed,
            health,
        })
    }
}
#[derive(Default)]
pub(super) struct Notifications {
    io: Option<Io>,
    stdio: Option<StdioIo>,
    resume: Option<ResumeSource>,
    policy: Option<NotificationRecoveryPolicy>,
    budget: Option<super::notification_recovery::Budget>,
    healthy_reset: Option<u64>,
    next_recovery: Option<Instant>,
}
#[derive(Clone)]
struct ResumeSource {
    id: u64,
    root: u64,
    http: reqwest::Client,
    credential: Arc<Credential>,
    timeout: Duration,
    sender: mpsc::Sender<Message>,
    wake: Arc<Notify>,
    origin: Instant,
    health: Arc<std::sync::Mutex<ConnectionHealth>>,
}
struct StdioIo {
    id: u64,
    opening_deadline: Instant,
    task: Option<tokio::task::JoinHandle<()>>,
    cancellation: Option<StreamCancellation>,
    failed: Arc<AtomicU8>,
}
impl Drop for StdioIo {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        if let Some(cancellation) = &self.cancellation {
            cancellation.abandon();
        }
    }
}
impl StdioIo {
    async fn stop(mut self) {
        if let Some(cancellation) = &self.cancellation {
            cancellation.cancel().await;
        }
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
    fn open(
        plan: NotificationRequest,
        writer: &StreamWriter,
        timeout: Duration,
        wake: Arc<Notify>,
    ) -> Result<Self> {
        let opening_deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::local(
                "ConfigurationError",
                "notification timeout exceeds clock range",
            )
        })?;
        let id = plan.resource_id;
        let Some(body) = plan.body else {
            return Ok(Self {
                id,
                opening_deadline,
                task: None,
                cancellation: None,
                failed: Arc::default(),
            });
        };
        let cancellation = writer.cancellation(serde_json::json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":plan.rpc_id.unwrap(),"reason":"catalog subscription ended"}}));
        let request = cancellation.clone();
        let writer = writer.clone();
        let failed = Arc::<AtomicU8>::default();
        let report = failed.clone();
        let task = tokio::spawn(async move {
            let result = tokio::time::timeout_at(
                opening_deadline.into(),
                writer.write_control_request(&body, request),
            )
            .await;
            if !matches!(result, Ok(Ok(()))) {
                report.store(if result.is_err() { 1 } else { 2 }, Ordering::SeqCst);
                wake.notify_one();
            }
        });
        Ok(Self {
            id,
            opening_deadline,
            task: Some(task),
            cancellation: Some(cancellation),
            failed,
        })
    }
}
impl Notifications {
    pub(super) fn new(policy: Option<NotificationRecoveryPolicy>) -> Self {
        Self {
            policy,
            ..Default::default()
        }
    }
    pub(super) fn recovery_deadline(&self) -> Option<Instant> {
        self.next_recovery
    }
    /// Pump accepted legacy replies even while the owner is waiting on a
    /// credential provider. A failed writer invalidates its resume snapshot.
    pub(super) async fn poll(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        self.expire(runtime);
        let status = runtime.notification_status();
        if let Some(io) = &self.io {
            let usable = !io.failed.load(Ordering::SeqCst)
                && status.as_ref().is_some_and(|s| {
                    s.resource_id == io.id
                        && (matches!(
                            s.phase,
                            NotificationPhase::Opening | NotificationPhase::Listening
                        ) || s.rpc_id.is_none() && s.phase == NotificationPhase::Failed)
                });
            if usable {
                if !active(runtime) {
                    io.reader.abort();
                }
                let mut failed = false;
                for reply in runtime.notification_replies(io.id)? {
                    io.outstanding.fetch_add(1, Ordering::SeqCst);
                    if io.replies.try_send(reply).is_err() {
                        io.outstanding.fetch_sub(1, Ordering::SeqCst);
                        let _ = runtime.abort_notifications(io.id, false);
                        failed = true;
                        self.resume = None;
                        break;
                    }
                }
                if failed || !active(runtime) && io.outstanding.load(Ordering::SeqCst) == 0 {
                    self.stop().await;
                }
            } else {
                if io.failed.load(Ordering::SeqCst) {
                    self.resume = None;
                    if active(runtime) {
                        let _ = runtime.abort_notifications(io.id, false);
                    }
                }
                self.stop().await;
            }
        }
        if let Some(source) = &self.resume {
            let now = source
                .origin
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
            runtime.advance_time(now)?;
            if !runtime.notification_status().is_some_and(|s| {
                s.resource_id == source.id
                    && (matches!(
                        s.phase,
                        NotificationPhase::Opening | NotificationPhase::Listening
                    ) || s.phase == NotificationPhase::Failed
                        && runtime
                            .notification_resume(s.resource_id)
                            .map_or_else(|e| e.code == "Busy", |_| true))
            }) {
                self.resume = None;
            }
        }
        if let Some(source) = &self.resume
            && self.healthy_reset != Some(source.id)
            && let Some(policy) = self.policy
            && runtime.notification_status().is_some_and(|s| {
                s.phase == NotificationPhase::Listening
                    || s.phase == NotificationPhase::Failed
                        && s.error_code.as_deref() != Some("Timeout")
            })
        {
            let health = source.health.lock().unwrap();
            if health.opened.is_some_and(|opened| {
                health
                    .ended
                    .unwrap_or_else(Instant::now)
                    .duration_since(opened)
                    >= policy.timeout
            }) {
                self.budget = None;
                self.healthy_reset = Some(source.id);
            }
        }
        Ok(())
    }
    pub(super) fn resume_delay(&self, runtime: &HostRuntime, id: u64) -> Result<Duration> {
        let info = runtime.notification_resume(id)?;
        self.resume.as_ref().filter(|s| s.id == id).ok_or_else(|| {
            Error::local(
                "ResumeUnavailable",
                "native listener has no matching frozen credential snapshot",
            )
        })?;
        if self.io.is_some() {
            return Ok(Duration::from_millis(5));
        }
        Ok(Duration::from_millis(info.retry_after_ms))
    }
    pub(super) fn resume_now(
        &mut self,
        runtime: &mut HostRuntime,
        id: u64,
        limit: Option<Duration>,
    ) -> Result<mcpx_core::NotificationStatus> {
        if !self.resume_delay(runtime, id)?.is_zero() {
            return Err(Error::local(
                "RetryLater",
                "notification retry or capability replies are still pending",
            ));
        }
        let mut source = self.resume.as_ref().unwrap().clone();
        let plan = runtime.resume_notifications(id)?;
        source.id = plan.resource_id;
        source.health = Arc::default();
        let opened = Io::open(
            plan,
            source.http.clone(),
            source.credential.clone(),
            limit.map_or(source.timeout, |v| v.min(source.timeout)),
            source.sender.clone(),
            source.wake.clone(),
        );
        match opened {
            Ok(io) => {
                source.health = io.health.clone();
                self.io = Some(io);
            }
            Err(error) => {
                self.resume = Some(source);
                let id = runtime.notification_status().unwrap().resource_id;
                let _ = runtime.abort_notifications(id, false);
                return Err(error);
            }
        }
        self.resume = Some(source);
        if limit.is_none() {
            self.budget = None;
        }
        Ok(runtime.notification_status().unwrap())
    }
    pub(super) async fn recover(
        &mut self,
        runtime: &mut HostRuntime,
        enabled: bool,
        manual_pending: bool,
    ) -> Result<()> {
        self.poll(runtime).await?;
        self.next_recovery = None;
        let Some(policy) = self.policy.filter(|_| enabled && !manual_pending) else {
            return Ok(());
        };
        let Some(source) = &self.resume else {
            return Ok(());
        };
        if !runtime
            .notification_status()
            .is_some_and(|s| s.phase == NotificationPhase::Failed)
        {
            return Ok(());
        }
        let id = source.id;
        let delay = match self.resume_delay(runtime, id) {
            Ok(delay) => delay,
            Err(error) if error.code == "Busy" => Duration::from_millis(5),
            Err(_) => return Ok(()),
        };
        let now = Instant::now();
        let failed_at = source.health.lock().unwrap().ended.unwrap_or(now);
        if self.budget.as_ref().is_none_or(|b| b.root != source.root) {
            self.budget = Some(super::notification_recovery::Budget {
                root: source.root,
                failed_id: id,
                deadline: failed_at + policy.timeout,
                next: failed_at + policy.backoff,
                remaining: policy.max_attempts,
            });
        }
        let budget = self.budget.as_mut().unwrap();
        if budget.failed_id != id {
            budget.failed_id = id;
            budget.next = failed_at + policy.backoff;
        }
        if budget.remaining == 0 || now >= budget.deadline {
            return Ok(());
        }
        let next = now
            .checked_add(delay)
            .unwrap_or(budget.deadline)
            .max(budget.next);
        if next > now {
            self.next_recovery = Some(next.min(budget.deadline));
            return Ok(());
        }
        let remaining = budget.deadline.duration_since(now);
        budget.remaining -= 1;
        let _ = self.resume_now(runtime, id, Some(remaining));
        Ok(())
    }
    pub(super) fn expire(&self, runtime: &mut HostRuntime) {
        if let Some(io) = &self.stdio
            && io.failed.load(Ordering::SeqCst) != 0
            && active(runtime)
        {
            let _ = runtime.abort_notifications(io.id, io.failed.load(Ordering::SeqCst) == 1);
        }
        if self
            .opening_deadline(runtime)
            .is_some_and(|deadline| Instant::now() >= deadline)
            && let Some(status) = runtime.notification_status()
        {
            let _ = runtime.abort_notifications(status.resource_id, true);
        }
    }
    pub(super) async fn expire_pending(&mut self, runtime: &mut HostRuntime) {
        self.expire(runtime);
        if runtime.notification_status().is_some_and(|s| {
            s.phase == NotificationPhase::Failed && s.error_code.as_deref() == Some("Timeout")
        }) && self
            .io
            .as_ref()
            .is_none_or(|io| io.outstanding.load(Ordering::SeqCst) == 0)
        {
            self.stop().await;
        }
    }
    pub(super) fn opening_deadline(&self, runtime: &HostRuntime) -> Option<Instant> {
        self.io
            .as_ref()
            .map(|io| (io.id, io.opening_deadline))
            .or_else(|| self.stdio.as_ref().map(|io| (io.id, io.opening_deadline)))
            .filter(|(id, _)| {
                runtime
                    .notification_status()
                    .is_some_and(|s| s.resource_id == *id && s.phase == NotificationPhase::Opening)
            })
            .map(|(_, deadline)| deadline)
    }
    pub(super) async fn stop(&mut self) {
        if let Some(io) = self.io.take() {
            io.stop().await;
        }
        if let Some(io) = self.stdio.take() {
            io.stop().await;
        }
    }
    pub(super) async fn disable(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        self.resume = None;
        self.budget = None;
        self.healthy_reset = None;
        self.next_recovery = None;
        if let Some(status) = runtime.notification_status() {
            if (status.phase == NotificationPhase::Closed
                || status.phase == NotificationPhase::Failed
                    && status.error_code.as_deref() == Some("RemoteProtocolError"))
                && let Some(io) = &mut self.stdio
            {
                io.cancellation = None;
            }
            runtime.stop_notifications(status.resource_id)?;
        }
        self.stop().await;
        Ok(())
    }
    pub(super) async fn start(
        &mut self,
        remote: &mut Remote,
        sender: &mpsc::Sender<Message>,
        wake: &Arc<Notify>,
        writer: Option<&StreamWriter>,
    ) -> Result<()> {
        if active(remote.host_runtime_mut()) {
            return Ok(());
        }
        self.disable(remote.host_runtime_mut()).await?;
        self.sync(remote, sender, true, wake, writer, true).await
    }
    pub(super) async fn sync(
        &mut self,
        remote: &mut Remote,
        sender: &mpsc::Sender<Message>,
        enabled: bool,
        wake: &Arc<Notify>,
        writer: Option<&StreamWriter>,
        force: bool,
    ) -> Result<()> {
        if !enabled {
            return Ok(());
        }
        if let Some(writer) = writer {
            return self.sync_stdio(remote, writer, wake, force).await;
        }
        self.poll(remote.host_runtime_mut()).await?;
        let status = remote.host_runtime_mut().notification_status();
        if self.io.is_some() {
            return Ok(());
        }
        self.stop().await;
        let eligible = force
            || status.is_none_or(|s| {
                s.phase == NotificationPhase::Cancelled
                    && s.error_code.as_deref() == Some("BindingStale")
            });
        if !eligible {
            return Ok(());
        }
        let (http, credential, timeout) = remote.driver().notification_snapshot();
        if remote
            .host_runtime_mut()
            .catalog_status(credential.auth_context_id())?
            .authorization_changed
        {
            return Ok(());
        }
        let plan = match remote.host_runtime_mut().start_notifications() {
            Ok(plan) => plan,
            Err(error) if error.code == "NotConnected" => return Ok(()),
            Err(error) => return Err(error),
        };
        let credential = Arc::new(credential);
        let id = plan.resource_id;
        let legacy = plan.method == "GET";
        let io = Io::open(
            plan,
            http.clone(),
            credential.clone(),
            timeout,
            sender.clone(),
            wake.clone(),
        )?;
        self.resume = legacy.then(|| ResumeSource {
            id,
            root: id,
            http,
            credential,
            timeout,
            sender: sender.clone(),
            wake: wake.clone(),
            origin: remote.driver().clock_origin(),
            health: io.health.clone(),
        });
        self.budget = None;
        self.healthy_reset = None;
        self.io = Some(io);
        Ok(())
    }
    async fn sync_stdio(
        &mut self,
        remote: &mut Remote,
        writer: &StreamWriter,
        wake: &Arc<Notify>,
        force: bool,
    ) -> Result<()> {
        self.expire(remote.host_runtime_mut());
        let status = remote.host_runtime_mut().notification_status();
        if status.as_ref().is_some_and(|s| {
            s.phase == NotificationPhase::Closed
                || s.phase == NotificationPhase::Failed
                    && s.error_code.as_deref() == Some("RemoteProtocolError")
        }) && let Some(io) = &mut self.stdio
        {
            io.cancellation = None;
        }
        if self.stdio.as_ref().is_some_and(|io| {
            status.as_ref().is_some_and(|s| {
                s.resource_id == io.id
                    && matches!(
                        s.phase,
                        NotificationPhase::Opening | NotificationPhase::Listening
                    )
            })
        }) {
            return Ok(());
        }
        self.stop().await;
        if !force
            && !status.is_none_or(|s| {
                s.phase == NotificationPhase::Cancelled
                    && s.error_code.as_deref() == Some("BindingStale")
            })
        {
            return Ok(());
        }
        let (_, credential, timeout) = remote.driver().notification_snapshot();
        if remote
            .host_runtime_mut()
            .catalog_status(credential.auth_context_id())?
            .authorization_changed
        {
            return Ok(());
        }
        let plan = match remote.host_runtime_mut().start_stdio_notifications() {
            Ok(plan) => plan,
            Err(error) if error.code == "NotConnected" => return Ok(()),
            Err(error) => return Err(error),
        };
        if plan.rpc_id.is_some() {
            let deadline = u64::try_from(timeout.as_millis())
                .ok()
                .and_then(|ms| remote.driver().now_ms().checked_add(ms))
                .ok_or_else(|| {
                    Error::local(
                        "ConfigurationError",
                        "notification timeout exceeds clock range",
                    )
                })?;
            remote
                .host_runtime_mut()
                .set_notification_deadline(plan.resource_id, deadline)?;
        }
        self.stdio = Some(StdioIo::open(plan, writer, timeout, wake.clone())?);
        Ok(())
    }
}
