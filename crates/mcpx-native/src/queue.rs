use crate::{Client, Execution, Remote, ServerConfig};
use mcpx_core::{
    ArgsBuilder, CallOutcome, Continuation, Error, Result, ToolBinding, ToolDescriptor,
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
mod notification_controls;
mod notification_recovery;
mod notifications;
pub use notification_recovery::NotificationRecoveryPolicy;
mod parallel;

#[derive(Clone, Debug)]
pub struct QueueOptions {
    /// Bounded legacy tool response GETs. Overrides a factory Client policy
    /// when Some; None inherits that builder's policy (disabled by default).
    pub response_recovery: Option<crate::ResponseRecoveryPolicy>,
    /// Opt in to one background HTTP catalog notification stream (HTTP queues only).
    pub http_notifications: bool,
    /// Opt in to a catalog subscription on the shared stdio channel.
    pub stdio_notifications: bool,
    /// Optional bounded legacy notification GET recovery; listening must also
    /// be enabled. Never retries business POSTs or renews an expired session.
    pub notification_recovery: Option<NotificationRecoveryPolicy>,
    /// Ordinary admitted requests, including active requests. Notification stop
    /// and status have two additional reserved slots and a 512-byte budget.
    pub capacity: usize,
    /// Includes time spent waiting in the queue.
    pub timeout: Duration,
    /// Serialized queued input bytes, separate from request count and response budgets.
    pub max_bytes: usize,
    /// Maximum typed/JSON, command and continuation calls in flight over HTTP or shared stdio.
    pub max_in_flight: usize,
}
impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            response_recovery: None,
            http_notifications: false,
            stdio_notifications: false,
            notification_recovery: None,
            capacity: 64,
            timeout: Duration::from_secs(60),
            max_bytes: 8 * 1024 * 1024,
            max_in_flight: 16,
        }
    }
}

fn allocate_queue_id(next: &AtomicU64) -> Result<u64> {
    let mut id = next.load(Ordering::Relaxed);
    loop {
        if !(1..=mcpx_core::MAX_HOST_ID).contains(&id) {
            return Err(Error::local("LimitExceeded", "operation ID exhausted"));
        }
        match next.compare_exchange_weak(id, id + 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Ok(id),
            Err(current) => id = current,
        }
    }
}

struct Shared {
    catalog_source: std::sync::OnceLock<mcpx_core::CatalogRevisionSource>,
    submission: std::sync::Mutex<()>,
    operation_events: Option<crate::operation_events::OperationProducer>,
    sender: mpsc::UnboundedSender<Job>,
    controls: mpsc::UnboundedSender<Job>,
    control_slots: Arc<Semaphore>,
    control_bytes: Arc<Semaphore>,
    resume_slot: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    shutdown: CancellationToken,
    closed: watch::Receiver<Option<Result<()>>>,
    wake: Arc<Notify>,
    next_id: AtomicU64,
    timeout: Duration,
}
impl Drop for Shared {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Cloneable Send + Sync access to a remote owned by a dedicated runtime thread.
/// Business calls and resumes run concurrently; catalog/configuration operations are barriers.
#[derive(Clone)]
pub struct QueuedRemote(Arc<Shared>);

enum Request {
    Bind(String),
    Call(u64, Value, Arc<()>),
    Resume(Continuation, Value),
    Execute(String, Vec<String>),
    Complete(String, usize),
    CommandSpec(String),
    SchemaRegistry(mcpx_core::SchemaRegistry),
    SchemaStats,
    CacheStats,
    CacheCapacity(usize),
    CacheClear,
    Refresh(mcpx_core::CatalogRevision),
    Reconnect,
    Elicitation(bool, bool),
    Events,
    NotificationStatus,
    NotificationStart,
    NotificationStop,
    NotificationResume(u64),
    ReleaseContinuation(Continuation),
}
enum Reply {
    Bound {
        id: u64,
        schema: mcpx_core::ToolSchema,
        lease: Arc<()>,
    },
    Outcome(CallOutcome),
    Execution(Execution),
    Completion(mcpx_command::Completion),
    CommandSpec(mcpx_command::CommandSpec),
    Events(mcpx_core::EventBatch),
    NotificationStatus(Option<mcpx_core::NotificationStatus>),
    SchemaStats(mcpx_core::SchemaRegistryStats),
    CacheStats(mcpx_core::ValidatorCacheStats),
    Unit,
}
struct Job {
    operation_event: Option<crate::operation_events::OperationTicket>,
    id: u64,
    request: Request,
    reply: oneshot::Sender<Result<Reply>>,
    cancellation: CancellationToken,
    deadline: Instant,
    _slot: OwnedSemaphorePermit,
    _bytes: Arc<OwnedSemaphorePermit>,
    _resume_slot: Option<OwnedSemaphorePermit>,
}

/// Dropping an unfinished operation cancels it, including while it is queued.
pub struct QueuedOperation<T> {
    id: u64,
    receiver: oneshot::Receiver<Result<Reply>>,
    cancellation: CancellationToken,
    wake: Arc<Notify>,
    decode: fn(Reply) -> Result<T>,
    finished: bool,
    _output: PhantomData<fn() -> T>,
}
#[derive(Clone, Debug)]
pub struct QueuedCancellation {
    id: u64,
    cancellation: CancellationToken,
    wake: Arc<Notify>,
}
impl QueuedCancellation {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
        self.wake.notify_one();
    }
}
impl<T> QueuedOperation<T> {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
        self.wake.notify_one();
    }
    pub fn cancellation_handle(&self) -> QueuedCancellation {
        QueuedCancellation {
            id: self.id,
            cancellation: self.cancellation.clone(),
            wake: self.wake.clone(),
        }
    }
}
impl<T> Future for QueuedOperation<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let operation = self.get_mut();
        match Pin::new(&mut operation.receiver).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                operation.finished = true;
                Poll::Ready(
                    result
                        .map_err(|_| closed_error())
                        .and_then(|r| r)
                        .and_then(operation.decode)
                        .map_err(|error| error.with_queue_operation(operation.id)),
                )
            }
        }
    }
}
impl<T> Drop for QueuedOperation<T> {
    fn drop(&mut self) {
        if !self.finished {
            self.cancel();
        }
    }
}

#[derive(Clone)]
pub struct QueuedBinding {
    remote: QueuedRemote,
    id: u64,
    schema: mcpx_core::ToolSchema,
    lease: Arc<()>,
}
pub struct QueuedArgsBuilder {
    inner: ArgsBuilder,
    lease: Arc<()>,
}
#[derive(Clone)]
pub struct QueuedArgs {
    value: Value,
    lease: Arc<()>,
}
impl QueuedArgs {
    pub fn value(&self) -> &Value {
        &self.value
    }
}
impl QueuedArgsBuilder {
    pub fn set(mut self, name: &str, value: impl Serialize) -> Result<Self> {
        self.inner = self.inner.set(name, value)?;
        Ok(self)
    }
    pub fn replace(mut self, name: &str, value: impl Serialize) -> Result<Self> {
        self.inner = self.inner.replace(name, value)?;
        Ok(self)
    }
    pub fn finish(self) -> Result<QueuedArgs> {
        let args = self.inner.finish()?;
        Ok(QueuedArgs {
            value: args.value().clone(),
            lease: self.lease,
        })
    }
}
impl QueuedBinding {
    pub fn descriptor(&self) -> &ToolDescriptor {
        self.schema.descriptor()
    }
    pub fn args(&self) -> Result<QueuedArgsBuilder> {
        Ok(QueuedArgsBuilder {
            inner: self.schema.bind().args(),
            lease: self.lease.clone(),
        })
    }
    /// Runs Serialize and Schema validation on the caller thread. The finished
    /// arguments retain this queued binding's lease and may cross threads.
    pub fn args_from(&self, value: impl Serialize) -> Result<QueuedArgs> {
        let args = self.schema.bind().args_from(value)?;
        Ok(QueuedArgs {
            value: args.value().clone(),
            lease: self.lease.clone(),
        })
    }
    pub fn submit_typed(&self, value: impl Serialize) -> Result<QueuedOperation<CallOutcome>> {
        self.submit(&self.args_from(value)?)
    }
    pub async fn call_typed(&self, value: impl Serialize) -> Result<CallOutcome> {
        self.submit_typed(value)?.await
    }
    pub fn submit(&self, args: &QueuedArgs) -> Result<QueuedOperation<CallOutcome>> {
        self.submit_with_timeout(args, self.remote.0.timeout)
    }
    pub fn submit_with_timeout(
        &self,
        args: &QueuedArgs,
        timeout: Duration,
    ) -> Result<QueuedOperation<CallOutcome>> {
        if !Arc::ptr_eq(&self.lease, &args.lease) {
            return Err(Error::local(
                "BindingMismatch",
                "arguments belong to another queued binding",
            ));
        }
        self.remote.submit_with_timeout(
            Request::Call(self.id, args.value.clone(), self.lease.clone()),
            outcome,
            timeout,
        )
    }
    pub async fn call(&self, args: &QueuedArgs) -> Result<CallOutcome> {
        self.submit(args)?.await
    }
    pub fn submit_json(&self, value: Value) -> Result<QueuedOperation<CallOutcome>> {
        let args = self.schema.bind().validate_json(value)?;
        self.remote.submit(
            Request::Call(self.id, args.value().clone(), self.lease.clone()),
            outcome,
        )
    }
}

fn closed_error() -> Error {
    Error::new("TransportClosed", "queued remote is closed")
}
fn unit(reply: Reply) -> Result<()> {
    match reply {
        Reply::Unit => Ok(()),
        _ => Err(Error::new("ProtocolError", "unexpected queue reply")),
    }
}
fn outcome(reply: Reply) -> Result<CallOutcome> {
    match reply {
        Reply::Outcome(value) => Ok(value),
        _ => Err(Error::new("ProtocolError", "unexpected queue reply")),
    }
}
fn execution(reply: Reply) -> Result<Execution> {
    match reply {
        Reply::Execution(value) => Ok(value),
        _ => Err(Error::new("ProtocolError", "unexpected queue reply")),
    }
}
fn notification_status(reply: Reply) -> Result<Option<mcpx_core::NotificationStatus>> {
    match reply {
        Reply::NotificationStatus(value) => Ok(value),
        _ => Err(Error::new("ProtocolError", "unexpected queue reply")),
    }
}

impl QueuedRemote {
    pub async fn connect(server: ServerConfig, options: QueueOptions) -> Result<Self> {
        Self::spawn_with(server, options, Client::new).await
    }
    /// The factory runs inside the owner thread and may create !Send providers/callbacks.
    pub async fn spawn_with(
        server: ServerConfig,
        options: QueueOptions,
        factory: impl FnOnce() -> Result<Client> + Send + 'static,
    ) -> Result<Self> {
        Self::spawn_inner(server, options, factory, None).await
    }
    pub async fn connect_with_events(
        server: ServerConfig,
        options: QueueOptions,
        limits: mcpx_core::EventLimits,
    ) -> Result<(Self, crate::OperationEventStream)> {
        Self::spawn_with_events(server, options, limits, Client::new).await
    }
    pub async fn spawn_with_events(
        server: ServerConfig,
        options: QueueOptions,
        limits: mcpx_core::EventLimits,
        factory: impl FnOnce() -> Result<Client> + Send + 'static,
    ) -> Result<(Self, crate::OperationEventStream)> {
        let (producer, stream) = crate::operation_events::channel(limits)?;
        let remote = Self::spawn_inner(server, options, factory, Some(producer)).await?;
        Ok((remote, stream))
    }
    async fn spawn_inner(
        server: ServerConfig,
        options: QueueOptions,
        factory: impl FnOnce() -> Result<Client> + Send + 'static,
        operation_events: Option<crate::operation_events::OperationProducer>,
    ) -> Result<Self> {
        if let Some(policy) = options.response_recovery {
            policy.validate()?;
        }
        if let Some(policy) = options.notification_recovery {
            policy.validate()?;
        }
        if !(1..=1024).contains(&options.capacity)
            || !(1..=16).contains(&options.max_in_flight)
            || options.timeout.is_zero()
            || !(1024..=64 * 1024 * 1024).contains(&options.max_bytes)
        {
            return Err(Error::local(
                "ConfigurationError",
                "queue requires capacity 1..1024, in-flight limit 1..16, byte budget 1 KiB..64 MiB and positive timeout",
            ));
        }
        if Instant::now().checked_add(options.timeout).is_none() {
            return Err(Error::local(
                "ConfigurationError",
                "queue timeout exceeds monotonic clock range",
            ));
        }
        let (sender, receiver) = mpsc::unbounded_channel();
        let (controls, control_receiver) = mpsc::unbounded_channel();
        let (ready, startup) = oneshot::channel();
        let (closed_sender, closed) = watch::channel(None);
        let shutdown = CancellationToken::new();
        let guard = shutdown.clone().drop_guard();
        let wake = Arc::new(Notify::new());
        let event_owner = operation_events.as_ref().map(|events| events.owner());
        let handle = Self(Arc::new(Shared {
            catalog_source: std::sync::OnceLock::new(),
            submission: std::sync::Mutex::new(()),
            operation_events,
            sender,
            controls,
            control_slots: Arc::new(Semaphore::new(2)),
            control_bytes: Arc::new(Semaphore::new(512)),
            resume_slot: Arc::new(Semaphore::new(1)),
            slots: Arc::new(Semaphore::new(options.capacity)),
            bytes: Arc::new(Semaphore::new(options.max_bytes)),
            shutdown: shutdown.clone(),
            closed,
            wake: wake.clone(),
            next_id: AtomicU64::new(1),
            timeout: options.timeout,
        }));
        let catalog_target = Arc::downgrade(&handle.0);
        std::thread::Builder::new()
            .name("mcpx-remote".into())
            .spawn(move || {
                let _event_owner = event_owner;
                let result = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime.block_on(async {
                        let mut client = match factory() {
                            Ok(client) => client,
                            Err(error) => {
                                let _ = ready.send(Err(error));
                                return Ok(());
                            }
                        };
                        if let Some(policy) = options.response_recovery {
                            client.response_recovery = Some(policy);
                        }
                        let cancel_connect = client.cancellation_handle();
                        let connect = server.connect(client, None);
                        tokio::pin!(connect);
                        let connected = tokio::select! {
                            biased;
                            remote = &mut connect => remote,
                            _ = shutdown.cancelled() => { cancel_connect.cancel(); connect.await }
                        };
                        let mut remote = match connected {
                            Ok(remote) => remote,
                            Err(error) => {
                                let _ = ready.send(Err(error));
                                return Ok(());
                            }
                        };
                        // This owner feeds old calls while recovering and owns
                        // the shared budget. Do not nest mutable preflight I/O.
                        remote.driver_mut().mutable_session_recovery = false;
                        if shutdown.is_cancelled() {
                            let _ = ready.send(Err(closed_error()));
                            return remote.close().await;
                        }
                        if let Some(shared) = catalog_target.upgrade() {
                            assert!(
                                shared
                                    .catalog_source
                                    .set(remote.catalog_revision_source())
                                    .is_ok()
                            );
                        }
                        if ready.send(Ok(())).is_err() {
                            return remote.close().await;
                        }
                        let result = parallel::run(
                            &mut remote,
                            receiver,
                            shutdown,
                            wake,
                            options.max_in_flight,
                            options.http_notifications,
                            options.stdio_notifications,
                            control_receiver,
                            options.notification_recovery,
                        )
                        .await;
                        let cleanup = remote.close().await;
                        result.and(cleanup)
                    }),
                    Err(_) => {
                        let _ = ready.send(Err(Error::local(
                            "ConfigurationError",
                            "cannot construct queue runtime",
                        )));
                        Ok(())
                    }
                };
                let _ = closed_sender.send(Some(result));
            })
            .map_err(|_| Error::local("ConfigurationError", "cannot start remote owner thread"))?;
        startup.await.map_err(|_| closed_error())??;
        guard.disarm();
        Ok(handle)
    }
    fn submit<T>(
        &self,
        request: Request,
        decode: fn(Reply) -> Result<T>,
    ) -> Result<QueuedOperation<T>> {
        self.submit_with_timeout(request, decode, self.0.timeout)
    }
    fn submit_with_timeout<T>(
        &self,
        request: Request,
        decode: fn(Reply) -> Result<T>,
        timeout: Duration,
    ) -> Result<QueuedOperation<T>> {
        if self.0.shutdown.is_cancelled() {
            return Err(closed_error().with_context(mcpx_core::ErrorContext::not_sent()));
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .filter(|_| !timeout.is_zero())
            .ok_or_else(|| {
                Error::local(
                    "ConfigurationError",
                    "operation timeout is zero or exceeds monotonic clock range",
                )
            })?;
        let size = match &request {
            Request::SchemaRegistry(registry) => registry.stats().bytes,
            Request::Bind(name) | Request::CommandSpec(name) | Request::Complete(name, _) => {
                name.len()
            }
            Request::Call(_, args, _) | Request::Resume(_, args) => args.to_string().len(),
            Request::Execute(tool, argv) => tool.len().saturating_add(
                serde_json::to_string(argv)
                    .expect("serializable argv")
                    .len(),
            ),
            _ => 0,
        }
        .saturating_add(128);
        let cleanup = matches!(
            request,
            Request::NotificationStop
                | Request::NotificationStatus
                | Request::NotificationResume(_)
        );
        let resume_slot = matches!(request, Request::NotificationResume(_))
            .then(|| {
                self.0.resume_slot.clone().try_acquire_owned().map_err(|_| {
                    Error::local("QueueFull", "a notification resume is already pending")
                })
            })
            .transpose()?;
        let bytes = if cleanup {
            &self.0.control_bytes
        } else {
            &self.0.bytes
        }
        .clone()
        .try_acquire_many_owned(
            u32::try_from(size)
                .map_err(|_| Error::local("LimitExceeded", "request exceeds queue byte budget"))?,
        )
        .map_err(|_| Error::local("QueueFull", "remote queue byte budget is full"))?;
        let slot = if cleanup {
            &self.0.control_slots
        } else {
            &self.0.slots
        }
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::local("QueueFull", "remote operation queue is full"))?;
        // Linearize acceptance, lifecycle publication and FIFO delivery together.
        // No await or user callback runs while holding this short admission lock.
        let _submission = self.0.submission.lock().unwrap();
        let operation_event = self
            .0
            .operation_events
            .as_ref()
            .map(|events| events.admit())
            .transpose()?;
        let id = match &operation_event {
            Some(ticket) => ticket.id(),
            None => allocate_queue_id(&self.0.next_id)?,
        };
        let (reply, receiver) = oneshot::channel();
        let cancellation = CancellationToken::new();
        if cleanup {
            &self.0.controls
        } else {
            &self.0.sender
        }
        .send(Job {
            operation_event,
            id,
            request,
            reply,
            cancellation: cancellation.clone(),
            deadline,
            _slot: slot,
            _bytes: Arc::new(bytes),
            _resume_slot: resume_slot,
        })
        .map_err(|_| {
            closed_error()
                .with_context(mcpx_core::ErrorContext::not_sent())
                .with_queue_operation(id)
        })?;
        Ok(QueuedOperation {
            id,
            receiver,
            cancellation,
            wake: self.0.wake.clone(),
            decode,
            finished: false,
            _output: PhantomData,
        })
    }
    pub async fn tool(&self, name: &str) -> Result<QueuedBinding> {
        if name.is_empty() || name.len() > 1024 {
            return Err(Error::local("InvalidToolName", "invalid tool name"));
        }
        let reply = self.submit(Request::Bind(name.into()), Ok)?.await?;
        let Reply::Bound { id, schema, lease } = reply else {
            return Err(Error::new("ProtocolError", "unexpected queue reply"));
        };
        Ok(QueuedBinding {
            remote: self.clone(),
            id,
            schema,
            lease,
        })
    }
    pub fn execute_argv(&self, tool: &str, argv: &[String]) -> Result<QueuedOperation<Execution>> {
        if argv.len() > 4096
            || tool.len().saturating_add(
                serde_json::to_string(argv)
                    .expect("serializable argv")
                    .len(),
            ) > 1024 * 1024
        {
            return Err(Error::local("LimitExceeded", "command exceeds 1 MiB"));
        }
        self.submit(Request::Execute(tool.into(), argv.to_vec()), execution)
    }
    pub fn execute(&self, tool: &str, command: &str) -> Result<QueuedOperation<Execution>> {
        if command.len() > 1024 * 1024 {
            return Err(Error::local("LimitExceeded", "command exceeds 1 MiB"));
        }
        self.execute_argv(tool, &mcpx_command::tokenize(command)?)
    }
    pub fn reconnect(&self) -> Result<QueuedOperation<()>> {
        self.submit(Request::Reconnect, unit)
    }
    pub fn catalog_revision(&self) -> mcpx_core::CatalogRevision {
        self.0
            .catalog_source
            .get()
            .expect("connected catalog source")
            .snapshot()
    }
    pub fn subscribe_catalog(&self) -> Result<mcpx_core::CatalogWatcher> {
        self.0
            .catalog_source
            .get()
            .expect("connected catalog source")
            .subscribe()
    }
    pub fn refresh(&self) -> Result<QueuedOperation<()>> {
        self.refresh_with_timeout(self.0.timeout)
    }
    /// Same-publication refreshes may coalesce; this operation keeps its own deadline.
    pub fn refresh_with_timeout(&self, timeout: Duration) -> Result<QueuedOperation<()>> {
        self.submit_with_timeout(Request::Refresh(self.catalog_revision()), unit, timeout)
    }
    pub fn set_schema_registry(
        &self,
        registry: mcpx_core::SchemaRegistry,
    ) -> Result<QueuedOperation<()>> {
        self.submit(Request::SchemaRegistry(registry), unit)
    }
    pub fn schema_registry_stats(&self) -> Result<QueuedOperation<mcpx_core::SchemaRegistryStats>> {
        self.submit(Request::SchemaStats, |reply| match reply {
            Reply::SchemaStats(stats) => Ok(stats),
            _ => Err(Error::new("ProtocolError", "unexpected queue reply")),
        })
    }
    pub fn validator_cache_stats(&self) -> Result<QueuedOperation<mcpx_core::ValidatorCacheStats>> {
        self.submit(Request::CacheStats, |reply| match reply {
            Reply::CacheStats(stats) => Ok(stats),
            _ => Err(Error::new("ProtocolError", "unexpected queue reply")),
        })
    }
    pub fn set_validator_cache_capacity(&self, capacity: usize) -> Result<QueuedOperation<()>> {
        if capacity > mcpx_core::MAX_VALIDATOR_CACHE_CAPACITY {
            return Err(Error::local(
                "ConfigurationError",
                "validator cache capacity must be 0..4096",
            ));
        }
        self.submit(Request::CacheCapacity(capacity), unit)
    }
    pub fn clear_validator_cache(&self) -> Result<QueuedOperation<()>> {
        self.submit(Request::CacheClear, unit)
    }
    pub fn complete_line(
        &self,
        line: &str,
        cursor: usize,
    ) -> Result<QueuedOperation<mcpx_command::Completion>> {
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
        self.submit(
            Request::Complete(line.into(), cursor),
            |reply| match reply {
                Reply::Completion(value) => Ok(value),
                _ => Err(Error::new("ProtocolError", "unexpected queue reply")),
            },
        )
    }
    pub fn command_spec(&self, tool: &str) -> Result<QueuedOperation<mcpx_command::CommandSpec>> {
        self.submit(Request::CommandSpec(tool.into()), |reply| match reply {
            Reply::CommandSpec(value) => Ok(value),
            _ => Err(Error::new("ProtocolError", "unexpected queue reply")),
        })
    }
    pub fn drain_events(&self) -> Result<QueuedOperation<mcpx_core::EventBatch>> {
        self.submit(Request::Events, |reply| match reply {
            Reply::Events(events) => Ok(events),
            _ => Err(Error::new("ProtocolError", "unexpected queue reply")),
        })
    }
    pub fn set_elicitation(&self, form: bool, url: bool) -> Result<QueuedOperation<()>> {
        self.submit(Request::Elicitation(form, url), unit)
    }
    pub fn notification_status(
        &self,
    ) -> Result<QueuedOperation<Option<mcpx_core::NotificationStatus>>> {
        self.notification_status_with_timeout(self.0.timeout)
    }
    pub fn notification_status_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueuedOperation<Option<mcpx_core::NotificationStatus>>> {
        self.submit_with_timeout(Request::NotificationStatus, notification_status, timeout)
    }
    /// Resolve current credentials and start (or explicitly restart) the catalog
    /// listener as a FIFO barrier. Does not wait for the remote ACK.
    pub fn start_notifications(
        &self,
    ) -> Result<QueuedOperation<Option<mcpx_core::NotificationStatus>>> {
        self.start_notifications_with_timeout(self.0.timeout)
    }
    pub fn start_notifications_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueuedOperation<Option<mcpx_core::NotificationStatus>>> {
        self.submit_with_timeout(Request::NotificationStart, notification_status, timeout)
    }
    /// Stop listening using reserved control capacity, without resolving credentials.
    /// Supersedes older starts that have not yet completed.
    pub fn stop_notifications(
        &self,
    ) -> Result<QueuedOperation<Option<mcpx_core::NotificationStatus>>> {
        self.stop_notifications_with_timeout(self.0.timeout)
    }
    pub fn stop_notifications_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueuedOperation<Option<mcpx_core::NotificationStatus>>> {
        self.submit_with_timeout(Request::NotificationStop, notification_status, timeout)
    }
    /// Resume this exact interrupted legacy listener with its frozen credentials.
    /// Uses reserved control capacity and waits for retry/reply deadlines without
    /// occupying a business slot or resolving a credential provider.
    pub fn resume_notifications(
        &self,
        resource_id: u64,
    ) -> Result<QueuedOperation<mcpx_core::NotificationStatus>> {
        self.resume_notifications_with_timeout(resource_id, self.0.timeout)
    }
    pub fn resume_notifications_with_timeout(
        &self,
        resource_id: u64,
        timeout: Duration,
    ) -> Result<QueuedOperation<mcpx_core::NotificationStatus>> {
        self.submit_with_timeout(
            Request::NotificationResume(resource_id),
            |reply| match reply {
                Reply::NotificationStatus(Some(status)) => Ok(status),
                _ => Err(Error::new(
                    "ProtocolError",
                    "unexpected notification resume reply",
                )),
            },
            timeout,
        )
    }
    pub fn resume(
        &self,
        handle: &Continuation,
        responses: Value,
    ) -> Result<QueuedOperation<CallOutcome>> {
        mcpx_core::parse_json(&responses.to_string())?;
        if responses.to_string().len() > 1024 * 1024 {
            return Err(Error::local(
                "LimitExceeded",
                "input responses exceed 1 MiB",
            ));
        }
        self.submit(Request::Resume(handle.clone(), responses), outcome)
    }
    pub fn release_continuation(&self, handle: &Continuation) -> Result<QueuedOperation<()>> {
        self.submit(Request::ReleaseContinuation(handle.clone()), unit)
    }
    pub async fn close(&self) -> Result<()> {
        self.0.shutdown.cancel();
        let mut closed = self.0.closed.clone();
        closed
            .wait_for(Option::is_some)
            .await
            .map_err(|_| closed_error())?
            .clone()
            .unwrap()
    }
}

struct SavedBinding {
    binding: ToolBinding,
    lease: Weak<()>,
}
async fn dispatch(
    remote: &mut Remote,
    bindings: &mut BTreeMap<u64, SavedBinding>,
    id: u64,
    request: Request,
) -> Result<Reply> {
    bindings.retain(|_, saved| saved.lease.strong_count() > 0);
    match request {
        Request::Bind(name) => {
            if bindings.len() >= 128 {
                return Err(Error::local(
                    "LimitExceeded",
                    "too many live queued binding handles",
                ));
            }
            let binding = remote.fresh_tool(&name).await?;
            let schema = binding.compiled_schema();
            let lease = Arc::new(());
            bindings.insert(
                id,
                SavedBinding {
                    binding,
                    lease: Arc::downgrade(&lease),
                },
            );
            Ok(Reply::Bound { id, schema, lease })
        }
        Request::Call(id, value, _lease) => {
            let binding = bindings
                .get(&id)
                .ok_or_else(|| Error::local("InvalidHandle", "queued binding is unavailable"))?
                .binding
                .clone();
            Ok(Reply::Outcome(remote.call_json(&binding, value).await?))
        }
        Request::Resume(handle, responses) => {
            Ok(Reply::Outcome(remote.resume(&handle, responses).await?))
        }
        Request::Execute(tool, argv) => {
            Ok(Reply::Execution(remote.execute_argv(&tool, &argv).await?))
        }
        Request::Complete(line, cursor) => Ok(Reply::Completion(
            remote.complete_line(&line, cursor).await?,
        )),
        Request::CommandSpec(name) => Ok(Reply::CommandSpec(remote.command_spec(&name).await?)),
        Request::SchemaRegistry(registry) => {
            remote.set_schema_registry(registry).await?;
            Ok(Reply::Unit)
        }
        Request::SchemaStats => Ok(Reply::SchemaStats(remote.schema_registry_stats())),
        Request::CacheStats => Ok(Reply::CacheStats(remote.validator_cache_stats())),
        Request::CacheCapacity(capacity) => {
            remote.set_validator_cache_capacity(capacity)?;
            Ok(Reply::Unit)
        }
        Request::CacheClear => {
            remote.clear_validator_cache();
            Ok(Reply::Unit)
        }
        Request::Reconnect => {
            remote.reconnect().await?;
            Ok(Reply::Unit)
        }
        Request::Refresh(observed) => {
            remote.refresh_if_unchanged(&observed).await?;
            Ok(Reply::Unit)
        }
        Request::Events => Ok(Reply::Events(remote.drain_events())),
        Request::NotificationStart
        | Request::NotificationStop
        | Request::NotificationStatus
        | Request::NotificationResume(_) => {
            unreachable!("notification controls are dispatched by the owner")
        }
        Request::Elicitation(form, url) => {
            remote.set_elicitation(form, url)?;
            Ok(Reply::Unit)
        }
        Request::ReleaseContinuation(handle) => {
            remote.release_continuation(&handle)?;
            Ok(Reply::Unit)
        }
    }
}

fn prune(pending: &mut VecDeque<Job>) {
    let mut keep = VecDeque::new();
    while let Some(job) = pending.pop_front() {
        let error = if job.cancellation.is_cancelled() || job.reply.is_closed() {
            Some(Error::local(
                "Cancelled",
                "queued operation cancelled before sending",
            ))
        } else if job.deadline <= Instant::now() {
            Some(Error::local(
                "Timeout",
                "operation expired while queued; no request sent",
            ))
        } else {
            None
        };
        if let Some(error) = error {
            parallel::finish(job, Err(error));
        } else {
            keep.push_back(job);
        }
    }
    *pending = keep;
}

#[cfg(test)]
mod id_tests {
    use super::*;

    #[test]
    fn simultaneous_final_queue_ids_are_unique_and_exhaustion_never_reuses_them() {
        let next = AtomicU64::new(mcpx_core::MAX_HOST_ID - 15);
        let mut ids = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..64)
                .map(|_| scope.spawn(|| allocate_queue_id(&next)))
                .collect();
            threads
                .into_iter()
                .filter_map(|thread| match thread.join().unwrap() {
                    Ok(id) => Some(id),
                    Err(error) => {
                        assert_eq!(error.code, "LimitExceeded");
                        None
                    }
                })
                .collect::<Vec<_>>()
        });
        ids.sort_unstable();
        assert_eq!(
            ids,
            ((mcpx_core::MAX_HOST_ID - 15)..=mcpx_core::MAX_HOST_ID).collect::<Vec<_>>()
        );
        assert_eq!(next.load(Ordering::Relaxed), mcpx_core::MAX_HOST_ID + 1);
        assert_eq!(allocate_queue_id(&next).unwrap_err().code, "LimitExceeded");
        assert_eq!(
            allocate_queue_id(&AtomicU64::new(u64::MAX))
                .unwrap_err()
                .code,
            "LimitExceeded"
        );
    }
}
