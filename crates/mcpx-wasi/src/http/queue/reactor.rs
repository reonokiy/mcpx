use super::*;
use mcpx_core::{CredentialProvider, SchemaRegistry, SchemaRegistryStats, ValidatorCacheStats};
use std::{future::Future, rc::Weak};

/// A local reactor owner. Its weak submission handles can be used by futures
/// passed to `run`; no queue borrow is held while those futures are polled.
/// Dropping this owner settles unfinished operations and performs no new I/O.
pub struct WasiReactor {
    queue: Rc<RefCell<WasiQueue>>,
}
/// A clonable, non-Send submission handle. It never owns the reactor or its
/// credentials. Drive its operation futures with the associated reactor.
/// Calls made reentrantly from an owner callback return `Busy`.
#[derive(Clone)]
pub struct WasiRemote {
    queue: Weak<RefCell<WasiQueue>>,
}
fn busy() -> Error {
    Error::local("Busy", "WASI reactor is executing an owner callback")
}
impl WasiQueue {
    /// Transfer this owner to a reactor that supports submission during an
    /// asynchronous workflow. Existing operation handles keep their identity.
    pub fn into_reactor(self) -> WasiReactor {
        WasiReactor::new(self)
    }
}
impl WasiReactor {
    pub fn new(queue: WasiQueue) -> Self {
        Self {
            queue: Rc::new(RefCell::new(queue)),
        }
    }
    pub fn remote(&self) -> WasiRemote {
        WasiRemote {
            queue: Rc::downgrade(&self.queue),
        }
    }
    /// Drive a borrowing, potentially non-Send root future and owner I/O.
    /// The future may submit more work through any of this owner's handles.
    /// Returning does not close the owner or settle unrelated operations.
    pub fn run<T>(&mut self, future: impl Future<Output = Result<T>>) -> Result<T> {
        self.queue.try_borrow().map_err(|_| busy())?.open()?;
        credentials::run_local(future, |wait| {
            let mut queue = self.queue.try_borrow_mut().map_err(|_| busy())?;
            queue.poll(wait)?;
            Ok(queue.stats())
        })
    }
    /// Bounded cooperative service for embedding in an external executor.
    /// A zero duration never waits for HTTP readiness.
    pub fn poll(&mut self, wait: Duration) -> Result<()> {
        self.queue.try_borrow_mut().map_err(|_| busy())?.poll(wait)
    }
    /// Close the queue, including bounded cancellation and session cleanup.
    /// A failed session cleanup can be retried by explicitly calling close again.
    pub fn close(&mut self) -> Result<()> {
        self.queue.try_borrow_mut().map_err(|_| busy())?.close()
    }
}
macro_rules! forward {
    ($(pub fn $name:ident(&self $(, $arg:ident: $ty:ty)*) -> $ret:ty;)*) => {
        $(pub fn $name(&self $(, $arg: $ty)*) -> $ret {
            self.access(|queue| queue.$name($($arg),*))
        })*
    };
}
impl WasiRemote {
    fn access<T>(&self, f: impl FnOnce(&mut WasiQueue) -> Result<T>) -> Result<T> {
        let queue = self
            .queue
            .upgrade()
            .ok_or_else(|| Error::local("TransportClosed", "WASI reactor owner was dropped"))?;
        let mut queue = queue.try_borrow_mut().map_err(|_| busy())?;
        f(&mut queue)
    }
    forward! {
        pub fn bind(&self, name: &str) -> Result<WasiOperation<ToolBinding>>;
        pub fn refresh(&self) -> Result<WasiOperation<()>>;
        pub fn reconnect(&self) -> Result<WasiOperation<()>>;
        pub fn submit_args(&self, binding: &ToolBinding, args: &BoundArgs) -> Result<WasiOperation<CallOutcome>>;
        pub fn submit_json(&self, binding: &ToolBinding, value: Value) -> Result<WasiOperation<CallOutcome>>;
        pub fn submit_raw(&self, binding: &ToolBinding, json: &str) -> Result<WasiOperation<CallOutcome>>;
        pub fn execute(&self, name: &str, command: &str) -> Result<WasiOperation<Execution>>;
        pub fn execute_argv(&self, name: &str, argv: &[String]) -> Result<WasiOperation<Execution>>;
        pub fn resume(&self, handle: &Continuation, responses: Value) -> Result<WasiOperation<CallOutcome>>;
        pub fn complete_line(&self, line: &str, cursor: usize) -> Result<WasiOperation<Completion>>;
        pub fn command_spec(&self, name: &str) -> Result<WasiOperation<CommandSpec>>;
        pub fn set_schema_registry(&self, registry: SchemaRegistry) -> Result<WasiOperation<()>>;
        pub fn schema_registry_stats(&self) -> Result<WasiOperation<SchemaRegistryStats>>;
        pub fn validator_cache_stats(&self) -> Result<WasiOperation<ValidatorCacheStats>>;
        pub fn set_validator_cache_capacity(&self, capacity: usize) -> Result<WasiOperation<()>>;
        pub fn clear_validator_cache(&self) -> Result<WasiOperation<()>>;
        pub fn set_catalog_ttl(&self, ttl_ms: u64) -> Result<WasiOperation<()>>;
        pub fn set_elicitation(&self, form: bool, url: bool) -> Result<WasiOperation<()>>;
        pub fn release_continuation(&self, handle: &Continuation) -> Result<WasiOperation<()>>;
        pub fn subscribe_catalog(&self) -> Result<mcpx_core::CatalogWatcher>;
        pub fn start_notifications(&self) -> Result<mcpx_core::NotificationStatus>;
        pub fn stop_notifications(&self) -> Result<()>;
        pub fn resume_notifications(&self, resource_id:u64) -> Result<WasiOperation<mcpx_core::NotificationStatus>>;
    }
    pub fn submit_typed(
        &self,
        binding: &ToolBinding,
        value: impl Serialize,
    ) -> Result<WasiOperation<CallOutcome>> {
        self.access(|queue| queue.submit_typed(binding, value))
    }
    pub fn set_timeout<T>(&self, operation: &WasiOperation<T>, timeout: Duration) -> Result<()> {
        self.access(|queue| queue.set_timeout(operation, timeout))
    }
    /// The same idle-only replacement rule as `WasiQueue` applies.
    pub fn set_credential_provider(
        &self,
        provider: impl CredentialProvider + 'static,
        reference: Option<String>,
    ) -> Result<()> {
        self.access(|queue| queue.set_credential_provider(provider, reference))
    }
    pub fn stats(&self) -> Result<WasiQueueStats> {
        self.access(|queue| Ok(queue.stats()))
    }
    pub fn is_suspended(&self) -> Result<bool> {
        self.access(|queue| Ok(queue.is_suspended()))
    }
    pub fn notification_status(&self) -> Result<Option<mcpx_core::NotificationStatus>> {
        self.access(|queue| Ok(queue.notification_status()))
    }
    pub fn drain_events(&self) -> Result<EventBatch> {
        self.access(|queue| Ok(queue.drain_events()))
    }
    pub fn drain_operation_events(&self) -> Result<OperationEventBatch> {
        self.access(|queue| Ok(queue.drain_operation_events()))
    }
}
