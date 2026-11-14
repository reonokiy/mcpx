use super::*;
use mcpx_core::{SchemaRegistry, SchemaRegistryStats, ValidatorCacheStats};

pub(super) enum Control {
    Registry(SchemaRegistry),
    SchemaStats,
    CacheStats,
    CacheCapacity(usize),
    CacheClear,
    Ttl(u64),
    Elicitation(bool, bool),
    Release(Continuation),
}
impl WasiQueue {
    /// Queue a local configuration barrier. No credential lookup or metadata
    /// refresh is needed; earlier calls finish before the replacement takes effect.
    pub fn set_schema_registry(&mut self, registry: SchemaRegistry) -> Result<WasiOperation<()>> {
        let bytes = registry.stats().bytes;
        self.check_bytes(bytes)?;
        self.enqueue(Request::Control(Control::Registry(registry)), bytes, unit)
    }
    pub fn schema_registry_stats(&mut self) -> Result<WasiOperation<SchemaRegistryStats>> {
        self.enqueue(Request::Control(Control::SchemaStats), 0, schema_stats)
    }
    pub fn validator_cache_stats(&mut self) -> Result<WasiOperation<ValidatorCacheStats>> {
        self.enqueue(Request::Control(Control::CacheStats), 0, cache_stats)
    }
    pub fn set_validator_cache_capacity(&mut self, capacity: usize) -> Result<WasiOperation<()>> {
        if capacity > mcpx_core::MAX_VALIDATOR_CACHE_CAPACITY {
            return Err(Error::local(
                "ConfigurationError",
                "validator cache capacity must be 0..4096",
            ));
        }
        self.enqueue(Request::Control(Control::CacheCapacity(capacity)), 0, unit)
    }
    pub fn clear_validator_cache(&mut self) -> Result<WasiOperation<()>> {
        self.enqueue(Request::Control(Control::CacheClear), 0, unit)
    }
    /// Queue a TTL change; subsequent work observes the new policy.
    pub fn set_catalog_ttl(&mut self, ttl_ms: u64) -> Result<WasiOperation<()>> {
        self.enqueue(Request::Control(Control::Ttl(ttl_ms)), 0, unit)
    }
    pub fn set_elicitation(&mut self, form: bool, url: bool) -> Result<WasiOperation<()>> {
        self.enqueue(Request::Control(Control::Elicitation(form, url)), 0, unit)
    }
    pub fn release_continuation(&mut self, handle: &Continuation) -> Result<WasiOperation<()>> {
        self.open()?;
        self.client.machine().check_continuation_owner(handle)?;
        self.enqueue(Request::Control(Control::Release(handle.clone())), 0, unit)
    }
    pub(super) fn apply_control(&mut self, control: &Control) -> Result<Reply> {
        let runtime = self.client.host_runtime_mut();
        match control {
            Control::Registry(registry) => runtime.set_schema_registry(registry.clone())?,
            Control::SchemaStats => return Ok(Reply::SchemaStats(runtime.schema_registry_stats())),
            Control::CacheStats => return Ok(Reply::CacheStats(runtime.validator_cache_stats())),
            Control::CacheCapacity(capacity) => runtime.set_validator_cache_capacity(*capacity)?,
            Control::CacheClear => runtime.clear_validator_cache(),
            Control::Ttl(ttl_ms) => runtime.set_catalog_ttl(*ttl_ms),
            Control::Elicitation(form, url) => runtime.set_elicitation(*form, *url)?,
            Control::Release(handle) => {
                runtime.machine().check_continuation_owner(handle)?;
                runtime.release_continuation(handle.id());
            }
        }
        Ok(Reply::Unit)
    }
}
