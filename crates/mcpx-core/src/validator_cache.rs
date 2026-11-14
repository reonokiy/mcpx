use crate::{Error, MAX_HOST_ID, Result, SchemaRegistry};
use jsonschema::Validator;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, Write},
    sync::Arc,
};

/// Bump whenever compilation, dialect, reference or annotation policy changes.
pub const SCHEMA_POLICY_VERSION: u32 = 2;
pub const DEFAULT_VALIDATOR_CACHE_CAPACITY: usize = 128;
pub const MAX_VALIDATOR_CACHE_CAPACITY: usize = 4096;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ValidatorCacheStats {
    pub capacity: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub compilations: u64,
    pub evictions: u64,
    pub counters_saturated: bool,
}

struct HashWriter {
    digest: Sha256,
    bytes: usize,
}
impl Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        if self.bytes > crate::MAX_SCHEMA_BYTES {
            return Err(io::Error::other("schema byte budget"));
        }
        self.digest.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn key(schema: &Value, registry: &SchemaRegistry) -> Result<[u8; 32]> {
    crate::check_json(schema, 0)?;
    if !schema.is_object() {
        return Err(Error::local(
            "SchemaUnsupported",
            "tool schema must be an object",
        ));
    }
    let mut digest = Sha256::new();
    digest.update(b"mcpx:validator:draft2020-12:");
    digest.update(SCHEMA_POLICY_VERSION.to_le_bytes());
    digest.update(registry.fingerprint());
    digest.update(value_fingerprint(schema)?);
    Ok(digest.finalize().into())
}
pub(crate) fn value_fingerprint(schema: &Value) -> Result<[u8; 32]> {
    let mut writer = HashWriter {
        digest: Sha256::new(),
        bytes: 0,
    };
    serde_json::to_writer(&mut writer, schema)
        .map_err(|_| Error::local("LimitExceeded", "schema exceeds 1 MiB"))?;
    Ok(writer.digest.finalize().into())
}

pub(crate) struct ValidatorCache {
    values: BTreeMap<[u8; 32], Arc<Validator>>,
    order: VecDeque<[u8; 32]>,
    stats: ValidatorCacheStats,
}
impl Default for ValidatorCache {
    fn default() -> Self {
        Self {
            values: Default::default(),
            order: Default::default(),
            stats: ValidatorCacheStats {
                capacity: DEFAULT_VALIDATOR_CACHE_CAPACITY,
                entries: 0,
                hits: 0,
                misses: 0,
                compilations: 0,
                evictions: 0,
                counters_saturated: false,
            },
        }
    }
}
fn count(counter: &mut u64, saturated: &mut bool) {
    if *counter == MAX_HOST_ID {
        *saturated = true;
    } else {
        *counter += 1;
    }
}
impl ValidatorCache {
    pub(crate) fn stats(&self) -> ValidatorCacheStats {
        ValidatorCacheStats {
            entries: self.values.len(),
            ..self.stats
        }
    }
    pub(crate) fn set_capacity(&mut self, capacity: usize) -> Result<()> {
        if capacity > MAX_VALIDATOR_CACHE_CAPACITY {
            return Err(Error::local(
                "ConfigurationError",
                "validator cache capacity must be 0..4096",
            ));
        }
        self.stats.capacity = capacity;
        while self.values.len() > capacity {
            self.evict();
        }
        Ok(())
    }
    fn evict(&mut self) {
        if let Some(key) = self.order.pop_front() {
            self.values.remove(&key);
            count(
                &mut self.stats.evictions,
                &mut self.stats.counters_saturated,
            );
        }
    }
    pub(crate) fn clear(&mut self) {
        self.values.clear();
        self.order = VecDeque::new();
    }
    pub(crate) fn compile(
        &mut self,
        schema: &Value,
        registry: &SchemaRegistry,
    ) -> Result<Arc<Validator>> {
        let key = key(schema, registry)?;
        if let Some(value) = self.values.get(&key).cloned() {
            count(&mut self.stats.hits, &mut self.stats.counters_saturated);
            self.order.retain(|entry| *entry != key);
            self.order.push_back(key);
            return Ok(value);
        }
        count(&mut self.stats.misses, &mut self.stats.counters_saturated);
        let value = Arc::new(registry.compile(schema)?);
        count(
            &mut self.stats.compilations,
            &mut self.stats.counters_saturated,
        );
        if self.stats.capacity > 0 {
            while self.values.len() >= self.stats.capacity {
                self.evict();
            }
            self.values.insert(key, value.clone());
            self.order.push_back(key);
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_counters_remain_exact_at_the_host_ceiling() {
        let mut value = MAX_HOST_ID - 1;
        let mut saturated = false;
        count(&mut value, &mut saturated);
        assert_eq!(value, MAX_HOST_ID);
        assert!(!saturated);
        count(&mut value, &mut saturated);
        assert_eq!(value, MAX_HOST_ID);
        assert!(saturated);
    }
    #[test]
    fn resource_contents_partition_the_key_even_without_a_context_clear() {
        use serde_json::json;
        let make = |value| {
            SchemaRegistry::new([(
                "https://schemas.example/a".into(),
                json!({"type":"object","properties":{"q":{"const":value}}}),
            )])
            .unwrap()
        };
        let first = make(1);
        let second = make(2);
        let schema = json!({"$ref":"https://schemas.example/a"});
        let mut cache = ValidatorCache::default();
        let a = cache.compile(&schema, &first).unwrap();
        let b = cache.compile(&schema, &second).unwrap();
        assert!(a.is_valid(&json!({"q":1})) && !a.is_valid(&json!({"q":2})));
        assert!(b.is_valid(&json!({"q":2})) && !b.is_valid(&json!({"q":1})));
        assert!(Arc::ptr_eq(&a, &cache.compile(&schema, &first).unwrap()));
        assert_eq!(
            (
                cache.stats().entries,
                cache.stats().compilations,
                cache.stats().hits
            ),
            (2, 2, 1)
        );
    }
}
