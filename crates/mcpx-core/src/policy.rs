use crate::{Error, Machine, Result};
use serde::Serialize;

/// Non-secret preflight state. The host owns credentials and asynchronous I/O.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct CatalogStatus {
    pub authorization_changed: bool,
    pub needs_refresh: bool,
    pub requires_exclusive: bool,
}

pub(crate) struct CatalogPolicy {
    scope: Option<String>,
    generation: u64,
    fresh_until: Option<u64>,
    ttl_ms: u64,
    now_ms: u64,
}
impl Default for CatalogPolicy {
    fn default() -> Self {
        Self {
            scope: None,
            generation: 0,
            fresh_until: None,
            ttl_ms: 60_000,
            now_ms: 0,
        }
    }
}
impl CatalogPolicy {
    fn deadline(&self, machine: &Machine) -> Option<u64> {
        if machine.is_catalog_stale() {
            return None;
        }
        if machine.catalog().generation() != self.generation {
            let ttl = machine
                .catalog_ttl_ms()
                .map_or(self.ttl_ms, |ttl| ttl.min(self.ttl_ms));
            Some(self.now_ms.saturating_add(ttl))
        } else {
            self.fresh_until
        }
    }
    pub(crate) fn observe(&mut self, machine: &Machine) {
        self.fresh_until = self.deadline(machine);
        self.generation = machine.catalog().generation();
    }
    pub(crate) fn advance(&mut self, machine: &Machine, now: u64) {
        self.observe(machine);
        self.now_ms = now;
    }
    pub(crate) fn set_ttl(&mut self, machine: &Machine, ttl: u64) {
        self.observe(machine);
        self.ttl_ms = ttl;
        self.fresh_until = None;
    }
    pub(crate) fn invalidate(&mut self, machine: &Machine) {
        self.generation = machine.catalog().generation();
        self.fresh_until = None;
    }
    pub(crate) fn expired(&self, machine: &Machine, now: u64) -> bool {
        self.deadline(machine)
            .is_none_or(|deadline| now >= deadline)
    }
    pub(crate) fn status(&self, machine: &Machine, scope: &str, now: u64) -> Result<CatalogStatus> {
        if scope.is_empty() || scope.len() > 256 {
            return Err(Error::local(
                "CredentialUnavailable",
                "authorization context must contain 1 to 256 bytes",
            ));
        }
        // A catalog discovered outside an authorization scope cannot be adopted
        // as another principal's catalog just by assigning its first scope ID.
        let changed = self
            .scope
            .as_ref()
            .map_or(machine.catalog().generation() > 0, |old| old != scope);
        let refresh = changed || self.expired(machine, now);
        Ok(CatalogStatus {
            authorization_changed: changed,
            needs_refresh: refresh,
            requires_exclusive: machine.is_busy()
                && (refresh || machine.active_action_id().is_some()),
        })
    }
    pub(crate) fn accept(&mut self, scope: &str) {
        self.scope = Some(scope.into());
    }
}
