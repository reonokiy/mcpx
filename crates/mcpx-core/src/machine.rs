use crate::continuation::{self, CallState, Continuation, SavedContinuation};
use crate::ids::IdKind;
use crate::{BoundArgs, Catalog, Dialect, Error, Result, ToolBinding, ToolDescriptor};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Action {
    Stream {
        /// Host action handle, distinct from the ID in the wire body.
        id: u64,
        rpc_id: Option<u64>,
        rpc_method: String,
        operation_id: u64,
        cancellation: Option<Value>,
        body: Value,
    },
    Http {
        /// Host action handle, distinct from the ID in the wire body.
        id: u64,
        rpc_id: Option<u64>,
        rpc_method: String,
        operation_id: u64,
        cancellation: Option<Value>,
        url: String,
        headers: BTreeMap<String, String>,
        body: Value,
    },
    Cancel {
        id: u64,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub operation_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_operation_id: Option<u64>,
    pub sequence: u64,
    pub kind: String,
    pub payload: Value,
}
impl Event {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.kind.as_str(),
            "catalog_updated"
                | "completed"
                | "tool_failed"
                | "failed"
                | "cancelled"
                | "needs_input"
                | "operation_completed"
                | "operation_needs_input"
                | "operation_failed"
                | "operation_cancelled"
        )
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputValidation {
    NotDeclared,
    Valid,
    Invalid,
    Indeterminate,
    NotApplicable,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_operation_id: Option<u64>,
    pub raw: Value,
    pub is_error: bool,
    pub output_validation: OutputValidation,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CallOutcome {
    Completed(ToolResponse),
    NeedsInput(Continuation),
}

enum Phase {
    Discover,
    Initialize,
    Initialized,
    List {
        tools: Vec<ToolDescriptor>,
        cursors: BTreeSet<String>,
        pages: usize,
        ttl_ms: Option<u64>,
        freshness: crate::CatalogRevision,
    },
    Call(CallState),
}

struct Pending {
    dispatched: bool,
    id: u64,
    rpc_id: Option<u64>,
    operation_id: u64,
    phase: Phase,
    resume_headers: Option<BTreeMap<String, String>>,
    http_session: Option<String>,
}

/// One logical remote. Drivers own I/O; the machine owns protocol transitions.
pub struct Machine {
    endpoint: String,
    dialect: Dialect,
    catalog: Catalog,
    session: Option<String>,
    session_expired: Option<u64>,
    next_action: u64,
    next_rpc: u64,
    pending: Option<Pending>,
    calls: BTreeMap<u64, Pending>,
    completions: BTreeMap<u64, Result<CallOutcome>>,
    queue_links: BTreeMap<u64, u64>,
    host_queue_scope: std::rc::Rc<std::cell::Cell<Option<u64>>>,
    actions: VecDeque<Action>,
    events: crate::event_queue::EventQueue,
    outcome: Option<CallOutcome>,
    last_tool_result: Option<u64>,
    catalog_stale: bool,
    auto: bool,
    stdio: bool,
    operation_id: u64,
    operation_terminal: bool,
    ready: bool,
    catalog_ttl_ms: Option<u64>,
    clock_ms: u64,
    continuation_owner: Arc<()>,
    continuations: BTreeMap<u64, SavedContinuation>,
    next_continuation: u64,
    elicitation_form: bool,
    elicitation_url: bool,
}

impl Machine {
    pub(crate) fn notification_rpc(&mut self, stdio: bool) -> Result<Option<u64>> {
        if self.stdio != stdio {
            return Err(Error::local(
                "CapabilityUnavailable",
                "notification transport does not match the remote",
            ));
        }
        if !self.ready || self.catalog.generation() == 0 {
            return Err(Error::local(
                "NotConnected",
                "discover a complete catalog before listening",
            ));
        }
        if self.dialect == Dialect::Legacy {
            Ok(None)
        } else {
            IdKind::Rpc.allocate(&mut self.next_rpc).map(Some)
        }
    }
    pub(crate) fn notification_gap(&mut self) {
        self.catalog_stale = true;
        self.catalog.invalidate_revision();
    }
    pub fn new(endpoint: String, dialect: Dialect) -> Result<Self> {
        if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
            return Err(Error::local(
                "InvalidEndpoint",
                "expected an HTTP or HTTPS endpoint",
            ));
        }
        Ok(Self::build(endpoint, dialect, false))
    }

    pub fn new_stdio(dialect: Dialect) -> Self {
        Self::build(String::new(), dialect, true)
    }

    fn build(endpoint: String, dialect: Dialect, stdio: bool) -> Self {
        Self {
            endpoint,
            dialect: if dialect == Dialect::Auto {
                Dialect::Modern
            } else {
                dialect
            },
            catalog: Catalog::default(),
            session: None,
            session_expired: None,
            next_action: IdKind::Action.first(),
            next_rpc: IdKind::Rpc.first(),
            pending: None,
            calls: BTreeMap::new(),
            completions: BTreeMap::new(),
            queue_links: BTreeMap::new(),
            host_queue_scope: Default::default(),
            actions: VecDeque::new(),
            events: Default::default(),
            outcome: None,
            last_tool_result: None,
            catalog_stale: false,
            auto: dialect == Dialect::Auto,
            stdio,
            operation_id: 0,
            operation_terminal: true,
            ready: false,
            catalog_ttl_ms: None,
            clock_ms: 0,
            continuation_owner: Arc::new(()),
            continuations: BTreeMap::new(),
            next_continuation: IdKind::Continuation.first(),
            elicitation_form: false,
            elicitation_url: false,
        }
    }
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }
    pub fn session_id(&self) -> Option<&str> {
        self.session.as_deref()
    }
    /// Last accepted tool outcome's unique operation ID, including input-required
    /// and domain errors. A receipt marker, not a monotonic completion ordering.
    /// Reading an old result and resetting transport do not change it.
    pub fn last_tool_result(&self) -> Option<u64> {
        self.last_tool_result
    }
    /// A server 404 for this legacy HTTP session has been observed. This latch
    /// survives unrelated positive replies and clears on an explicit reset or
    /// authorization change. It never authorizes replay of a failed operation.
    pub fn http_session_expired(&self) -> bool {
        self.session_expired.is_some()
    }
    /// First observation in the supplied monotonic host clock, in milliseconds.
    pub fn http_session_expired_at(&self) -> Option<u64> {
        self.session_expired
    }
    pub(crate) fn expire_http_session(&mut self, observed: Option<&str>) -> bool {
        if !self.stdio && self.dialect == Dialect::Legacy && observed.is_some() {
            if self.session.as_deref() == observed {
                self.session_expired.get_or_insert(self.clock_ms);
            }
            true
        } else {
            false
        }
    }
    pub(crate) fn session_404(&mut self, id: u64) -> bool {
        let snapshot = self
            .calls
            .get(&id)
            .or_else(|| self.pending.as_ref().filter(|p| p.id == id))
            .and_then(|p| p.http_session.clone());
        self.expire_http_session(snapshot.as_deref())
    }
    pub fn catalog_ttl_ms(&self) -> Option<u64> {
        self.catalog_ttl_ms
    }
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    pub fn now_ms(&self) -> u64 {
        self.clock_ms
    }
    pub fn is_catalog_stale(&self) -> bool {
        self.catalog_stale
    }
    /// Hosts supply monotonic milliseconds; backwards movement is rejected.
    pub fn advance_time(&mut self, now_ms: u64) -> Result<()> {
        if now_ms < self.clock_ms {
            return Err(Error::local("InvalidClock", "host clock moved backwards"));
        }
        self.clock_ms = now_ms;
        Ok(())
    }
    pub fn set_elicitation(&mut self, form: bool, url: bool) -> Result<()> {
        self.idle()?;
        self.elicitation_form = form;
        self.elicitation_url = url;
        Ok(())
    }
    pub fn release_continuation(&mut self, id: u64) {
        self.continuations.remove(&id);
    }
    /// Reset a connection epoch without reusing IDs or discarding delivered results.
    pub fn reset_transport(&mut self) -> Result<()> {
        self.idle()?;
        if self.outcome.is_some() {
            return Err(Error::local(
                "Busy",
                "consume the completed serial outcome before resetting transport",
            ));
        }
        let registry = self.catalog.schema_registry().clone();
        self.catalog.invalidate();
        self.catalog.set_schema_registry(registry);
        self.catalog_stale = true;
        self.catalog_ttl_ms = None;
        self.ready = false;
        self.session = None;
        self.session_expired = None;
        self.continuations.clear();
        self.outcome = None;
        self.actions.clear();
        if self.auto {
            self.dialect = Dialect::Modern;
        }
        Ok(())
    }
    pub fn invalidate_authorization(&mut self) -> Result<()> {
        self.idle()?;
        self.catalog.invalidate();
        self.catalog_stale = true;
        self.catalog_ttl_ms = None;
        self.ready = false;
        self.session = None;
        self.session_expired = None;
        self.continuations.clear();
        Ok(())
    }
    pub fn start_catalog_refresh(&mut self) -> Result<()> {
        if !self.ready {
            return self.start_discovery();
        }
        self.idle()?;
        self.catalog.next_generation()?;
        self.begin_operation()?;
        self.list(vec![], BTreeSet::new(), 0, None, None, None)?;
        Ok(())
    }
    pub fn active_action_id(&self) -> Option<u64> {
        self.pending.as_ref().map(|p| p.id)
    }
    pub fn has_action(&self, id: u64) -> bool {
        self.active_action_id() == Some(id) || self.calls.contains_key(&id)
    }
    /// An initialized notification has an action ID but no wire RPC ID.
    pub fn rpc_id_for_action(&self, id: u64) -> Option<u64> {
        self.calls
            .get(&id)
            .or_else(|| self.pending.as_ref().filter(|pending| pending.id == id))
            .and_then(|pending| pending.rpc_id)
    }
    /// Resolve a wire response into the bounded active action set.
    pub fn action_for_rpc_id(&self, rpc_id: u64) -> Option<u64> {
        self.calls
            .values()
            .chain(self.pending.iter())
            .find(|pending| pending.rpc_id == Some(rpc_id))
            .map(|pending| pending.id)
    }
    pub fn is_past_rpc(&self, id: u64) -> bool {
        IdKind::Rpc.contains(id) && id < self.next_rpc
    }
    pub fn action_operation(&self, id: u64) -> Option<u64> {
        self.calls
            .get(&id)
            .or_else(|| self.pending.as_ref().filter(|pending| pending.id == id))
            .map(|pending| pending.operation_id)
    }
    fn resumable_call(&self, id: u64) -> Result<&Pending> {
        let pending = self
            .calls
            .get(&id)
            .or_else(|| self.pending.as_ref().filter(|p| p.id == id))
            .ok_or_else(|| Error::local("InvalidHandle", "response action is unavailable"))?;
        if self.stdio
            || self.dialect != Dialect::Legacy
            || !pending.dispatched
            || !matches!(pending.phase, Phase::Call(_))
        {
            return Err(Error::local(
                "ResumeUnavailable",
                "only dispatched legacy HTTP tool responses can resume",
            ));
        }
        Ok(pending)
    }
    pub(crate) fn response_resume_route(
        &self,
        id: u64,
    ) -> Result<(u64, u64, BTreeMap<String, String>)> {
        let pending = self.resumable_call(id)?;
        let mut headers = pending.resume_headers.clone().ok_or_else(|| {
            Error::local("ResumeUnavailable", "original HTTP headers are unavailable")
        })?;
        headers.remove("content-type");
        headers.insert("accept".into(), "text/event-stream".into());
        Ok((
            pending.operation_id,
            pending.rpc_id.expect("call has RPC ID"),
            headers,
        ))
    }
    /// Replace only the host stream handle. The business operation, wire RPC,
    /// progress token, frozen call state and reserved terminal remain intact.
    pub(crate) fn resume_response_action(&mut self, id: u64) -> Result<u64> {
        self.resumable_call(id)?;
        let next = IdKind::Action.allocate(&mut self.next_action)?;
        if let Some(mut pending) = self.calls.remove(&id) {
            pending.id = next;
            self.calls.insert(next, pending);
        } else {
            self.pending.as_mut().expect("checked pending").id = next;
        }
        Ok(next)
    }
    pub fn take_completion(&mut self, operation_id: u64) -> Option<Result<CallOutcome>> {
        let result = self.completions.remove(&operation_id);
        if result.is_some() {
            self.queue_links.remove(&operation_id);
        }
        result
    }
    pub fn queue_scope(&self, queue_id: u64) -> Result<crate::QueueScope> {
        crate::ids::available(queue_id)?;
        Ok(crate::QueueScope {
            slot: self.host_queue_scope.clone(),
            previous: self.host_queue_scope.replace(Some(queue_id)),
        })
    }
    pub fn link_queue_operation(&mut self, operation_id: u64, queue_id: u64) -> Result<()> {
        crate::ids::available(queue_id)?;
        if !self
            .calls
            .values()
            .any(|pending| pending.operation_id == operation_id)
            && !self.completions.contains_key(&operation_id)
        {
            return Err(Error::local(
                "InvalidOperation",
                "concurrent operation is unavailable",
            ));
        }
        self.queue_links.insert(operation_id, queue_id);
        Ok(())
    }
    pub fn is_auto_probe(&self) -> bool {
        self.auto
            && matches!(
                self.pending.as_ref().map(|p| &p.phase),
                Some(Phase::Discover)
            )
    }
    pub fn probe_timeout(&mut self) -> Result<()> {
        if !self.stdio || !self.is_auto_probe() {
            return Err(Error::local("Timeout", "discovery probe timed out"));
        }
        self.pending = None;
        self.actions.clear();
        self.dialect = Dialect::Legacy;
        let result = self.initialize();
        if let Err(error) = &result {
            self.fail(error.clone());
        }
        result
    }
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
    pub fn validator_cache_stats(&self) -> crate::ValidatorCacheStats {
        self.catalog.validator_cache_stats()
    }
    pub fn set_validator_cache_capacity(&mut self, capacity: usize) -> Result<()> {
        self.catalog.set_validator_cache_capacity(capacity)
    }
    pub fn clear_validator_cache(&mut self) {
        self.catalog.clear_validator_cache();
    }
    pub fn set_schema_registry(&mut self, registry: crate::SchemaRegistry) -> Result<()> {
        if self.is_busy() {
            return Err(Error::local(
                "Busy",
                "finish active operations before replacing schema resources",
            ));
        }
        self.catalog.set_schema_registry(registry);
        self.continuations.clear();
        Ok(())
    }
    pub fn bind(&mut self, name: &str) -> Result<ToolBinding> {
        self.catalog.bind(name)
    }
    pub fn has_business_operation(&self) -> bool {
        self.pending
            .iter()
            .chain(self.calls.values())
            .any(|pending| matches!(pending.phase, Phase::Call(_)))
    }
    pub fn is_busy(&self) -> bool {
        self.pending.is_some() || !self.calls.is_empty()
    }
    /// Handoff is conservative: the host may send immediately after this call.
    pub fn drain_actions(&mut self) -> Vec<Action> {
        let actions: Vec<_> = self.actions.drain(..).collect();
        for action in &actions {
            let id = match action {
                Action::Http { id, .. } | Action::Stream { id, .. } => *id,
                Action::Cancel { .. } => continue,
            };
            if let Some(pending) = self.calls.get_mut(&id) {
                pending.dispatched = true;
            } else if let Some(pending) = self.pending.as_mut().filter(|p| p.id == id) {
                pending.dispatched = true;
            }
        }
        actions
    }
    fn error_context(&self, pending: &Pending) -> crate::ErrorContext {
        crate::ErrorContext {
            operation_id: Some(pending.operation_id),
            queue_operation_id: self.queue_links.get(&pending.operation_id).copied(),
            execution_certainty: if pending.dispatched && matches!(pending.phase, Phase::Call(_)) {
                crate::ExecutionCertainty::Unknown
            } else {
                crate::ExecutionCertainty::NotSent
            },
        }
    }
    pub fn action_error_context(&self, id: u64) -> Option<crate::ErrorContext> {
        self.calls
            .get(&id)
            .or_else(|| self.pending.as_ref().filter(|p| p.id == id))
            .map(|pending| self.error_context(pending))
    }
    pub fn drain_events(&mut self) -> crate::EventBatch {
        self.events.drain()
    }
    pub fn event_stats(&self) -> crate::EventStats {
        self.events.stats()
    }
    pub fn set_event_limits(&mut self, limits: crate::EventLimits) -> Result<()> {
        self.events.set_limits(limits)
    }
    pub fn take_outcome(&mut self) -> Option<CallOutcome> {
        self.outcome.take()
    }

    fn idle(&self) -> Result<()> {
        if self.is_busy() {
            return Err(Error::local(
                "Busy",
                "this remote already has an active operation",
            ));
        }
        self.events.can_reserve()
    }
    fn event(&mut self, kind: &str, payload: Value) {
        self.event_for(self.operation_id, kind, payload)
            .expect("reserved terminal event");
    }
    fn event_for(&mut self, operation_id: u64, kind: &str, payload: Value) -> Result<()> {
        self.events.push(Event {
            operation_id,
            queue_operation_id: self.queue_links.get(&operation_id).copied(),
            sequence: 0,
            kind: kind.into(),
            payload,
        })
    }

    fn begin_operation(&mut self) -> Result<()> {
        IdKind::Action.available(self.next_action)?;
        IdKind::Rpc.available(self.next_rpc)?;
        let mut operation = self.operation_id;
        IdKind::Operation.increment(&mut operation)?;
        self.events.reserve()?;
        self.queue_links.retain(|id, _| {
            self.calls
                .values()
                .any(|pending| pending.operation_id == *id)
                || self.completions.contains_key(id)
        });
        if let Some(id) = self.host_queue_scope.get() {
            self.queue_links.insert(operation, id);
        }
        self.operation_id = operation;
        self.operation_terminal = false;
        self.outcome = None;
        Ok(())
    }

    fn request(&mut self, method: &str, mut params: Value, phase: Phase) -> Result<()> {
        // Check both domains before changing either allocation counter.
        IdKind::Action.available(self.next_action)?;
        let notification = matches!(phase, Phase::Initialized);
        if !notification {
            IdKind::Rpc.available(self.next_rpc)?;
        }
        let id = IdKind::Action.allocate(&mut self.next_action)?;
        let rpc_id = if notification {
            None
        } else {
            Some(IdKind::Rpc.allocate(&mut self.next_rpc)?)
        };
        if matches!(phase, Phase::Call(_)) {
            params["_meta"] =
                json!({"progressToken":format!("mcpx-{}", rpc_id.expect("calls have a wire ID"))});
        }
        let mut headers = BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            (
                "accept".into(),
                "application/json, text/event-stream".into(),
            ),
            ("mcp-protocol-version".into(), self.dialect.version().into()),
        ]);
        if let Some(session) = &self.session {
            headers.insert("mcp-session-id".into(), session.clone());
        }
        if self.dialect == Dialect::Modern {
            headers.insert("mcp-method".into(), method.into());
            if let Some(name) = params.get("name").and_then(Value::as_str) {
                headers.insert("mcp-name".into(), mcpx_protocol::encode_header(name));
            }
        }
        let mut body = if matches!(phase, Phase::Initialized) {
            json!({"jsonrpc":"2.0", "method":method, "params":params})
        } else {
            self.dialect
                .request(rpc_id.expect("requests have a wire ID"), method, params)
        };
        if self.dialect == Dialect::Modern && (self.elicitation_form || self.elicitation_url) {
            let mut modes = serde_json::Map::new();
            if self.elicitation_form {
                modes.insert("form".into(), json!({}));
            }
            if self.elicitation_url {
                modes.insert("url".into(), json!({}));
            }
            body["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] =
                json!({"elicitation":modes});
        }
        let cancellation = rpc_id.filter(|_| method != "initialize" && (self.stdio || self.dialect == Dialect::Legacy))
            .map(|rpc_id| json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":rpc_id}}));
        let resume_headers =
            (!self.stdio && self.dialect == Dialect::Legacy && matches!(phase, Phase::Call(_)))
                .then(|| headers.clone());
        let http_session = (!self.stdio && self.dialect == Dialect::Legacy)
            .then(|| self.session.clone())
            .flatten();
        let action = if self.stdio {
            Action::Stream {
                id,
                rpc_id,
                rpc_method: method.into(),
                operation_id: self.operation_id,
                cancellation,
                body,
            }
        } else {
            Action::Http {
                id,
                rpc_id,
                rpc_method: method.into(),
                operation_id: self.operation_id,
                cancellation,
                url: self.endpoint.clone(),
                headers,
                body,
            }
        };
        self.actions.push_back(action);
        self.pending = Some(Pending {
            dispatched: false,
            id,
            rpc_id,
            operation_id: self.operation_id,
            phase,
            resume_headers,
            http_session,
        });
        Ok(())
    }

    pub fn start_discovery(&mut self) -> Result<()> {
        self.idle()?;
        self.catalog.next_generation()?;
        self.begin_operation()?;
        if self.dialect == Dialect::Legacy {
            self.initialize()?;
        } else {
            self.request("server/discover", json!({}), Phase::Discover)?;
        }
        Ok(())
    }

    fn initialize(&mut self) -> Result<()> {
        self.session = None;
        self.request(
            "initialize",
            json!({
                "protocolVersion": self.dialect.version(), "capabilities": {},
                "clientInfo": {"name":"mcpx", "version":env!("CARGO_PKG_VERSION")}
            }),
            Phase::Initialize,
        )
    }

    pub fn start_call(&mut self, binding: &ToolBinding, args: &BoundArgs) -> Result<()> {
        binding.check_args(args)?;
        self.start_call_state(
            CallState {
                binding: binding.clone(),
                args: args.clone(),
                rounds: 0,
                deadline: None,
            },
            None,
        )
    }

    fn start_call_state(
        &mut self,
        call: CallState,
        inputs: Option<(Value, Option<Value>)>,
    ) -> Result<()> {
        self.start_call_state_mode(call, inputs, false)
    }
    fn call_capacity(&self, concurrent: bool) -> Result<()> {
        if !concurrent {
            return self.idle();
        }
        if self.pending.is_some() {
            return Err(Error::local("Busy", "a serial/control operation is active"));
        }
        if !self.ready {
            return Err(Error::local(
                "NotInitialized",
                "discover the remote before concurrent calls",
            ));
        }
        if self.calls.len() + self.completions.len() >= 16 {
            return Err(Error::local(
                "ConcurrencyLimit",
                "16 active or unconsumed concurrent calls are retained",
            ));
        }
        self.events.can_reserve()
    }
    pub fn start_concurrent_call(
        &mut self,
        binding: &ToolBinding,
        args: &BoundArgs,
    ) -> Result<u64> {
        binding.check_args(args)?;
        self.start_call_state_mode(
            CallState {
                binding: binding.clone(),
                args: args.clone(),
                rounds: 0,
                deadline: None,
            },
            None,
            true,
        )?;
        Ok(self.operation_id)
    }
    fn start_call_state_mode(
        &mut self,
        call: CallState,
        inputs: Option<(Value, Option<Value>)>,
        concurrent: bool,
    ) -> Result<()> {
        let binding = &call.binding;
        let args = &call.args;
        self.call_capacity(concurrent)?;
        if self.catalog_stale {
            return Err(Error::local(
                "BindingStale",
                "remote reported a catalog change; refresh before calling",
            ));
        }
        self.catalog.check(binding)?;
        binding.check_args(args)?;
        let mirrored = if self.dialect == Dialect::Modern && !self.stdio {
            mcpx_protocol::parameter_headers(&binding.descriptor().input_schema, args.value())
                .map_err(|_| {
                    Error::local(
                        "HeaderParameterError",
                        "parameter cannot be mirrored to the required protocol header",
                    )
                })?
        } else {
            BTreeMap::new()
        };
        self.begin_operation()?;
        let mut params = json!({"name":binding.descriptor().name, "arguments":args.value()});
        if let Some((responses, state)) = inputs {
            params["inputResponses"] = responses;
            if let Some(state) = state {
                params["requestState"] = state;
            }
        }
        self.request("tools/call", params, Phase::Call(call))?;
        if let Some(Action::Http { headers, .. }) = self.actions.back_mut() {
            headers.extend(mirrored);
        }
        if concurrent {
            let pending = self.pending.take().expect("call creates a pending request");
            self.calls.insert(pending.id, pending);
        }
        Ok(())
    }

    pub fn resume(&mut self, handle: &Continuation, responses: Value) -> Result<()> {
        self.check_continuation_owner(handle)?;
        self.resume_id(handle.id, responses)
    }

    pub fn check_continuation_owner(&self, handle: &Continuation) -> Result<()> {
        if !Arc::ptr_eq(&handle.owner, &self.continuation_owner) {
            return Err(Error::local(
                "InvalidContinuation",
                "continuation belongs to another remote",
            ));
        }
        Ok(())
    }

    pub fn check_continuation(&self, handle: &Continuation) -> Result<()> {
        self.check_continuation_owner(handle)?;
        let saved = self.continuations.get(&handle.id).ok_or_else(|| {
            Error::local(
                "InvalidContinuation",
                "continuation was consumed, released, or invalidated",
            )
        })?;
        if self.clock_ms >= saved.handle.expires_at_ms {
            return Err(Error::local("ContinuationExpired", "continuation expired"));
        }
        Ok(())
    }

    pub fn resume_id(&mut self, id: u64, responses: Value) -> Result<()> {
        self.resume_id_mode(id, responses, false).map(|_| ())
    }
    pub fn resume_concurrent(&mut self, handle: &Continuation, responses: Value) -> Result<u64> {
        self.check_continuation_owner(handle)?;
        self.resume_id_mode(handle.id, responses, true)
    }
    pub fn resume_concurrent_id(&mut self, id: u64, responses: Value) -> Result<u64> {
        self.resume_id_mode(id, responses, true)
    }
    fn resume_id_mode(&mut self, id: u64, responses: Value, concurrent: bool) -> Result<u64> {
        self.call_capacity(concurrent)?;
        let saved = self.continuations.get(&id).ok_or_else(|| {
            Error::local(
                "InvalidContinuation",
                "continuation was consumed, released, or invalidated",
            )
        })?;
        if self.clock_ms >= saved.handle.expires_at_ms {
            self.continuations.remove(&id);
            return Err(Error::local("ContinuationExpired", "continuation expired"));
        }
        self.catalog.check(&saved.call.binding)?;
        if self.catalog_stale {
            return Err(Error::local(
                "BindingStale",
                "refresh the catalog before resuming",
            ));
        }
        let responses = Value::Object(saved.responses(responses)?);
        let state = saved.handle.raw.get("requestState").cloned();
        // Allocation failure is a local admission error, not a consumed retry.
        IdKind::Action.available(self.next_action)?;
        IdKind::Rpc.available(self.next_rpc)?;
        let mut operation = self.operation_id;
        IdKind::Operation.increment(&mut operation)?;
        let saved = self.continuations.remove(&id).unwrap();
        // Consume before dispatch: a failed/unknown execution must never be retried implicitly.
        self.start_call_state_mode(saved.call, Some((responses, state)), concurrent)?;
        Ok(self.operation_id)
    }

    pub fn notify(&mut self, message: &Value) -> Result<()> {
        let request_id = message
            .pointer("/params/progressToken")
            .and_then(Value::as_str)
            .and_then(|token| token.strip_prefix("mcpx-"))
            .and_then(|id| id.parse::<u64>().ok());
        let operation_id = request_id
            .and_then(|id| self.action_for_rpc_id(id))
            .and_then(|id| self.action_operation(id));
        if message.get("method").and_then(Value::as_str) == Some("notifications/progress") {
            if message.pointer("/params/progressToken").is_some() && operation_id.is_none() {
                return Ok(());
            }
            if operation_id.is_none() && !self.calls.is_empty() {
                return Ok(());
            }
        }
        let fallback =
            if message.get("method").and_then(Value::as_str) == Some("notifications/progress") {
                self.pending
                    .as_ref()
                    .map_or(0, |pending| pending.operation_id)
            } else {
                0
            };
        self.notify_for_operation(operation_id.unwrap_or(fallback), message)
    }
    pub fn notify_for_request(&mut self, id: u64, message: &Value) -> Result<()> {
        let operation_id = self.action_operation(id).ok_or_else(|| {
            Error::local(
                "UnexpectedResponse",
                "notification source is no longer active",
            )
        })?;
        if message.get("method").and_then(Value::as_str) == Some("notifications/progress")
            && message
                .pointer("/params/progressToken")
                .is_some_and(|token| {
                    token.as_str()
                        != self
                            .rpc_id_for_action(id)
                            .map(|id| format!("mcpx-{id}"))
                            .as_deref()
                })
        {
            return Ok(());
        }
        self.notify_for_operation(operation_id, message)
    }
    fn notify_for_operation(&mut self, operation_id: u64, message: &Value) -> Result<()> {
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || message.get("id").is_some()
            || message.get("method").and_then(Value::as_str).is_none()
        {
            return Err(Error::local(
                "ProtocolError",
                "invalid notification envelope",
            ));
        }
        let kind = match message.get("method").and_then(Value::as_str) {
            Some("notifications/progress") => "progress",
            Some("notifications/message") => "log",
            Some("notifications/tools/list_changed") => "catalog_stale",
            _ => "notification",
        };
        if kind == "catalog_stale" {
            self.catalog.invalidate_revision();
            self.catalog_stale = true;
        }
        self.event_for(operation_id, kind, message.clone())
    }

    pub fn respond(
        &mut self,
        id: u64,
        status: u16,
        headers: &BTreeMap<String, String>,
        message: Option<Value>,
    ) -> Result<()> {
        if let Some(mut pending) = self.calls.remove(&id) {
            pending.dispatched = true;
            let context = self.error_context(&pending);
            let operation_id = pending.operation_id;
            let result = self
                .respond_inner(pending, status, headers, message)
                .and_then(|()| {
                    self.outcome.take().ok_or_else(|| {
                        Error::local(
                            "ProtocolError",
                            "concurrent call completed without an outcome",
                        )
                    })
                })
                .map_err(|error| error.with_context(context));
            let error = result.as_ref().err().cloned();
            self.complete_concurrent(operation_id, id, result);
            return error.map_or(Ok(()), Err);
        }
        if self.pending.as_ref().map(|p| p.id) != Some(id) {
            return Err(Error::local(
                "UnexpectedResponse",
                "response does not match the active action",
            ));
        }
        let mut pending = self.pending.take().expect("checked active action");
        pending.dispatched = true;
        let context = self.error_context(&pending);
        let result = self
            .respond_inner(pending, status, headers, message)
            .map_err(|error| error.with_context(context));
        if let Err(error) = &result {
            self.fail(error.clone());
        }
        result
    }

    fn respond_inner(
        &mut self,
        pending: Pending,
        status: u16,
        headers: &BTreeMap<String, String>,
        message: Option<Value>,
    ) -> Result<()> {
        let operation_id = pending.operation_id;
        if message
            .as_ref()
            .is_some_and(|message| message.get("method").is_some())
        {
            return Err(Error::local(
                "ProtocolError",
                "a server request or notification is not a response",
            ));
        }
        if self.auto && matches!(pending.phase, Phase::Discover) && status != 401 && status != 403 {
            let code = message
                .as_ref()
                .and_then(|m| m.pointer("/error/code"))
                .and_then(Value::as_i64);
            let modern_error = code.is_some_and(|c| (-32029..=-32020).contains(&c));
            let legacy_error = code.is_some() && !modern_error && (status == 200 || status == 400);
            if legacy_error
                && message.as_ref().is_some_and(|m| {
                    m.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
                        || m.get("id").and_then(Value::as_u64) != pending.rpc_id
                })
            {
                return Err(Error::local(
                    "UnexpectedResponse",
                    "discovery error does not match the probe request",
                ));
            }
            if !modern_error && (status == 400 || legacy_error) {
                self.dialect = Dialect::Legacy;
                self.initialize()?;
                return Ok(());
            }
        }
        if status == 401 || status == 403 {
            return Err(Error::local("AuthFailed", "remote authentication failed"));
        }
        if status == 404 && self.expire_http_session(pending.http_session.as_deref()) {
            return Err(Error::local(
                "SessionExpired",
                "MCP HTTP session is unavailable",
            ));
        }
        if message
            .as_ref()
            .and_then(|m| m.pointer("/error/code"))
            .and_then(Value::as_i64)
            == Some(-32022)
        {
            return Err(Error::local(
                "UnsupportedProtocolVersion",
                "remote rejected the supported protocol versions",
            ));
        }
        if !(200..300).contains(&status) {
            return Err(Error::local(
                "HttpError",
                format!("remote HTTP status {status}"),
            ));
        }
        if self.dialect == Dialect::Legacy
            && let Some(session) = headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("mcp-session-id"))
                .map(|(_, v)| v)
        {
            self.session = Some(session.clone());
        }
        if matches!(pending.phase, Phase::Initialized) {
            if status != 202 || message.is_some() {
                return Err(Error::local(
                    "ProtocolError",
                    "expected empty 202 for initialized notification",
                ));
            }
            self.ready = true;
            self.list(vec![], BTreeSet::new(), 0, None, None, None)?;
            return Ok(());
        }
        let message =
            message.ok_or_else(|| Error::local("ProtocolError", "request has no response"))?;
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || message.get("id").and_then(Value::as_u64) != pending.rpc_id
        {
            return Err(Error::local(
                "UnexpectedResponse",
                "invalid JSON-RPC envelope or response ID",
            ));
        }
        if message.get("error").is_some() {
            return Err(Error::local(
                "RemoteProtocolError",
                "remote returned a JSON-RPC error",
            ));
        }
        let result = message
            .get("result")
            .ok_or_else(|| Error::local("ProtocolError", "missing result"))?;
        match pending.phase {
            Phase::Discover => {
                let supported = result
                    .get("supportedVersions")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        Error::local("ProtocolError", "discovery is missing supportedVersions")
                    })?;
                if !supported
                    .iter()
                    .any(|v| v.as_str() == Some(self.dialect.version()))
                {
                    return Err(Error::local(
                        "UnsupportedProtocolVersion",
                        "remote does not support the selected version",
                    ));
                }
                self.ready = true;
                self.list(vec![], BTreeSet::new(), 0, None, None, None)?;
            }
            Phase::Initialize => {
                if result.get("protocolVersion").and_then(Value::as_str)
                    != Some(self.dialect.version())
                {
                    return Err(Error::local(
                        "UnsupportedProtocolVersion",
                        "server selected an unsupported version",
                    ));
                }
                self.request("notifications/initialized", json!({}), Phase::Initialized)?;
            }
            Phase::List {
                mut tools,
                mut cursors,
                pages,
                mut ttl_ms,
                freshness,
            } => {
                if self.dialect == Dialect::Modern
                    && let Some(ttl) = result.get("ttlMs")
                {
                    let ttl = ttl.as_u64().ok_or_else(|| {
                        Error::local("ProtocolError", "catalog TTL must be a nonnegative integer")
                    })?;
                    ttl_ms = Some(ttl_ms.map_or(ttl, |previous| previous.min(ttl)));
                }
                let mut page: Vec<ToolDescriptor> = serde_json::from_value(
                    result
                        .get("tools")
                        .cloned()
                        .ok_or_else(|| Error::local("ProtocolError", "missing tools"))?,
                )
                .map_err(|_| Error::local("ProtocolError", "invalid tool descriptor"))?;
                if self.dialect == Dialect::Modern && !self.stdio {
                    let mut event_error = None;
                    page.retain(|tool| {
                        if mcpx_protocol::validate_header_annotations(&tool.input_schema).is_err() {
                            if let Err(error) = self.event_for(operation_id, "notification", json!({"code":"ToolRejected", "tool":tool.name,"reason":"invalid x-mcp-header annotation"})) { event_error = Some(error); }
                            false
                        } else {true}
                    });
                    if let Some(error) = event_error {
                        return Err(error);
                    }
                }
                tools.extend(page);
                if tools.len() > 10_000
                    || pages >= 100
                    || serde_json::to_vec(&tools)
                        .map_or(true, |bytes| bytes.len() > mcpx_protocol::MAX_BODY)
                {
                    return Err(Error::local(
                        "LimitExceeded",
                        "catalog page or tool limit exceeded",
                    ));
                }
                if let Some(cursor) = result.get("nextCursor").and_then(Value::as_str) {
                    if !cursors.insert(cursor.into()) {
                        return Err(Error::local("InvalidCatalog", "repeated pagination cursor"));
                    }
                    self.list(
                        tools,
                        cursors,
                        pages + 1,
                        Some(cursor),
                        ttl_ms,
                        Some(freshness),
                    )?;
                } else {
                    self.catalog.replace(tools)?;
                    self.catalog_stale =
                        !self.catalog.revision_source().context_matches(&freshness);
                    self.catalog_ttl_ms = ttl_ms;
                    self.event_for(
                        operation_id,
                        "catalog_updated",
                        json!({"generation":self.catalog.generation(),"stale":self.catalog_stale}),
                    )
                    .expect("reserved terminal event");
                    self.operation_terminal = true;
                }
            }
            Phase::Call(mut call) => {
                if result.get("resultType").and_then(Value::as_str) == Some("input_required") {
                    if self.dialect != Dialect::Modern {
                        return Err(Error::local(
                            "ProtocolError",
                            "legacy result cannot require modern inputs",
                        ));
                    }
                    call.rounds += 1;
                    if call.rounds > continuation::MAX_ROUNDS {
                        return Err(Error::local(
                            "LimitExceeded",
                            "interaction round limit exceeded",
                        ));
                    }
                    let deadline = *call.deadline.get_or_insert(
                        self.clock_ms
                            .saturating_add(continuation::CONTINUATION_TTL_MS),
                    );
                    if self.clock_ms >= deadline {
                        return Err(Error::local(
                            "ContinuationExpired",
                            "interaction deadline exceeded",
                        ));
                    }
                    self.continuations
                        .retain(|_, state| state.handle.expires_at_ms > self.clock_ms);
                    let bytes = result
                        .to_string()
                        .len()
                        .saturating_add(call.args.value().to_string().len());
                    if self.continuations.len() >= continuation::MAX_CONTINUATIONS
                        || self
                            .continuations
                            .values()
                            .map(|s| s.bytes)
                            .sum::<usize>()
                            .saturating_add(bytes)
                            > continuation::MAX_CONTINUATION_BYTES
                    {
                        return Err(Error::local(
                            "LimitExceeded",
                            "continuation storage limit exceeded",
                        ));
                    }
                    let requests = continuation::input_requests(
                        result,
                        self.elicitation_form,
                        self.elicitation_url,
                        &mut self.catalog,
                    )?;
                    let handle = Continuation {
                        operation_id,
                        queue_operation_id: self.queue_links.get(&operation_id).copied(),
                        id: IdKind::Continuation.allocate(&mut self.next_continuation)?,
                        expires_at_ms: deadline,
                        raw: Arc::new(result.clone()),
                        owner: self.continuation_owner.clone(),
                    };
                    self.continuations.insert(
                        handle.id,
                        SavedContinuation {
                            call,
                            handle: handle.clone(),
                            requests,
                            bytes,
                        },
                    );
                    self.event_for(
                        operation_id,
                        "needs_input",
                        json!({"continuation_id":handle.id}),
                    )
                    .expect("reserved terminal event");
                    self.outcome = Some(CallOutcome::NeedsInput(handle));
                    self.last_tool_result = Some(operation_id);
                    self.operation_terminal = true;
                } else {
                    if result
                        .get("resultType")
                        .is_some_and(|kind| kind.as_str() != Some("complete"))
                        || !result.get("content").is_some_and(Value::is_array)
                        || result
                            .get("isError")
                            .is_some_and(|value| !value.is_boolean())
                    {
                        return Err(Error::local("ProtocolError", "invalid tool result shape"));
                    }
                    let is_error = result
                        .get("isError")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let output_validation = if is_error {
                        OutputValidation::NotApplicable
                    } else {
                        call.binding
                            .validate_output_status(result.get("structuredContent"))
                    };
                    let response = ToolResponse {
                        operation_id: Some(operation_id),
                        queue_operation_id: self.queue_links.get(&operation_id).copied(),
                        raw: result.clone(),
                        is_error,
                        output_validation,
                    };
                    self.event_for(
                        operation_id,
                        if is_error { "tool_failed" } else { "completed" },
                        json!({"is_error":is_error,"output_validation":response.output_validation}),
                    )
                    .expect("reserved terminal event");
                    self.outcome = Some(CallOutcome::Completed(response));
                    self.last_tool_result = Some(operation_id);
                    self.operation_terminal = true;
                }
            }
            Phase::Initialized => unreachable!(),
        }
        Ok(())
    }

    fn list(
        &mut self,
        tools: Vec<ToolDescriptor>,
        cursors: BTreeSet<String>,
        pages: usize,
        cursor: Option<&str>,
        ttl_ms: Option<u64>,
        freshness: Option<crate::CatalogRevision>,
    ) -> Result<()> {
        let freshness = freshness.unwrap_or_else(|| self.catalog.revision_source().snapshot());
        let params = cursor.map_or_else(|| json!({}), |cursor| json!({"cursor":cursor}));
        self.request(
            "tools/list",
            params,
            Phase::List {
                tools,
                cursors,
                pages,
                ttl_ms,
                freshness,
            },
        )
    }

    fn complete_concurrent(
        &mut self,
        operation_id: u64,
        request_id: u64,
        result: Result<CallOutcome>,
    ) {
        self.actions.retain(|action| match action {
            Action::Http { id, .. } | Action::Stream { id, .. } | Action::Cancel { id } => {
                *id != request_id
            }
        });
        if let Err(error) = &result {
            self.event_for(
                operation_id,
                if error.code == "Cancelled" {
                    "cancelled"
                } else {
                    "failed"
                },
                serde_json::to_value(error).expect("serializable error"),
            )
            .expect("reserved terminal event");
        }
        self.completions.insert(operation_id, result);
        if self.calls.is_empty() && self.pending.is_none() {
            self.operation_terminal = true;
        }
    }
    pub fn fail_request(&mut self, id: u64, error: Error) -> Result<()> {
        if let Some(pending) = self.calls.remove(&id) {
            let error = error.with_context(self.error_context(&pending));
            self.complete_concurrent(pending.operation_id, id, Err(error));
            return Ok(());
        }
        if self.active_action_id() == Some(id) {
            self.fail(error);
            return Ok(());
        }
        Err(Error::local(
            "UnexpectedResponse",
            "request is no longer active",
        ))
    }
    pub fn cancel_operation(&mut self, operation_id: u64) -> Result<()> {
        let id = self
            .calls
            .iter()
            .find(|(_, pending)| pending.operation_id == operation_id)
            .map(|(id, _)| *id)
            .ok_or_else(|| {
                Error::local(
                    "InvalidOperation",
                    "concurrent operation is no longer active",
                )
            })?;
        self.fail_request(
            id,
            Error::local("Cancelled", "operation cancelled; execution may be unknown"),
        )?;
        self.actions.push_back(Action::Cancel { id });
        Ok(())
    }
    pub fn fail(&mut self, error: Error) {
        let ids: Vec<_> = self.calls.keys().copied().collect();
        for id in ids {
            let _ = self.fail_request(id, error.clone());
        }
        if self.operation_terminal {
            return;
        }
        let error = self.pending.as_ref().map_or_else(
            || error.clone(),
            |pending| error.clone().with_context(self.error_context(pending)),
        );
        self.operation_terminal = true;
        self.pending = None;
        self.actions.clear();
        self.event(
            "failed",
            serde_json::to_value(error).expect("serializable error"),
        );
    }

    pub fn cancel(&mut self) {
        let operations: Vec<_> = self.calls.values().map(|p| p.operation_id).collect();
        self.actions.clear();
        for operation_id in operations {
            let _ = self.cancel_operation(operation_id);
        }
        if let Some(pending) = self.pending.take() {
            self.operation_terminal = true;
            self.actions.push_back(Action::Cancel { id: pending.id });
            let error = Error::local("Cancelled", "operation cancelled")
                .with_context(self.error_context(&pending));
            self.event(
                "cancelled",
                serde_json::to_value(error).expect("serializable error"),
            );
        }
    }
}

#[cfg(test)]
mod id_tests {
    use super::*;
    use crate::MAX_HOST_ID;

    #[test]
    fn final_catalog_generation_is_exact_and_exhaustion_precedes_metadata_admission() {
        let (mut machine, binding, args) = remote();
        machine.catalog.seed_generation_for_test(MAX_HOST_ID - 1);
        machine.start_catalog_refresh().unwrap();
        respond(
            &mut machine,
            json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
        )
        .unwrap();
        let events = machine.drain_events().events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["generation"].as_u64(), Some(MAX_HOST_ID));
        // A fresh, unchanged final publication can still serve explicit tool calls.
        machine.start_call(&binding, &args).unwrap();
        respond(&mut machine, json!({"content":[]})).unwrap();
        assert!(machine.take_outcome().is_some());
        machine.drain_events();
        let counters = (machine.operation_id, machine.next_action, machine.next_rpc);
        for refresh in [true, false] {
            let error = if refresh {
                machine.start_catalog_refresh()
            } else {
                machine.start_discovery()
            }
            .unwrap_err();
            assert_eq!(error.code, "LimitExceeded");
            assert_eq!(
                error.execution_certainty,
                crate::ExecutionCertainty::NotSent
            );
            assert_eq!(
                (machine.operation_id, machine.next_action, machine.next_rpc),
                counters
            );
            assert!(machine.drain_actions().is_empty());
            assert!(machine.drain_events().events.is_empty());
            assert!(!machine.is_busy());
        }
    }

    #[test]
    fn exhausted_catalog_does_not_prevent_authority_or_transport_invalidation() {
        for transport in [false, true] {
            let (mut machine, binding, args) = remote();
            machine.catalog.seed_generation_for_test(MAX_HOST_ID);
            machine.session = Some("old-session".into());
            let revision = machine.catalog.revision_source().snapshot();
            if transport {
                machine.reset_transport().unwrap();
            } else {
                machine.invalidate_authorization().unwrap();
            }
            assert!(machine.is_catalog_stale());
            assert!(machine.session_id().is_none());
            assert!(!machine.ready);
            assert_eq!(machine.catalog.generation(), MAX_HOST_ID);
            assert_ne!(machine.catalog.revision_source().snapshot(), revision);
            assert_eq!(
                machine.start_call(&binding, &args).unwrap_err().code,
                "BindingStale"
            );
            assert_eq!(
                machine.start_catalog_refresh().unwrap_err().code,
                "LimitExceeded"
            );
            assert!(machine.drain_actions().is_empty());
        }
    }

    fn remote() -> (Machine, ToolBinding, BoundArgs) {
        let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
        machine
            .catalog
            .replace(vec![
                serde_json::from_value(json!({
                    "name":"echo", "inputSchema":{"type":"object"}
                }))
                .unwrap(),
            ])
            .unwrap();
        machine.ready = true;
        machine.elicitation_form = true;
        let binding = machine.bind("echo").unwrap();
        let args = binding.args().finish().unwrap();
        (machine, binding, args)
    }
    #[test]
    fn response_stream_action_exhaustion_does_not_mutate_a_pending_call() {
        let (mut machine, binding, args) = remote();
        machine.dialect = Dialect::Legacy;
        let operation = machine.start_concurrent_call(&binding, &args).unwrap();
        let actions = machine.drain_actions();
        let Action::Http { id, rpc_id, .. } = actions[0] else {
            panic!()
        };
        machine.next_action = IdKind::Action.last();
        let last = machine.resume_response_action(id).unwrap();
        assert_eq!(last, IdKind::Action.last());
        assert_eq!(machine.action_operation(last), Some(operation));
        assert_eq!(machine.rpc_id_for_action(last), rpc_id);
        assert_eq!(
            machine.resume_response_action(last).unwrap_err().code,
            "LimitExceeded"
        );
        assert!(machine.has_action(last));
        assert_eq!(machine.action_operation(last), Some(operation));
        assert!(machine.drain_actions().is_empty());
    }
    fn respond(machine: &mut Machine, result: Value) -> Result<()> {
        let actions = machine.drain_actions();
        let [Action::Http { id, .. }] = actions.as_slice() else {
            panic!()
        };
        machine.respond(
            *id,
            200,
            &BTreeMap::new(),
            Some(json!({"jsonrpc":"2.0","id":machine.rpc_id_for_action(*id).unwrap(),"result":result})),
        )
    }
    fn required() -> Value {
        json!({"resultType":"input_required","requestState":"original state"})
    }

    #[test]
    fn scoped_serial_operations_do_not_retain_historical_queue_links() {
        let (mut machine, binding, args) = remote();
        for id in 1..=128 {
            let scope = machine.queue_scope(id).unwrap();
            machine.start_call(&binding, &args).unwrap();
            drop(scope);
            respond(&mut machine, json!({"content":[]})).unwrap();
            let CallOutcome::Completed(response) = machine.take_outcome().unwrap() else {
                panic!()
            };
            assert_eq!(response.queue_operation_id, Some(id));
            assert!(machine.queue_links.len() <= 1);
            machine.drain_events();
        }
    }
    #[test]
    fn final_request_and_operation_ids_are_exact_and_never_reused() {
        let (mut machine, binding, args) = remote();
        machine.next_action = IdKind::Action.last();
        machine.operation_id = IdKind::Operation.last() - 16;
        let operation = machine.start_concurrent_call(&binding, &args).unwrap();
        assert_eq!(operation, IdKind::Operation.last());
        respond(&mut machine, json!({"content":[]})).unwrap();
        assert!(machine.take_completion(operation).unwrap().is_ok());
        assert_eq!(
            machine
                .start_concurrent_call(&binding, &args)
                .unwrap_err()
                .code,
            "LimitExceeded"
        );
        assert_eq!(machine.operation_id, IdKind::Operation.last());
        assert!(machine.drain_actions().is_empty());
        assert!(!machine.is_busy());
    }

    #[test]
    fn exhaustion_is_atomic_before_call_or_discovery_admission() {
        for exhausted_domain in 0..3 {
            let (mut machine, binding, args) = remote();
            match exhausted_domain {
                0 => machine.next_action = MAX_HOST_ID + 1,
                1 => machine.next_rpc = MAX_HOST_ID + 1,
                _ => machine.operation_id = IdKind::Operation.last(),
            }
            let before = (machine.operation_id, machine.next_action, machine.next_rpc);
            assert_eq!(
                machine
                    .start_concurrent_call(&binding, &args)
                    .unwrap_err()
                    .code,
                "LimitExceeded"
            );
            assert_eq!(machine.start_discovery().unwrap_err().code, "LimitExceeded");
            assert_eq!(
                (machine.operation_id, machine.next_action, machine.next_rpc),
                before
            );
            assert!(machine.drain_actions().is_empty());
            assert!(!machine.is_busy());
        }
    }

    #[test]
    fn exhausted_catalog_pagination_has_one_failure_and_no_orphaned_action() {
        for domain in [IdKind::Action, IdKind::Rpc] {
            let (mut machine, _, _) = remote();
            if domain == IdKind::Action {
                machine.next_action = domain.last();
            } else {
                machine.next_rpc = domain.last();
            }
            let generation = machine.catalog.generation();
            machine.start_catalog_refresh().unwrap();
            assert_eq!(
                respond(&mut machine, json!({"tools":[],"nextCursor":"next"}))
                    .unwrap_err()
                    .code,
                "LimitExceeded"
            );
            assert!(!machine.is_busy());
            assert!(machine.drain_actions().is_empty());
            assert_eq!(machine.catalog.generation(), generation);
            let events = machine.drain_events().events;
            assert_eq!(
                events.iter().filter(|event| event.kind == "failed").count(),
                1
            );
            assert!(events.iter().all(|event| event.kind != "catalog_updated"));
        }
    }

    #[test]
    fn exhausted_resume_admission_preserves_the_unconsumed_continuation() {
        let (mut machine, binding, args) = remote();
        machine.next_continuation = (1 << 32) + IdKind::Continuation.first();
        machine.start_call(&binding, &args).unwrap();
        respond(&mut machine, required()).unwrap();
        let CallOutcome::NeedsInput(handle) = machine.take_outcome().unwrap() else {
            panic!()
        };
        assert_eq!(handle.id(), (1 << 32) + IdKind::Continuation.first());
        let before = (machine.next_action, machine.next_rpc, machine.operation_id);
        for domain in [IdKind::Action, IdKind::Rpc, IdKind::Operation] {
            match domain {
                IdKind::Action => machine.next_action = MAX_HOST_ID + 1,
                IdKind::Rpc => machine.next_rpc = MAX_HOST_ID + 1,
                _ => machine.operation_id = IdKind::Operation.last(),
            }
            assert_eq!(
                machine.resume(&handle, json!({})).unwrap_err().code,
                "LimitExceeded"
            );
            assert!(machine.check_continuation(&handle).is_ok());
            assert!(machine.drain_actions().is_empty());
            // Simulate additional capacity only in this private test harness.
            (machine.next_action, machine.next_rpc, machine.operation_id) = before;
        }
        machine.resume(&handle, json!({})).unwrap();
        respond(&mut machine, json!({"content":[]})).unwrap();
        assert!(matches!(
            machine.take_outcome(),
            Some(CallOutcome::Completed(_))
        ));
    }

    #[test]
    fn continuation_exhaustion_cannot_wrap_into_an_existing_handle() {
        let (mut machine, binding, args) = remote();
        machine.next_continuation = IdKind::Continuation.last();
        machine.start_call(&binding, &args).unwrap();
        respond(&mut machine, required()).unwrap();
        let CallOutcome::NeedsInput(handle) = machine.take_outcome().unwrap() else {
            panic!()
        };
        assert_eq!(handle.id(), IdKind::Continuation.last());
        machine.start_call(&binding, &args).unwrap();
        assert_eq!(
            respond(&mut machine, required()).unwrap_err().code,
            "LimitExceeded"
        );
        assert!(machine.check_continuation(&handle).is_ok());
        assert!(!machine.is_busy());
        assert!(machine.drain_actions().is_empty());
    }
}
