use crate::ids::IdKind;
use crate::{BoundArgs, Dialect, Error, Machine, Result, ToolBinding};
use mcpx_protocol::ResponseDecoder;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(crate) struct Response {
    id: u64,
    pub(crate) status: u16,
    headers: BTreeMap<String, String>,
    pub(crate) decoder: ResponseDecoder,
    bytes: usize,
}

/// A synchronous ABI for asynchronous hosts, shared by WebAssembly entry points.
pub struct HostRuntime {
    policy: crate::policy::CatalogPolicy,
    pub(crate) machine: Machine,
    pub(crate) notifications: Option<crate::notifications::NotificationState>,
    pub(crate) next_notification: u64,
    bindings: BTreeMap<u64, ToolBinding>,
    next_binding: u64,
    pub(crate) responses: BTreeMap<u64, Response>,
    pub(crate) response_resumptions: BTreeMap<u64, crate::response_resume::Stream>,
}

impl HostRuntime {
    pub fn new(endpoint: String, legacy: bool) -> Result<Self> {
        Self::with_dialect(
            endpoint,
            if legacy {
                Dialect::Legacy
            } else {
                Dialect::Modern
            },
        )
    }
    pub fn auto(endpoint: String) -> Result<Self> {
        Self::with_dialect(endpoint, Dialect::Auto)
    }
    pub fn with_dialect(endpoint: String, dialect: Dialect) -> Result<Self> {
        Ok(Self {
            policy: Default::default(),
            machine: Machine::new(endpoint, dialect)?,
            notifications: None,
            next_notification: IdKind::Notification.first(),
            bindings: BTreeMap::new(),
            next_binding: IdKind::Binding.first(),
            responses: BTreeMap::new(),
            response_resumptions: BTreeMap::new(),
        })
    }
    pub fn discover(&mut self) -> Result<()> {
        self.policy.observe(&self.machine);
        self.machine.start_discovery()?;
        self.policy.invalidate(&self.machine);
        Ok(())
    }
    pub fn stdio(dialect: Dialect) -> Self {
        Self {
            policy: Default::default(),
            machine: Machine::new_stdio(dialect),
            notifications: None,
            next_notification: IdKind::Notification.first(),
            bindings: BTreeMap::new(),
            next_binding: IdKind::Binding.first(),
            responses: BTreeMap::new(),
            response_resumptions: BTreeMap::new(),
        }
    }
    pub fn machine(&self) -> &Machine {
        &self.machine
    }
    pub fn advance_time(&mut self, now_ms: u64) -> Result<()> {
        // Observe a completed catalog at the last input's time before advancing.
        self.policy.observe(&self.machine);
        self.machine.advance_time(now_ms)?;
        self.expire_notification_deadline();
        self.policy.advance(&self.machine, now_ms);
        Ok(())
    }
    pub fn endpoint(&self) -> &str {
        self.machine.endpoint()
    }
    pub fn set_schema_registry(&mut self, registry: crate::SchemaRegistry) -> Result<()> {
        self.machine.set_schema_registry(registry)?;
        self.bindings.clear();
        Ok(())
    }
    pub fn set_schema_registry_json(&mut self, text: &str) -> Result<()> {
        self.set_schema_registry(crate::SchemaRegistry::from_json(text)?)
    }
    pub fn schema_registry_stats(&self) -> crate::SchemaRegistryStats {
        self.machine.catalog().schema_registry().stats()
    }
    pub fn validator_cache_stats(&self) -> crate::ValidatorCacheStats {
        self.machine.validator_cache_stats()
    }
    pub fn set_validator_cache_capacity(&mut self, capacity: usize) -> Result<()> {
        self.machine.set_validator_cache_capacity(capacity)
    }
    pub fn clear_validator_cache(&mut self) {
        self.machine.clear_validator_cache();
    }
    pub fn set_catalog_ttl(&mut self, ttl_ms: u64) {
        self.policy.set_ttl(&self.machine, ttl_ms);
    }
    pub fn catalog_needs_refresh_at(&self, now_ms: u64) -> bool {
        self.policy.expired(&self.machine, now_ms)
    }
    pub fn catalog_status_at(&self, scope: &str, now_ms: u64) -> Result<crate::CatalogStatus> {
        self.policy.status(&self.machine, scope, now_ms)
    }
    pub fn catalog_status(&self, scope: &str) -> Result<crate::CatalogStatus> {
        self.policy.status(&self.machine, scope, self.policy_now())
    }
    fn policy_now(&self) -> u64 {
        self.machine.now_ms()
    }
    pub fn accept_authorization(&mut self, scope: &str) -> Result<bool> {
        let status = self.catalog_status(scope)?;
        if status.authorization_changed {
            if self.busy() {
                return Err(Error::local(
                    "BindingStale",
                    "authorization changed; wait for in-flight calls before refreshing",
                ));
            }
            self.invalidate_authorization()?;
        }
        self.policy.accept(scope);
        Ok(status.authorization_changed)
    }
    pub fn set_elicitation(&mut self, form: bool, url: bool) -> Result<()> {
        self.machine.set_elicitation(form, url)
    }
    pub fn resume_bound(
        &mut self,
        continuation: &crate::Continuation,
        responses: Value,
    ) -> Result<()> {
        self.machine.resume(continuation, responses)
    }
    pub fn resume(&mut self, id: u64, responses: &str) -> Result<()> {
        self.machine.resume_id(id, crate::parse_json(responses)?)
    }
    pub fn release_continuation(&mut self, id: u64) {
        self.machine.release_continuation(id);
    }
    pub fn refresh_catalog(&mut self) -> Result<()> {
        self.policy.observe(&self.machine);
        self.machine.start_catalog_refresh()?;
        self.policy.invalidate(&self.machine);
        Ok(())
    }
    pub fn reset_transport(&mut self) -> Result<()> {
        self.machine.reset_transport()?;
        self.cancel_notification_context("BindingStale");
        self.bindings.clear();
        self.responses.clear();
        self.response_resumptions.clear();
        self.policy.invalidate(&self.machine);
        Ok(())
    }
    pub fn invalidate_authorization(&mut self) -> Result<()> {
        self.machine.invalidate_authorization()?;
        self.cancel_notification_context("BindingStale");
        self.bindings.clear();
        self.response_resumptions.clear();
        self.policy.invalidate(&self.machine);
        Ok(())
    }
    pub fn tool_binding(&mut self, name: &str) -> Result<ToolBinding> {
        self.machine.bind(name)
    }
    pub fn call_bound(&mut self, binding: &ToolBinding, args: &BoundArgs) -> Result<()> {
        self.machine.start_call(binding, args)
    }
    pub fn call_concurrent_bound(
        &mut self,
        binding: &ToolBinding,
        args: &BoundArgs,
    ) -> Result<u64> {
        self.machine.start_concurrent_call(binding, args)
    }
    pub fn call_concurrent(&mut self, id: u64, args: &str) -> Result<u64> {
        let binding = self.binding(id)?.clone();
        let args = binding.validate_json(crate::parse_json(args)?)?;
        self.call_concurrent_bound(&binding, &args)
    }
    pub fn take_completion(&mut self, operation_id: u64) -> Option<Result<crate::CallOutcome>> {
        self.machine.take_completion(operation_id)
    }
    pub fn link_queue_operation(&mut self, operation_id: u64, queue_id: u64) -> Result<()> {
        self.machine.link_queue_operation(operation_id, queue_id)
    }
    pub fn resume_concurrent_bound(
        &mut self,
        handle: &crate::Continuation,
        responses: Value,
    ) -> Result<u64> {
        self.machine.resume_concurrent(handle, responses)
    }
    pub fn resume_concurrent(&mut self, id: u64, responses: &str) -> Result<u64> {
        self.machine
            .resume_concurrent_id(id, crate::parse_json(responses)?)
    }
    pub fn completion(&mut self, operation_id: u64) -> Result<Option<String>> {
        self.take_completion(operation_id)
            .transpose()
            .map(|outcome| {
                outcome.map(|value| serde_json::to_string(&value).expect("serializable outcome"))
            })
    }
    pub fn cancel_operation(&mut self, operation_id: u64) -> Result<()> {
        let ids: Vec<_> = self
            .responses
            .keys()
            .chain(self.response_resumptions.keys())
            .copied()
            .filter(|id| self.machine.action_operation(*id) == Some(operation_id))
            .collect();
        self.machine.cancel_operation(operation_id)?;
        for id in ids {
            self.responses.remove(&id);
            self.response_resumptions.remove(&id);
        }
        Ok(())
    }
    pub fn fail_request(&mut self, id: u64, error: Error) -> Result<()> {
        self.responses.remove(&id);
        self.response_resumptions.remove(&id);
        self.machine.fail_request(id, error)
    }
    pub fn action_error_context(&self, id: u64) -> Option<crate::ErrorContext> {
        self.machine.action_error_context(id)
    }
    pub fn take_actions(&mut self) -> Vec<crate::Action> {
        self.machine.drain_actions()
    }
    pub fn take_events(&mut self) -> crate::EventBatch {
        self.machine.drain_events()
    }
    pub fn event_stats(&self) -> crate::EventStats {
        self.machine.event_stats()
    }
    pub fn set_event_limits(&mut self, limits: crate::EventLimits) -> Result<()> {
        self.machine.set_event_limits(limits)
    }
    pub fn take_outcome(&mut self) -> Option<crate::CallOutcome> {
        self.machine.take_outcome()
    }
    pub fn abort_error(&mut self, error: Error) {
        self.responses.clear();
        self.response_resumptions.clear();
        self.machine.fail(error);
    }
    pub fn probe_timeout(&mut self) -> Result<()> {
        self.machine.probe_timeout()
    }
    pub fn complete_write(&mut self, id: u64) -> Result<()> {
        self.machine.respond(id, 202, &BTreeMap::new(), None)
    }
    pub fn stream_message(&mut self, id: u64, message: Value) -> Result<Option<Value>> {
        self.check_response_stream_input(id)?;
        if !self.machine.has_action(id) {
            return Err(Error::new(
                "UnexpectedResponse",
                "stream action is no longer active",
            ));
        }
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(Error::new("ProtocolError", "invalid JSON-RPC envelope"));
        }
        if message
            .get("method")
            .is_some_and(|method| !method.is_string())
        {
            return Err(Error::new(
                "ProtocolError",
                "JSON-RPC method must be a string",
            ));
        }
        if message.get("method").is_some() && message.get("id").is_some() {
            if self.machine.dialect() == Dialect::Legacy {
                return Ok(Some(
                    json!({"jsonrpc":"2.0","id":message["id"],"error":{"code":-32601,"message":"Client capability not supported"}}),
                ));
            }
            return Err(Error::new(
                "ProtocolError",
                "modern server sent an unsolicited request",
            ));
        }
        if message.get("id").is_none() {
            self.machine.notify_for_request(id, &message)?;
        } else {
            self.machine
                .respond(id, 200, &BTreeMap::new(), Some(message))?;
        }
        Ok(None)
    }
    pub fn stream_input(&mut self, message: Value) -> Result<Option<Value>> {
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(Error::new("ProtocolError", "invalid JSON-RPC envelope"));
        }
        if message
            .get("method")
            .is_some_and(|method| !method.is_string())
        {
            return Err(Error::new(
                "ProtocolError",
                "JSON-RPC method must be a string",
            ));
        }
        if self.stdio_notification_message(&message) {
            return Ok(None);
        }
        if message.get("method").is_some() && message.get("id").is_some() {
            if self.machine.dialect() == Dialect::Legacy {
                return Ok(Some(
                    json!({"jsonrpc":"2.0","id":message["id"],"error":{"code":-32601,"message":"Client capability not supported"}}),
                ));
            }
            return Err(Error::new(
                "ProtocolError",
                "modern server sent an unsolicited request",
            ));
        }
        if message.get("id").is_none() {
            self.machine.notify(&message)?;
            return Ok(None);
        }
        let id = message["id"]
            .as_u64()
            .ok_or_else(|| Error::new("UnexpectedResponse", "invalid stream response ID"))?;
        let Some(action) = self.machine.action_for_rpc_id(id) else {
            if self.machine.is_past_rpc(id) {
                return Ok(None);
            }
            return Err(Error::new(
                "UnexpectedResponse",
                "stream wire response ID was never allocated",
            ));
        };
        self.stream_message(action, message)
    }
    pub fn bind(&mut self, name: &str) -> Result<u64> {
        if self.bindings.len() >= 128 {
            return Err(Error::local(
                "LimitExceeded",
                "too many live binding handles",
            ));
        }
        IdKind::Binding.available(self.next_binding)?;
        let binding = self.machine.bind(name)?;
        let id = IdKind::Binding.allocate(&mut self.next_binding)?;
        self.bindings.insert(id, binding);
        Ok(id)
    }
    pub fn release_binding(&mut self, id: u64) {
        self.bindings.remove(&id);
    }
    fn binding(&self, id: u64) -> Result<&ToolBinding> {
        self.bindings
            .get(&id)
            .ok_or_else(|| Error::local("InvalidHandle", "binding handle is unavailable"))
    }
    pub fn tools(&self) -> String {
        serde_json::to_string(&self.machine.catalog().tools().collect::<Vec<_>>()).unwrap()
    }
    pub fn validate(&self, id: u64, args: &str) -> Result<String> {
        let args = self.binding(id)?.validate_json(crate::parse_json(args)?)?;
        Ok(args.value().to_string())
    }
    pub fn call(&mut self, id: u64, args: &str) -> Result<()> {
        let binding = self.binding(id)?.clone();
        let args = binding.validate_json(crate::parse_json(args)?)?;
        self.call_args(&binding, &args)
    }
    fn call_args(&mut self, binding: &ToolBinding, args: &BoundArgs) -> Result<()> {
        self.machine.start_call(binding, args)
    }
    pub fn actions(&mut self) -> String {
        // Preserve numeric tokens inside the body when a JavaScript host parses actions.
        let actions: Vec<Value> = self
            .machine
            .drain_actions()
            .into_iter()
            .map(|action| {
                let mut value = serde_json::to_value(action).unwrap();
                for field in ["body", "cancellation"] {
                    if value.get(field).is_some_and(|value| !value.is_null()) {
                        value[field] = Value::String(value[field].to_string());
                    }
                }
                value
            })
            .collect();
        serde_json::to_string(&actions).unwrap()
    }
    pub fn events(&mut self) -> String {
        let batch = self.machine.drain_events();
        let events: Vec<Value> = batch
            .events
            .into_iter()
            .map(|event| {
                let payload = event.payload.to_string();
                let mut event = serde_json::to_value(event).unwrap();
                event.as_object_mut().unwrap().remove("payload");
                event["payload_json"] = Value::String(payload);
                event
            })
            .collect();
        json!({"events":events,"dropped":batch.dropped}).to_string()
    }
    pub fn outcome(&mut self) -> String {
        serde_json::to_string(&self.machine.take_outcome()).unwrap()
    }
    pub fn busy(&self) -> bool {
        self.machine.is_busy()
    }
    pub fn cancel(&mut self) {
        self.responses.clear();
        self.response_resumptions.clear();
        self.machine.cancel();
    }
    pub fn fail(&mut self, message: &str) {
        self.responses.clear();
        self.response_resumptions.clear();
        self.machine.fail(Error::new("HostError", message));
    }
    fn response_slot(&self, id: u64) -> Result<()> {
        self.check_response_stream_input(id)?;
        if !self.machine.has_action(id) {
            return Err(Error::new(
                "UnexpectedResponse",
                "response action is no longer active",
            ));
        }
        if self.responses.contains_key(&id) {
            return Err(Error::new("Busy", "a response is already open"));
        }
        Ok(())
    }
    pub fn begin_response(&mut self, id: u64, status: u16, headers: &str) -> Result<bool> {
        self.response_slot(id)?;
        if headers.len() > 65536 {
            return Err(Error::new(
                "LimitExceeded",
                "response headers exceed byte limit",
            ));
        }
        let headers: BTreeMap<String, String> = serde_json::from_str(headers)
            .map_err(|_| Error::new("InvalidJson", "invalid response headers"))?;
        self.begin_response_headers(id, status, headers)
    }
    /// Typed hosts supply fields directly. Validation happens before opening the decoder.
    pub fn begin_response_headers(
        &mut self,
        id: u64,
        status: u16,
        fields: impl IntoIterator<Item = (String, String)>,
    ) -> Result<bool> {
        self.response_slot(id)?;
        if !(100..=599).contains(&status) {
            return Err(Error::new(
                "InvalidResponse",
                "HTTP status must be in 100..599",
            ));
        }
        let mut headers: BTreeMap<String, String> = BTreeMap::new();
        let mut bytes = 0usize;
        for (index, (name, value)) in fields.into_iter().enumerate() {
            bytes = bytes.saturating_add(name.len()).saturating_add(value.len());
            if index >= 256 || bytes > 65536 {
                return Err(Error::new(
                    "LimitExceeded",
                    "response headers exceed count or byte limit",
                ));
            }
            if name.is_empty()
                || name.len() > 128
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
                || value
                    .bytes()
                    .any(|byte| byte == 127 || byte < 32 && byte != b'\t')
            {
                return Err(Error::new(
                    "InvalidResponse",
                    "invalid response header field",
                ));
            }
            let name = name.to_ascii_lowercase();
            if let Some(previous) = headers.get_mut(&name) {
                if ["content-type", "mcp-session-id", "mcp-protocol-version"]
                    .contains(&name.as_str())
                {
                    if previous != &value {
                        return Err(Error::new(
                            "InvalidResponse",
                            "conflicting singleton response headers",
                        ));
                    }
                } else {
                    previous.push_str(", ");
                    previous.push_str(&value);
                }
            } else {
                headers.insert(name, value);
            }
        }
        self.check_resumed_response_status(id, status)?;
        // A session-bearing 404 is conclusive at headers, even when an error
        // body is malformed, unbounded or never finishes. Auto discovery probes
        // and stateless/modern endpoints still use their normal response decoder.
        if status == 404 && self.machine.session_404(id) {
            self.response_resumptions.remove(&id);
            self.machine.respond(id, status, &headers, None)?;
            return Ok(true);
        }
        if status == 202 || (!(200..300).contains(&status) && !matches!(status, 400 | 404)) {
            self.response_resumptions.remove(&id);
            self.machine.respond(id, status, &headers, None)?;
            return Ok(true);
        }
        let content_type = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map(|(_, value)| value.as_str())
            .unwrap_or("");
        let decoder = match ResponseDecoder::new(content_type) {
            Ok(decoder) => decoder,
            Err(_) if matches!(status, 400 | 404) => {
                self.response_resumptions.remove(&id);
                self.machine.respond(id, status, &headers, None)?;
                return Ok(true);
            }
            Err(_) => {
                let error = Error::new("ProtocolError", "unsupported response content type")
                    .with_context(
                        self.machine
                            .action_error_context(id)
                            .expect("active response"),
                    );
                self.machine.fail_request(id, error.clone())?;
                self.response_resumptions.remove(&id);
                return Err(error);
            }
        };
        let decoder = self.prepare_resumable_decoder(id, status, &headers, decoder)?;
        self.responses.insert(
            id,
            Response {
                id,
                status,
                headers,
                decoder,
                bytes: 0,
            },
        );
        Ok(false)
    }
    fn messages(&mut self, id: u64, messages: Vec<Value>) -> Result<bool> {
        for message in messages {
            if self.response_resumptions.contains_key(&id)
                && message.get("method").is_some()
                && message.get("id").is_some()
            {
                self.validate_response_server_request(&message)?;
                if let Some(reply) = self.stream_message(id, message)? {
                    self.queue_response_reply(id, reply)?;
                }
                continue;
            }
            if message.get("id").is_some() || message.get("error").is_some() {
                let response = self
                    .responses
                    .remove(&id)
                    .ok_or_else(|| Error::new("InvalidHandle", "response already closed"))?;
                self.machine.respond(
                    response.id,
                    response.status,
                    &response.headers,
                    Some(message),
                )?;
                self.complete_resumable_response(id);
                return Ok(true);
            }
            self.machine.notify_for_request(id, &message)?;
        }
        Ok(false)
    }
    pub fn push_response(&mut self, bytes: &[u8]) -> Result<bool> {
        self.push_response_for(self.single_response_id()?, bytes)
    }
    fn single_response_id(&self) -> Result<u64> {
        if self.responses.len() > 1 {
            return Err(Error::new(
                "AmbiguousResponse",
                "specify the response action ID when multiple streams are open",
            ));
        }
        self.responses
            .keys()
            .next()
            .copied()
            .ok_or_else(|| Error::new("InvalidHandle", "no open response"))
    }
    pub fn push_response_for(&mut self, id: u64, bytes: &[u8]) -> Result<bool> {
        let context = self.machine.action_error_context(id);
        let response = self
            .responses
            .get_mut(&id)
            .ok_or_else(|| Error::new("InvalidHandle", "no open response"))?;
        response.bytes = response.bytes.saturating_add(bytes.len());
        let result = if self.response_resumptions.contains_key(&id) && bytes.len() > 65536 {
            Err(Error::local(
                "LimitExceeded",
                "resumable response chunk exceeds 64 KiB",
            ))
        } else if matches!(response.status, 400 | 404) && response.bytes > 65536 {
            Err(Error::new(
                "LimitExceeded",
                "error response exceeds byte limit",
            ))
        } else {
            response
                .decoder
                .push(bytes)
                .map_err(|_| Error::new("ProtocolError", "invalid or oversized response stream"))
        };
        let result = result
            .and_then(|messages| self.messages(id, messages))
            .map_err(|error| {
                context.map_or_else(
                    || error.clone(),
                    |context| error.clone().with_context(context),
                )
            });
        if let Err(error) = &result {
            self.responses.remove(&id);
            self.response_resumptions.remove(&id);
            if self.machine.has_action(id) {
                self.machine.fail_request(id, error.clone())?;
            }
        }
        result
    }
    pub fn finish_response(&mut self) -> Result<()> {
        self.finish_response_for(self.single_response_id()?)
    }
    pub fn finish_response_for(&mut self, id: u64) -> Result<()> {
        let response = self
            .responses
            .remove(&id)
            .ok_or_else(|| Error::new("InvalidHandle", "no open response"))?;
        self.response_resumptions.remove(&id);
        let messages = match response.decoder.finish() {
            Ok(messages) => messages,
            Err(_) if matches!(response.status, 400 | 404) => {
                return self
                    .machine
                    .respond(response.id, response.status, &response.headers, None);
            }
            Err(_) => {
                let error = Error::new("ProtocolError", "incomplete response").with_context(
                    self.machine
                        .action_error_context(id)
                        .expect("active response"),
                );
                self.machine.fail_request(id, error.clone())?;
                return Err(error);
            }
        };
        let mut final_message = None;
        for message in messages {
            if message.get("id").is_some() || message.get("error").is_some() {
                final_message = Some(message);
            } else {
                self.machine.notify_for_request(id, &message)?;
            }
        }
        self.machine.respond(
            response.id,
            response.status,
            &response.headers,
            final_message,
        )
    }
    pub fn snapshot(&self) -> Value {
        json!({"busy": self.busy(), "generation": self.machine.catalog().generation()})
    }
}

#[cfg(test)]
mod id_tests {
    use super::*;

    #[test]
    fn binding_handles_above_u32_and_at_the_host_limit_remain_distinct() {
        let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
        runtime.discover().unwrap();
        for result in [
            json!({"supportedVersions":["2026-07-28"]}),
            json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
        ] {
            let actions = runtime.take_actions();
            let [crate::Action::Http { id, .. }] = actions.as_slice() else {
                panic!()
            };
            runtime
                .machine
                .respond(
                    *id,
                    200,
                    &BTreeMap::new(),
                    Some(json!({"jsonrpc":"2.0","id":runtime.machine.rpc_id_for_action(*id).unwrap(),"result":result})),
                )
                .unwrap();
        }
        runtime.next_binding = (1 << 32) + IdKind::Binding.first();
        let large = runtime.bind("echo").unwrap();
        assert_eq!(large, (1 << 32) + IdKind::Binding.first());
        assert_eq!(runtime.validate(large, "{}").unwrap(), "{}");
        runtime.next_binding = IdKind::Binding.last();
        let last = runtime.bind("echo").unwrap();
        assert_eq!(last, IdKind::Binding.last());
        assert_eq!(runtime.bind("echo").unwrap_err().code, "LimitExceeded");
        assert_eq!(runtime.validate(last, "{}").unwrap(), "{}");
        assert_eq!(runtime.validate(large, "{}").unwrap(), "{}");
        assert_eq!(runtime.bindings.len(), 2);
    }
}
