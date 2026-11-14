use mcpx_command::{CommandRuntimeExt, HostRuntime};
use wasm_bindgen::prelude::*;

fn js_error(error: mcpx_core::Error) -> JsValue {
    JsValue::from_str(&serde_json::to_string(&error).unwrap())
}

fn exact_host_id(value: f64) -> Option<u64> {
    (value.is_finite()
        && value.fract() == 0.0
        && (1.0..=mcpx_core::MAX_HOST_ID as f64).contains(&value))
    .then_some(value as u64)
}

fn host_id(value: &JsValue) -> Result<u64, JsValue> {
    value.as_f64().and_then(exact_host_id).ok_or_else(|| {
        js_error(mcpx_core::Error::new(
            "InvalidHandle",
            "handle must be a positive safe integer Number",
        ))
    })
}

#[wasm_bindgen]
pub struct Runtime(HostRuntime, mcpx_core::CatalogWatcher);

impl Runtime {
    fn wrap(runtime: HostRuntime) -> Self {
        let watcher = runtime
            .machine()
            .catalog()
            .revision_source()
            .subscribe()
            .expect("new runtime");
        Self(runtime, watcher)
    }
}

#[wasm_bindgen]
impl Runtime {
    pub fn enable_response_resumption(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0
            .enable_response_resumption(host_id(id)?)
            .map_err(js_error)
    }
    pub fn interrupt_response_for(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0
            .interrupt_response_for(host_id(id)?)
            .map_err(js_error)
    }
    pub fn response_resume(
        &self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<String, JsValue> {
        self.0
            .response_resume(host_id(id)?)
            .map(|v| serde_json::to_string(&v).unwrap())
            .map_err(js_error)
    }
    pub fn resume_response(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<String, JsValue> {
        self.0
            .resume_response(host_id(id)?)
            .map(|v| serde_json::to_string(&v).unwrap())
            .map_err(js_error)
    }
    pub fn response_replies(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<String, JsValue> {
        self.0
            .response_replies(host_id(id)?)
            .map(|v| serde_json::to_string(&v).unwrap())
            .map_err(js_error)
    }
    pub fn release_response_resumption(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0
            .release_response_resumption(host_id(id)?)
            .map_err(js_error)
    }
    pub fn exhaust_response_resumption(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0
            .exhaust_response_resumption(host_id(id)?)
            .map_err(js_error)
    }
    pub fn start_notifications(&mut self) -> Result<String, JsValue> {
        self.0
            .start_notifications()
            .map(|request| serde_json::to_string(&request).unwrap())
            .map_err(js_error)
    }
    pub fn notification_status(&self) -> String {
        serde_json::to_string(&self.0.notification_status()).unwrap()
    }
    pub fn notification_resume(
        &self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<String, JsValue> {
        self.0
            .notification_resume(host_id(id)?)
            .map(|info| serde_json::to_string(&info).unwrap())
            .map_err(js_error)
    }
    pub fn resume_notifications(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<String, JsValue> {
        self.0
            .resume_notifications(host_id(id)?)
            .map(|request| serde_json::to_string(&request).unwrap())
            .map_err(js_error)
    }
    pub fn begin_notifications(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] status: &JsValue,
        content_type: &str,
    ) -> Result<(), JsValue> {
        let status = status
            .as_f64()
            .filter(|value| {
                value.is_finite() && value.fract() == 0.0 && (100.0..=599.0).contains(value)
            })
            .ok_or_else(|| {
                js_error(mcpx_core::Error::local(
                    "InvalidResponse",
                    "invalid notification HTTP status",
                ))
            })?;
        self.0
            .begin_notifications(host_id(id)?, status as u16, content_type)
            .map_err(js_error)
    }
    pub fn push_notifications(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        bytes: &[u8],
    ) -> Result<bool, JsValue> {
        self.0
            .push_notifications(host_id(id)?, bytes)
            .map_err(js_error)
    }
    pub fn finish_notifications(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0.finish_notifications(host_id(id)?).map_err(js_error)
    }
    pub fn stop_notifications(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0.stop_notifications(host_id(id)?).map_err(js_error)
    }
    pub fn abort_notifications(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        timeout: bool,
    ) -> Result<(), JsValue> {
        self.0
            .abort_notifications(host_id(id)?, timeout)
            .map_err(js_error)
    }
    pub fn notification_replies(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<String, JsValue> {
        self.0
            .notification_replies(host_id(id)?)
            .map(|replies| serde_json::to_string(&replies).unwrap())
            .map_err(js_error)
    }
    pub fn auto(endpoint: String) -> Result<Runtime, JsValue> {
        HostRuntime::auto(endpoint)
            .map(Self::wrap)
            .map_err(js_error)
    }
    #[wasm_bindgen(constructor)]
    pub fn new(endpoint: String, legacy: bool) -> Result<Runtime, JsValue> {
        HostRuntime::new(endpoint, legacy)
            .map(Self::wrap)
            .map_err(js_error)
    }
    pub fn discover(&mut self) -> Result<(), JsValue> {
        self.0.discover().map_err(js_error)
    }
    pub fn reset_transport(&mut self) -> Result<(), JsValue> {
        self.0.reset_transport().map_err(js_error)
    }
    /// Non-consuming evidence for the owning host's opt-in session lifecycle.
    /// Keep operation IDs as decimal strings, including outside JS safe integers.
    pub fn http_session_status(&self) -> String {
        serde_json::json!({
            "expired_at_ms": self.0.machine().http_session_expired_at(),
            "last_tool_result": self.0.machine().last_tool_result().map(|id| id.to_string()),
        })
        .to_string()
    }
    pub fn endpoint(&self) -> String {
        self.0.endpoint().into()
    }
    pub fn catalog_status(&self, scope: &str) -> Result<String, JsValue> {
        self.0
            .catalog_status(scope)
            .map(|status| serde_json::to_string(&status).unwrap())
            .map_err(js_error)
    }
    /// Destructive latest-state read for the owning host, independent of events().
    pub fn catalog_change(&mut self) -> String {
        use std::task::Poll;
        match self.1.try_changed() {
            Poll::Pending => serde_json::json!({"kind":"pending"}),
            Poll::Ready(None) => serde_json::json!({"kind":"closed"}),
            Poll::Ready(Some(change)) => serde_json::json!({"kind":"changed", "published":change.published, "invalidated":change.invalidated}),
        }.to_string()
    }
    pub fn accept_authorization(&mut self, scope: &str) -> Result<bool, JsValue> {
        self.0.accept_authorization(scope).map_err(js_error)
    }
    pub fn refresh_catalog(&mut self) -> Result<(), JsValue> {
        self.0.refresh_catalog().map_err(js_error)
    }
    pub fn set_catalog_ttl(&mut self, ttl_ms: f64) -> Result<(), JsValue> {
        if !ttl_ms.is_finite()
            || ttl_ms.fract() != 0.0
            || !(0.0..=9_007_199_254_740_991.0).contains(&ttl_ms)
        {
            return Err(js_error(mcpx_core::Error::new(
                "InvalidConfig",
                "catalog TTL must be a nonnegative safe integer",
            )));
        }
        self.0.set_catalog_ttl(ttl_ms as u64);
        Ok(())
    }
    pub fn advance_time(&mut self, now_ms: f64) -> Result<(), JsValue> {
        if !now_ms.is_finite()
            || now_ms.fract() != 0.0
            || !(0.0..=9_007_199_254_740_991.0).contains(&now_ms)
        {
            return Err(js_error(mcpx_core::Error::new(
                "InvalidClock",
                "clock must be a nonnegative safe integer",
            )));
        }
        self.0.advance_time(now_ms as u64).map_err(js_error)
    }
    pub fn set_elicitation(&mut self, form: bool, url: bool) -> Result<(), JsValue> {
        self.0.set_elicitation(form, url).map_err(js_error)
    }
    pub fn resume(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        responses: &str,
    ) -> Result<(), JsValue> {
        self.0.resume(host_id(id)?, responses).map_err(js_error)
    }
    pub fn release_continuation(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0.release_continuation(host_id(id)?);
        Ok(())
    }
    pub fn bind(&mut self, name: &str) -> Result<f64, JsValue> {
        self.0.bind(name).map(|id| id as f64).map_err(js_error)
    }
    pub fn release_binding(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0.release_binding(host_id(id)?);
        Ok(())
    }
    pub fn tools(&self) -> String {
        self.0.tools()
    }
    pub fn set_schema_registry(&mut self, resources: &str) -> Result<(), JsValue> {
        self.0.set_schema_registry_json(resources).map_err(js_error)
    }
    pub fn schema_registry_stats(&self) -> String {
        serde_json::to_string(&self.0.schema_registry_stats()).expect("schema registry stats")
    }
    pub fn validator_cache_stats(&self) -> String {
        serde_json::to_string(&self.0.validator_cache_stats()).expect("validator cache stats")
    }
    pub fn clear_validator_cache(&mut self) {
        self.0.clear_validator_cache();
    }
    pub fn set_validator_cache_capacity(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] capacity: &JsValue,
    ) -> Result<(), JsValue> {
        let capacity = capacity
            .as_f64()
            .filter(|v| v.is_finite() && v.fract() == 0.0 && *v >= 0.0 && *v <= 4096.0)
            .ok_or_else(|| {
                js_error(mcpx_core::Error::new(
                    "ConfigurationError",
                    "validator cache capacity must be 0..4096",
                ))
            })?;
        self.0
            .set_validator_cache_capacity(capacity as usize)
            .map_err(js_error)
    }
    pub fn command_spec(&self, tool: &str) -> Result<String, JsValue> {
        self.0
            .command_spec(tool)
            .map(|value| serde_json::to_string(&value).expect("command spec JSON"))
            .map_err(js_error)
    }
    pub fn complete_line(
        &self,
        line: &str,
        #[wasm_bindgen(unchecked_param_type = "number")] cursor: &JsValue,
    ) -> Result<String, JsValue> {
        let cursor = cursor
            .as_f64()
            .filter(|v| {
                v.is_finite()
                    && v.fract() == 0.0
                    && *v >= 0.0
                    && *v <= mcpx_command::MAX_COMPLETION_INPUT_BYTES as f64
            })
            .ok_or_else(|| {
                js_error(mcpx_core::Error::new(
                    "InvalidCursor",
                    "completion cursor must be a UTF-8 byte boundary",
                ))
            })? as usize;
        self.0
            .complete_line(line, cursor)
            .map(|value| serde_json::to_string(&value).expect("completion JSON"))
            .map_err(js_error)
    }
    pub fn validate(
        &self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        args: &str,
    ) -> Result<String, JsValue> {
        self.0.validate(host_id(id)?, args).map_err(js_error)
    }
    pub fn call(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        args: &str,
    ) -> Result<(), JsValue> {
        self.0.call(host_id(id)?, args).map_err(js_error)
    }
    pub fn call_concurrent(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        args: &str,
    ) -> Result<f64, JsValue> {
        self.0
            .call_concurrent(host_id(id)?, args)
            .map(|id| id as f64)
            .map_err(js_error)
    }
    pub fn completion(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] operation_id: &JsValue,
    ) -> Result<Option<String>, JsValue> {
        self.0.completion(host_id(operation_id)?).map_err(js_error)
    }
    pub fn cancel_operation(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] operation_id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0
            .cancel_operation(host_id(operation_id)?)
            .map_err(js_error)
    }
    pub fn resume_concurrent(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        responses: &str,
    ) -> Result<f64, JsValue> {
        self.0
            .resume_concurrent(host_id(id)?, responses)
            .map(|id| id as f64)
            .map_err(js_error)
    }
    pub fn command(&mut self, tool: &str, argv: &str) -> Result<Option<String>, JsValue> {
        let argv: Vec<String> =
            serde_json::from_str(argv).map_err(|_| JsValue::from_str("invalid argv JSON"))?;
        self.0.command(tool, &argv).map_err(js_error)
    }
    pub fn command_line(&mut self, line: &str) -> Result<Option<String>, JsValue> {
        self.0.command_line(line).map_err(js_error)
    }
    pub fn command_concurrent(&mut self, tool: &str, argv: &str) -> Result<String, JsValue> {
        let argv: Vec<String> = serde_json::from_str(argv)
            .map_err(|_| js_error(mcpx_core::Error::new("InvalidJson", "invalid argv JSON")))?;
        self.0
            .command_concurrent(tool, &argv)
            .map(|value| value.to_json())
            .map_err(js_error)
    }
    pub fn command_line_concurrent(&mut self, line: &str) -> Result<String, JsValue> {
        self.0
            .command_line_concurrent(line)
            .map(|value| value.to_json())
            .map_err(js_error)
    }
    pub fn actions(&mut self) -> String {
        self.0.actions()
    }
    pub fn set_event_limits(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] capacity: &JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] max_bytes: &JsValue,
    ) -> Result<(), JsValue> {
        let integer = |value: &JsValue, low: f64, high: f64| {
            value
                .as_f64()
                .filter(|value| value.fract() == 0.0 && (low..=high).contains(value))
        };
        let invalid = || {
            js_error(mcpx_core::Error::new(
                "ConfigurationError",
                "invalid event limits",
            ))
        };
        let capacity = integer(capacity, 4.0, 4096.0).ok_or_else(invalid)? as usize;
        let max_bytes = integer(max_bytes, 49152.0, 67108864.0).ok_or_else(invalid)? as usize;
        self.0
            .set_event_limits(mcpx_core::EventLimits {
                capacity,
                max_bytes,
            })
            .map_err(js_error)
    }
    pub fn event_stats(&self) -> String {
        serde_json::to_string(&self.0.event_stats()).unwrap()
    }
    pub fn events(&mut self) -> String {
        self.0.events()
    }
    pub fn outcome(&mut self) -> String {
        self.0.outcome()
    }
    pub fn busy(&self) -> bool {
        self.0.busy()
    }
    pub fn cancel(&mut self) {
        self.0.cancel();
    }
    pub fn fail(&mut self, message: &str) {
        self.0.fail(message);
    }
    pub fn fail_request(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        message: &str,
    ) -> Result<(), JsValue> {
        self.0
            .fail_request(host_id(id)?, mcpx_core::Error::new("HostError", message))
            .map_err(js_error)
    }
    pub fn timeout_request(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0
            .fail_request(
                host_id(id)?,
                mcpx_core::Error::new(
                    "Timeout",
                    "operation deadline exceeded; execution may be unknown",
                ),
            )
            .map_err(js_error)
    }
    pub fn begin_response(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] status: &JsValue,
        headers: &str,
    ) -> Result<bool, JsValue> {
        let id = host_id(id)?;
        let status = status
            .as_f64()
            .filter(|value| value.fract() == 0.0 && (100.0..=599.0).contains(value))
            .ok_or_else(|| {
                js_error(mcpx_core::Error::new(
                    "InvalidResponse",
                    "HTTP status must be an integer in 100..599",
                ))
            })? as u16;
        self.0.begin_response(id, status, headers).map_err(js_error)
    }
    pub fn push_response(&mut self, bytes: &[u8]) -> Result<bool, JsValue> {
        self.0.push_response(bytes).map_err(js_error)
    }
    pub fn push_response_for(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
        bytes: &[u8],
    ) -> Result<bool, JsValue> {
        self.0
            .push_response_for(host_id(id)?, bytes)
            .map_err(js_error)
    }
    pub fn finish_response(&mut self) -> Result<(), JsValue> {
        self.0.finish_response().map_err(js_error)
    }
    pub fn finish_response_for(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] id: &JsValue,
    ) -> Result<(), JsValue> {
        self.0.finish_response_for(host_id(id)?).map_err(js_error)
    }
}

#[cfg(test)]
mod tests {
    use super::exact_host_id;

    #[test]
    fn handles_are_exact_above_u32_and_invalid_numbers_never_alias() {
        assert_eq!(exact_host_id(4_294_967_297.0), Some(4_294_967_297));
        assert_eq!(
            exact_host_id(mcpx_core::MAX_HOST_ID as f64),
            Some(mcpx_core::MAX_HOST_ID)
        );
        for value in [
            0.0,
            -1.0,
            1.5,
            f64::NAN,
            f64::INFINITY,
            9_007_199_254_740_992.0,
        ] {
            assert_eq!(exact_host_id(value), None);
        }
    }
}
