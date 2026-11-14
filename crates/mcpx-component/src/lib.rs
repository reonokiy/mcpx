use mcpx_command::{CommandRuntimeExt, HostRuntime};
use std::cell::RefCell;

wit_bindgen::generate!({path: "../../wit", world: "mcpx"});

use exports::mcpx::runtime::engine as abi;

mod convert;

struct Component;
struct Remote(RefCell<HostRuntime>, RefCell<mcpx_core::CatalogWatcher>);

fn notification_request(request: mcpx_core::NotificationRequest) -> abi::NotificationRequest {
    abi::NotificationRequest {
        resource_id: request.resource_id,
        rpc_id: request.rpc_id,
        method: if request.method == "GET" {
            abi::HttpMethod::Get
        } else {
            abi::HttpMethod::Post
        },
        url: request.url,
        headers: request
            .headers
            .into_iter()
            .map(|(name, value)| abi::Header { name, value })
            .collect(),
        body: request.body.map(|value| value.to_string().into_bytes()),
    }
}

fn error(error: mcpx_core::Error) -> abi::RuntimeError {
    abi::RuntimeError {
        operation_id: error.operation_id,
        queue_operation_id: error.queue_operation_id,
        execution_certainty: match error.execution_certainty {
            mcpx_core::ExecutionCertainty::NotSent => abi::ExecutionCertainty::NotSent,
            mcpx_core::ExecutionCertainty::Unknown => abi::ExecutionCertainty::Unknown,
            mcpx_core::ExecutionCertainty::ResultReceived => {
                abi::ExecutionCertainty::ResultReceived
            }
            mcpx_core::ExecutionCertainty::NeedsInput => abi::ExecutionCertainty::NeedsInput,
        },
        retry_advice: match error.retry_advice {
            mcpx_core::RetryAdvice::None => abi::RetryAdvice::None,
            mcpx_core::RetryAdvice::CorrectInput => abi::RetryAdvice::CorrectInput,
            mcpx_core::RetryAdvice::RefreshBinding => abi::RetryAdvice::RefreshBinding,
            mcpx_core::RetryAdvice::RefreshAuthentication => {
                abi::RetryAdvice::RefreshAuthentication
            }
            mcpx_core::RetryAdvice::ExplicitPolicy => abi::RetryAdvice::ExplicitPolicy,
            mcpx_core::RetryAdvice::InspectExecution => abi::RetryAdvice::InspectExecution,
            mcpx_core::RetryAdvice::InspectResult => abi::RetryAdvice::InspectResult,
            mcpx_core::RetryAdvice::ResumeInput => abi::RetryAdvice::ResumeInput,
        },
        code: error.code,
        message: error.message,
        issues: error
            .issues
            .into_iter()
            .map(|issue| abi::ValidationIssue {
                instance_path: issue.instance_path,
                schema_path: issue.schema_path,
            })
            .collect(),
    }
}
fn command_admission(
    value: mcpx_command::CommandAdmission,
) -> exports::mcpx::runtime::engine::CommandAdmission {
    use exports::mcpx::runtime::engine::CommandAdmission;
    match value {
        mcpx_command::CommandAdmission::Help(text) => CommandAdmission::Help(text),
        mcpx_command::CommandAdmission::Operation(id) => CommandAdmission::Operation(id),
    }
}

impl Remote {
    fn new(runtime: HostRuntime) -> Self {
        let watcher = runtime
            .machine()
            .catalog()
            .revision_source()
            .subscribe()
            .expect("new runtime");
        Self(RefCell::new(runtime), RefCell::new(watcher))
    }
    fn read<T>(&self, f: impl FnOnce(&HostRuntime) -> T) -> T {
        f(&self.0.borrow())
    }
    fn edit<T>(
        &self,
        f: impl FnOnce(&mut HostRuntime) -> mcpx_core::Result<T>,
    ) -> Result<T, abi::RuntimeError> {
        let mut state = self.0.borrow_mut();
        f(&mut state).map_err(error)
    }
}

impl exports::mcpx::runtime::engine::Guest for Component {
    type Remote = Remote;
    fn create_auto(
        endpoint: String,
    ) -> Result<exports::mcpx::runtime::engine::Remote, abi::RuntimeError> {
        let runtime = HostRuntime::auto(endpoint).map_err(error)?;
        Ok(exports::mcpx::runtime::engine::Remote::new(Remote::new(
            runtime,
        )))
    }
    fn create(
        endpoint: String,
        legacy: bool,
    ) -> Result<exports::mcpx::runtime::engine::Remote, abi::RuntimeError> {
        let runtime = HostRuntime::new(endpoint, legacy).map_err(error)?;
        Ok(exports::mcpx::runtime::engine::Remote::new(Remote::new(
            runtime,
        )))
    }
}

impl exports::mcpx::runtime::engine::GuestRemote for Remote {
    fn enable_response_resumption(&self, id: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.enable_response_resumption(id))
    }
    fn interrupt_response_for(&self, id: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.interrupt_response_for(id))
    }
    fn response_resume(&self, id: u64) -> Result<abi::ResponseResume, abi::RuntimeError> {
        self.read(|r| r.response_resume(id))
            .map_err(error)
            .map(|info| abi::ResponseResume {
                action_id: info.action_id,
                rpc_id: info.rpc_id,
                operation_id: info.operation_id,
                retry_after_ms: info.retry_after_ms,
            })
    }
    fn resume_response(&self, id: u64) -> Result<abi::ResponseResumeRequest, abi::RuntimeError> {
        self.edit(|r| r.resume_response(id))
            .map(|plan| abi::ResponseResumeRequest {
                action_id: plan.action_id,
                rpc_id: plan.rpc_id,
                operation_id: plan.operation_id,
                method: abi::HttpMethod::Get,
                url: plan.url,
                headers: plan
                    .headers
                    .into_iter()
                    .map(|(name, value)| abi::Header { name, value })
                    .collect(),
                body: None,
            })
    }
    fn response_replies(&self, id: u64) -> Result<Vec<String>, abi::RuntimeError> {
        self.edit(|r| r.response_replies(id))
            .map(|replies| replies.into_iter().map(|v| v.to_string()).collect())
    }
    fn release_response_resumption(&self, id: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.release_response_resumption(id))
    }
    fn exhaust_response_resumption(&self, id: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.exhaust_response_resumption(id))
    }
    fn notification_resume(&self, id: u64) -> Result<abi::NotificationResume, abi::RuntimeError> {
        self.read(|r| r.notification_resume(id))
            .map_err(error)
            .map(|info| abi::NotificationResume {
                resource_id: info.resource_id,
                retry_after_ms: info.retry_after_ms,
            })
    }
    fn resume_notifications(&self, id: u64) -> Result<abi::NotificationRequest, abi::RuntimeError> {
        self.edit(|r| r.resume_notifications(id))
            .map(notification_request)
    }
    fn start_notifications(&self) -> Result<abi::NotificationRequest, abi::RuntimeError> {
        self.edit(HostRuntime::start_notifications)
            .map(notification_request)
    }
    fn notification_status(&self) -> Option<abi::NotificationStatus> {
        self.read(HostRuntime::notification_status)
            .map(|status| abi::NotificationStatus {
                resource_id: status.resource_id,
                rpc_id: status.rpc_id,
                error_code: status.error_code,
                phase: match status.phase {
                    mcpx_core::NotificationPhase::Opening => abi::NotificationPhase::Opening,
                    mcpx_core::NotificationPhase::Listening => abi::NotificationPhase::Listening,
                    mcpx_core::NotificationPhase::Closed => abi::NotificationPhase::Closed,
                    mcpx_core::NotificationPhase::Failed => abi::NotificationPhase::Failed,
                    mcpx_core::NotificationPhase::Cancelled => abi::NotificationPhase::Cancelled,
                },
            })
    }
    fn begin_notifications(
        &self,
        id: u64,
        status: u16,
        content_type: String,
    ) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.begin_notifications(id, status, &content_type))
    }
    fn push_notifications(&self, id: u64, bytes: Vec<u8>) -> Result<bool, abi::RuntimeError> {
        self.edit(|r| r.push_notifications(id, &bytes))
    }
    fn finish_notifications(&self, id: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.finish_notifications(id))
    }
    fn stop_notifications(&self, id: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.stop_notifications(id))
    }
    fn abort_notifications(&self, id: u64, timeout: bool) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.abort_notifications(id, timeout))
    }
    fn notification_replies(&self, id: u64) -> Result<Vec<String>, abi::RuntimeError> {
        self.edit(|r| r.notification_replies(id))
            .map(|replies| replies.into_iter().map(|value| value.to_string()).collect())
    }
    fn catalog_change(&self) -> abi::CatalogPoll {
        use std::task::Poll;
        match self.1.borrow_mut().try_changed() {
            Poll::Pending => abi::CatalogPoll::Pending,
            Poll::Ready(None) => abi::CatalogPoll::Closed,
            Poll::Ready(Some(change)) => abi::CatalogPoll::Changed(abi::CatalogChange {
                published: change.published,
                invalidated: change.invalidated,
            }),
        }
    }
    fn validator_cache_stats(&self) -> abi::ValidatorCacheStats {
        let s = self.read(HostRuntime::validator_cache_stats);
        abi::ValidatorCacheStats {
            capacity: s.capacity as u32,
            entries: s.entries as u32,
            hits: s.hits,
            misses: s.misses,
            compilations: s.compilations,
            evictions: s.evictions,
            counters_saturated: s.counters_saturated,
        }
    }
    fn set_validator_cache_capacity(&self, capacity: u32) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.set_validator_cache_capacity(capacity as usize))
    }
    fn clear_validator_cache(&self) {
        self.0.borrow_mut().clear_validator_cache();
    }
    fn set_schema_registry(
        &self,
        resources: Vec<abi::SchemaResource>,
    ) -> Result<(), abi::RuntimeError> {
        let registry = mcpx_core::SchemaRegistry::from_resources(resources.into_iter().map(|r| {
            mcpx_core::SchemaResource {
                uri: r.uri,
                schema_json: r.schema_json,
            }
        }))
        .map_err(error)?;
        self.edit(|r| r.set_schema_registry(registry))
    }
    fn schema_registry_stats(&self) -> abi::SchemaRegistryStats {
        let stats = self.read(HostRuntime::schema_registry_stats);
        abi::SchemaRegistryStats {
            resources: stats.resources as u32,
            bytes: stats.bytes as u64,
            schema_nodes: stats.schema_nodes as u32,
            patterns: stats.patterns as u32,
            pattern_bytes: stats.pattern_bytes as u32,
        }
    }
    fn command_spec(&self, tool: String) -> Result<abi::CommandSpec, abi::RuntimeError> {
        self.read(|r| r.command_spec(&tool))
            .map(|spec| abi::CommandSpec {
                tool: spec.tool,
                description: spec.description,
                json_only: spec.json_only,
                parameters: spec
                    .parameters
                    .into_iter()
                    .map(|p| abi::ParameterSpec {
                        name: p.name,
                        flag: p.flag,
                        description: p.description,
                        value_kind: p.value_kind,
                        required: p.required,
                        values: p.values,
                    })
                    .collect(),
            })
            .map_err(error)
    }
    fn complete_line(
        &self,
        line: String,
        cursor: u32,
    ) -> Result<abi::CommandCompletion, abi::RuntimeError> {
        self.read(|r| r.complete_line(&line, cursor as usize))
            .map(|c| abi::CommandCompletion {
                token: c.token,
                start: c.start as u32,
                end: c.end as u32,
                truncated: c.truncated,
                candidates: c
                    .candidates
                    .into_iter()
                    .map(|v| abi::CompletionCandidate {
                        value: v.value,
                        replacement: v.replacement,
                        description: v.description,
                        kind: match v.kind {
                            mcpx_command::CandidateKind::Tool => abi::CandidateKind::Tool,
                            mcpx_command::CandidateKind::Option => abi::CandidateKind::Option,
                            mcpx_command::CandidateKind::Value => abi::CandidateKind::Value,
                        },
                    })
                    .collect(),
            })
            .map_err(error)
    }
    fn endpoint(&self) -> String {
        self.read(|r| r.endpoint().into())
    }
    fn catalog_status(
        &self,
        scope: String,
    ) -> Result<exports::mcpx::runtime::engine::CatalogStatus, abi::RuntimeError> {
        self.read(|r| r.catalog_status(&scope))
            .map(|status| exports::mcpx::runtime::engine::CatalogStatus {
                authorization_changed: status.authorization_changed,
                needs_refresh: status.needs_refresh,
                requires_exclusive: status.requires_exclusive,
            })
            .map_err(error)
    }
    fn accept_authorization(&self, scope: String) -> Result<bool, abi::RuntimeError> {
        self.edit(|r| r.accept_authorization(&scope))
    }
    fn refresh_catalog(&self) -> Result<(), abi::RuntimeError> {
        self.edit(HostRuntime::refresh_catalog)
    }
    fn set_catalog_ttl(&self, ttl_ms: u64) {
        self.0.borrow_mut().set_catalog_ttl(ttl_ms);
    }
    fn discover(&self) -> Result<(), abi::RuntimeError> {
        self.edit(HostRuntime::discover)
    }
    fn reset_transport(&self) -> Result<(), abi::RuntimeError> {
        self.edit(HostRuntime::reset_transport)
    }
    fn http_session_status(&self) -> abi::HttpSessionStatus {
        self.read(|runtime| abi::HttpSessionStatus {
            expired_at_ms: runtime.machine().http_session_expired_at(),
            last_tool_result: runtime.machine().last_tool_result(),
        })
    }
    fn advance_time(&self, now_ms: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.advance_time(now_ms))
    }
    fn set_elicitation(&self, form: bool, url: bool) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.set_elicitation(form, url))
    }
    fn resume(&self, id: u64, responses: String) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.resume(id, &responses))
    }
    fn release_continuation(&self, id: u64) {
        let _ = self.edit(|r| {
            r.release_continuation(id);
            Ok(())
        });
    }
    fn bind(&self, name: String) -> Result<u64, abi::RuntimeError> {
        self.edit(|r| r.bind(&name))
    }
    fn release_binding(&self, id: u64) {
        let _ = self.edit(|r| {
            r.release_binding(id);
            Ok(())
        });
    }
    fn tools(&self) -> Vec<abi::ToolDescriptor> {
        self.read(|r| {
            r.machine()
                .catalog()
                .tools()
                .cloned()
                .map(convert::tool)
                .collect()
        })
    }
    fn validate(&self, id: u64, args: String) -> Result<String, abi::RuntimeError> {
        self.edit(|r| r.validate(id, &args))
    }
    fn call(&self, id: u64, args: String) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.call(id, &args))
    }
    fn call_concurrent(&self, id: u64, args: String) -> Result<u64, abi::RuntimeError> {
        self.edit(|r| r.call_concurrent(id, &args))
    }
    fn completion(&self, operation_id: u64) -> Result<Option<abi::CallOutcome>, abi::RuntimeError> {
        self.edit(|r| r.take_completion(operation_id).transpose())
            .map(|outcome| outcome.map(convert::outcome))
    }
    fn cancel_operation(&self, operation_id: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.cancel_operation(operation_id))
    }
    fn resume_concurrent(&self, id: u64, responses: String) -> Result<u64, abi::RuntimeError> {
        self.edit(|r| r.resume_concurrent(id, &responses))
    }
    fn command(
        &self,
        tool: String,
        argv: Vec<String>,
    ) -> Result<Option<String>, abi::RuntimeError> {
        self.edit(|r| r.command(&tool, &argv))
    }
    fn command_line(&self, line: String) -> Result<Option<String>, abi::RuntimeError> {
        self.edit(|r| r.command_line(&line))
    }
    fn command_concurrent(
        &self,
        tool: String,
        argv: Vec<String>,
    ) -> Result<exports::mcpx::runtime::engine::CommandAdmission, abi::RuntimeError> {
        self.edit(|r| r.command_concurrent(&tool, &argv))
            .map(command_admission)
    }
    fn command_line_concurrent(
        &self,
        line: String,
    ) -> Result<exports::mcpx::runtime::engine::CommandAdmission, abi::RuntimeError> {
        self.edit(|r| r.command_line_concurrent(&line))
            .map(command_admission)
    }
    fn actions(&self) -> Vec<abi::HostAction> {
        self.0
            .borrow_mut()
            .take_actions()
            .into_iter()
            .map(convert::action)
            .collect()
    }
    fn events(&self) -> abi::EventBatch {
        let batch = self.0.borrow_mut().take_events();
        abi::EventBatch {
            events: batch.events.into_iter().map(convert::event).collect(),
            dropped: batch.dropped,
        }
    }
    fn set_event_limits(&self, capacity: u32, max_bytes: u32) -> Result<(), abi::RuntimeError> {
        self.edit(|r| {
            r.set_event_limits(mcpx_core::EventLimits {
                capacity: capacity as usize,
                max_bytes: max_bytes as usize,
            })
        })
    }
    fn event_stats(&self) -> abi::EventStats {
        let stats = self.read(HostRuntime::event_stats);
        abi::EventStats {
            records: stats.records as u32,
            bytes: stats.bytes as u64,
            reserved_terminals: stats.reserved_terminals as u32,
            dropped: stats.dropped,
            sequence: stats.sequence,
        }
    }
    fn outcome(&self) -> Option<abi::CallOutcome> {
        self.0.borrow_mut().take_outcome().map(convert::outcome)
    }
    fn busy(&self) -> bool {
        self.read(HostRuntime::busy)
    }
    fn cancel(&self) {
        let _ = self.edit(|r| {
            r.cancel();
            Ok(())
        });
    }
    fn fail(&self, message: String) {
        let _ = self.edit(|r| {
            r.fail(&message);
            Ok(())
        });
    }
    fn fail_request(&self, id: u64, message: String) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.fail_request(id, mcpx_core::Error::new("HostError", message)))
    }
    fn timeout_request(&self, id: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| {
            r.fail_request(
                id,
                mcpx_core::Error::new(
                    "Timeout",
                    "operation deadline exceeded; execution may be unknown",
                ),
            )
        })
    }
    fn begin_response(
        &self,
        id: u64,
        status: u16,
        headers: Vec<abi::Header>,
    ) -> Result<bool, abi::RuntimeError> {
        self.edit(|r| {
            r.begin_response_headers(
                id,
                status,
                headers
                    .into_iter()
                    .map(|header| (header.name, header.value)),
            )
        })
    }
    fn push_response(&self, bytes: Vec<u8>) -> Result<bool, abi::RuntimeError> {
        self.edit(|r| r.push_response(&bytes))
    }
    fn push_response_for(&self, id: u64, bytes: Vec<u8>) -> Result<bool, abi::RuntimeError> {
        self.edit(|r| r.push_response_for(id, &bytes))
    }
    fn finish_response(&self) -> Result<(), abi::RuntimeError> {
        self.edit(HostRuntime::finish_response)
    }
    fn finish_response_for(&self, id: u64) -> Result<(), abi::RuntimeError> {
        self.edit(|r| r.finish_response_for(id))
    }
}

#[cfg(target_arch = "wasm32")]
export!(Component);
