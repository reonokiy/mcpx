use mcpx_core::{
    Action, CallOutcome, Client, Credential, Driver, DriverContext, HostRuntime, OutputValidation,
    Result,
};
use serde_json::json;
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    task::{Context, Poll, Waker},
};

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("in-memory driver must be ready"),
    }
}

struct RetryClose {
    attempts: usize,
    pending: bool,
    abort_pending: bool,
}
impl Driver for RetryClose {
    async fn prepare(&mut self) -> Result<DriverContext> {
        panic!("closed clients must not resolve credentials")
    }
    fn now_ms(&self) -> u64 {
        0
    }
    async fn execute(&mut self, _: Action, _: &mut HostRuntime) -> Result<()> {
        panic!("closed clients must not send requests")
    }
    async fn abort(&mut self, _: &mut HostRuntime, _: &mcpx_core::Error) {
        if self.abort_pending {
            std::future::pending().await
        }
    }
    async fn close(&mut self, _: &mut HostRuntime) -> Result<()> {
        self.attempts += 1;
        if self.attempts == 1 {
            if self.pending {
                return std::future::pending().await;
            }
            return Err(mcpx_core::Error::new(
                "SessionCleanupFailed",
                "fixture cleanup failed",
            ));
        }
        Ok(())
    }
}
#[test]
fn failed_cleanup_remains_retryable_while_new_operations_stay_closed() {
    let mut client = Client::new(
        RetryClose {
            attempts: 0,
            pending: false,
            abort_pending: false,
        },
        HostRuntime::auto("https://example.com/mcp".into()).unwrap(),
    );
    assert_eq!(
        ready(client.close()).unwrap_err().code,
        "SessionCleanupFailed"
    );
    assert_eq!(
        ready(client.fresh_tool("echo")).err().unwrap().code,
        "TransportClosed"
    );
    ready(client.close()).unwrap();
    ready(client.close()).unwrap();
    assert_eq!(client.driver().attempts, 2);
}
#[test]
fn dropped_cleanup_can_be_retried_without_reopening_the_client() {
    let mut client = Client::new(
        RetryClose {
            attempts: 0,
            pending: true,
            abort_pending: false,
        },
        HostRuntime::auto("https://example.com/mcp".into()).unwrap(),
    );
    {
        let future = client.close();
        let mut future = std::pin::pin!(future);
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(
        ready(client.fresh_tool("echo")).err().unwrap().code,
        "TransportClosed"
    );
    ready(client.close()).unwrap();
    ready(client.close()).unwrap();
    assert_eq!(client.driver().attempts, 2);
}

#[test]
fn dropping_close_during_request_abort_does_not_reopen_the_client() {
    let mut client = Client::new(
        RetryClose {
            attempts: 1,
            pending: false,
            abort_pending: true,
        },
        HostRuntime::auto("https://example.com/mcp".into()).unwrap(),
    );
    client.host_runtime_mut().discover().unwrap();
    {
        let future = client.close();
        let mut future = std::pin::pin!(future);
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(
        ready(client.fresh_tool("echo")).err().unwrap().code,
        "TransportClosed"
    );
    ready(client.close()).unwrap();
    ready(client.close()).unwrap();
    assert_eq!(client.driver().attempts, 2);
}

#[derive(Default)]
struct State {
    can_reconnect: Cell<bool>,
    reconnect_pending: Cell<bool>,
    reconnect_failed: Cell<bool>,
    reconnects: Cell<usize>,
    prepares: Cell<usize>,
    finishes: Cell<usize>,
    now: Cell<u64>,
    identity: RefCell<String>,
    methods: RefCell<Vec<String>>,
    bodies: RefCell<Vec<serde_json::Value>>,
    schema_changed: Cell<bool>,
    mode: Cell<u8>,
    stalled: Cell<bool>,
    aborts: Cell<usize>,
}
struct MemoryDriver(Rc<State>);

#[test]
fn shared_host_drive_retains_preparation_and_leaves_outcomes_to_the_owner() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let args = binding.args_from(json!({"message":"shared"})).unwrap();
    let prepares = state.prepares.get();
    let finishes = state.finishes.get();
    client
        .host_runtime_mut()
        .call_bound(&binding, &args)
        .unwrap();
    {
        let (runtime, driver) = client.host_parts_mut();
        ready(mcpx_core::drive_host(driver, runtime)).unwrap();
    }
    assert_eq!(state.prepares.get(), prepares);
    assert_eq!(state.finishes.get(), finishes);
    assert!(!client.host_runtime_mut().busy());
    let receipt = client.machine().last_tool_result();
    assert!(receipt.is_some());
    assert!(matches!(
        client.host_runtime_mut().take_outcome(),
        Some(CallOutcome::Completed(_))
    ));
    assert_eq!(client.machine().last_tool_result(), receipt);
    client.host_runtime_mut().reset_transport().unwrap();
    assert_eq!(client.machine().last_tool_result(), receipt);
}

#[test]
fn shared_host_drive_preserves_failure_context_and_aborts_once() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let args = binding.args_from(json!({"message":"shared"})).unwrap();
    state.mode.set(5);
    let finishes = state.finishes.get();
    let aborts = state.aborts.get();
    client
        .host_runtime_mut()
        .call_bound(&binding, &args)
        .unwrap();
    let e = {
        let (runtime, driver) = client.host_parts_mut();
        ready(mcpx_core::drive_host(driver, runtime)).unwrap_err()
    };
    assert_eq!(e.code, "Timeout");
    assert_eq!(
        e.execution_certainty,
        mcpx_core::ExecutionCertainty::Unknown
    );
    assert!(e.operation_id.is_some());
    assert_eq!(state.finishes.get(), finishes);
    assert_eq!(state.aborts.get(), aborts + 1);
    assert!(!client.host_runtime_mut().busy());
}

#[test]
fn conditional_refresh_reuses_only_a_new_complete_fresh_publication() {
    let (mut client, state) = client();
    let observed = client.catalog_revision();
    let prepares = state.prepares.get();
    assert!(ready(client.refresh_if_unchanged(&observed)).unwrap());
    let requests = state.methods.borrow().len();
    assert!(!ready(client.refresh_if_unchanged(&observed)).unwrap());
    assert_eq!(state.methods.borrow().len(), requests);
    assert_eq!(state.prepares.get(), prepares + 2);
    // A new explicit refresh submitted after publication must still do I/O.
    let current = client.catalog_revision();
    assert!(ready(client.refresh_if_unchanged(&current)).unwrap());
    state.now.set(500);
    assert!(ready(client.refresh_if_unchanged(&observed)).unwrap());
    client.set_catalog_ttl(0);
    assert!(ready(client.refresh_if_unchanged(&observed)).unwrap());
    assert!(ready(client.refresh_if_unchanged(&observed)).unwrap());
}

#[test]
fn conditional_refresh_does_not_share_failed_or_changed_context_results() {
    let (mut client, state) = client();
    let observed = client.catalog_revision();
    state.mode.set(7);
    assert_eq!(
        ready(client.refresh_if_unchanged(&observed))
            .unwrap_err()
            .code,
        "HostError"
    );
    assert_eq!(client.catalog_revision(), observed);
    state.mode.set(0);
    assert!(ready(client.refresh_if_unchanged(&observed)).unwrap());
    assert!(!ready(client.refresh_if_unchanged(&observed)).unwrap());
    *state.identity.borrow_mut() = "another owner".into();
    assert!(ready(client.refresh_if_unchanged(&observed)).unwrap());
    // Even a fresh publication under the new identity cannot satisfy old tokens.
    assert!(ready(client.refresh_if_unchanged(&observed)).unwrap());
    let current = client.catalog_revision();
    ready(client.refresh()).unwrap();
    ready(client.set_schema_registry(mcpx_core::SchemaRegistry::default())).unwrap();
    assert!(ready(client.refresh_if_unchanged(&current)).unwrap());
    assert!(ready(client.refresh_if_unchanged(&current)).unwrap());
}

#[test]
fn foreign_refresh_token_is_rejected_before_credentials_or_transport() {
    let (other, _) = client();
    let (mut client, state) = client();
    let prepares = state.prepares.get();
    let requests = state.methods.borrow().len();
    assert_eq!(
        ready(client.refresh_if_unchanged(&other.catalog_revision()))
            .unwrap_err()
            .code,
        "CatalogMismatch"
    );
    assert_eq!(state.prepares.get(), prepares);
    assert_eq!(state.methods.borrow().len(), requests);
}

#[test]
fn final_close_ends_catalog_watch_even_when_driver_cleanup_fails() {
    let mut client = Client::new(
        RetryClose {
            attempts: 0,
            pending: false,
            abort_pending: false,
        },
        HostRuntime::auto("https://example.com/mcp".into()).unwrap(),
    );
    let mut watch = client.subscribe_catalog().unwrap();
    assert!(ready(client.close()).is_err());
    assert!(ready(watch.changed()).is_none());
    assert_eq!(
        client.subscribe_catalog().err().unwrap().code,
        "TransportClosed"
    );
}
impl Driver for MemoryDriver {
    fn finish(&mut self) {
        self.0.finishes.set(self.0.finishes.get() + 1);
    }
    fn supports_reconnect(&self) -> bool {
        self.0.can_reconnect.get()
    }
    async fn reconnect(&mut self, _: &mut HostRuntime) -> Result<()> {
        self.0.reconnects.set(self.0.reconnects.get() + 1);
        if self.0.reconnect_pending.get() {
            std::future::pending::<()>().await;
        }
        if self.0.reconnect_failed.get() {
            return Err(mcpx_core::Error::new(
                "HostError",
                "fixture reconnect failed",
            ));
        }
        Ok(())
    }
    async fn prepare(&mut self) -> Result<DriverContext> {
        self.0.prepares.set(self.0.prepares.get() + 1);
        if self.0.mode.get() == 6 {
            return Err(mcpx_core::Error::new(
                "CredentialUnavailable",
                "fixture provider failed",
            ));
        }
        Ok(DriverContext {
            auth_context_id: self.0.identity.borrow().clone(),
        })
    }
    fn now_ms(&self) -> u64 {
        self.0.now.get()
    }
    async fn execute(&mut self, action: Action, runtime: &mut HostRuntime) -> Result<()> {
        let Action::Http { id, body, .. } = action else {
            panic!()
        };
        let method = body["method"].as_str().unwrap();
        self.0.methods.borrow_mut().push(method.into());
        self.0.bodies.borrow_mut().push(body.clone());
        if self.0.stalled.get() {
            std::future::pending::<()>().await;
        }
        if self.0.mode.get() == 5 && method == "tools/call" {
            return Err(mcpx_core::Error::new(
                "Timeout",
                "fixture driver interrupted",
            ));
        }
        if self.0.mode.get() == 7 && method == "tools/list" {
            return Err(mcpx_core::Error::new(
                "HostError",
                "fixture catalog transport failed",
            ));
        }
        if self.0.mode.get() == 8 && method == "tools/call" {
            return Ok(());
        }
        let result = match method {
            "server/discover" => json!({"supportedVersions":["2026-07-28"]}),
            "tools/list" => {
                let mut result = json!({"ttlMs":500,"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":if self.0.schema_changed.get() {"integer"} else {"string"}}},"required":["message"]},"outputSchema":{"type":"object","required":["message"]}}]});
                if self.0.mode.get() == 9 {
                    result["tools"][0]["outputSchema"]["properties"] =
                        json!({"message":{"pattern":r"^(a|a)*\1b"}});
                }
                result
            }
            "tools/call" => match self.0.mode.get() {
                1 => json!({"content":[],"isError":true}),
                2 => json!({"content":[],"structuredContent":{}}),
                3 => {
                    json!({"resultType":"input_required","requestState":"opaque-state"})
                }
                4 if body["params"].get("inputResponses").is_none() => {
                    json!({"resultType":"input_required","requestState":"unchanged","inputRequests":{"confirm":{"method":"elicitation/create","params":{"message":"Continue?","requestedSchema":{"type":"object","properties":{"yes":{"type":"boolean"}},"required":["yes"]}}}}})
                }
                _ => json!({"content":[],"structuredContent":body["params"]["arguments"]}),
            },
            _ => panic!("unexpected method"),
        };
        runtime.begin_response(id, 200, r#"{"content-type":"application/json"}"#)?;
        runtime.push_response(
            json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(id).unwrap(),"result":result})
                .to_string()
                .as_bytes(),
        )?;
        runtime.finish_response()
    }
    async fn abort(&mut self, _runtime: &mut HostRuntime, _cause: &mcpx_core::Error) {
        self.0.aborts.set(self.0.aborts.get() + 1);
    }
}

fn pending_input(client: &mut Client<MemoryDriver>, state: &State) -> mcpx_core::Continuation {
    client.set_elicitation(true, false).unwrap();
    state.mode.set(4);
    let binding = client.tool("echo").unwrap();
    let args = binding
        .args()
        .set("message", "saved")
        .unwrap()
        .finish()
        .unwrap();
    let CallOutcome::NeedsInput(handle) = ready(client.call(&binding, &args)).unwrap() else {
        panic!()
    };
    handle
}

#[test]
fn client_resume_revalidates_a_fresh_catalog_and_preserves_the_original_request() {
    let (mut client, state) = client();
    let handle = pending_input(&mut client, &state);
    state.now.set(500);
    let outcome = ready(client.resume(
        &handle,
        json!({"confirm":{"action":"accept","content":{"yes":true}}}),
    ))
    .unwrap();
    assert!(matches!(outcome, CallOutcome::Completed(_)));
    let bodies = state.bodies.borrow();
    let calls: Vec<_> = bodies
        .iter()
        .filter(|body| body["method"] == "tools/call")
        .collect();
    assert_eq!(calls.len(), 2);
    assert_ne!(calls[0]["id"], calls[1]["id"]);
    assert_eq!(
        calls[0]["params"]["arguments"],
        calls[1]["params"]["arguments"]
    );
    assert_eq!(calls[1]["params"]["requestState"], "unchanged");
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|m| *m == "tools/list")
            .count(),
        2
    );
}

#[test]
fn form_schemas_share_the_owner_cache_and_live_continuations_survive_its_clear() {
    let (mut client, state) = client();
    let first = pending_input(&mut client, &state);
    let before = client.validator_cache_stats();
    let second = pending_input(&mut client, &state);
    let after = client.validator_cache_stats();
    assert_eq!(after.compilations, before.compilations);
    assert_eq!(after.hits, before.hits + 1);
    client.clear_validator_cache();
    assert_eq!(client.validator_cache_stats().entries, 0);
    let result = ready(client.resume(
        &first,
        json!({"confirm":{"action":"accept","content":{"yes":true}}}),
    ))
    .unwrap();
    assert!(matches!(result, CallOutcome::Completed(_)));
    client.release_continuation(&second).unwrap();
}

#[test]
fn client_resume_stops_on_identity_schema_expiry_or_release_without_an_extra_call() {
    for mode in 0..4 {
        let (mut client, state) = client();
        let handle = pending_input(&mut client, &state);
        let expected = match mode {
            0 => {
                *state.identity.borrow_mut() = "other".into();
                "InvalidContinuation"
            }
            1 => {
                state.now.set(500);
                state.schema_changed.set(true);
                "BindingStale"
            }
            2 => {
                state.now.set(300_000);
                "ContinuationExpired"
            }
            _ => {
                client.release_continuation(&handle).unwrap();
                "InvalidContinuation"
            }
        };
        let count = state.methods.borrow().len();
        assert_eq!(
            ready(client.resume(&handle, json!({"confirm":{"action":"cancel"}})))
                .unwrap_err()
                .code,
            expected
        );
        assert_eq!(
            state
                .methods
                .borrow()
                .iter()
                .filter(|m| *m == "tools/call")
                .count(),
            1
        );
        if mode != 1 {
            assert_eq!(state.methods.borrow().len(), count);
        }
    }
}

fn client() -> (Client<MemoryDriver>, Rc<State>) {
    let state = Rc::new(State::default());
    *state.identity.borrow_mut() = "reader".into();
    let mut client = Client::new(
        MemoryDriver(state.clone()),
        HostRuntime::auto("http://localhost/mcp".into()).unwrap(),
    );
    ready(client.refresh()).unwrap();
    (client, state)
}

#[test]
fn prepared_catalog_read_uses_one_context_and_revalidates_ttl_identity_and_failures() {
    let (mut client, state) = client();
    let prepares = state.prepares.get();
    let finishes = state.finishes.get();
    let requests = state.methods.borrow().len();
    let context = ready(client.driver_mut().prepare()).unwrap();
    ready(client.with_prepared_catalog(context, |_| Ok(()))).unwrap();
    assert_eq!(state.prepares.get(), prepares + 1);
    assert_eq!(state.finishes.get(), finishes + 1);
    assert_eq!(state.methods.borrow().len(), requests);
    let old = client.tool("echo").unwrap();
    state.now.set(500);
    state.schema_changed.set(true);
    let context = ready(client.driver_mut().prepare()).unwrap();
    ready(client.with_prepared_catalog(context, |_| Ok(()))).unwrap();
    assert_eq!(state.methods.borrow().len(), requests + 1);
    assert!(
        client
            .tool("echo")
            .unwrap()
            .validate_json(json!({"message":42}))
            .is_ok()
    );
    *state.identity.borrow_mut() = "new identity".into();
    let context = ready(client.driver_mut().prepare()).unwrap();
    ready(client.with_prepared_catalog(context, |_| Ok(()))).unwrap();
    let args = old.validate_json(json!({"message":"old"})).unwrap();
    assert_eq!(
        ready(client.call(&old, &args)).unwrap_err().code,
        "BindingStale"
    );
    state.now.set(1000);
    state.mode.set(7);
    let finishes = state.finishes.get();
    let context = ready(client.driver_mut().prepare()).unwrap();
    assert_eq!(
        ready(client.with_prepared_catalog(context, |_| Ok(())))
            .unwrap_err()
            .code,
        "HostError"
    );
    assert_eq!(state.finishes.get(), finishes + 1);
    assert!(!client.machine().is_busy());
}

#[test]
fn prepared_catalog_busy_and_closed_rejection_cannot_consume_peer_work() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let args = binding
        .validate_json(json!({"message":"in flight"}))
        .unwrap();
    let operation = ready(client.begin_concurrent_call(&binding, &args)).unwrap();
    let context = ready(client.driver_mut().prepare()).unwrap();
    let finishes = state.finishes.get();
    assert_eq!(
        ready(client.with_prepared_catalog(context, |_| Ok(())))
            .unwrap_err()
            .code,
        "Busy"
    );
    assert_eq!(state.finishes.get(), finishes + 1);
    let action = client.host_runtime_mut().take_actions().pop().unwrap();
    let (runtime, driver) = client.host_parts_mut();
    ready(driver.execute(action, runtime)).unwrap();
    assert!(
        client
            .take_concurrent_completion(operation)
            .unwrap()
            .is_ok()
    );
    ready(client.close()).unwrap();
    let requests = state.methods.borrow().len();
    assert_eq!(
        ready(client.with_prepared_catalog(
            DriverContext {
                auth_context_id: "reader".into()
            },
            |_| Ok(())
        ))
        .unwrap_err()
        .code,
        "TransportClosed"
    );
    assert_eq!(state.methods.borrow().len(), requests);
}

#[test]
fn typed_calls_share_preflight_and_do_not_hide_business_or_interactive_states() {
    #[derive(serde::Serialize)]
    struct Input<'a> {
        message: &'a str,
    }
    #[derive(serde::Deserialize)]
    struct Output {
        message: String,
    }
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let before = state.prepares.get();
    let CallOutcome::Completed(response) = ready(client.call_typed(
        &binding,
        Input {
            message: "typed-client",
        },
    ))
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        response.deserialize::<Output>().unwrap().message,
        "typed-client"
    );
    assert_eq!(state.prepares.get(), before + 1);
    let before = state.prepares.get();
    let requests = state.methods.borrow().len();
    assert_eq!(
        ready(client.call_typed(&binding, json!({"message":3})))
            .unwrap_err()
            .code,
        "ValidationFailed"
    );
    assert_eq!(state.prepares.get(), before);
    assert_eq!(state.methods.borrow().len(), requests);
    state.mode.set(1);
    let CallOutcome::Completed(response) = ready(client.call_typed(
        &binding,
        Input {
            message: "tool-failure",
        },
    ))
    .unwrap() else {
        panic!()
    };
    assert!(response.is_error);
    assert_eq!(
        response.deserialize::<Output>().err().unwrap().code,
        "ToolFailure"
    );
    state.mode.set(2);
    let CallOutcome::Completed(response) = ready(client.call_typed(
        &binding,
        Input {
            message: "invalid-output",
        },
    ))
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        response.deserialize::<Output>().err().unwrap().code,
        "OutputValidation"
    );
    state.mode.set(3);
    assert!(matches!(
        ready(client.call_typed(
            &binding,
            Input {
                message: "still-pending"
            }
        ))
        .unwrap(),
        CallOutcome::NeedsInput(_)
    ));
    ready(client.close()).unwrap();
}

#[test]
fn ttl_refresh_uses_tools_list_and_preserves_unchanged_bindings() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let args = binding
        .args()
        .set("message", "same")
        .unwrap()
        .finish()
        .unwrap();
    ready(client.call(&binding, &args)).unwrap();
    state.now.set(499);
    ready(client.call(&binding, &args)).unwrap();
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|method| method.as_str() == "tools/list")
            .count(),
        1
    );
    state.now.set(500);
    ready(client.call(&binding, &args)).unwrap();
    assert_eq!(
        state.methods.borrow().as_slice(),
        [
            "server/discover",
            "tools/list",
            "tools/call",
            "tools/call",
            "tools/list",
            "tools/call"
        ]
    );
}

#[test]
fn local_catalog_reads_resolve_once_refresh_on_expiry_and_never_read_the_old_identity() {
    let (mut client, state) = client();
    let read = |machine: &mcpx_core::Machine| {
        Ok(
            machine.catalog().tools().next().unwrap().input_schema["properties"]["message"]["type"]
                .as_str()
                .unwrap()
                .to_owned(),
        )
    };
    let before = state.prepares.get();
    assert_eq!(ready(client.with_fresh_catalog(read)).unwrap(), "string");
    assert_eq!(state.prepares.get(), before + 1);
    assert_eq!(
        state.methods.borrow().as_slice(),
        ["server/discover", "tools/list"]
    );
    state.schema_changed.set(true);
    state.now.set(500);
    assert_eq!(ready(client.with_fresh_catalog(read)).unwrap(), "integer");
    assert_eq!(state.prepares.get(), before + 2);
    state.schema_changed.set(false);
    *state.identity.borrow_mut() = "another-principal".into();
    assert_eq!(ready(client.with_fresh_catalog(read)).unwrap(), "string");
    assert_eq!(state.prepares.get(), before + 3);
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|s| s.as_str() == "tools/call")
            .count(),
        0
    );
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|s| s.as_str() == "tools/list")
            .count(),
        3
    );
    state.identity.borrow_mut().clear();
    let called = Cell::new(false);
    let result = ready(client.with_fresh_catalog(|_| {
        called.set(true);
        Ok(())
    }));
    assert_eq!(result.unwrap_err().code, "CredentialUnavailable");
    assert!(!called.get());
}

#[test]
fn schema_resource_changes_invalidate_bindings_without_extending_ttl_and_do_not_cross_identity() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    state.now.set(499);
    let registry = mcpx_core::SchemaRegistry::new([(
        "https://schemas.example/public".into(),
        json!({"type":"object"}),
    )])
    .unwrap();
    ready(client.set_schema_registry(registry)).unwrap();
    assert_eq!(client.schema_registry_stats().resources, 1);
    assert_eq!(
        client.machine().catalog().check(&binding).unwrap_err().code,
        "BindingStale"
    );
    state.now.set(500);
    ready(client.with_fresh_catalog(|_| Ok(()))).unwrap();
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|m| m.as_str() == "tools/list")
            .count(),
        2
    );
    *state.identity.borrow_mut() = "other-schema-owner".into();
    ready(client.with_fresh_catalog(|_| Ok(()))).unwrap();
    assert_eq!(client.schema_registry_stats().resources, 0);
}

#[test]
fn changed_schema_after_ttl_refresh_stops_old_arguments_before_call() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let args = binding
        .args()
        .set("message", "old")
        .unwrap()
        .finish()
        .unwrap();
    state.schema_changed.set(true);
    state.now.set(500);
    assert_eq!(
        ready(client.call(&binding, &args)).unwrap_err().code,
        "BindingStale"
    );
    assert!(
        !state
            .methods
            .borrow()
            .iter()
            .any(|method| method == "tools/call")
    );
    let binding = client.tool("echo").unwrap();
    ready(client.call_json(&binding, json!({"message":10}))).unwrap();
}

#[test]
fn authorization_change_invalidates_handles_without_sending_business_requests() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let args = binding
        .args()
        .set("message", "private")
        .unwrap()
        .finish()
        .unwrap();
    *state.identity.borrow_mut() = "other-user".into();
    assert_eq!(
        ready(client.call(&binding, &args)).unwrap_err().code,
        "BindingStale"
    );
    assert_eq!(state.methods.borrow().len(), 2);
    assert!(client.tool("echo").is_err());
    ready(client.refresh()).unwrap();
    let new_binding = client.tool("echo").unwrap();
    assert!(new_binding.check_args(&args).is_err());
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|m| m.as_str() == "server/discover")
            .count(),
        2
    );
}

#[test]
fn checked_calls_preserve_failed_raw_responses_and_unfinished_input() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let args = binding
        .args()
        .set("message", "value")
        .unwrap()
        .finish()
        .unwrap();
    for (mode, code) in [
        (1, "ToolFailure"),
        (2, "OutputValidation"),
        (3, "NeedsInput"),
    ] {
        state.mode.set(mode);
        let error = ready(client.call_checked(&binding, &args)).unwrap_err();
        assert_eq!(error.error.code, code);
        assert!(error.error.operation_id.is_some());
        assert_eq!(
            error.error.execution_certainty,
            if mode == 3 {
                mcpx_core::ExecutionCertainty::NeedsInput
            } else {
                mcpx_core::ExecutionCertainty::ResultReceived
            }
        );
        assert_eq!(
            error.error.retry_advice,
            if mode == 3 {
                mcpx_core::RetryAdvice::ResumeInput
            } else {
                mcpx_core::RetryAdvice::InspectResult
            }
        );
        assert!(error.outcome.is_some());
    }
    state.mode.set(0);
    assert!(matches!(
        ready(client.call_checked(&binding, &args))
            .unwrap()
            .output_validation,
        OutputValidation::Valid
    ));
    assert!(matches!(
        ready(client.call(&binding, &args)).unwrap(),
        CallOutcome::Completed(_)
    ));
}

#[test]
fn dropped_future_can_be_explicitly_cancelled_before_reusing_the_client() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let args = binding
        .args()
        .set("message", "x")
        .unwrap()
        .finish()
        .unwrap();
    state.stalled.set(true);
    {
        let mut future = std::pin::pin!(client.call(&binding, &args));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(
        ready(client.call(&binding, &args)).unwrap_err().code,
        "Busy"
    );
    ready(client.cancel_active());
    assert_eq!(state.aborts.get(), 1);
    state.stalled.set(false);
    ready(client.call(&binding, &args)).unwrap();
    assert_eq!(
        client
            .drain_events()
            .events
            .iter()
            .filter(|event| event.kind == "cancelled")
            .count(),
        1
    );
}

#[test]
fn credential_debug_redacts_bearer_and_invalid_header_values_are_rejected() {
    let credential = Credential::bearer("PRIVATE_TOKEN".into(), "identity".into()).unwrap();
    assert!(!format!("{credential:?}").contains("PRIVATE_TOKEN"));
    assert!(Credential::bearer("bad\r\nvalue".into(), "identity".into()).is_err());
    assert!(Credential::bearer("token".into(), String::new()).is_err());
}

#[test]
fn async_client_admits_sixteen_calls_for_an_external_host_without_forcing_send() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    let args = binding
        .validate_json(json!({"message":"concurrent"}))
        .unwrap();
    let mut operations = Vec::new();
    for _ in 0..16 {
        operations.push(ready(client.begin_concurrent_call(&binding, &args)).unwrap());
    }
    assert_eq!(
        ready(client.begin_concurrent_call(&binding, &args))
            .unwrap_err()
            .code,
        "ConcurrencyLimit"
    );
    let actions = client.host_runtime_mut().take_actions();
    assert_eq!(actions.len(), 16);
    for action in actions.into_iter().rev() {
        let (runtime, driver) = client.host_parts_mut();
        ready(driver.execute(action, runtime)).unwrap();
    }
    for operation in operations {
        assert!(matches!(
            client.take_concurrent_completion(operation),
            Some(Ok(CallOutcome::Completed(_)))
        ));
    }
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|method| *method == "tools/call")
            .count(),
        16
    );
    assert!(!client.machine().is_busy());
}

#[test]
fn concurrent_preflight_blocks_new_scope_and_expired_catalog_while_old_calls_finish() {
    for auth_change in [false, true] {
        let (mut client, state) = client();
        let binding = client.tool("echo").unwrap();
        let args = binding
            .validate_json(json!({"message":"in flight"}))
            .unwrap();
        let operation = ready(client.begin_concurrent_call(&binding, &args)).unwrap();
        if auth_change {
            *state.identity.borrow_mut() = "new scope".into();
        } else {
            state.now.set(500);
        }
        assert_eq!(
            ready(client.begin_concurrent_call(&binding, &args))
                .unwrap_err()
                .code,
            if auth_change { "BindingStale" } else { "Busy" }
        );
        let actions = client.host_runtime_mut().take_actions();
        assert_eq!(actions.len(), 1);
        let (runtime, driver) = client.host_parts_mut();
        ready(driver.execute(actions.into_iter().next().unwrap(), runtime)).unwrap();
        assert!(
            client
                .take_concurrent_completion(operation)
                .unwrap()
                .is_ok()
        );
        if auth_change {
            assert_eq!(
                ready(client.begin_concurrent_call(&binding, &args))
                    .unwrap_err()
                    .code,
                "BindingStale"
            );
        } else {
            let next = ready(client.begin_concurrent_call(&binding, &args)).unwrap();
            client.host_runtime_mut().cancel_operation(next).unwrap();
            assert_eq!(
                client
                    .take_concurrent_completion(next)
                    .unwrap()
                    .unwrap_err()
                    .code,
                "Cancelled"
            );
        }
        assert_eq!(
            state
                .methods
                .borrow()
                .iter()
                .filter(|m| *m == "tools/call")
                .count(),
            1
        );
    }
}

#[test]
fn changing_client_ttl_invalidates_the_previous_expiry() {
    let (mut client, state) = client();
    let binding = client.tool("echo").unwrap();
    client.set_catalog_ttl(0);
    ready(client.call_json(&binding, json!({"message":"refresh"}))).unwrap();
    assert_eq!(
        state.methods.borrow().as_slice(),
        ["server/discover", "tools/list", "tools/list", "tools/call"]
    );
}

#[test]
fn named_builder_binds_the_new_identity_once_and_can_return_without_a_business_call() {
    use std::ops::ControlFlow;
    let (mut client, state) = client();
    let old = client.tool("echo").unwrap();
    state.schema_changed.set(true);
    *state.identity.borrow_mut() = "writer".into();
    let before = state.prepares.get();
    let result = ready(client.call_tool_with("echo", |binding| {
        Ok(ControlFlow::<String, _>::Continue(
            binding.validate_json(json!({"message":42}))?,
        ))
    }))
    .unwrap();
    let ControlFlow::Continue(CallOutcome::Completed(response)) = result else {
        panic!()
    };
    assert_eq!(response.raw["structuredContent"]["message"], 42);
    assert_eq!(state.prepares.get(), before + 1);
    let old_args = old.validate_json(json!({"message":"old"})).unwrap();
    assert_eq!(
        ready(client.call(&old, &old_args)).unwrap_err().code,
        "BindingStale"
    );
    let methods = state.methods.borrow().len();
    let before = state.prepares.get();
    let result = ready(client.call_tool_with("echo", |_| {
        Ok(ControlFlow::<_, mcpx_core::BoundArgs>::Break("local value"))
    }))
    .unwrap();
    assert!(matches!(result, ControlFlow::Break("local value")));
    assert_eq!(state.prepares.get(), before + 1);
    assert_eq!(state.methods.borrow().len(), methods);
}

#[test]
fn prepared_named_and_resume_admissions_keep_one_preflight_and_one_shot_handles() {
    use std::ops::ControlFlow;
    let (mut client, state) = client();
    let before = state.prepares.get();
    let context = ready(client.driver_mut().prepare()).unwrap();
    let started = ready(client.admit_prepared_tool(context, "echo", |binding| {
        Ok(ControlFlow::<String, _>::Continue(
            binding.validate_json(json!({"message":"named"}))?,
        ))
    }))
    .unwrap();
    let ControlFlow::Continue(operation) = started else {
        panic!()
    };
    assert_eq!(state.prepares.get(), before + 1);
    let action = client.host_runtime_mut().take_actions().pop().unwrap();
    let (runtime, driver) = client.host_parts_mut();
    ready(driver.execute(action, runtime)).unwrap();
    assert!(
        client
            .take_concurrent_completion(operation)
            .unwrap()
            .is_ok()
    );

    let handle = pending_input(&mut client, &state);
    let before = state.prepares.get();
    let context = ready(client.driver_mut().prepare()).unwrap();
    let error = ready(client.admit_prepared_resume(
        context,
        &handle,
        json!({"confirm":{"action":"accept","content":{"yes":"bad"}}}),
    ))
    .unwrap_err();
    assert_eq!(error.code, "ValidationFailed");
    assert_eq!(state.prepares.get(), before + 1);
    client.machine().check_continuation(&handle).unwrap();
    let context = ready(client.driver_mut().prepare()).unwrap();
    let operation = ready(client.admit_prepared_resume(
        context,
        &handle,
        json!({"confirm":{"action":"accept","content":{"yes":true}}}),
    ))
    .unwrap();
    let actions = client.host_runtime_mut().take_actions();
    assert_eq!(actions.len(), 1);
    let context = ready(client.driver_mut().prepare()).unwrap();
    assert_eq!(
        ready(client.admit_prepared_resume(
            context,
            &handle,
            json!({"confirm":{"action":"cancel"}})
        ))
        .unwrap_err()
        .code,
        "InvalidContinuation"
    );
    assert!(client.host_runtime_mut().take_actions().is_empty());
    let (runtime, driver) = client.host_parts_mut();
    ready(driver.execute(actions.into_iter().next().unwrap(), runtime)).unwrap();
    assert!(
        client
            .take_concurrent_completion(operation)
            .unwrap()
            .is_ok()
    );
}

#[test]
fn driver_and_preflight_errors_report_evidence_without_replaying_calls() {
    use mcpx_core::{ExecutionCertainty as Certainty, RetryAdvice};
    for mode in [5, 6, 7, 8] {
        let (mut client, state) = client();
        let binding = client.tool("echo").unwrap();
        let args = binding.args_from(json!({"message":"fixture"})).unwrap();
        state.mode.set(mode);
        if mode == 7 {
            state.now.set(501);
        }
        let error = ready(client.call(&binding, &args)).unwrap_err();
        let sent = state
            .methods
            .borrow()
            .iter()
            .filter(|method| *method == "tools/call")
            .count();
        if mode == 6 {
            assert_eq!(error.code, "CredentialUnavailable");
            assert_eq!(error.execution_certainty, Certainty::NotSent);
            assert_eq!(error.retry_advice, RetryAdvice::RefreshAuthentication);
            assert!(error.operation_id.is_none());
            assert_eq!(sent, 0);
        } else {
            assert!(error.operation_id.is_some());
            assert_eq!(
                error.execution_certainty,
                if mode == 7 {
                    Certainty::NotSent
                } else {
                    Certainty::Unknown
                }
            );
            assert_eq!(
                error.retry_advice,
                if mode == 7 {
                    RetryAdvice::ExplicitPolicy
                } else {
                    RetryAdvice::InspectExecution
                }
            );
            assert_eq!(sent, usize::from(mode != 7));
            let event = client
                .drain_events()
                .events
                .into_iter()
                .find(|event| event.kind == "failed")
                .unwrap();
            assert_eq!(event.payload, serde_json::to_value(error).unwrap());
        }
    }
}

#[test]
fn failed_and_dropped_reconnect_block_admission_until_success_without_business_replay() {
    let (mut client, state) = client();
    let old = client.tool("echo").unwrap();
    assert_eq!(
        ready(client.reconnect()).unwrap_err().code,
        "CapabilityUnavailable"
    );
    let handle = pending_input(&mut client, &state);
    state.can_reconnect.set(true);
    state.reconnect_pending.set(true);
    {
        let mut future = std::pin::pin!(client.reconnect());
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert!(client.is_suspended());
    assert_eq!(
        ready(client.call_json(&old, json!({"message":"blocked"})))
            .unwrap_err()
            .code,
        "TransportClosed"
    );
    assert_eq!(
        ready(client.admit_prepared_call(
            DriverContext {
                auth_context_id: "reader".into()
            },
            &old,
            &old.args_from(json!({"message":"blocked"})).unwrap()
        ))
        .unwrap_err()
        .code,
        "TransportClosed"
    );
    state.reconnect_pending.set(false);
    state.reconnect_failed.set(true);
    let error = ready(client.reconnect()).unwrap_err();
    assert_eq!(
        error.execution_certainty,
        mcpx_core::ExecutionCertainty::NotSent
    );
    assert!(client.is_suspended());
    state.reconnect_failed.set(false);
    ready(client.reconnect()).unwrap();
    assert!(!client.is_suspended());
    assert_eq!(
        ready(client.call_json(&old, json!({"message":"stale"})))
            .unwrap_err()
            .code,
        "BindingStale"
    );
    assert_eq!(
        ready(client.resume(&handle, json!({}))).unwrap_err().code,
        "InvalidContinuation"
    );
    state.mode.set(0);
    let fresh = client.tool("echo").unwrap();
    ready(client.call_json(&fresh, json!({"message":"fresh"}))).unwrap();
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|method| method.as_str() == "tools/call")
            .count(),
        2
    );
    ready(client.close()).unwrap();
    assert_eq!(
        ready(client.reconnect()).unwrap_err().code,
        "TransportClosed"
    );
}
#[test]
fn reconnect_refuses_business_in_flight_and_cleans_abandoned_metadata_on_retry() {
    let (mut client, state) = client();
    state.can_reconnect.set(true);
    let binding = client.tool("echo").unwrap();
    state.stalled.set(true);
    {
        let mut future = std::pin::pin!(client.call_json(&binding, json!({"message":"once"})));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(ready(client.reconnect()).unwrap_err().code, "Busy");
    assert_eq!(state.reconnects.get(), 0);
    ready(client.cancel_active());
    {
        let mut future = std::pin::pin!(client.reconnect());
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    state.stalled.set(false);
    ready(client.reconnect()).unwrap();
    assert!(!client.host_runtime_mut().busy());
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|method| method.as_str() == "tools/call")
            .count(),
        1
    );
    let ids: Vec<_> = state
        .bodies
        .borrow()
        .iter()
        .filter_map(|body| body["id"].as_u64())
        .collect();
    assert_eq!(
        ids.len(),
        ids.iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    );
}

#[test]
fn checked_call_keeps_indeterminate_output_and_received_result_evidence() {
    let (mut client, state) = client();
    state.mode.set(9);
    ready(client.refresh()).unwrap();
    let binding = client.tool("echo").unwrap();
    let message = format!("{}!", "a".repeat(40));
    let args = binding
        .args()
        .set("message", &message)
        .unwrap()
        .finish()
        .unwrap();
    let failure = ready(client.call_checked(&binding, &args)).err().unwrap();
    assert_eq!(failure.error.code, "OutputValidationLimitExceeded");
    assert_eq!(
        failure.error.execution_certainty,
        mcpx_core::ExecutionCertainty::ResultReceived
    );
    assert_eq!(
        failure.error.retry_advice,
        mcpx_core::RetryAdvice::InspectResult
    );
    let Some(CallOutcome::Completed(response)) = failure.outcome else {
        panic!()
    };
    assert!(matches!(
        response.output_validation,
        OutputValidation::Indeterminate
    ));
    assert_eq!(response.raw["structuredContent"]["message"], message);
    assert_eq!(
        state
            .methods
            .borrow()
            .iter()
            .filter(|method| method.as_str() == "tools/call")
            .count(),
        1
    );
}
