use mcpx_core::{Action, Dialect, HostRuntime, NotificationPhase};
use serde_json::{Value, json};

#[test]
fn scoped_identity_is_bounded_and_does_not_create_a_bearer() {
    for scope in ["".into(), "é".repeat(129)] {
        assert_eq!(
            mcpx_core::Credential::scoped(scope).unwrap_err().code,
            "CredentialUnavailable"
        );
    }
    let credential = mcpx_core::Credential::scoped("é".repeat(128)).unwrap();
    assert!(credential.token().is_none());
    assert_eq!(credential.auth_context_id().len(), 256);
}

#[test]
fn serialized_stream_read_cannot_accept_ack_after_its_deadline() {
    let mut runtime = connected(false);
    let plan = runtime.start_stdio_notifications().unwrap();
    let rpc = plan.rpc_id.unwrap();
    runtime
        .set_notification_deadline(plan.resource_id, 10)
        .unwrap();
    runtime
        .set_notification_deadline(plan.resource_id, 100)
        .unwrap();
    runtime.advance_time(10).unwrap();
    runtime.stream_input(ack(rpc)).unwrap();
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Failed
    );
    assert_eq!(
        runtime.notification_status().unwrap().error_code.as_deref(),
        Some("Timeout")
    );
    assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
}

fn metadata(runtime: &mut HostRuntime) {
    while runtime.busy() {
        let actions = runtime.take_actions();
        assert_eq!(actions.len(), 1);
        let Action::Stream {
            id, body, rpc_id, ..
        } = &actions[0]
        else {
            panic!()
        };
        if rpc_id.is_none() {
            runtime.complete_write(*id).unwrap();
            continue;
        }
        let result = match body["method"].as_str().unwrap() {
            "server/discover" => json!({"supportedVersions":["2026-07-28"]}),
            "initialize" => json!({"protocolVersion":"2025-11-25"}),
            "tools/list" => json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
            _ => panic!(),
        };
        runtime
            .stream_input(json!({"jsonrpc":"2.0","id":rpc_id,"result":result}))
            .unwrap();
    }
}
fn connected(legacy: bool) -> HostRuntime {
    let mut runtime = HostRuntime::stdio(if legacy {
        Dialect::Legacy
    } else {
        Dialect::Modern
    });
    runtime.accept_authorization("alice").unwrap();
    runtime.discover().unwrap();
    metadata(&mut runtime);
    runtime
}
fn ack(rpc: u64) -> Value {
    json!({"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":rpc},"notifications":{"toolsListChanged":true}}})
}
fn change(rpc: u64) -> Value {
    json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":rpc}}})
}

#[test]
fn modern_stdio_subscription_leaves_all_business_slots_available() {
    let mut runtime = connected(false);
    assert_eq!(
        runtime.start_notifications().unwrap_err().code,
        "CapabilityUnavailable"
    );
    let plan = runtime.start_stdio_notifications().unwrap();
    let rpc = plan.rpc_id.unwrap();
    assert_eq!(plan.method, "STDIO");
    assert!(plan.url.is_empty() && plan.headers.is_empty());
    assert_eq!(
        plan.body.unwrap()["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
        "2026-07-28"
    );
    let binding = runtime.bind("echo").unwrap();
    let mut operations = Vec::new();
    for _ in 0..16 {
        operations.push(runtime.call_concurrent(binding, "{}").unwrap());
    }
    let actions = runtime.take_actions();
    assert_eq!(actions.len(), 16);
    runtime.stream_input(ack(rpc)).unwrap();
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Listening
    );
    runtime.stream_input(change(rpc)).unwrap();
    assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
    for action in actions.into_iter().rev() {
        let Action::Stream { rpc_id, .. } = action else {
            panic!()
        };
        assert_ne!(rpc_id, Some(rpc));
        runtime
            .stream_input(json!({"jsonrpc":"2.0","id":rpc_id,"result":{"content":[]}}))
            .unwrap();
    }
    for operation in operations {
        assert!(runtime.take_completion(operation).unwrap().is_ok());
    }
    runtime.stream_input(json!({"jsonrpc":"2.0","id":rpc,"result":{"resultType":"complete","_meta":{"io.modelcontextprotocol/subscriptionId":rpc}}})).unwrap();
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Closed
    );
}

#[test]
fn failed_subscription_does_not_poison_peer_calls_or_consume_their_replies() {
    for bad in ["ack", "scope", "domain", "unscoped", "server-request"] {
        let mut runtime = connected(false);
        let plan = runtime.start_stdio_notifications().unwrap();
        let rpc = plan.rpc_id.unwrap();
        let binding = runtime.bind("echo").unwrap();
        let operation = runtime.call_concurrent(binding, "{}").unwrap();
        let Action::Stream {
            id: action,
            rpc_id: wire,
            ..
        } = runtime.take_actions().pop().unwrap()
        else {
            panic!()
        };
        runtime
            .stream_input(if bad == "ack" { change(rpc) } else { ack(rpc) })
            .unwrap();
        let message = match bad {
            "scope" => change(rpc + (1_u64 << 32)),
            "domain" => change(action),
            "unscoped" => {
                json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{}})
            }
            "server-request" => json!({"jsonrpc":"2.0","id":rpc,"method":"roots/list"}),
            _ => ack(rpc),
        };
        runtime.stream_input(message).unwrap();
        assert_eq!(
            runtime.notification_status().unwrap().phase,
            NotificationPhase::Failed,
            "{bad}"
        );
        runtime
            .stream_input(json!({"jsonrpc":"2.0","id":wire,"result":{"content":[]}}))
            .unwrap();
        assert!(runtime.take_completion(operation).unwrap().is_ok());
    }
}

#[test]
fn old_stdio_subscription_frames_cannot_cross_replacement_or_identity() {
    let mut runtime = connected(false);
    let first = runtime.start_stdio_notifications().unwrap();
    let rpc = first.rpc_id.unwrap();
    runtime.stream_input(ack(rpc)).unwrap();
    runtime
        .abort_notifications(first.resource_id, true)
        .unwrap();
    runtime.refresh_catalog().unwrap();
    metadata(&mut runtime);
    let next = runtime.start_stdio_notifications().unwrap();
    let next_rpc = next.rpc_id.unwrap();
    assert_ne!(rpc, next_rpc);
    runtime.stream_input(ack(rpc)).unwrap();
    runtime.stream_input(change(rpc)).unwrap();
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Opening
    );
    assert!(!runtime.catalog_status("alice").unwrap().needs_refresh);
    runtime.stream_input(ack(next_rpc)).unwrap();
    runtime.accept_authorization("bob").unwrap();
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Cancelled
    );
    runtime.discover().unwrap();
    metadata(&mut runtime);
    let revision = runtime.machine().catalog().revision_source().snapshot();
    runtime.stream_input(change(next_rpc)).unwrap();
    assert_eq!(
        revision,
        runtime.machine().catalog().revision_source().snapshot()
    );
}

#[test]
fn legacy_stdio_notifications_are_passive_and_transport_mismatch_allocates_nothing() {
    let mut runtime = connected(true);
    let plan = runtime.start_stdio_notifications().unwrap();
    assert!(plan.body.is_none() && plan.rpc_id.is_none());
    assert!(runtime.take_actions().is_empty());
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Listening
    );
    runtime
        .stream_input(
            json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{}}),
        )
        .unwrap();
    assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
    let reply = runtime
        .stream_input(json!({"jsonrpc":"2.0","id":-3,"method":"roots/list"}))
        .unwrap()
        .unwrap();
    assert_eq!(reply["error"]["code"], -32601);
    let mut http = HostRuntime::new("https://example.com/mcp".into(), false).unwrap();
    assert_eq!(
        http.start_stdio_notifications().unwrap_err().code,
        "CapabilityUnavailable"
    );
    assert!(http.notification_status().is_none());
}
