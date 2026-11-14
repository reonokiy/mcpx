use mcpx_core::{Action, HostRuntime};
use serde_json::{Value, json};

fn respond(runtime: &mut HostRuntime, result: Value) {
    let actions = runtime.take_actions();
    let [Action::Http { id, .. }] = actions.as_slice() else {
        panic!("expected one HTTP action")
    };
    runtime
        .begin_response(*id, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    runtime
        .push_response_for(
            *id,
            json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(*id).unwrap(),"result":result})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    runtime.finish_response_for(*id).unwrap();
}
fn catalog() -> Value {
    json!({"ttlMs":500,"tools":[{"name":"echo","inputSchema":{"type":"object"}}]})
}
fn remote() -> HostRuntime {
    let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
    assert!(!runtime.accept_authorization("alice").unwrap());
    runtime.discover().unwrap();
    respond(&mut runtime, json!({"supportedVersions":["2026-07-28"]}));
    runtime.advance_time(100).unwrap();
    respond(&mut runtime, catalog());
    runtime.take_events();
    runtime
}

#[test]
fn catalog_deadline_is_shared_and_is_not_extended_by_reads_or_late_clock_updates() {
    let mut runtime = remote();
    assert!(
        !runtime
            .catalog_status_at("alice", 599)
            .unwrap()
            .needs_refresh
    );
    assert!(
        runtime
            .catalog_status_at("alice", 600)
            .unwrap()
            .needs_refresh
    );
    runtime.advance_time(700).unwrap();
    assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
    runtime.refresh_catalog().unwrap();
    runtime.advance_time(800).unwrap();
    respond(&mut runtime, catalog());
    assert!(
        !runtime
            .catalog_status_at("alice", 1299)
            .unwrap()
            .needs_refresh
    );
    assert!(
        runtime
            .catalog_status_at("alice", 1300)
            .unwrap()
            .needs_refresh
    );
    // A shorter configured TTL invalidates an already observed deadline.
    runtime.set_catalog_ttl(10);
    assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
    runtime.refresh_catalog().unwrap();
    respond(&mut runtime, catalog());
    assert!(
        !runtime
            .catalog_status_at("alice", 809)
            .unwrap()
            .needs_refresh
    );
    assert!(
        runtime
            .catalog_status_at("alice", 810)
            .unwrap()
            .needs_refresh
    );
}

#[test]
fn identity_switch_requires_drain_and_cannot_relabel_active_calls() {
    let mut runtime = remote();
    let binding = runtime.tool_binding("echo").unwrap();
    let args = binding.args().finish().unwrap();
    let operation = runtime.call_concurrent_bound(&binding, &args).unwrap();
    assert!(!runtime.catalog_status("alice").unwrap().requires_exclusive);
    let changed = runtime.catalog_status("bob").unwrap();
    assert!(changed.authorization_changed && changed.needs_refresh && changed.requires_exclusive);
    assert_eq!(
        runtime.accept_authorization("bob").unwrap_err().code,
        "BindingStale"
    );
    assert!(
        !runtime
            .catalog_status("alice")
            .unwrap()
            .authorization_changed
    );
    respond(&mut runtime, json!({"content":[]}));
    assert!(runtime.take_completion(operation).unwrap().is_ok());
    assert!(runtime.accept_authorization("bob").unwrap());
    assert!(runtime.call_concurrent_bound(&binding, &args).is_err());
    assert!(!runtime.catalog_status("bob").unwrap().authorization_changed);
    assert!(runtime.catalog_status("bob").unwrap().needs_refresh);
}

#[test]
fn unscoped_catalog_cannot_be_adopted_and_context_ids_are_bounded_in_bytes() {
    let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
    runtime.discover().unwrap();
    respond(&mut runtime, json!({"supportedVersions":["2026-07-28"]}));
    respond(&mut runtime, catalog());
    assert!(runtime.accept_authorization("first").unwrap());
    assert!(runtime.catalog_status("first").unwrap().needs_refresh);
    for scope in ["".into(), "界".repeat(86)] {
        assert_eq!(
            runtime.catalog_status(&scope).unwrap_err().code,
            "CredentialUnavailable"
        );
    }
    assert!(runtime.catalog_status(&"界".repeat(85)).is_ok());
}
