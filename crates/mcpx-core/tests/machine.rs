use mcpx_core::{Action, CallOutcome, Dialect, Machine, OutputValidation};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn next(machine: &mut Machine) -> (u64, Value) {
    let mut actions = machine.drain_actions();
    assert_eq!(actions.len(), 1);
    let Action::Http { id, body, .. } = actions.remove(0) else {
        panic!()
    };
    (id, body)
}
fn reply(machine: &mut Machine, id: u64, result: Value) {
    machine
        .respond(
            id,
            200,
            &BTreeMap::new(),
            Some(json!({"jsonrpc":"2.0","id":machine.rpc_id_for_action(id).unwrap(),"result":result})),
        )
        .unwrap();
}
fn discover(machine: &mut Machine) {
    machine.start_discovery().unwrap();
    let (id, request) = next(machine);
    assert_eq!(request["method"], "server/discover");
    reply(machine, id, json!({"supportedVersions":["2026-07-28"]}));
    let (id, request) = next(machine);
    assert_eq!(request["method"], "tools/list");
    reply(
        machine,
        id,
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object","required":["message"],"properties":{"message":{"type":"string"}}},"outputSchema":{"type":"object","required":["message"]}}]}),
    );
}

#[test]
fn modern_discovery_call_output_and_notification_flow() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    discover(&mut machine);
    let binding = machine.bind("echo").unwrap();
    let args = binding
        .args()
        .set("message", "hello")
        .unwrap()
        .finish()
        .unwrap();
    machine.start_call(&binding, &args).unwrap();
    let (id, request) = next(&mut machine);
    assert_eq!(request["params"]["arguments"]["message"], "hello");
    assert_eq!(
        request["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
        "2026-07-28"
    );
    machine
        .notify(&json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}}))
        .unwrap();
    reply(
        &mut machine,
        id,
        json!({"content":[],"structuredContent":{"message":"hello"}}),
    );
    let CallOutcome::Completed(response) = machine.take_outcome().unwrap() else {
        panic!()
    };
    assert!(matches!(
        response.output_validation,
        OutputValidation::Valid
    ));
    assert_eq!(
        machine
            .drain_events()
            .events
            .iter()
            .map(|e| e.kind.as_str())
            .collect::<Vec<_>>(),
        ["catalog_updated", "progress", "completed"]
    );
}

#[test]
fn legacy_initialize_requires_empty_notification_ack_and_retains_session() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Legacy).unwrap();
    machine.start_discovery().unwrap();
    let (id, request) = next(&mut machine);
    assert_eq!(request["method"], "initialize");
    machine.respond(id, 200, &BTreeMap::from([("mcp-session-id".into(), "session".into())]), Some(json!({"jsonrpc":"2.0","id":machine.rpc_id_for_action(id).unwrap(),"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}}}}))).unwrap();
    let (id, request) = next(&mut machine);
    assert!(request.get("id").is_none());
    machine.respond(id, 202, &BTreeMap::new(), None).unwrap();
    let actions = machine.drain_actions();
    let Action::Http { headers, .. } = &actions[0] else {
        panic!()
    };
    assert_eq!(headers["mcp-session-id"], "session");
}

#[test]
fn auth_failure_and_cancel_do_not_replay_business_calls() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    machine.start_discovery().unwrap();
    let (id, _) = next(&mut machine);
    assert_eq!(
        machine
            .respond(id, 401, &BTreeMap::new(), None)
            .unwrap_err()
            .code,
        "AuthFailed"
    );
    assert!(!machine.is_busy());
    assert!(machine.drain_actions().is_empty());
    discover(&mut machine);
    let binding = machine.bind("echo").unwrap();
    let args = binding
        .args()
        .set("message", "x")
        .unwrap()
        .finish()
        .unwrap();
    machine.start_call(&binding, &args).unwrap();
    let (id, _) = next(&mut machine);
    machine.cancel();
    assert!(matches!(
        machine.drain_actions().as_slice(),
        [Action::Cancel { .. }]
    ));
    assert!(
        machine
            .respond(id, 200, &BTreeMap::new(), Some(json!({})))
            .is_err()
    );
    assert!(machine.take_outcome().is_none());
}

#[test]
fn repeated_pagination_cursor_does_not_publish_partial_catalog() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    machine.start_discovery().unwrap();
    let (id, _) = next(&mut machine);
    reply(
        &mut machine,
        id,
        json!({"supportedVersions":["2026-07-28"]}),
    );
    let (id, _) = next(&mut machine);
    reply(&mut machine, id, json!({"tools":[],"nextCursor":"same"}));
    let (id, _) = next(&mut machine);
    let error = machine
        .respond(
            id,
            200,
            &BTreeMap::new(),
            Some(json!({"jsonrpc":"2.0","id":machine.rpc_id_for_action(id).unwrap(),"result":{"tools":[],"nextCursor":"same"}})),
        )
        .unwrap_err();
    assert_eq!(error.code, "InvalidCatalog");
    assert_eq!(machine.catalog().generation(), 0);
}

#[test]
fn invalidation_between_catalog_pages_cannot_publish_a_fresh_snapshot() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    discover(&mut machine);
    machine.drain_events();
    let mut watcher = machine.catalog().revision_source().subscribe().unwrap();
    machine.start_catalog_refresh().unwrap();
    let (id, _) = next(&mut machine);
    reply(&mut machine, id, json!({"tools":[],"nextCursor":"second"}));
    {
        use std::future::Future;
        let mut future = std::pin::pin!(watcher.changed());
        assert!(
            future
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
    }
    machine
        .notify(&json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"}))
        .unwrap();
    let (id, _) = next(&mut machine);
    reply(
        &mut machine,
        id,
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
    );
    assert!(machine.is_catalog_stale());
    let binding = machine.bind("echo").unwrap();
    let args = binding.args().finish().unwrap();
    assert_eq!(
        machine.start_call(&binding, &args).unwrap_err().code,
        "BindingStale"
    );
    assert!(machine.drain_actions().is_empty());
    assert!(
        machine
            .drain_events()
            .events
            .iter()
            .any(|event| event.kind == "catalog_updated" && event.payload["stale"] == true)
    );
    {
        use std::future::Future;
        let mut future = std::pin::pin!(watcher.changed());
        let std::task::Poll::Ready(Some(change)) = future
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
        else {
            panic!()
        };
        assert!(change.published && change.invalidated);
    }
    machine.start_catalog_refresh().unwrap();
    let (id, _) = next(&mut machine);
    reply(
        &mut machine,
        id,
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
    );
    assert!(!machine.is_catalog_stale());
    let binding = machine.bind("echo").unwrap();
    machine
        .start_call(&binding, &binding.args().finish().unwrap())
        .unwrap();
    assert_eq!(next(&mut machine).1["method"], "tools/call");
}

#[test]
fn observed_catalog_changes_block_old_bindings_before_sending() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    discover(&mut machine);
    let binding = machine.bind("echo").unwrap();
    let args = binding
        .args()
        .set("message", "x")
        .unwrap()
        .finish()
        .unwrap();
    machine
        .notify(&json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"}))
        .unwrap();
    assert_eq!(
        machine.start_call(&binding, &args).unwrap_err().code,
        "BindingStale"
    );
    assert!(machine.drain_actions().is_empty());
}

#[test]
fn malformed_tool_results_do_not_become_successful_calls() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    discover(&mut machine);
    let binding = machine.bind("echo").unwrap();
    let args = binding
        .args()
        .set("message", "x")
        .unwrap()
        .finish()
        .unwrap();
    machine.start_call(&binding, &args).unwrap();
    let (id, _) = next(&mut machine);
    assert_eq!(
        machine
            .respond(
                id,
                200,
                &BTreeMap::new(),
                Some(
                    json!({"jsonrpc":"2.0","id":machine.rpc_id_for_action(id).unwrap(),"result":{}})
                )
            )
            .unwrap_err()
            .code,
        "ProtocolError"
    );
    assert!(machine.take_outcome().is_none());
}

#[test]
fn auto_http_downgrades_only_on_legacy_probe_evidence() {
    for (status, code, fallback) in [
        (400, Some(-32601), true),
        (400, None, true),
        (200, Some(-32602), true),
        (401, None, false),
        (403, None, false),
        (500, None, false),
        (404, None, false),
        (400, Some(-32020), false),
        (400, Some(-32022), false),
    ] {
        let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Auto).unwrap();
        machine.start_discovery().unwrap();
        let (id, _) = next(&mut machine);
        let message = code.map(|code| json!({"jsonrpc":"2.0","id":machine.rpc_id_for_action(id).unwrap(),"error":{"code":code,"message":"probe rejected"}}));
        let result = machine.respond(id, status, &BTreeMap::new(), message);
        if fallback {
            result.unwrap();
            let (_, request) = next(&mut machine);
            assert_eq!(request["method"], "initialize");
            assert_eq!(machine.dialect(), Dialect::Legacy);
        } else {
            assert!(result.is_err());
            assert!(machine.drain_actions().is_empty());
            assert_eq!(machine.dialect(), Dialect::Modern);
        }
    }
}

#[test]
fn modern_call_carries_required_routing_headers() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    discover(&mut machine);
    let binding = machine.bind("echo").unwrap();
    let args = binding
        .args()
        .set("message", "x")
        .unwrap()
        .finish()
        .unwrap();
    machine.start_call(&binding, &args).unwrap();
    let actions = machine.drain_actions();
    let Action::Http { headers, .. } = &actions[0] else {
        panic!()
    };
    assert_eq!(headers["mcp-method"], "tools/call");
    assert_eq!(headers["mcp-name"], "echo");
}

#[test]
fn auto_probe_does_not_accept_an_error_for_another_request() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Auto).unwrap();
    machine.start_discovery().unwrap();
    let (id, _) = next(&mut machine);
    assert_eq!(
        machine
            .respond(
                id,
                400,
                &BTreeMap::new(),
                Some(json!({"jsonrpc":"2.0","id":machine.rpc_id_for_action(id).unwrap()+16,"error":{"code":-32601}}))
            )
            .unwrap_err()
            .code,
        "UnexpectedResponse"
    );
    assert!(machine.drain_actions().is_empty());
    assert_eq!(machine.dialect(), Dialect::Modern);
}

#[test]
fn invalid_header_tools_are_filtered_and_valid_headers_reach_actions() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    machine.start_discovery().unwrap();
    let (id, _) = next(&mut machine);
    reply(
        &mut machine,
        id,
        json!({"supportedVersions":["2026-07-28"]}),
    );
    let (id, _) = next(&mut machine);
    reply(
        &mut machine,
        id,
        json!({"tools":[
            {"name":"valid","inputSchema":{"type":"object","properties":{"region":{"type":"string","x-mcp-header":"Region"}}}},
            {"name":"invalid","inputSchema":{"type":"object","oneOf":[{"properties":{"x":{"type":"string","x-mcp-header":"X"}}}]}}
        ]}),
    );
    assert_eq!(
        machine
            .catalog()
            .tools()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>(),
        ["valid"]
    );
    let binding = machine.bind("valid").unwrap();
    let args = binding
        .args()
        .set("region", " padded ")
        .unwrap()
        .finish()
        .unwrap();
    machine.start_call(&binding, &args).unwrap();
    let actions = machine.drain_actions();
    let Action::Http { headers, .. } = &actions[0] else {
        panic!()
    };
    assert_eq!(
        headers["mcp-param-region"],
        mcpx_protocol::encode_header(" padded ")
    );
    assert!(
        machine
            .drain_events()
            .events
            .iter()
            .any(|event| event.payload["code"] == "ToolRejected")
    );
}

#[test]
fn repeated_host_failures_do_not_duplicate_or_replace_terminal_events() {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    machine.start_discovery().unwrap();
    let (id, _) = next(&mut machine);
    let _ = machine.respond(id, 401, &BTreeMap::new(), None);
    machine.fail(mcpx_core::Error::new("HostError", "wrapper failed"));
    assert_eq!(
        machine
            .drain_events()
            .events
            .iter()
            .filter(|event| event.kind == "failed")
            .count(),
        1
    );
    machine.start_discovery().unwrap();
    machine.cancel();
    machine.fail(mcpx_core::Error::new("HostError", "fetch was aborted"));
    let events = machine.drain_events().events;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "cancelled");
}

#[test]
fn catalog_ttl_is_the_minimum_of_all_pages_including_zero() {
    for first in [0, 1000] {
        let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
        machine.start_discovery().unwrap();
        let (id, _) = next(&mut machine);
        reply(
            &mut machine,
            id,
            json!({"supportedVersions":["2026-07-28"]}),
        );
        let (id, _) = next(&mut machine);
        reply(
            &mut machine,
            id,
            json!({"tools":[],"ttlMs":first,"nextCursor":"page2"}),
        );
        let (id, _) = next(&mut machine);
        reply(&mut machine, id, json!({"tools":[],"ttlMs":500}));
        assert_eq!(machine.catalog_ttl_ms(), Some(first.min(500)));
        machine.start_catalog_refresh().unwrap();
        assert_eq!(next(&mut machine).1["method"], "tools/list");
    }
}
