use mcpx_core::{Action, CallOutcome, Dialect, HostRuntime};
use serde_json::{Value, json};
use std::collections::BTreeSet;

struct Metadata {
    action: u64,
    rpc: Option<u64>,
    operation: u64,
    method: String,
    body: Value,
    cancellation: Option<Value>,
}
fn action(runtime: &mut HostRuntime) -> Metadata {
    let actions = runtime.take_actions();
    assert_eq!(actions.len(), 1);
    let (id, rpc_id, operation_id, rpc_method, body, cancellation) =
        match actions.into_iter().next().unwrap() {
            Action::Http {
                id,
                rpc_id,
                operation_id,
                rpc_method,
                body,
                cancellation,
                ..
            }
            | Action::Stream {
                id,
                rpc_id,
                operation_id,
                rpc_method,
                body,
                cancellation,
            } => (id, rpc_id, operation_id, rpc_method, body, cancellation),
            _ => panic!(),
        };
    assert_eq!(body.get("id").and_then(Value::as_u64), rpc_id);
    assert_eq!(body["method"], rpc_method);
    if let Some(rpc_id) = rpc_id {
        assert_eq!(BTreeSet::from([id, rpc_id, operation_id]).len(), 3);
    }
    Metadata {
        action: id,
        rpc: rpc_id,
        operation: operation_id,
        method: rpc_method,
        body,
        cancellation,
    }
}
fn feed(runtime: &mut HostRuntime, request: &Metadata, result: Value, stdio: bool) {
    let body = json!({"jsonrpc":"2.0","id":request.rpc.unwrap(),"result":result});
    if stdio {
        runtime.stream_input(body).unwrap();
    } else {
        runtime
            .begin_response(
                request.action,
                200,
                r#"{"content-type":"application/json"}"#,
            )
            .unwrap();
        runtime
            .push_response_for(request.action, body.to_string().as_bytes())
            .unwrap();
        runtime.finish_response_for(request.action).unwrap();
    }
}
fn remote(legacy: bool, stdio: bool) -> HostRuntime {
    let mut runtime = if stdio {
        HostRuntime::stdio(if legacy {
            Dialect::Legacy
        } else {
            Dialect::Modern
        })
    } else {
        HostRuntime::new("http://localhost/mcp".into(), legacy).unwrap()
    };
    runtime.discover().unwrap();
    let first = action(&mut runtime);
    let discovery_operation = first.operation;
    if legacy {
        assert!(first.cancellation.is_none());
        feed(
            &mut runtime,
            &first,
            json!({"protocolVersion":"2025-11-25"}),
            stdio,
        );
        let initialized = action(&mut runtime);
        assert!(initialized.rpc.is_none() && initialized.cancellation.is_none());
        assert_eq!(initialized.operation, discovery_operation);
        runtime.complete_write(initialized.action).unwrap();
    } else {
        feed(
            &mut runtime,
            &first,
            json!({"supportedVersions":["2026-07-28"]}),
            stdio,
        );
    }
    let listing = action(&mut runtime);
    assert_eq!(listing.method, "tools/list");
    assert_eq!(listing.operation, discovery_operation);
    if legacy {
        // A notification uses an action ID without consuming a wire RPC ID.
        assert_ne!(
            first.rpc.unwrap().abs_diff(first.action),
            listing.rpc.unwrap().abs_diff(listing.action)
        );
    }
    feed(
        &mut runtime,
        &listing,
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
        stdio,
    );
    runtime.take_events();
    runtime
}

#[test]
fn all_core_domains_are_distinct_and_wrong_domain_handles_cannot_alias_live_resources() {
    for legacy in [false, true] {
        for stdio in [false, true] {
            let mut runtime = remote(legacy, stdio);
            let binding = runtime.bind("echo").unwrap();
            let operation = runtime.call_concurrent(binding, "{}").unwrap();
            let request = action(&mut runtime);
            assert_eq!(request.operation, operation);
            let rpc = request.rpc.unwrap();
            assert_eq!(
                BTreeSet::from([binding, operation, request.action, rpc]).len(),
                4
            );
            for foreign in [binding, operation, rpc] {
                assert_eq!(
                    runtime.begin_response(foreign, 200, "{}").unwrap_err().code,
                    "UnexpectedResponse"
                );
            }
            for foreign in [binding, request.action, rpc] {
                assert_eq!(
                    runtime.cancel_operation(foreign).unwrap_err().code,
                    "InvalidOperation"
                );
            }
            runtime.release_binding(operation);
            assert_eq!(runtime.validate(binding, "{}").unwrap(), "{}");
            assert!(runtime.take_completion(request.action).is_none());
            for token in [request.action, rpc] {
                runtime.stream_input(json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":format!("mcpx-{token}"),"progress":1}})).unwrap();
            }
            let progress = runtime.take_events().events;
            assert_eq!(progress.len(), 1);
            assert_eq!(progress[0].operation_id, operation);
            if legacy || stdio {
                let cancel = request.cancellation.as_ref().unwrap();
                assert_eq!(cancel["params"]["requestId"], rpc);
                assert!(cancel["params"].get("arguments").is_none());
            } else {
                assert!(request.cancellation.is_none());
            }
            feed(
                &mut runtime,
                &request,
                if legacy {
                    json!({"content":[]})
                } else {
                    json!({"resultType":"input_required","requestState":"original"})
                },
                stdio,
            );
            // Modern continuation format belongs only to the modern protocol.
            let outcome = runtime.take_completion(operation).unwrap();
            if legacy {
                assert!(matches!(outcome.unwrap(), CallOutcome::Completed(_)));
                continue;
            }
            let CallOutcome::NeedsInput(handle) = outcome.unwrap() else {
                panic!()
            };
            assert_eq!(
                BTreeSet::from([binding, operation, request.action, rpc, handle.id()]).len(),
                5
            );
            runtime.release_continuation(binding);
            assert_eq!(
                runtime.resume_concurrent(operation, "{}").unwrap_err().code,
                "InvalidContinuation"
            );
            let resumed = runtime.resume_concurrent(handle.id(), "{}").unwrap();
            let request = action(&mut runtime);
            assert_eq!(request.body["params"]["requestState"], "original");
            feed(&mut runtime, &request, json!({"content":[]}), stdio);
            assert!(runtime.take_completion(resumed).unwrap().is_ok());
        }
    }
}

#[test]
fn shared_stream_routes_wire_ids_and_ignores_only_previously_allocated_wire_replies() {
    let mut runtime = remote(false, true);
    let binding = runtime.bind("echo").unwrap();
    let first = runtime.call_concurrent(binding, "{}").unwrap();
    let a = action(&mut runtime);
    let second = runtime.call_concurrent(binding, "{}").unwrap();
    let b = action(&mut runtime);
    runtime.cancel_operation(first).unwrap();
    runtime.take_actions();
    assert_eq!(
        runtime.take_completion(first).unwrap().unwrap_err().code,
        "Cancelled"
    );
    runtime
        .stream_input(json!({"jsonrpc":"2.0","id":a.rpc.unwrap(),"result":{"content":[]}}))
        .unwrap();
    for foreign in [a.action, a.operation, binding, b.action] {
        assert_eq!(
            runtime
                .stream_input(json!({"jsonrpc":"2.0","id":foreign,"result":{"content":[]}}))
                .unwrap_err()
                .code,
            "UnexpectedResponse"
        );
    }
    feed(&mut runtime, &b, json!({"content":[]}), true);
    assert!(runtime.take_completion(second).unwrap().is_ok());
}

#[test]
fn an_http_body_echoing_the_host_action_id_fails_only_that_operation() {
    let mut runtime = remote(false, false);
    let binding = runtime.bind("echo").unwrap();
    let first = runtime.call_concurrent(binding, "{}").unwrap();
    let a = action(&mut runtime);
    let second = runtime.call_concurrent(binding, "{}").unwrap();
    let b = action(&mut runtime);
    runtime
        .begin_response(a.action, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    runtime
        .push_response_for(
            a.action,
            json!({"jsonrpc":"2.0","id":a.action,"result":{"content":[]}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    assert_eq!(
        runtime.finish_response_for(a.action).unwrap_err().code,
        "UnexpectedResponse"
    );
    assert_eq!(
        runtime.take_completion(first).unwrap().unwrap_err().code,
        "UnexpectedResponse"
    );
    feed(&mut runtime, &b, json!({"content":[]}), false);
    assert!(runtime.take_completion(second).unwrap().is_ok());
    let events = runtime.take_events().events;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "failed" && event.operation_id == first)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "completed" && event.operation_id == second)
            .count(),
        1
    );
}

#[test]
fn shared_stream_does_not_confuse_malformed_responses_with_notifications() {
    let mut runtime = remote(false, true);
    let binding = runtime.bind("echo").unwrap();
    let operation = runtime.call_concurrent(binding, "{}").unwrap();
    let request = action(&mut runtime);
    for message in [
        json!({"jsonrpc":"2.0","result":{"content":[]}}),
        json!({"jsonrpc":"2.0","error":{"code":-32603}}),
        json!({"jsonrpc":"2.0","method":null}),
        json!({"jsonrpc":"2.0","method":123,"id":request.rpc}),
    ] {
        assert_eq!(
            runtime.stream_input(message).unwrap_err().code,
            "ProtocolError"
        );
    }
    runtime
        .stream_input(
            json!({"jsonrpc":"2.0","method":"future/notification","params":{"opaque":true}}),
        )
        .unwrap();
    feed(&mut runtime, &request, json!({"content":[]}), true);
    assert!(runtime.take_completion(operation).unwrap().is_ok());
    assert!(
        runtime
            .take_events()
            .events
            .iter()
            .any(|event| event.kind == "notification")
    );
}

#[test]
fn a_server_request_cannot_masquerade_as_legacy_downgrade_evidence() {
    let mut runtime = HostRuntime::auto("http://localhost/mcp".into()).unwrap();
    runtime.discover().unwrap();
    let request = action(&mut runtime);
    runtime
        .begin_response(
            request.action,
            400,
            r#"{"content-type":"application/json"}"#,
        )
        .unwrap();
    runtime.push_response_for(request.action, json!({"jsonrpc":"2.0","id":request.rpc.unwrap(),"method":"roots/list","error":{"code":-32601}}).to_string().as_bytes()).unwrap();
    assert_eq!(
        runtime
            .finish_response_for(request.action)
            .unwrap_err()
            .code,
        "ProtocolError"
    );
    assert_eq!(runtime.machine().dialect(), Dialect::Modern);
    assert!(runtime.take_actions().is_empty());
    assert!(!runtime.busy());
}
