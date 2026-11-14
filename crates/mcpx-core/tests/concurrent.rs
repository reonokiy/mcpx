use mcpx_core::{Action, CallOutcome, Dialect, Error, HostRuntime, ToolBinding};
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn action(runtime: &mut HostRuntime) -> (u64, Value) {
    let mut actions = runtime.take_actions();
    assert_eq!(actions.len(), 1);
    match actions.remove(0) {
        Action::Http { id, body, .. } | Action::Stream { id, body, .. } => (id, body),
        _ => panic!(),
    }
}
fn rpc(runtime: &HostRuntime, action: u64) -> u64 {
    runtime.machine().rpc_id_for_action(action).unwrap()
}
fn response(runtime: &mut HostRuntime, id: u64, result: Value) {
    runtime
        .begin_response(id, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    runtime
        .push_response_for(
            id,
            json!({"jsonrpc":"2.0","id":rpc(runtime,id),"result":result})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    runtime.finish_response_for(id).unwrap();
}
fn remote() -> (HostRuntime, ToolBinding) {
    let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
    runtime.discover().unwrap();
    let (id, _) = action(&mut runtime);
    response(
        &mut runtime,
        id,
        json!({"supportedVersions":["2026-07-28"]}),
    );
    let (id, _) = action(&mut runtime);
    response(
        &mut runtime,
        id,
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":"string"}},"required":["message"]}}]}),
    );
    let binding = runtime.tool_binding("echo").unwrap();
    runtime.take_events();
    (runtime, binding)
}
fn start(runtime: &mut HostRuntime, binding: &ToolBinding, message: &str) -> (u64, u64) {
    let args = binding
        .args()
        .set("message", message)
        .unwrap()
        .finish()
        .unwrap();
    let operation = runtime.call_concurrent_bound(binding, &args).unwrap();
    let (id, body) = action(runtime);
    assert_eq!(
        body["params"]["_meta"]["progressToken"],
        format!("mcpx-{}", rpc(runtime, id))
    );
    (operation, id)
}
fn completed(runtime: &mut HostRuntime, operation: u64) -> Value {
    let CallOutcome::Completed(response) = runtime.take_completion(operation).unwrap().unwrap()
    else {
        panic!()
    };
    response.raw
}

#[test]
fn sixteen_interleaved_sse_streams_complete_in_reverse_order_with_correct_event_ids() {
    let (mut runtime, binding) = remote();
    let mut calls = Vec::new();
    for index in 0..16 {
        let (operation, id) = start(&mut runtime, &binding, &format!("message-{index}"));
        runtime
            .begin_response(id, 200, r#"{"content-type":"text/event-stream"}"#)
            .unwrap();
        calls.push((operation, id));
    }
    let args = binding
        .validate_json(json!({"message":"overflow"}))
        .unwrap();
    assert_eq!(
        runtime
            .call_concurrent_bound(&binding, &args)
            .unwrap_err()
            .code,
        "ConcurrencyLimit"
    );
    assert_eq!(
        runtime.push_response(b"data: {}\n\n").unwrap_err().code,
        "AmbiguousResponse"
    );
    let rpcs: std::collections::BTreeMap<_, _> = calls
        .iter()
        .map(|(_, id)| (*id, rpc(&runtime, *id)))
        .collect();
    for (operation, id) in calls.iter().rev().copied() {
        let progress = json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":format!("mcpx-{}", rpc(&runtime,id)),"progress":1,"message":"进度"}});
        let result = json!({"jsonrpc":"2.0","id":rpc(&runtime,id),"result":{"content":[],"structuredContent":{"operation":operation}}});
        let wire = format!("data: {progress}\n\ndata: {result}\n\n");
        let mut done = false;
        for chunk in wire.as_bytes().chunks(3) {
            done = runtime.push_response_for(id, chunk).unwrap();
        }
        assert!(done);
        assert_eq!(
            completed(&mut runtime, operation)["structuredContent"]["operation"],
            operation
        );
    }
    assert!(!runtime.busy());
    let events = runtime.take_events().events;
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    assert_eq!(events.iter().filter(|e| e.kind == "completed").count(), 16);
    assert_eq!(events.iter().filter(|e| e.kind == "progress").count(), 16);
    assert_eq!(
        events
            .iter()
            .map(|e| e.operation_id)
            .collect::<BTreeSet<_>>(),
        calls.iter().map(|(op, _)| *op).collect()
    );
    for event in events.iter().filter(|e| e.kind == "progress") {
        let id = calls
            .iter()
            .find(|(op, _)| *op == event.operation_id)
            .unwrap()
            .1;
        assert_eq!(
            event.payload["params"]["progressToken"],
            format!("mcpx-{}", rpcs[&id])
        );
    }
}

#[test]
fn unconsumed_results_are_bounded_and_read_once() {
    let (mut runtime, binding) = remote();
    let mut operations = Vec::new();
    for _ in 0..16 {
        let (operation, id) = start(&mut runtime, &binding, "x");
        response(&mut runtime, id, json!({"content":[]}));
        operations.push(operation);
    }
    let args = binding
        .validate_json(json!({"message":"overflow"}))
        .unwrap();
    assert_eq!(
        runtime
            .call_concurrent_bound(&binding, &args)
            .unwrap_err()
            .code,
        "ConcurrencyLimit"
    );
    assert!(runtime.take_completion(operations[0]).is_some());
    assert!(runtime.take_completion(operations[0]).is_none());
    let (operation, _) = start(&mut runtime, &binding, "freed slot");
    runtime.cancel_operation(operation).unwrap();
    assert_eq!(
        runtime
            .take_completion(operation)
            .unwrap()
            .unwrap_err()
            .code,
        "Cancelled"
    );
    for operation in operations.into_iter().skip(1) {
        assert!(runtime.take_completion(operation).unwrap().is_ok());
    }
}

#[test]
fn shared_input_ignores_late_replies_but_rejects_unallocated_ids_and_modern_requests() {
    let (mut runtime, binding) = remote();
    let (first, first_id) = start(&mut runtime, &binding, "cancelled");
    let (second, second_id) = start(&mut runtime, &binding, "active");
    runtime.link_queue_operation(second, 555).unwrap();
    let first_rpc = rpc(&runtime, first_id);
    runtime.cancel_operation(first).unwrap();
    assert_eq!(
        runtime.take_completion(first).unwrap().unwrap_err().code,
        "Cancelled"
    );
    assert!(
        runtime
            .stream_input(json!({"jsonrpc":"2.0","id":first_rpc,"result":{"content":[]}}))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        runtime
            .stream_input(json!({"jsonrpc":"2.0","id":99999,"result":{"content":[]}}))
            .unwrap_err()
            .code,
        "UnexpectedResponse"
    );
    assert_eq!(
        runtime
            .stream_input(json!({"jsonrpc":"2.0","id":"server-id","method":"roots/list"}))
            .unwrap_err()
            .code,
        "ProtocolError"
    );
    runtime.stream_input(json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":format!("mcpx-{}", rpc(&runtime,second_id)),"progress":1}})).unwrap();
    runtime.stream_input(json!({"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":"global diagnostic"}})).unwrap();
    runtime
        .stream_input(json!({"jsonrpc":"2.0","id":rpc(&runtime,second_id),"result":{"content":[]}}))
        .unwrap();
    completed(&mut runtime, second);
    let events = runtime.take_events().events;
    assert_eq!(events.iter().filter(|event| event.is_terminal()).count(), 2);
    assert!(
        events
            .iter()
            .any(|event| event.kind == "progress" && event.operation_id == second)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == "progress" && event.queue_operation_id == Some(555))
    );
    assert!(events.iter().any(|event| event.kind == "log"
        && event.operation_id == 0
        && event.queue_operation_id.is_none()));
}

#[test]
fn one_bad_response_does_not_clear_another_calls_actions_or_response() {
    let (mut runtime, binding) = remote();
    let (first, id) = start(&mut runtime, &binding, "bad");
    runtime
        .begin_response(id, 200, r#"{"content-type":"text/event-stream"}"#)
        .unwrap();
    let args = binding.validate_json(json!({"message":"good"})).unwrap();
    let second = runtime.call_concurrent_bound(&binding, &args).unwrap();
    assert_eq!(
        runtime
            .push_response_for(id, b"data: not-json\n\n")
            .unwrap_err()
            .code,
        "ProtocolError"
    );
    assert_eq!(
        runtime.take_completion(first).unwrap().unwrap_err().code,
        "ProtocolError"
    );
    let (second_id, _) = action(&mut runtime);
    runtime
        .begin_response(second_id, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    assert_eq!(
        runtime.begin_response(id, 200, "{}").unwrap_err().code,
        "UnexpectedResponse"
    );
    runtime
        .push_response_for(
            second_id,
            json!({"jsonrpc":"2.0","id":rpc(&runtime,second_id),"result":{"content":[]}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    runtime.finish_response_for(second_id).unwrap();
    completed(&mut runtime, second);
    assert!(!runtime.busy());
}

#[test]
fn mismatched_rpc_ids_fail_only_the_stream_that_received_them() {
    let (mut runtime, binding) = remote();
    let (first, first_id) = start(&mut runtime, &binding, "first");
    let (second, second_id) = start(&mut runtime, &binding, "second");
    runtime
        .begin_response(first_id, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    runtime
        .push_response_for(
            first_id,
            json!({"jsonrpc":"2.0","id":rpc(&runtime,second_id),"result":{"content":[]}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    assert_eq!(
        runtime.finish_response_for(first_id).unwrap_err().code,
        "UnexpectedResponse"
    );
    assert_eq!(
        runtime.take_completion(first).unwrap().unwrap_err().code,
        "UnexpectedResponse"
    );
    response(&mut runtime, second_id, json!({"content":[]}));
    completed(&mut runtime, second);
}

#[test]
fn scoped_cancellation_and_late_data_cannot_replace_another_terminal_result() {
    let (mut runtime, binding) = remote();
    let (first, first_id) = start(&mut runtime, &binding, "first");
    let (second, second_id) = start(&mut runtime, &binding, "second");
    for id in [first_id, second_id] {
        runtime
            .begin_response(id, 200, r#"{"content-type":"text/event-stream"}"#)
            .unwrap();
    }
    runtime.cancel_operation(first).unwrap();
    assert!(matches!(runtime.take_actions().as_slice(),[Action::Cancel {id}] if *id == first_id));
    assert_eq!(
        runtime.take_completion(first).unwrap().unwrap_err().code,
        "Cancelled"
    );
    assert_eq!(
        runtime
            .push_response_for(first_id, b"data: {}\n\n")
            .unwrap_err()
            .code,
        "InvalidHandle"
    );
    let wire = format!(
        "data: {}\n\n",
        json!({"jsonrpc":"2.0","id":rpc(&runtime,second_id),"result":{"content":[]}})
    );
    runtime
        .push_response_for(second_id, wire.as_bytes())
        .unwrap();
    completed(&mut runtime, second);
    runtime.fail("late host failure");
    let events = runtime.take_events().events;
    assert_eq!(events.iter().filter(|e| e.is_terminal()).count(), 2);
    assert!(runtime.cancel_operation(first).is_err());
}

#[test]
fn control_operations_are_barriers_and_global_failure_finishes_each_active_call_once() {
    let (mut runtime, binding) = remote();
    let (first, _) = start(&mut runtime, &binding, "first");
    let (second, _) = start(&mut runtime, &binding, "second");
    assert_eq!(runtime.refresh_catalog().unwrap_err().code, "Busy");
    assert_eq!(runtime.invalidate_authorization().unwrap_err().code, "Busy");
    runtime.abort_error(Error::new("TransportError", "connection lost"));
    runtime.abort_error(Error::new("HostError", "duplicate failure"));
    for operation in [first, second] {
        assert_eq!(
            runtime
                .take_completion(operation)
                .unwrap()
                .unwrap_err()
                .code,
            "TransportError"
        );
    }
    assert_eq!(
        runtime
            .take_events()
            .events
            .iter()
            .filter(|e| e.is_terminal())
            .count(),
        2
    );
    runtime.refresh_catalog().unwrap();
    let (id, _) = action(&mut runtime);
    response(&mut runtime, id, json!({"tools":[]}));
}

#[test]
fn independent_continuations_retain_their_own_arguments_and_state() {
    let (mut runtime, binding) = remote();
    let (first, first_id) = start(&mut runtime, &binding, "one");
    let (second, second_id) = start(&mut runtime, &binding, "two");
    response(
        &mut runtime,
        second_id,
        json!({"resultType":"input_required","requestState":"state two"}),
    );
    response(
        &mut runtime,
        first_id,
        json!({"resultType":"input_required","requestState":"state one"}),
    );
    for (operation, message, state) in [(first, "one", "state one"), (second, "two", "state two")] {
        let CallOutcome::NeedsInput(handle) = runtime.take_completion(operation).unwrap().unwrap()
        else {
            panic!()
        };
        let resumed = runtime.resume_concurrent_bound(&handle, json!({})).unwrap();
        let (id, body) = action(&mut runtime);
        assert_eq!(body["params"]["arguments"]["message"], message);
        assert_eq!(body["params"]["requestState"], state);
        response(&mut runtime, id, json!({"content":[]}));
        completed(&mut runtime, resumed);
        assert_eq!(
            runtime
                .resume_concurrent_bound(&handle, json!({}))
                .unwrap_err()
                .code,
            "InvalidContinuation"
        );
    }
}

#[test]
fn legacy_stdio_routes_out_of_order_responses_and_progress_by_request_id() {
    let mut runtime = HostRuntime::stdio(Dialect::Legacy);
    runtime.discover().unwrap();
    let (id, _) = action(&mut runtime);
    runtime
        .stream_message(
            id,
            json!({"jsonrpc":"2.0","id":rpc(&runtime,id),"result":{"protocolVersion":"2025-11-25"}}),
        )
        .unwrap();
    let (id, _) = action(&mut runtime);
    runtime.complete_write(id).unwrap();
    let (id, _) = action(&mut runtime);
    runtime.stream_message(id,json!({"jsonrpc":"2.0","id":rpc(&runtime,id),"result":{"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}})).unwrap();
    let binding = runtime.tool_binding("echo").unwrap();
    runtime.take_events();
    let mut calls = Vec::new();
    for index in 0..16 {
        calls.push(start(&mut runtime, &binding, &format!("{index}")));
    }
    for (operation, id) in calls.into_iter().rev() {
        runtime.stream_message(id,json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":format!("mcpx-{}", rpc(&runtime,id)),"progress":1}})).unwrap();
        runtime.stream_message(id,json!({"jsonrpc":"2.0","id":rpc(&runtime,id),"result":{"content":[],"structuredContent":{"operation":operation}}})).unwrap();
        assert_eq!(
            completed(&mut runtime, operation)["structuredContent"]["operation"],
            operation
        );
    }
    assert_eq!(
        runtime
            .take_events()
            .events
            .iter()
            .filter(|e| e.kind == "progress")
            .count(),
        16
    );
}
