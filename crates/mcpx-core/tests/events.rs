use mcpx_core::{Action, EventLimits, HostRuntime};
use serde_json::{Value, json};
fn reply(runtime: &mut HostRuntime, action: &Action, result: Value) {
    let (id, rpc_id) = match action {
        Action::Http { id, rpc_id, .. } => (*id, rpc_id.unwrap()),
        _ => panic!(),
    };
    runtime
        .begin_response(id, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    runtime
        .push_response_for(
            id,
            json!({"jsonrpc":"2.0","id":rpc_id,"result":result})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    runtime.finish_response_for(id).unwrap();
}
fn remote() -> HostRuntime {
    let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
    runtime
        .set_event_limits(EventLimits {
            capacity: 4,
            max_bytes: 49152,
        })
        .unwrap();
    runtime.discover().unwrap();
    for result in [
        json!({"supportedVersions":["2026-07-28"]}),
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
    ] {
        let action = runtime.take_actions().remove(0);
        reply(&mut runtime, &action, result);
    }
    let batch = runtime.take_events();
    assert_eq!(batch.dropped, 0);
    assert_eq!(batch.events.len(), 1);
    runtime
}
#[test]
fn empty_loss_batches_are_observable_and_all_admitted_terminals_still_fit() {
    let mut runtime = remote();
    let binding = runtime.bind("echo").unwrap();
    let operations: Vec<_> = (0..3)
        .map(|_| runtime.call_concurrent(binding, "{}").unwrap())
        .collect();
    assert_eq!(
        runtime.call_concurrent(binding, "{}").unwrap_err().code,
        "ConsumerLagged"
    );
    let actions = runtime.take_actions();
    assert_eq!(actions.len(), 3);
    for _ in 0..7 {
        runtime.stream_input(json!({"jsonrpc":"2.0","method":"notifications/message","params":{"data":"dropped"}})).unwrap();
    }
    let loss = runtime.take_events();
    assert_eq!(loss.dropped, 7);
    assert!(loss.events.is_empty());
    assert_eq!(runtime.take_events().dropped, 0);
    assert_eq!(runtime.event_stats().reserved_terminals, 3);
    assert_eq!(
        runtime
            .set_event_limits(EventLimits::default())
            .unwrap_err()
            .code,
        "Busy"
    );
    runtime.cancel_operation(operations[0]).unwrap();
    runtime.take_actions();
    for action in &actions[1..] {
        reply(&mut runtime, action, json!({"content":[]}));
    }
    let batch = runtime.take_events();
    assert_eq!(batch.dropped, 0);
    assert_eq!(batch.events.len(), 3);
    assert!(batch.events.iter().all(|event| event.is_terminal()));
    assert_eq!(runtime.event_stats().reserved_terminals, 0);
    assert_eq!(runtime.event_stats().bytes, 0);
    for operation in operations {
        assert!(runtime.take_completion(operation).is_some());
    }
    runtime.set_event_limits(EventLimits::default()).unwrap();
}
