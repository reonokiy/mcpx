use mcpx_core::{
    Action, CallOutcome, Error, ExecutionCertainty as Certainty, HostRuntime, RetryAdvice,
    ToolBinding,
};
use serde_json::{Value, json};

fn action(runtime: &mut HostRuntime) -> (u64, u64) {
    let actions = runtime.take_actions();
    assert_eq!(actions.len(), 1);
    match actions.into_iter().next().unwrap() {
        Action::Http { id, rpc_id, .. } => (id, rpc_id.unwrap()),
        _ => panic!("expected request"),
    }
}
fn reply(runtime: &mut HostRuntime, id: u64, rpc: u64, result: Value) {
    runtime
        .begin_response(id, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    runtime
        .push_response_for(
            id,
            json!({"jsonrpc":"2.0","id":rpc,"result":result})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    runtime.finish_response_for(id).unwrap();
}
fn remote() -> (HostRuntime, ToolBinding) {
    let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
    runtime.discover().unwrap();
    let (id, rpc) = action(&mut runtime);
    reply(
        &mut runtime,
        id,
        rpc,
        json!({"supportedVersions":["2026-07-28"]}),
    );
    let (id, rpc) = action(&mut runtime);
    reply(
        &mut runtime,
        id,
        rpc,
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
    );
    let binding = runtime.tool_binding("echo").unwrap();
    runtime.take_events();
    (runtime, binding)
}
#[test]
fn queued_and_dispatched_cancellation_have_distinct_evidence_and_matching_events() {
    for dispatched in [false, true] {
        let (mut runtime, binding) = remote();
        let args = binding.args_from(json!({})).unwrap();
        let operation = runtime.call_concurrent_bound(&binding, &args).unwrap();
        runtime.link_queue_operation(operation, 17).unwrap();
        if dispatched {
            action(&mut runtime);
        }
        runtime.cancel_operation(operation).unwrap();
        let error = runtime.take_completion(operation).unwrap().unwrap_err();
        assert_eq!(error.operation_id, Some(operation));
        assert_eq!(error.queue_operation_id, Some(17));
        assert_eq!(
            error.execution_certainty,
            if dispatched {
                Certainty::Unknown
            } else {
                Certainty::NotSent
            }
        );
        assert_eq!(
            error.retry_advice,
            if dispatched {
                RetryAdvice::InspectExecution
            } else {
                RetryAdvice::None
            }
        );
        let event = runtime
            .take_events()
            .events
            .into_iter()
            .find(|event| event.kind == "cancelled")
            .unwrap();
        assert_eq!(event.payload, serde_json::to_value(&error).unwrap());
    }
}
#[test]
fn response_errors_keep_context_after_pending_state_is_removed() {
    for mode in ["content_type", "truncated", "wrong_id", "unauthorized"] {
        let (mut runtime, binding) = remote();
        let operation = runtime
            .call_concurrent_bound(&binding, &binding.args_from(json!({})).unwrap())
            .unwrap();
        runtime.link_queue_operation(operation, 23).unwrap();
        let (id, rpc) = action(&mut runtime);
        let error = match mode {
            "content_type" => runtime
                .begin_response(id, 200, r#"{"content-type":"text/html"}"#)
                .unwrap_err(),
            "unauthorized" => runtime.begin_response(id, 401, "{}").unwrap_err(),
            mode => {
                runtime
                    .begin_response(id, 200, r#"{"content-type":"application/json"}"#)
                    .unwrap();
                let body = if mode == "truncated" {
                    "{".into()
                } else {
                    json!({"jsonrpc":"2.0","id":rpc+1,"result":{"content":[]}}).to_string()
                };
                runtime.push_response_for(id, body.as_bytes()).unwrap();
                runtime.finish_response_for(id).unwrap_err()
            }
        };
        let stored = runtime.take_completion(operation).unwrap().unwrap_err();
        assert_eq!(
            serde_json::to_value(&error).unwrap(),
            serde_json::to_value(&stored).unwrap(),
            "{mode}"
        );
        assert_eq!(error.operation_id, Some(operation));
        assert_eq!(error.queue_operation_id, Some(23));
        assert_eq!(error.execution_certainty, Certainty::Unknown);
        assert_eq!(error.retry_advice, RetryAdvice::InspectExecution);
        assert!(!runtime.busy());
        assert!(runtime.take_actions().is_empty());
    }
}
#[test]
fn metadata_io_failure_never_claims_a_business_request_was_dispatched() {
    let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
    runtime.discover().unwrap();
    let (id, _) = action(&mut runtime);
    runtime
        .fail_request(id, Error::new("HostError", "fixture transport failed"))
        .unwrap();
    let events = runtime.take_events();
    let failure = events
        .events
        .iter()
        .find(|event| event.kind == "failed")
        .unwrap();
    let error: Error = serde_json::from_value(failure.payload.clone()).unwrap();
    assert_eq!(error.operation_id, Some(failure.operation_id));
    assert_eq!(error.execution_certainty, Certainty::NotSent);
    assert_eq!(error.retry_advice, RetryAdvice::ExplicitPolicy);
}
#[test]
fn completed_and_resumed_attempts_preserve_their_own_operation_evidence() {
    let (mut runtime, binding) = remote();
    let operation = runtime
        .call_concurrent_bound(&binding, &binding.args_from(json!({})).unwrap())
        .unwrap();
    let (id, rpc) = action(&mut runtime);
    reply(
        &mut runtime,
        id,
        rpc,
        json!({"content":[],"structuredContent":{"n":18446744073709551615_u64}}),
    );
    let CallOutcome::Completed(response) = runtime.take_completion(operation).unwrap().unwrap()
    else {
        panic!()
    };
    let error = response.deserialize::<Vec<String>>().unwrap_err();
    assert_eq!(error.operation_id, Some(operation));
    assert_eq!(error.execution_certainty, Certainty::ResultReceived);
    assert_eq!(error.retry_advice, RetryAdvice::InspectResult);
    assert_eq!(
        response.raw["structuredContent"]["n"].as_u64(),
        Some(u64::MAX)
    );
    let original = runtime
        .call_concurrent_bound(&binding, &binding.args_from(json!({})).unwrap())
        .unwrap();
    let (id, rpc) = action(&mut runtime);
    reply(
        &mut runtime,
        id,
        rpc,
        json!({"resultType":"input_required","requestState":"opaque"}),
    );
    let CallOutcome::NeedsInput(handle) = runtime.take_completion(original).unwrap().unwrap()
    else {
        panic!()
    };
    assert_eq!(handle.operation_id(), original);
    let resumed = runtime.resume_concurrent(handle.id(), "{}").unwrap();
    runtime.cancel_operation(resumed).unwrap();
    let error = runtime.take_completion(resumed).unwrap().unwrap_err();
    assert_ne!(original, resumed);
    assert_eq!(error.operation_id, Some(resumed));
    assert_eq!(error.execution_certainty, Certainty::NotSent);
    assert_eq!(handle.operation_id(), original);
}
#[test]
fn legacy_errors_deserialize_conservatively_and_local_validation_is_not_sent() {
    let error: Error = serde_json::from_str(r#"{"code":"Timeout","message":"legacy"}"#).unwrap();
    assert_eq!(error.execution_certainty, Certainty::Unknown);
    assert_eq!(error.retry_advice, RetryAdvice::None);
    assert!(error.operation_id.is_none());
    let error = mcpx_core::parse_json("{bad").unwrap_err();
    assert_eq!(error.execution_certainty, Certainty::NotSent);
    assert_eq!(error.retry_advice, RetryAdvice::CorrectInput);
}

#[test]
fn host_queue_scope_links_metadata_at_creation_and_survives_guard_release() {
    let (mut runtime, binding) = remote();
    let scope = runtime.machine().queue_scope(71).unwrap();
    runtime.refresh_catalog().unwrap();
    drop(scope);
    let (id, rpc) = action(&mut runtime);
    reply(
        &mut runtime,
        id,
        rpc,
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
    );
    let batch = runtime.take_events();
    assert!(
        batch
            .events
            .iter()
            .any(|event| event.kind == "catalog_updated")
    );
    assert!(
        batch
            .events
            .iter()
            .all(|event| event.queue_operation_id == Some(71))
    );
    let op = runtime
        .call_concurrent_bound(&binding, &binding.args_from(json!({})).unwrap())
        .unwrap();
    let (id, rpc) = action(&mut runtime);
    reply(&mut runtime, id, rpc, json!({"content":[]}));
    let CallOutcome::Completed(response) = runtime.take_completion(op).unwrap().unwrap() else {
        panic!()
    };
    assert!(response.queue_operation_id.is_none());
}

#[test]
fn transport_reset_protects_unconsumed_serial_results_and_retains_concurrent_completions() {
    let (mut runtime, binding) = remote();
    let args = binding.args_from(json!({})).unwrap();
    runtime.call_bound(&binding, &args).unwrap();
    let (id, rpc) = action(&mut runtime);
    reply(
        &mut runtime,
        id,
        rpc,
        json!({"content":[],"structuredContent":{"kept":1}}),
    );
    assert_eq!(runtime.reset_transport().unwrap_err().code, "Busy");
    let CallOutcome::Completed(first) = runtime.take_outcome().unwrap() else {
        panic!()
    };
    assert_eq!(first.raw["structuredContent"]["kept"], 1);
    let op = runtime.call_concurrent_bound(&binding, &args).unwrap();
    let (id, rpc) = action(&mut runtime);
    reply(
        &mut runtime,
        id,
        rpc,
        json!({"content":[],"structuredContent":{"kept":2}}),
    );
    runtime.reset_transport().unwrap();
    let CallOutcome::Completed(second) = runtime.take_completion(op).unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(second.raw["structuredContent"]["kept"], 2);
    assert_eq!(second.operation_id, Some(op));
}
