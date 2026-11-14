use mcpx_core::{Action, CallOutcome, Continuation, Dialect, Machine};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn action(machine: &mut Machine) -> (u64, Value) {
    let actions = machine.drain_actions();
    let [Action::Http { id, body, .. }] = actions.as_slice() else {
        panic!("expected one HTTP action")
    };
    (*id, body.clone())
}
fn respond(machine: &mut Machine, id: u64, result: Value) -> mcpx_core::Result<()> {
    machine.respond(
        id,
        200,
        &BTreeMap::new(),
        Some(json!({"jsonrpc":"2.0","id":machine.rpc_id_for_action(id).unwrap(),"result":result})),
    )
}
fn tools(changed: bool) -> Value {
    json!({"tools":[{"name":"echo","inputSchema":{"type":"object","required":["message"],"properties":{"message":{"type":if changed {"integer"} else {"string"}}}}}]})
}
fn remote() -> Machine {
    let mut machine = Machine::new("http://localhost/mcp".into(), Dialect::Modern).unwrap();
    machine.set_elicitation(true, true).unwrap();
    machine.start_discovery().unwrap();
    let (id, body) = action(&mut machine);
    assert_eq!(
        body["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"],
        json!({"elicitation":{"form":{},"url":{}}})
    );
    respond(
        &mut machine,
        id,
        json!({"supportedVersions":["2026-07-28"]}),
    )
    .unwrap();
    let (id, _) = action(&mut machine);
    respond(&mut machine, id, tools(false)).unwrap();
    machine.drain_events();
    machine
}
fn required() -> Value {
    json!({"resultType":"input_required","inputRequests":{"confirm":{"method":"elicitation/create","params":{"message":"Continue?","requestedSchema":{"type":"object","properties":{"approved":{"type":"boolean"}},"required":["approved"],"additionalProperties":false}}}},"requestState":"opaque\u{0}state: 不解析"})
}
fn input() -> Value {
    json!({"confirm":{"action":"accept","content":{"approved":true}}})
}
fn start(machine: &mut Machine, result: Value) -> (u64, Continuation) {
    let binding = machine.bind("echo").unwrap();
    let args = binding
        .args()
        .set("message", "original")
        .unwrap()
        .finish()
        .unwrap();
    machine.start_call(&binding, &args).unwrap();
    let (id, _) = action(machine);
    respond(machine, id, result).unwrap();
    let CallOutcome::NeedsInput(handle) = machine.take_outcome().unwrap() else {
        panic!()
    };
    (id, handle)
}

#[test]
fn resume_preserves_original_arguments_and_opaque_state_with_a_new_request_id() {
    let mut machine = remote();
    let (first, handle) = start(&mut machine, required());
    assert!(!format!("{handle:?}").contains("opaque"));
    machine.resume(&handle, input()).unwrap();
    let (second, body) = action(&mut machine);
    assert_ne!(first, second);
    assert_eq!(body["params"]["arguments"], json!({"message":"original"}));
    assert_eq!(body["params"]["requestState"], required()["requestState"]);
    assert_eq!(body["params"]["inputResponses"], input());
    respond(
        &mut machine,
        second,
        json!({"resultType":"complete","content":[]}),
    )
    .unwrap();
    assert!(matches!(
        machine.take_outcome(),
        Some(CallOutcome::Completed(_))
    ));
    assert_eq!(
        machine.resume(&handle, input()).unwrap_err().code,
        "InvalidContinuation"
    );
    assert!(machine.drain_actions().is_empty());
}

#[test]
fn invalid_responses_do_not_consume_the_handle_or_send_requests() {
    let mut machine = remote();
    let (_, handle) = start(&mut machine, required());
    for responses in [
        json!({}),
        json!({"wrong":{"action":"cancel"}}),
        json!({"confirm":{"action":"accept","content":{"approved":"yes"}}}),
        json!({"confirm":{"action":"decline","content":{}}}),
        json!({"confirm":{"action":"cancel","surprise":true}}),
    ] {
        assert_eq!(
            machine.resume(&handle, responses).unwrap_err().code,
            "ValidationFailed"
        );
        assert!(machine.drain_actions().is_empty());
    }
    machine
        .resume(&handle, json!({"confirm":{"action":"decline"}}))
        .unwrap();
    let (id, body) = action(&mut machine);
    assert_eq!(
        body["params"]["inputResponses"]["confirm"]["action"],
        "decline"
    );
    machine.cancel();
    machine.drain_actions();
    assert_eq!(
        machine.resume(&handle, input()).unwrap_err().code,
        "InvalidContinuation"
    );
    assert!(
        machine
            .respond(
                id,
                200,
                &BTreeMap::new(),
                Some(json!({"jsonrpc":"2.0","id":body["id"],"result":{"content":[]}}))
            )
            .is_err()
    );
}

#[test]
fn state_only_and_input_only_results_have_distinct_resume_parameters() {
    let mut machine = remote();
    let (_, handle) = start(
        &mut machine,
        json!({"resultType":"input_required","requestState":"exact"}),
    );
    machine.resume(&handle, json!({})).unwrap();
    let (id, body) = action(&mut machine);
    assert_eq!(body["params"]["requestState"], "exact");
    let mut result = required();
    result.as_object_mut().unwrap().remove("requestState");
    respond(&mut machine, id, result).unwrap();
    let CallOutcome::NeedsInput(handle) = machine.take_outcome().unwrap() else {
        panic!()
    };
    machine.resume(&handle, input()).unwrap();
    let (_, body) = action(&mut machine);
    assert!(body["params"].get("requestState").is_none());
}

#[test]
fn remote_ownership_identity_and_release_invalidate_continuations() {
    let mut machine = remote();
    let (_, handle) = start(&mut machine, required());
    let mut other = remote();
    let (_, own) = start(&mut other, required());
    assert_eq!(own.id(), handle.id());
    assert_eq!(
        other.resume(&handle, input()).unwrap_err().code,
        "InvalidContinuation"
    );
    assert!(other.drain_actions().is_empty());
    other.release_continuation(own.id());
    assert_eq!(
        other.resume(&own, input()).unwrap_err().code,
        "InvalidContinuation"
    );
    machine.invalidate_authorization().unwrap();
    assert_eq!(
        machine.resume(&handle, input()).unwrap_err().code,
        "InvalidContinuation"
    );
}

#[test]
fn schema_changes_and_observed_notifications_prevent_resuming_old_calls() {
    let mut machine = remote();
    let (_, handle) = start(&mut machine, required());
    machine
        .notify(&json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"}))
        .unwrap();
    assert_eq!(
        machine.resume(&handle, input()).unwrap_err().code,
        "BindingStale"
    );
    machine.start_catalog_refresh().unwrap();
    let (id, _) = action(&mut machine);
    respond(&mut machine, id, tools(true)).unwrap();
    assert_eq!(
        machine.resume(&handle, input()).unwrap_err().code,
        "BindingStale"
    );
    assert!(machine.drain_actions().is_empty());
}

#[test]
fn interaction_deadline_is_not_extended_by_another_round() {
    let mut machine = remote();
    machine.advance_time(100).unwrap();
    let (_, handle) = start(&mut machine, required());
    assert_eq!(handle.expires_at_ms(), 300_100);
    machine.advance_time(299_999).unwrap();
    machine.resume(&handle, input()).unwrap();
    let (id, _) = action(&mut machine);
    respond(&mut machine, id, required()).unwrap();
    let CallOutcome::NeedsInput(handle) = machine.take_outcome().unwrap() else {
        panic!()
    };
    assert_eq!(handle.expires_at_ms(), 300_100);
    machine.advance_time(300_100).unwrap();
    assert_eq!(
        machine.resume(&handle, input()).unwrap_err().code,
        "ContinuationExpired"
    );
    assert!(machine.drain_actions().is_empty());
    assert_eq!(machine.advance_time(10).unwrap_err().code, "InvalidClock");
}

#[test]
fn continuation_capacity_and_round_limits_are_enforced() {
    let mut machine = remote();
    let mut handles = Vec::new();
    for _ in 0..32 {
        handles.push(start(&mut machine, required()).1);
        machine.drain_events();
    }
    let binding = machine.bind("echo").unwrap();
    let args = binding.validate_json(json!({"message":"x"})).unwrap();
    machine.start_call(&binding, &args).unwrap();
    let (id, _) = action(&mut machine);
    assert_eq!(
        respond(&mut machine, id, required()).unwrap_err().code,
        "LimitExceeded"
    );
    machine.drain_events();
    let mut handle = handles.remove(0);
    for _ in 1..16 {
        machine.resume(&handle, input()).unwrap();
        let (id, _) = action(&mut machine);
        respond(&mut machine, id, required()).unwrap();
        let Some(CallOutcome::NeedsInput(next)) = machine.take_outcome() else {
            panic!()
        };
        handle = next;
        machine.drain_events();
    }
    machine.resume(&handle, input()).unwrap();
    let (id, _) = action(&mut machine);
    assert_eq!(
        respond(&mut machine, id, required()).unwrap_err().code,
        "LimitExceeded"
    );
    assert!(!machine.is_busy());
}

#[test]
fn unadvertised_or_malformed_input_requests_fail_without_a_resumable_handle() {
    for result in [
        json!({"resultType":"input_required"}),
        json!({"resultType":"input_required","requestState":{}}),
        json!({"resultType":"input_required","inputRequests":[]}),
        json!({"resultType":"input_required","inputRequests":{"x":{"method":"sampling/createMessage","params":{}}}}),
        required(),
    ] {
        let mut machine = remote();
        machine.set_elicitation(false, false).unwrap();
        let binding = machine.bind("echo").unwrap();
        let args = binding.validate_json(json!({"message":"x"})).unwrap();
        machine.start_call(&binding, &args).unwrap();
        let (id, _) = action(&mut machine);
        assert!(respond(&mut machine, id, result).is_err());
        assert!(machine.take_outcome().is_none());
        assert!(!machine.is_busy());
    }
}

#[test]
fn mismatched_arguments_fail_before_starting_a_call() {
    let mut machine = remote();
    let mut other = remote();
    let binding = machine.bind("echo").unwrap();
    let foreign = other
        .bind("echo")
        .unwrap()
        .validate_json(json!({"message":"x"}))
        .unwrap();
    assert_eq!(
        machine.start_call(&binding, &foreign).unwrap_err().code,
        "BindingMismatch"
    );
    assert!(!machine.is_busy());
    assert!(machine.drain_actions().is_empty());
}

#[test]
fn url_elicitation_returns_only_an_explicit_action() {
    let mut machine = remote();
    let (_, handle) = start(
        &mut machine,
        json!({"resultType":"input_required","inputRequests":{"login":{"method":"elicitation/create","params":{"mode":"url","message":"Sign in","url":"https://example.com/authorize","elicitationId":"login-1"}}}}),
    );
    assert_eq!(
        handle.raw()["inputRequests"]["login"]["params"]["url"],
        "https://example.com/authorize"
    );
    assert_eq!(
        machine
            .resume(
                &handle,
                json!({"login":{"action":"accept","content":{"secret":"must not be forwarded"}}})
            )
            .unwrap_err()
            .code,
        "ValidationFailed"
    );
    assert!(machine.drain_actions().is_empty());
    machine
        .resume(&handle, json!({"login":{"action":"accept"}}))
        .unwrap();
    let (_, body) = action(&mut machine);
    assert_eq!(
        body["params"]["inputResponses"],
        json!({"login":{"action":"accept"}})
    );
    assert!(body["params"].get("requestState").is_none());
}

#[test]
fn expired_interaction_cannot_issue_a_new_handle_after_a_late_response() {
    let mut machine = remote();
    let (_, handle) = start(&mut machine, required());
    machine.resume(&handle, input()).unwrap();
    let (id, _) = action(&mut machine);
    machine.advance_time(300_000).unwrap();
    assert_eq!(
        respond(&mut machine, id, required()).unwrap_err().code,
        "ContinuationExpired"
    );
    assert!(machine.take_outcome().is_none());
    assert!(!machine.is_busy());
    assert_eq!(
        machine
            .drain_events()
            .events
            .iter()
            .filter(|e| e.kind == "failed")
            .count(),
        1
    );
}

#[test]
fn continuation_byte_budget_rejects_a_large_state_and_release_recovers_capacity() {
    let mut machine = remote();
    let binding = machine.bind("echo").unwrap();
    let args = binding.validate_json(json!({"message":"x"})).unwrap();
    machine.start_call(&binding, &args).unwrap();
    let (id, _) = action(&mut machine);
    assert_eq!(
        respond(
            &mut machine,
            id,
            json!({"resultType":"input_required","requestState":"x".repeat(8*1024*1024)})
        )
        .unwrap_err()
        .code,
        "LimitExceeded"
    );
    machine.drain_events();
    let mut handles = Vec::new();
    for _ in 0..32 {
        handles.push(start(&mut machine, required()).1);
        machine.drain_events();
    }
    machine.release_continuation(handles[0].id());
    let (_, next) = start(&mut machine, required());
    assert!(next.id() > handles.last().unwrap().id());
}
