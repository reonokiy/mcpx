use mcpx_core::{
    Action, CallOutcome, Dialect, Error, ExecutionCertainty, HostRuntime, ToolBinding,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

fn action(r: &mut HostRuntime) -> (u64, u64, Value) {
    let mut actions = r.take_actions();
    assert_eq!(actions.len(), 1);
    let Action::Http {
        id, rpc_id, body, ..
    } = actions.remove(0)
    else {
        panic!()
    };
    (id, rpc_id.unwrap_or(0), body)
}
fn connected(dialect: Dialect, session: bool) -> (HostRuntime, ToolBinding) {
    let mut r = HostRuntime::with_dialect("https://example.com/mcp".into(), dialect).unwrap();
    r.accept_authorization("alice").unwrap();
    r.discover().unwrap();
    while r.busy() {
        let (id, rpc, body) = action(&mut r);
        let mut fields = BTreeMap::from([("content-type".into(), "application/json".into())]);
        let mut status = 200;
        let result = match body["method"].as_str().unwrap() {
            "initialize" => {
                if session {
                    fields.insert("mcp-session-id".into(), "original-session".into());
                }
                json!({"protocolVersion":"2025-11-25"})
            }
            "notifications/initialized" => {
                status = 202;
                Value::Null
            }
            "server/discover" if dialect == Dialect::Auto => {
                status = 400;
                Value::Null
            }
            "server/discover" => json!({"supportedVersions":["2026-07-28"]}),
            "tools/list" => json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}),
            _ => panic!(),
        };
        if r.begin_response_headers(id, status, fields).unwrap() {
            continue;
        }
        let wire = if status == 400 {
            json!({"jsonrpc":"2.0","id":rpc,"error":{"code":-32601,"message":"initialize first"}})
        } else {
            json!({"jsonrpc":"2.0","id":rpc,"result":result})
        };
        r.push_response_for(id, wire.to_string().as_bytes())
            .unwrap();
        r.finish_response_for(id).unwrap();
    }
    let binding = r.tool_binding("echo").unwrap();
    r.take_events();
    (r, binding)
}
fn start(r: &mut HostRuntime, binding: &ToolBinding, serial: bool) -> (u64, u64, u64) {
    let args = binding.args_from(json!({"message":"original"})).unwrap();
    if serial {
        let _scope = r.machine().queue_scope(777).unwrap();
        r.call_bound(binding, &args).unwrap();
    } else {
        r.call_concurrent_bound(binding, &args).unwrap();
    }
    let (id, rpc, body) = action(r);
    assert_eq!(body["method"], "tools/call");
    let op = r.machine().action_operation(id).unwrap();
    if !serial {
        r.link_queue_operation(op, 100 + op).unwrap();
    }
    (op, id, rpc)
}
fn open(r: &mut HostRuntime, id: u64) {
    r.begin_response(id, 200, r#"{"content-type":"text/event-stream"}"#)
        .unwrap();
}

#[test]
fn host_exhaustion_requires_a_paused_drained_call_and_keeps_original_context() {
    let (mut r, binding) = connected(Dialect::Legacy, true);
    let (op, id, _) = start(&mut r, &binding, false);
    assert_eq!(
        r.exhaust_response_resumption(id).unwrap_err().code,
        "InvalidHandle"
    );
    r.enable_response_resumption(id).unwrap();
    open(&mut r, id);
    assert_eq!(
        r.exhaust_response_resumption(id).unwrap_err().code,
        "ResumeUnavailable"
    );
    push(
        &mut r,
        id,
        "id: cursor\ndata:\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"cap\",\"method\":\"roots/list\"}\n\n",
    );
    r.interrupt_response_for(id).unwrap();
    assert_eq!(r.exhaust_response_resumption(id).unwrap_err().code, "Busy");
    assert_eq!(r.response_replies(id).unwrap().len(), 1);
    r.exhaust_response_resumption(id).unwrap();
    let error = r.take_completion(op).unwrap().unwrap_err();
    assert_eq!(error.code, "ResponseRecoveryExhausted");
    assert_eq!(error.operation_id, Some(op));
    assert_eq!(error.queue_operation_id, Some(100 + op));
    assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
    assert_eq!(
        r.exhaust_response_resumption(id).unwrap_err().code,
        "InvalidHandle"
    );
    assert!(r.take_actions().is_empty());
    let events = r.take_events();
    assert_eq!(
        events.events.iter().filter(|e| e.kind == "failed").count(),
        1
    );
}
fn push(r: &mut HostRuntime, id: u64, wire: &str) {
    assert!(!r.push_response_for(id, wire.as_bytes()).unwrap());
}
fn finish(r: &mut HostRuntime, id: u64, rpc: u64, op: u64, serial: bool) {
    let result = json!({"jsonrpc":"2.0","id":rpc,"result":{"content":[],"structuredContent":{"value":18446744073709551615u64}}});
    assert!(
        r.push_response_for(id, format!("data: {result}\n\n").as_bytes())
            .unwrap()
    );
    let outcome = if serial {
        r.take_outcome().unwrap()
    } else {
        r.take_completion(op).unwrap().unwrap()
    };
    let CallOutcome::Completed(response) = outcome else {
        panic!()
    };
    assert_eq!(response.operation_id, Some(op));
    assert_eq!(
        response.queue_operation_id,
        Some(if serial { 777 } else { 100 + op })
    );
    assert_eq!(
        response.raw["structuredContent"]["value"].as_u64(),
        Some(u64::MAX)
    );
}

#[test]
fn resume_keeps_operation_rpc_progress_and_session_but_rejects_old_stream_input() {
    for serial in [false, true] {
        for dialect in [Dialect::Legacy, Dialect::Auto] {
            let (mut r, b) = connected(dialect, true);
            let (op, id, rpc) = start(&mut r, &b, serial);
            r.enable_response_resumption(id).unwrap();
            open(&mut r, id);
            push(
                &mut r,
                id,
                "id: 游标/é\nretry: 25\ndata:\n\nid: uncommitted\ndata: {",
            );
            r.advance_time(100).unwrap();
            r.interrupt_response_for(id).unwrap();
            assert!(r.busy());
            assert!(r.take_completion(op).is_none());
            assert!(r.take_actions().is_empty());
            assert_eq!(r.response_resume(id).unwrap().retry_after_ms, 25);
            assert_eq!(r.resume_response(id).unwrap_err().code, "RetryLater");
            assert_eq!(
                r.begin_response(id, 200, "{}").unwrap_err().code,
                "InvalidHandle"
            );
            assert_eq!(
                r.push_response_for(id, b"data: {}\n\n").unwrap_err().code,
                "InvalidHandle"
            );
            r.advance_time(125).unwrap();
            let next = r.resume_response(id).unwrap();
            assert_ne!(next.action_id, id);
            assert_eq!(next.rpc_id, rpc);
            assert_eq!(next.operation_id, op);
            assert_eq!(next.method, "GET");
            assert!(next.body.is_none());
            assert_eq!(next.headers["last-event-id"], "游标/é");
            assert_eq!(next.headers["mcp-session-id"], "original-session");
            assert!(!next.headers.contains_key("content-type"));
            assert!(!format!("{next:?}").contains("游标"));
            assert!(r.take_actions().is_empty());
            assert_eq!(r.response_resume(id).unwrap_err().code, "InvalidHandle");
            assert_eq!(
                r.stream_message(id, json!({})).unwrap_err().code,
                "UnexpectedResponse"
            );
            open(&mut r, next.action_id);
            let progress = json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":format!("mcpx-{rpc}"),"progress":1}});
            push(&mut r, next.action_id, &format!("data: {progress}\n\n"));
            finish(&mut r, next.action_id, rpc, op, serial);
            assert!(r.response_replies(next.action_id).unwrap().is_empty());
            let events = r.take_events();
            assert_eq!(events.events.iter().filter(|e| e.is_terminal()).count(), 1);
            let progress = events.events.iter().find(|e| e.kind == "progress").unwrap();
            assert_eq!(progress.operation_id, op);
            assert_eq!(
                progress.queue_operation_id,
                Some(if serial { 777 } else { 100 + op })
            );
        }
    }
}
#[test]
fn sixteen_suspended_calls_keep_their_slots_and_resume_independently() {
    let (mut r, b) = connected(Dialect::Legacy, true);
    let mut calls = Vec::new();
    for n in 0..16 {
        let (op, id, rpc) = start(&mut r, &b, false);
        r.enable_response_resumption(id).unwrap();
        open(&mut r, id);
        push(&mut r, id, &format!("id: cursor-{n}\nretry: 0\n\n"));
        r.interrupt_response_for(id).unwrap();
        calls.push((op, id, rpc));
    }
    let args = b.args().finish().unwrap();
    assert_eq!(
        r.call_concurrent_bound(&b, &args).unwrap_err().code,
        "ConcurrencyLimit"
    );
    let mut ids = BTreeSet::new();
    for (op, id, rpc) in calls.into_iter().rev() {
        let next = r.resume_response(id).unwrap();
        assert!(ids.insert(next.action_id));
        open(&mut r, next.action_id);
        finish(&mut r, next.action_id, rpc, op, false);
        r.response_replies(next.action_id).unwrap();
    }
    assert!(!r.busy());
    assert_eq!(
        r.take_events()
            .events
            .iter()
            .filter(|e| e.is_terminal())
            .count(),
        16
    );
}
#[test]
fn accepted_capability_replies_block_resumption_and_survive_a_final_result() {
    let (mut r, b) = connected(Dialect::Legacy, true);
    let (op, id, rpc) = start(&mut r, &b, false);
    r.enable_response_resumption(id).unwrap();
    open(&mut r, id);
    let request = json!({"jsonrpc":"2.0","id":-7,"method":"roots/list","params":{}});
    push(&mut r, id, &format!("id: c\nretry: 0\ndata: {request}\n\n"));
    r.interrupt_response_for(id).unwrap();
    assert_eq!(r.response_resume(id).unwrap_err().code, "Busy");
    assert_eq!(r.resume_response(id).unwrap_err().code, "Busy");
    let replies = r.response_replies(id).unwrap();
    assert_eq!(replies[0]["id"], -7);
    assert_eq!(replies[0]["error"]["code"], -32601);
    let next = r.resume_response(id).unwrap();
    open(&mut r, next.action_id);
    push(&mut r, next.action_id, &format!("data: {request}\n\n"));
    finish(&mut r, next.action_id, rpc, op, false);
    assert_eq!(r.response_replies(next.action_id).unwrap().len(), 1);
    assert_eq!(
        r.response_replies(next.action_id).unwrap_err().code,
        "InvalidHandle"
    );
}
#[test]
fn cancelling_suspended_or_resumed_calls_preserves_unknown_and_one_terminal() {
    for resumed in [false, true] {
        let (mut r, b) = connected(Dialect::Legacy, true);
        let (op, id, _) = start(&mut r, &b, false);
        r.enable_response_resumption(id).unwrap();
        open(&mut r, id);
        push(&mut r, id, "id: x\nretry: 0\n\n");
        r.interrupt_response_for(id).unwrap();
        let current = if resumed {
            r.resume_response(id).unwrap().action_id
        } else {
            id
        };
        assert_eq!(
            r.release_response_resumption(current).unwrap_err().code,
            "Busy"
        );
        r.cancel_operation(op).unwrap();
        let error = r.take_completion(op).unwrap().unwrap_err();
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        assert_eq!(error.operation_id, Some(op));
        assert_eq!(error.queue_operation_id, Some(100 + op));
        assert_eq!(
            r.response_resume(current).unwrap_err().code,
            "InvalidHandle"
        );
        assert!(matches!(r.take_actions().as_slice(),[Action::Cancel{id}]if *id==current));
        assert_eq!(
            r.take_events()
                .events
                .iter()
                .filter(|e| e.is_terminal())
                .count(),
            1
        );
    }
}
#[test]
fn modern_stdio_metadata_and_undispatched_actions_cannot_enable_resumption() {
    let (mut modern, b) = connected(Dialect::Modern, false);
    let (_, id, _) = start(&mut modern, &b, false);
    assert_eq!(
        modern.enable_response_resumption(id).unwrap_err().code,
        "ResumeUnavailable"
    );
    let (mut r, b) = connected(Dialect::Legacy, false);
    let args = b.args().finish().unwrap();
    r.call_bound(&b, &args).unwrap();
    let id = r.machine().active_action_id().unwrap();
    assert_eq!(
        r.enable_response_resumption(id).unwrap_err().code,
        "ResumeUnavailable"
    );
    let (id, _, _) = action(&mut r);
    r.enable_response_resumption(id).unwrap();
    let mut meta = HostRuntime::new("https://example.com/mcp".into(), true).unwrap();
    meta.discover().unwrap();
    let (id, _, _) = action(&mut meta);
    assert_eq!(
        meta.enable_response_resumption(id).unwrap_err().code,
        "ResumeUnavailable"
    );
    let mut stdio = HostRuntime::stdio(Dialect::Legacy);
    stdio.discover().unwrap();
    let actions = stdio.take_actions();
    let Action::Stream { id, .. } = actions[0] else {
        panic!()
    };
    assert_eq!(
        stdio.enable_response_resumption(id).unwrap_err().code,
        "ResumeUnavailable"
    );
}
#[test]
fn untracked_eof_and_json_responses_keep_existing_terminal_semantics() {
    for tracked in [false, true] {
        let (mut r, b) = connected(Dialect::Legacy, true);
        let (op, id, rpc) = start(&mut r, &b, false);
        if tracked {
            r.enable_response_resumption(id).unwrap();
        }
        open(&mut r, id);
        push(&mut r, id, "id: x\n\n");
        r.finish_response_for(id).unwrap_err();
        assert_eq!(
            r.take_completion(op)
                .unwrap()
                .unwrap_err()
                .execution_certainty,
            ExecutionCertainty::Unknown
        );
        assert_eq!(r.response_resume(id).unwrap_err().code, "InvalidHandle");
        let (op, id, _) = start(&mut r, &b, false);
        r.enable_response_resumption(id).unwrap();
        r.begin_response(id, 200, r#"{"content-type":"application/json"}"#)
            .unwrap();
        assert_eq!(
            r.interrupt_response_for(id).unwrap_err().code,
            "ResumeUnavailable"
        );
        let rpc = r.machine().rpc_id_for_action(id).unwrap_or(rpc);
        r.push_response_for(
            id,
            json!({"jsonrpc":"2.0","id":rpc,"result":{"content":[]}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        r.finish_response_for(id).unwrap();
        assert!(r.take_completion(op).unwrap().is_ok());
    }
}
#[test]
fn bad_or_reset_cursors_refuse_interrupt_without_destroying_the_open_response() {
    for wire in [
        "data:\n\n",
        "id: x\n\nid:\n\n",
        "id:  padded \n\n",
        "id: a\tb\n\n",
    ] {
        let (mut r, b) = connected(Dialect::Legacy, true);
        let (op, id, rpc) = start(&mut r, &b, false);
        r.enable_response_resumption(id).unwrap();
        open(&mut r, id);
        push(&mut r, id, wire);
        if wire.contains("a\tb") {
            r.interrupt_response_for(id).unwrap();
        } else {
            assert_eq!(
                r.interrupt_response_for(id).unwrap_err().code,
                "ResumeUnavailable"
            );
            finish(&mut r, id, rpc, op, false);
            r.response_replies(id).unwrap();
        }
    }
}
#[test]
fn malformed_frames_bad_server_ids_and_reply_overflow_terminate_without_resume() {
    for wire in [
        "id: x\n\ndata: {bad}\n\n".to_string(),
        format!(
            "data: {}\n\n",
            json!({"jsonrpc":"2.0","id":null,"method":"roots/list"})
        ),
        format!(
            "data: {}\n\n",
            json!({"jsonrpc":"2.0","id":{} ,"method":"roots/list"})
        ),
        format!(
            "data: {}\n\n",
            json!({"jsonrpc":"2.0","id":-7,"method":"roots/list"})
        )
        .repeat(17),
    ] {
        let (mut r, b) = connected(Dialect::Legacy, true);
        let (op, id, _) = start(&mut r, &b, false);
        r.enable_response_resumption(id).unwrap();
        open(&mut r, id);
        assert!(r.push_response_for(id, wire.as_bytes()).is_err());
        assert!(r.take_completion(op).unwrap().is_err());
        assert_eq!(r.response_resume(id).unwrap_err().code, "InvalidHandle");
    }
}
#[test]
fn inherited_cursor_and_retry_survive_an_opening_failure_and_repeated_partial_eof() {
    let (mut r, b) = connected(Dialect::Legacy, false);
    let (op, id, rpc) = start(&mut r, &b, false);
    r.enable_response_resumption(id).unwrap();
    open(&mut r, id);
    push(&mut r, id, "id: keep\nretry: 0\n\n");
    r.interrupt_response_for(id).unwrap();
    let next = r.resume_response(id).unwrap();
    assert!(!next.headers.contains_key("mcp-session-id"));
    r.interrupt_response_for(next.action_id).unwrap();
    let again = r.resume_response(next.action_id).unwrap();
    open(&mut r, again.action_id);
    push(
        &mut r,
        again.action_id,
        "retry: 3\nid: not-committed\ndata: {",
    );
    r.advance_time(10).unwrap();
    r.interrupt_response_for(again.action_id).unwrap();
    assert_eq!(
        r.response_resume(again.action_id).unwrap().retry_after_ms,
        3
    );
    r.advance_time(13).unwrap();
    let last = r.resume_response(again.action_id).unwrap();
    assert_eq!(last.headers["last-event-id"], "keep");
    open(&mut r, last.action_id);
    finish(&mut r, last.action_id, rpc, op, false);
    r.release_response_resumption(last.action_id).unwrap();
}
#[test]
fn retry_overflow_and_authentication_changes_do_not_retarget_the_original_operation() {
    let (mut r, b) = connected(Dialect::Legacy, true);
    let (op, id, _) = start(&mut r, &b, false);
    r.enable_response_resumption(id).unwrap();
    open(&mut r, id);
    push(&mut r, id, "id: c\nretry: 184467440737095516160000\n\n");
    r.interrupt_response_for(id).unwrap();
    assert_eq!(r.response_resume(id).unwrap_err().code, "LimitExceeded");
    assert_eq!(
        r.accept_authorization("bob").unwrap_err().code,
        "BindingStale"
    );
    assert_eq!(r.reset_transport().unwrap_err().code, "Busy");
    r.fail_request(id, Error::local("TransportError", "host ended recovery"))
        .unwrap();
    assert_eq!(
        r.take_completion(op)
            .unwrap()
            .unwrap_err()
            .execution_certainty,
        ExecutionCertainty::Unknown
    );
    r.accept_authorization("bob").unwrap();
    assert_eq!(r.response_resume(id).unwrap_err().code, "InvalidHandle");
}
#[test]
fn resumed_get_requires_sse_and_preserves_auth_session_and_endpoint_rejections() {
    for (status, mime, code) in [
        (200, "application/json", "ProtocolError"),
        (401, "application/json", "AuthFailed"),
        (404, "application/json", "SessionExpired"),
        (405, "application/json", "HttpError"),
    ] {
        let (mut r, b) = connected(Dialect::Legacy, true);
        let (op, id, _) = start(&mut r, &b, false);
        r.enable_response_resumption(id).unwrap();
        open(&mut r, id);
        push(&mut r, id, "id: x\nretry: 0\n\n");
        r.interrupt_response_for(id).unwrap();
        let next = r.resume_response(id).unwrap();
        let error = r
            .begin_response_headers(
                next.action_id,
                status,
                [("content-type".into(), mime.into())],
            )
            .unwrap_err();
        assert_eq!(error.code, code);
        let error = r.take_completion(op).unwrap().unwrap_err();
        assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
        assert_eq!(
            r.response_resume(next.action_id).unwrap_err().code,
            "InvalidHandle"
        );
        assert!(r.take_actions().is_empty());
    }
}
#[test]
fn terminal_trackers_are_bounded_and_releasable_and_large_chunks_fail_only_their_call() {
    let (mut r, b) = connected(Dialect::Legacy, true);
    let mut retained = Vec::new();
    for _ in 0..16 {
        let (op, id, rpc) = start(&mut r, &b, false);
        r.enable_response_resumption(id).unwrap();
        open(&mut r, id);
        finish(&mut r, id, rpc, op, false);
        retained.push(id);
    }
    let (op, id, rpc) = start(&mut r, &b, false);
    assert_eq!(
        r.enable_response_resumption(id).unwrap_err().code,
        "LimitExceeded"
    );
    r.release_response_resumption(retained[0]).unwrap();
    r.enable_response_resumption(id).unwrap();
    open(&mut r, id);
    let (peer, peer_id, peer_rpc) = start(&mut r, &b, false);
    open(&mut r, peer_id);
    assert_eq!(
        r.push_response_for(id, &vec![b' '; 65537])
            .unwrap_err()
            .code,
        "LimitExceeded"
    );
    assert!(r.take_completion(op).unwrap().is_err());
    finish(&mut r, peer_id, peer_rpc, peer, false);
    for id in retained.into_iter().skip(1) {
        r.response_replies(id).unwrap();
    }
    let (op, id, _) = start(&mut r, &b, false);
    r.enable_response_resumption(id).unwrap();
    open(&mut r, id);
    let rpc = r.machine().rpc_id_for_action(id).unwrap_or(rpc);
    finish(&mut r, id, rpc, op, false);
    r.response_replies(id).unwrap();
}

#[test]
fn request_session_snapshot_survives_another_response_changing_the_current_session() {
    let (mut r, b) = connected(Dialect::Legacy, true);
    let (op, id, rpc) = start(&mut r, &b, false);
    let (peer, peer_id, peer_rpc) = start(&mut r, &b, false);
    r.begin_response_headers(
        peer_id,
        200,
        [
            ("content-type".into(), "application/json".into()),
            ("mcp-session-id".into(), "other-session".into()),
        ],
    )
    .unwrap();
    r.push_response_for(
        peer_id,
        json!({"jsonrpc":"2.0","id":peer_rpc,"result":{"content":[]}})
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    r.finish_response_for(peer_id).unwrap();
    r.take_completion(peer).unwrap().unwrap();
    assert_eq!(r.machine().session_id(), Some("other-session"));
    r.enable_response_resumption(id).unwrap();
    open(&mut r, id);
    push(&mut r, id, "id: c\nretry: 0\n\n");
    r.interrupt_response_for(id).unwrap();
    let next = r.resume_response(id).unwrap();
    assert_eq!(next.headers["mcp-session-id"], "original-session");
    open(&mut r, next.action_id);
    finish(&mut r, next.action_id, rpc, op, false);
    r.response_replies(next.action_id).unwrap();
}

#[test]
fn a_session_post_404_is_terminal_at_headers_and_keeps_the_first_observation() {
    let (mut r, binding) = connected(Dialect::Legacy, true);
    let (operation, id, _) = start(&mut r, &binding, false);
    let (peer, peer_id, rpc) = start(&mut r, &binding, false);
    r.advance_time(7).unwrap();
    let error = r
        .begin_response_headers(
            id,
            404,
            [("content-type".into(), "application/json".into())],
        )
        .unwrap_err();
    assert_eq!(error.code, "SessionExpired");
    assert_eq!(error.execution_certainty, ExecutionCertainty::Unknown);
    assert_eq!(error.operation_id, Some(operation));
    assert_eq!(r.machine().http_session_expired_at(), Some(7));
    assert!(r.machine().has_action(peer_id));
    r.take_completion(operation).unwrap().unwrap_err();
    r.advance_time(500).unwrap();
    open(&mut r, peer_id);
    finish(&mut r, peer_id, rpc, peer, false);
    assert_eq!(r.machine().http_session_expired_at(), Some(7));
    assert!(!r.machine().has_action(id));
    r.reset_transport().unwrap();
    assert!(!r.machine().http_session_expired());
    assert!(r.take_actions().is_empty());
}

#[test]
fn old_session_404_does_not_expire_a_newer_current_session() {
    for resumed in [false, true] {
        let (mut r, binding) = connected(Dialect::Legacy, true);
        let (operation, id, _) = start(&mut r, &binding, false);
        let (peer, peer_id, rpc) = start(&mut r, &binding, false);
        if resumed {
            r.enable_response_resumption(id).unwrap();
            open(&mut r, id);
            push(&mut r, id, "id: c\nretry: 0\ndata:\n\n");
            r.interrupt_response_for(id).unwrap();
        }
        r.begin_response_headers(
            peer_id,
            200,
            [
                ("content-type".into(), "text/event-stream".into()),
                ("mcp-session-id".into(), "new-session".into()),
            ],
        )
        .unwrap();
        finish(&mut r, peer_id, rpc, peer, false);
        assert_eq!(r.machine().session_id(), Some("new-session"));
        let target = if resumed {
            r.resume_response(id).unwrap().action_id
        } else {
            id
        };
        assert_eq!(
            r.begin_response_headers(target, 404, []).unwrap_err().code,
            "SessionExpired"
        );
        assert!(!r.machine().http_session_expired());
        assert_eq!(
            r.take_completion(operation)
                .unwrap()
                .unwrap_err()
                .execution_certainty,
            ExecutionCertainty::Unknown
        );
    }
}

#[test]
fn stateless_and_modern_404s_never_latch_session_expiry() {
    for dialect in [Dialect::Modern, Dialect::Legacy] {
        let (mut r, binding) = connected(dialect, false);
        let (operation, id, _) = start(&mut r, &binding, false);
        assert_eq!(
            r.begin_response_headers(id, 404, []).unwrap_err().code,
            "HttpError"
        );
        assert!(!r.machine().http_session_expired());
        r.take_completion(operation).unwrap().unwrap_err();
    }
}

#[test]
fn notification_404_latches_the_current_session_without_consuming_the_status() {
    let (mut r, _) = connected(Dialect::Legacy, true);
    let id = r.start_notifications().unwrap().resource_id;
    r.advance_time(51).unwrap();
    assert_eq!(
        r.begin_notifications(id, 404, "application/json")
            .unwrap_err()
            .code,
        "SessionExpired"
    );
    assert_eq!(r.machine().http_session_expired_at(), Some(51));
    assert_eq!(
        r.notification_status().unwrap().error_code.as_deref(),
        Some("SessionExpired")
    );
    r.reset_transport().unwrap();
    assert!(!r.machine().http_session_expired());
}
