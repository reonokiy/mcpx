use mcpx_core::{Action, HostRuntime, NotificationPhase};
use serde_json::{Value, json};

fn respond(runtime: &mut HostRuntime, action: &Action, result: Value) {
    let Action::Http { id, rpc_id, .. } = action else {
        panic!()
    };
    if rpc_id.is_none() {
        runtime.begin_response(*id, 202, "{}").unwrap();
        return;
    }
    runtime
        .begin_response(*id, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    runtime
        .push_response_for(
            *id,
            json!({"jsonrpc":"2.0","id":rpc_id,"result":result})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    runtime.finish_response_for(*id).unwrap();
}
fn connected(legacy: bool) -> HostRuntime {
    let mut runtime = HostRuntime::new("https://example.com/mcp".into(), legacy).unwrap();
    runtime.accept_authorization("alice").unwrap();
    runtime.discover().unwrap();
    while runtime.busy() {
        let actions = runtime.take_actions();
        assert_eq!(actions.len(), 1);
        let Action::Http { body, .. } = &actions[0] else {
            panic!()
        };
        let result = match body["method"].as_str().unwrap() {
            "server/discover" => json!({"supportedVersions":["2026-07-28"]}),
            "initialize" => json!({"protocolVersion":"2025-11-25"}),
            "notifications/initialized" => Value::Null,
            "tools/list" => {
                json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":"string"}}}}]})
            }
            _ => panic!(),
        };
        respond(&mut runtime, &actions[0], result);
    }
    runtime.take_events();
    runtime
}
fn frame(value: Value) -> Vec<u8> {
    format!("data: {value}\r\n\r\n").into_bytes()
}
fn ack(rpc: u64) -> Value {
    json!({"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":rpc},"notifications":{"toolsListChanged":true}}})
}
fn changed(rpc: u64) -> Value {
    json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":rpc}}})
}

fn interrupted_legacy(wire: &[u8]) -> (HostRuntime, u64) {
    let mut runtime = connected(true);
    let id = runtime.start_notifications().unwrap().resource_id;
    runtime
        .begin_notifications(id, 200, "text/event-stream")
        .unwrap();
    runtime.push_notifications(id, wire).unwrap();
    runtime.advance_time(100).unwrap();
    runtime.finish_notifications(id).unwrap_err();
    (runtime, id)
}

#[test]
fn legacy_resume_obeys_retry_and_uses_new_resource_without_any_business_replay() {
    let (mut runtime, old) =
        interrupted_legacy("id: 游标/é\nretry: 25\ndata:\n\nid: not-committed\ndata: {".as_bytes());
    assert_eq!(runtime.notification_resume(old).unwrap().retry_after_ms, 25);
    assert_eq!(
        runtime.resume_notifications(old).unwrap_err().code,
        "RetryLater"
    );
    assert_eq!(runtime.notification_status().unwrap().resource_id, old);
    runtime.advance_time(124).unwrap();
    assert_eq!(runtime.notification_resume(old).unwrap().retry_after_ms, 1);
    runtime.advance_time(125).unwrap();
    let next = runtime.resume_notifications(old).unwrap();
    assert_ne!(next.resource_id, old);
    assert_eq!(next.method, "GET");
    assert!(next.body.is_none() && next.rpc_id.is_none());
    assert_eq!(next.headers["last-event-id"], "游标/é");
    assert_eq!(next.headers["mcp-protocol-version"], "2025-11-25");
    assert!(runtime.take_actions().is_empty());
    assert_eq!(
        runtime
            .push_notifications(old, b"data: {}\n\n")
            .unwrap_err()
            .code,
        "InvalidHandle"
    );
    assert_eq!(
        runtime.abort_notifications(old, false).unwrap_err().code,
        "InvalidHandle"
    );
    runtime
        .begin_notifications(next.resource_id, 200, "text/event-stream")
        .unwrap();
    runtime
        .push_notifications(next.resource_id, b": no new ID\n\n")
        .unwrap();
    runtime.finish_notifications(next.resource_id).unwrap_err();
    runtime.advance_time(150).unwrap();
    assert_eq!(
        runtime
            .resume_notifications(next.resource_id)
            .unwrap()
            .headers["last-event-id"],
        "游标/é"
    );
}

#[test]
fn legacy_resume_requires_usable_cursor_and_respects_default_and_overflow_delays() {
    for wire in [
        "data:\n\n",
        "id: old\n\nid:\n\n",
        "id:  leading\n\n",
        "id: trailing \n\n",
        "id: bad\u{1}header\n\n",
    ] {
        let (mut runtime, id) = interrupted_legacy(wire.as_bytes());
        assert_eq!(
            runtime.resume_notifications(id).unwrap_err().code,
            "ResumeUnavailable"
        );
    }
    let (runtime, id) = interrupted_legacy(b"id: cursor\n\n");
    assert_eq!(
        runtime.notification_resume(id).unwrap().retry_after_ms,
        1000
    );
    let (mut runtime, id) = interrupted_legacy(b"id: valid\ttab\nretry: 0\n\n");
    assert_eq!(
        runtime.resume_notifications(id).unwrap().headers["last-event-id"],
        "valid\ttab"
    );
    let (runtime, id) =
        interrupted_legacy(b"id: cursor\nretry: 999999999999999999999999999999\n\n");
    assert_eq!(
        runtime.notification_resume(id).unwrap_err().code,
        "LimitExceeded"
    );
}

#[test]
fn legacy_resume_cannot_cross_stop_identity_session_errors_or_invalid_messages() {
    for kind in [
        "stop",
        "identity",
        "transport",
        "auth",
        "session",
        "malformed",
    ] {
        let mut runtime = connected(true);
        let id = runtime.start_notifications().unwrap().resource_id;
        runtime
            .begin_notifications(id, 200, "text/event-stream")
            .unwrap();
        runtime
            .push_notifications(id, b"id: cursor\nretry: 0\n\n")
            .unwrap();
        if kind == "malformed" {
            runtime
                .push_notifications(id, b"data: nope\n\n")
                .unwrap_err();
        } else {
            runtime.abort_notifications(id, false).unwrap();
            match kind {
                "stop" => runtime.stop_notifications(id).unwrap(),
                "transport" => runtime.reset_transport().unwrap(),
                "identity" => {
                    runtime.accept_authorization("bob").unwrap();
                }
                _ => {
                    let next = runtime.resume_notifications(id).unwrap();
                    runtime
                        .begin_notifications(
                            next.resource_id,
                            if kind == "auth" { 401 } else { 404 },
                            "text/event-stream",
                        )
                        .unwrap_err();
                    assert_eq!(
                        runtime
                            .notification_resume(next.resource_id)
                            .unwrap_err()
                            .code,
                        "ResumeUnavailable"
                    );
                    continue;
                }
            }
        }
        assert_eq!(
            runtime.notification_resume(id).unwrap_err().code,
            "ResumeUnavailable"
        );
    }
    let mut modern = connected(false);
    let plan = modern.start_notifications().unwrap();
    modern
        .begin_notifications(plan.resource_id, 200, "text/event-stream")
        .unwrap();
    modern
        .push_notifications(plan.resource_id, &frame(ack(plan.rpc_id.unwrap())))
        .unwrap();
    modern
        .push_notifications(plan.resource_id, b"id: cursor\nretry: 0\n\n")
        .unwrap();
    modern.abort_notifications(plan.resource_id, false).unwrap();
    assert_eq!(
        modern
            .resume_notifications(plan.resource_id)
            .unwrap_err()
            .code,
        "ResumeUnavailable"
    );
}

#[test]
fn legacy_resume_waits_for_accepted_replies_and_does_not_suppress_invalidation() {
    let mut runtime = connected(true);
    let id = runtime.start_notifications().unwrap().resource_id;
    runtime
        .begin_notifications(id, 200, "text/event-stream")
        .unwrap();
    runtime.push_notifications(id, b"id: cursor\nretry: 0\ndata: {\"jsonrpc\":\"2.0\",\"id\":-7,\"method\":\"roots/list\"}\n\n").unwrap();
    runtime.finish_notifications(id).unwrap_err();
    assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
    assert_eq!(runtime.notification_resume(id).unwrap_err().code, "Busy");
    assert_eq!(runtime.notification_replies(id).unwrap().len(), 1);
    let next = runtime.resume_notifications(id).unwrap();
    runtime
        .begin_notifications(next.resource_id, 200, "text/event-stream")
        .unwrap();
    assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
    runtime
        .push_notifications(next.resource_id, b"id:\ndata:\n\n")
        .unwrap();
    runtime.finish_notifications(next.resource_id).unwrap_err();
    assert_eq!(
        runtime
            .notification_resume(next.resource_id)
            .unwrap_err()
            .code,
        "ResumeUnavailable"
    );
}

#[test]
fn legacy_resume_opening_transport_failure_retains_only_the_prior_checkpoint() {
    let (mut runtime, id) = interrupted_legacy(b"id: cursor\nretry: 0\n\n");
    let second = runtime.resume_notifications(id).unwrap();
    runtime
        .abort_notifications(second.resource_id, false)
        .unwrap();
    let third = runtime.resume_notifications(second.resource_id).unwrap();
    assert_ne!(second.resource_id, third.resource_id);
    assert_eq!(third.headers["last-event-id"], "cursor");
    runtime
        .begin_notifications(third.resource_id, 401, "text/event-stream")
        .unwrap_err();
    assert_eq!(
        runtime
            .resume_notifications(third.resource_id)
            .unwrap_err()
            .code,
        "ResumeUnavailable"
    );
}

#[test]
fn modern_subscription_is_separate_from_calls_and_validates_ack_changes_and_final_result() {
    let mut runtime = connected(false);
    let binding = runtime.bind("echo").unwrap();
    let request = runtime.start_notifications().unwrap();
    assert!(!format!("{request:?}").contains("example.com"));
    let id = request.resource_id;
    let rpc = request.rpc_id.unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.headers["mcp-method"], "subscriptions/listen");
    assert!(!request.headers.contains_key("mcp-session-id"));
    assert_eq!(
        request.body.unwrap()["params"]["notifications"],
        json!({"toolsListChanged":true})
    );
    runtime
        .begin_notifications(id, 200, "text/event-stream; charset=utf-8")
        .unwrap();
    let bytes = frame(ack(rpc));
    for chunk in bytes.chunks(1) {
        assert!(!runtime.push_notifications(id, chunk).unwrap());
    }
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Listening
    );
    assert!(!runtime.catalog_status("alice").unwrap().needs_refresh);
    let operation = runtime
        .call_concurrent(binding, r#"{"message":"ordinary"}"#)
        .unwrap();
    let action = runtime.take_actions().pop().unwrap();
    let Action::Http {
        id: action_id,
        rpc_id: wire,
        ..
    } = action
    else {
        panic!()
    };
    assert_ne!(id, action_id);
    assert_ne!(id, rpc);
    assert_ne!(id, operation);
    assert_eq!(wire.unwrap(), rpc + 16);
    runtime
        .push_notifications(id, &frame(changed(rpc)))
        .unwrap();
    assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
    respond(
        &mut runtime,
        &action,
        json!({"content":[],"structuredContent":{"message":"ordinary"}}),
    );
    assert!(runtime.take_completion(operation).unwrap().is_ok());
    let final_result = json!({"jsonrpc":"2.0","id":rpc,"result":{"resultType":"complete","_meta":{"io.modelcontextprotocol/subscriptionId":rpc}}});
    assert!(
        runtime
            .push_notifications(id, &frame(final_result))
            .unwrap()
    );
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Closed
    );
    assert_eq!(
        runtime.push_notifications(id, b"ignored").unwrap_err().code,
        "InvalidHandle"
    );
}

#[test]
fn modern_rejects_wrong_ids_unrequested_notifications_bad_filters_and_ordering() {
    for variant in 0..7 {
        let mut runtime = connected(false);
        let request = runtime.start_notifications().unwrap();
        let id = request.resource_id;
        let rpc = request.rpc_id.unwrap();
        runtime
            .begin_notifications(id, 200, "text/event-stream")
            .unwrap();
        let mut message = ack(rpc);
        match variant {
            0 => message = changed(rpc),
            1 => {
                message["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"] =
                    json!(rpc + (1 << 32))
            }
            2 => {
                message["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"] =
                    json!(rpc.to_string())
            }
            3 => message["params"]["notifications"]["toolsListChanged"] = json!(false),
            4 => message["params"]["notifications"]["promptsListChanged"] = json!(true),
            5 => {
                runtime.push_notifications(id, &frame(ack(rpc))).unwrap();
                message["method"] = json!("notifications/progress");
            }
            6 => {
                runtime.push_notifications(id, &frame(ack(rpc))).unwrap();
            }
            _ => unreachable!(),
        }
        let error = runtime.push_notifications(id, &frame(message)).unwrap_err();
        assert_eq!(
            error.code,
            if variant == 3 {
                "CapabilityUnavailable"
            } else {
                "ProtocolError"
            }
        );
        assert_eq!(
            runtime.notification_status().unwrap().phase,
            NotificationPhase::Failed
        );
        assert!(
            !runtime
                .take_events()
                .events
                .iter()
                .any(|event| event.kind == "catalog_stale")
        );
        assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
    }
}

#[test]
fn legacy_get_accepts_notifications_and_bounds_unsupported_request_replies() {
    let mut runtime = connected(true);
    let request = runtime.start_notifications().unwrap();
    let id = request.resource_id;
    assert_eq!(request.method, "GET");
    assert!(request.body.is_none() && request.rpc_id.is_none());
    assert!(!request.headers.contains_key("mcp-method"));
    runtime
        .begin_notifications(id, 200, "TEXT/EVENT-STREAM")
        .unwrap();
    runtime
        .push_notifications(
            id,
            &frame(json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"})),
        )
        .unwrap();
    assert!(runtime.catalog_status("alice").unwrap().needs_refresh);
    for index in 0..16 {
        runtime.push_notifications(id,&frame(json!({"jsonrpc":"2.0","id":index,"method":"sampling/createMessage","params":{}}))).unwrap();
    }
    let replies = runtime.notification_replies(id).unwrap();
    assert_eq!(replies.len(), 16);
    assert_eq!(replies[3]["id"], 3);
    assert_eq!(replies[3]["error"]["code"], -32601);
    assert!(runtime.notification_replies(id).unwrap().is_empty());
    for index in 0..17 {
        let result = runtime.push_notifications(
            id,
            &frame(json!({"jsonrpc":"2.0","id":index,"method":"roots/list"})),
        );
        if index == 16 {
            assert_eq!(result.unwrap_err().code, "LimitExceeded");
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn contexts_close_old_resources_without_reusing_handles_or_accepting_late_frames() {
    let mut runtime = connected(false);
    let first = runtime.start_notifications().unwrap();
    assert_eq!(runtime.start_notifications().unwrap_err().code, "Busy");
    assert_eq!(
        runtime
            .begin_notifications(first.resource_id + (1 << 32), 200, "text/event-stream")
            .unwrap_err()
            .code,
        "InvalidHandle"
    );
    runtime.accept_authorization("bob").unwrap();
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Cancelled
    );
    assert_eq!(
        runtime
            .push_notifications(first.resource_id, &frame(ack(first.rpc_id.unwrap())))
            .unwrap_err()
            .code,
        "InvalidHandle"
    );
    assert_eq!(
        runtime.start_notifications().unwrap_err().code,
        "NotConnected"
    );
    let mut other = connected(false);
    let second = other.start_notifications().unwrap();
    other.reset_transport().unwrap();
    assert_eq!(
        other.notification_status().unwrap().phase,
        NotificationPhase::Cancelled
    );
    assert_eq!(
        other
            .begin_notifications(second.resource_id, 200, "text/event-stream")
            .unwrap_err()
            .code,
        "InvalidHandle"
    );
}

#[test]
fn malformed_framing_http_failures_disconnects_and_limits_remain_local() {
    for (status, mime, code) in [
        (401, "text/event-stream", "AuthenticationRequired"),
        (405, "text/event-stream", "CapabilityUnavailable"),
        (200, "application/json", "ProtocolError"),
        (200, "text/event-stream\r\nx: value", "ProtocolError"),
        (999, "text/event-stream", "InvalidResponse"),
    ] {
        let mut runtime = connected(false);
        let request = runtime.start_notifications().unwrap();
        assert_eq!(
            runtime
                .begin_notifications(request.resource_id, status, mime)
                .unwrap_err()
                .code,
            code
        );
        assert_eq!(
            runtime.notification_status().unwrap().phase,
            NotificationPhase::Failed
        );
        assert!(!runtime.busy());
    }
    for partial in [false, true] {
        let mut runtime = connected(false);
        let request = runtime.start_notifications().unwrap();
        runtime
            .begin_notifications(request.resource_id, 200, "text/event-stream")
            .unwrap();
        if partial {
            runtime
                .push_notifications(request.resource_id, b"data: {")
                .unwrap();
        }
        assert_eq!(
            runtime
                .finish_notifications(request.resource_id)
                .unwrap_err()
                .code,
            if partial {
                "ProtocolError"
            } else {
                "SubscriptionDisconnected"
            }
        );
    }
    let mut runtime = connected(false);
    let request = runtime.start_notifications().unwrap();
    runtime
        .begin_notifications(request.resource_id, 200, "text/event-stream")
        .unwrap();
    assert_eq!(
        runtime
            .push_notifications(request.resource_id, &vec![0; 65537])
            .unwrap_err()
            .code,
        "LimitExceeded"
    );
    let next = runtime.start_notifications().unwrap();
    assert_ne!(request.resource_id, next.resource_id);
    runtime.stop_notifications(next.resource_id).unwrap();
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Cancelled
    );
    let failed = runtime.start_notifications().unwrap();
    runtime
        .abort_notifications(failed.resource_id, true)
        .unwrap();
    assert_eq!(
        runtime.notification_status().unwrap().error_code.as_deref(),
        Some("Timeout")
    );
}

#[test]
fn long_lived_sse_limits_frames_and_chunks_without_a_cumulative_response_limit() {
    let mut runtime = connected(false);
    let request = runtime.start_notifications().unwrap();
    let id = request.resource_id;
    runtime
        .begin_notifications(id, 200, "text/event-stream")
        .unwrap();
    runtime
        .push_notifications(id, &frame(ack(request.rpc_id.unwrap())))
        .unwrap();
    let comments = b": keepalive\r\n\r\n".repeat(4096);
    assert!(comments.len() <= 65536);
    for _ in 0..300 {
        assert!(!runtime.push_notifications(id, &comments).unwrap());
    }
    assert!(comments.len() * 300 > 16 * 1024 * 1024);
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Listening
    );
    let long_line = vec![b'x'; 65536];
    for _ in 0..128 {
        runtime.push_notifications(id, &long_line).unwrap();
    }
    assert_eq!(
        runtime.push_notifications(id, b"x").unwrap_err().code,
        "LimitExceeded"
    );
    assert_eq!(
        runtime.notification_status().unwrap().phase,
        NotificationPhase::Failed
    );
}

#[test]
fn legacy_eof_keeps_accepted_capability_replies_but_context_changes_discard_them() {
    for revoke in [false, true] {
        let mut runtime = connected(true);
        let request = runtime.start_notifications().unwrap();
        let id = request.resource_id;
        runtime
            .begin_notifications(id, 200, "text/event-stream")
            .unwrap();
        runtime
            .push_notifications(
                id,
                &frame(json!({"jsonrpc":"2.0","id":-7,"method":"roots/list"})),
            )
            .unwrap();
        assert_eq!(
            runtime.finish_notifications(id).unwrap_err().code,
            "SubscriptionDisconnected"
        );
        assert_eq!(runtime.start_notifications().unwrap_err().code, "Busy");
        if revoke {
            runtime.accept_authorization("bob").unwrap();
            assert_eq!(
                runtime.notification_replies(id).unwrap_err().code,
                "InvalidHandle"
            );
        } else {
            assert_eq!(
                runtime.notification_replies(id).unwrap()[0]["error"]["code"],
                -32601
            );
            assert_ne!(runtime.start_notifications().unwrap().resource_id, id);
        }
    }
}
