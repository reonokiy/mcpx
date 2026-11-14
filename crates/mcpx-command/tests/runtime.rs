use mcpx_command::HostRuntime;
use serde_json::{Value, json};

fn action(runtime: &mut HostRuntime) -> (u64, Value) {
    let actions: Vec<Value> = serde_json::from_str(&runtime.actions()).unwrap();
    let request = actions.into_iter().find(|a| a["kind"] == "http").unwrap();
    (
        request["id"].as_u64().unwrap(),
        serde_json::from_str(request["body"].as_str().unwrap()).unwrap(),
    )
}

#[test]
fn runtime_reads_modern_error_body_before_deciding_on_downgrade() {
    for code in [-32020, -32022, -32601] {
        let mut runtime = HostRuntime::auto("http://localhost/mcp".into()).unwrap();
        runtime.discover().unwrap();
        let (id, _) = action(&mut runtime);
        assert!(
            !runtime
                .begin_response(id, 400, r#"{"content-type":"application/json"}"#)
                .unwrap()
        );
        let wire =
            json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(id).unwrap(),"error":{"code":code,"message":"rejected"}}).to_string();
        for bytes in wire.as_bytes().chunks(3) {
            assert!(!runtime.push_response(bytes).unwrap());
        }
        let result = runtime.finish_response();
        if code == -32601 {
            result.unwrap();
            assert_eq!(action(&mut runtime).1["method"], "initialize");
        } else {
            assert!(result.is_err());
            assert!(!runtime.busy());
            assert_eq!(runtime.actions(), "[]");
        }
    }
}

#[test]
fn late_response_does_not_consume_the_new_operation() {
    let mut runtime = HostRuntime::auto("http://localhost/mcp".into()).unwrap();
    runtime.discover().unwrap();
    let (old, _) = action(&mut runtime);
    runtime.cancel();
    runtime.discover().unwrap();
    let (current, _) = action(&mut runtime);
    assert!(
        runtime
            .begin_response(old, 200, r#"{"content-type":"application/json"}"#)
            .is_err()
    );
    assert!(runtime.busy());
    runtime
        .begin_response(current, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    runtime
        .push_response(
            json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(current).unwrap(),"result":{"supportedVersions":["2026-07-28"]}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    runtime.finish_response().unwrap();
    assert_eq!(action(&mut runtime).1["method"], "tools/list");
}

#[test]
fn malformed_stream_releases_response_and_allows_fresh_discovery() {
    let mut runtime = HostRuntime::auto("http://localhost/mcp".into()).unwrap();
    runtime.discover().unwrap();
    let (id, _) = action(&mut runtime);
    runtime
        .begin_response(id, 200, r#"{"content-type":"text/event-stream"}"#)
        .unwrap();
    assert!(runtime.push_response(b"data: not-json\n\n").is_err());
    assert!(!runtime.busy());
    runtime.discover().unwrap();
    let (next, _) = action(&mut runtime);
    assert_ne!(id, next);
    assert!(
        !runtime
            .begin_response(next, 200, r#"{"content-type":"application/json"}"#)
            .unwrap()
    );
}

#[test]
fn runtime_command_line_uses_literal_shared_tokenizer_and_local_help() {
    use mcpx_command::CommandRuntimeExt;
    let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
    runtime.discover().unwrap();
    for result in [
        json!({"supportedVersions":["2026-07-28"]}),
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":"string"}},"required":["message"]}}]}),
    ] {
        let (id, _) = action(&mut runtime);
        runtime
            .begin_response(id, 200, r#"{"content-type":"application/json"}"#)
            .unwrap();
        runtime
            .push_response(
                &serde_json::to_vec(&json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(id).unwrap(),"result":result})).unwrap(),
            )
            .unwrap();
        runtime.finish_response().unwrap();
    }
    assert_eq!(runtime.command_line("  ").unwrap_err().code, "CommandError");
    assert_eq!(
        runtime.command_line("echo --message '").unwrap_err().code,
        "TokenizeError"
    );
    assert!(
        runtime
            .command_line("echo --help")
            .unwrap()
            .unwrap()
            .contains("--message")
    );
    assert_eq!(runtime.actions(), "[]");
    runtime
        .command_line(r#"echo --message "雪 $HOME $(echo x)""#)
        .unwrap();
    let (_, rpc) = action(&mut runtime);
    assert_eq!(rpc["params"]["arguments"]["message"], "雪 $HOME $(echo x)");
}

fn discovered_runtime() -> HostRuntime {
    let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
    runtime.discover().unwrap();
    for result in [
        json!({"supportedVersions":["2026-07-28"]}),
        json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":"string"}},"required":["message"]}}]}),
    ] {
        let (id, _) = action(&mut runtime);
        runtime
            .begin_response(id, 200, r#"{"content-type":"application/json"}"#)
            .unwrap();
        runtime
            .push_response(
                &serde_json::to_vec(&json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(id).unwrap(),"result":result})).unwrap(),
            )
            .unwrap();
        runtime.finish_response().unwrap();
    }
    runtime.take_events();
    runtime
}

#[test]
fn command_admission_shares_sixteen_slots_and_errors_or_help_do_not_disturb_them() {
    use mcpx_command::{CommandAdmission, CommandRuntimeExt};
    let mut runtime = discovered_runtime();
    let binding = runtime.tool_binding("echo").unwrap();
    let mut calls = Vec::new();
    for index in 0..16 {
        let value = format!("message {index} $HOME");
        let operation = match index % 3 {
            0 => runtime
                .call_concurrent_bound(
                    &binding,
                    &binding.validate_json(json!({"message":value})).unwrap(),
                )
                .unwrap(),
            1 => match runtime
                .command_concurrent("echo", &["--message".into(), value.clone()])
                .unwrap()
            {
                CommandAdmission::Operation(id) => id,
                _ => panic!(),
            },
            _ => match runtime
                .command_line_concurrent(&format!("echo --message '{value}'"))
                .unwrap()
            {
                CommandAdmission::Operation(id) => id,
                _ => panic!(),
            },
        };
        let (request, body) = action(&mut runtime);
        assert_eq!(body["params"]["arguments"]["message"], value);
        calls.push((operation, request, value));
    }
    assert!(
        matches!(runtime.command_line_concurrent("echo --help").unwrap(), CommandAdmission::Help(text) if text.contains("--message"))
    );
    for (line, expected) in [
        ("echo --unknown x", "CommandParseError"),
        ("echo --message '", "TokenizeError"),
        ("echo --message overflow", "ConcurrencyLimit"),
    ] {
        let error = match runtime.command_line_concurrent(line) {
            Ok(_) => panic!("expected error"),
            Err(error) => error,
        };
        assert_eq!(error.code, expected);
    }
    assert!(runtime.take_actions().is_empty());
    for (operation, request, value) in calls.into_iter().rev() {
        runtime
            .begin_response(request, 200, r#"{"content-type":"text/event-stream"}"#)
            .unwrap();
        let frame = format!(
            "data: {}\n\n",
            json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(request).unwrap(),"result":{"content":[],"structuredContent":{"message":value}}})
        );
        assert!(
            runtime
                .push_response_for(request, frame.as_bytes())
                .unwrap()
        );
        let mcpx_core::CallOutcome::Completed(response) =
            runtime.take_completion(operation).unwrap().unwrap()
        else {
            panic!()
        };
        assert_eq!(response.raw["structuredContent"]["message"], value);
        assert!(runtime.take_completion(operation).is_none());
    }
    assert!(!runtime.busy());
    assert_eq!(
        runtime
            .take_events()
            .events
            .iter()
            .filter(|event| event.kind == "completed")
            .count(),
        16
    );
}

#[test]
fn cancelling_one_named_command_leaves_other_typed_and_named_calls_available() {
    use mcpx_command::{CommandAdmission, CommandRuntimeExt};
    let mut runtime = discovered_runtime();
    let mut operations = Vec::new();
    for value in ["first", "second"] {
        let CommandAdmission::Operation(operation) = runtime
            .command_line_concurrent(&format!("echo --message {value}"))
            .unwrap()
        else {
            panic!()
        };
        let (request, _) = action(&mut runtime);
        operations.push((operation, request));
    }
    runtime.cancel_operation(operations[0].0).unwrap();
    assert_eq!(
        runtime
            .take_completion(operations[0].0)
            .unwrap()
            .unwrap_err()
            .code,
        "Cancelled"
    );
    runtime.take_actions();
    let request = operations[1].1;
    runtime
        .begin_response(request, 200, r#"{"content-type":"application/json"}"#)
        .unwrap();
    runtime
        .push_response_for(
            request,
            &serde_json::to_vec(&json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(request).unwrap(),"result":{"content":[]}}))
                .unwrap(),
        )
        .unwrap();
    runtime.finish_response_for(request).unwrap();
    assert!(runtime.take_completion(operations[1].0).unwrap().is_ok());
    assert!(matches!(
        runtime.command_line_concurrent("echo --help").unwrap(),
        CommandAdmission::Help(_)
    ));
}
