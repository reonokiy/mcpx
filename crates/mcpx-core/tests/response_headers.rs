use mcpx_core::{Action, HostRuntime};
use serde_json::json;

fn pending() -> (HostRuntime, u64) {
    let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
    runtime.discover().unwrap();
    let actions = runtime.take_actions();
    let [Action::Http { id, .. }] = actions.as_slice() else {
        panic!()
    };
    (runtime, *id)
}
fn correct(runtime: &mut HostRuntime, id: u64) {
    runtime
        .begin_response_headers(
            id,
            200,
            [
                ("Content-Type".into(), "application/json".into()),
                ("content-type".into(), "application/json".into()),
            ],
        )
        .unwrap();
    runtime
        .push_response_for(
            id,
            json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(id).unwrap(),"result":{"supportedVersions":["2026-07-28"]}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    runtime.finish_response_for(id).unwrap();
    assert_eq!(runtime.take_actions().len(), 1);
}

#[test]
fn invalid_status_and_field_shapes_do_not_open_or_damage_the_live_response() {
    let (mut runtime, id) = pending();
    for status in [0, 99, 600, u16::MAX] {
        assert_eq!(
            runtime
                .begin_response_headers(id, status, [])
                .unwrap_err()
                .code,
            "InvalidResponse"
        );
    }
    for fields in [
        vec![("".into(), "value".into())],
        vec![("bad:name".into(), "value".into())],
        vec![("x-test".into(), "bad\r\nheader".into())],
        vec![("x-test".into(), "bad\0header".into())],
        vec![
            ("Content-Type".into(), "application/json".into()),
            ("content-type".into(), "text/event-stream".into()),
        ],
        vec![
            ("Mcp-Session-Id".into(), "one".into()),
            ("mcp-session-id".into(), "two".into()),
        ],
    ] {
        assert_eq!(
            runtime
                .begin_response_headers(id, 200, fields)
                .unwrap_err()
                .code,
            "InvalidResponse"
        );
    }
    assert!(runtime.busy());
    correct(&mut runtime, id);
}

#[test]
fn header_count_and_bytes_are_bounded_on_typed_and_json_paths() {
    let (mut runtime, id) = pending();
    assert_eq!(
        runtime
            .begin_response_headers(
                id,
                200,
                (0..257).map(|index| (format!("x-{index}"), "ok".into()))
            )
            .unwrap_err()
            .code,
        "LimitExceeded"
    );
    assert_eq!(
        runtime
            .begin_response_headers(id, 200, [("x-large".into(), "x".repeat(65536))])
            .unwrap_err()
            .code,
        "LimitExceeded"
    );
    assert_eq!(
        runtime
            .begin_response(id, 200, &" ".repeat(65537))
            .unwrap_err()
            .code,
        "LimitExceeded"
    );
    correct(&mut runtime, id);
}

#[test]
fn repeated_extension_fields_are_accepted_without_changing_singleton_semantics() {
    let (mut runtime, id) = pending();
    runtime
        .begin_response_headers(
            id,
            200,
            [
                ("Content-Type".into(), "application/json".into()),
                ("X-Extension".into(), "first".into()),
                ("x-extension".into(), "second".into()),
            ],
        )
        .unwrap();
    runtime
        .push_response_for(
            id,
            json!({"jsonrpc":"2.0","id":runtime.machine().rpc_id_for_action(id).unwrap(),"result":{"supportedVersions":["2026-07-28"]}})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    runtime.finish_response_for(id).unwrap();
    assert_eq!(runtime.take_actions().len(), 1);
}
