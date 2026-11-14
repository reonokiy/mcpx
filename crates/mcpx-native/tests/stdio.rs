use mcpx_core::{CallOutcome, Dialect};
use mcpx_native::{Client, StdioConfig};
use serde_json::json;
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn fixture(args: &[&str]) -> StdioConfig {
    let script =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/stdio_server.py");
    let mut argv = vec![script.to_str().unwrap().into()];
    argv.extend(args.iter().map(|s| (*s).into()));
    StdioConfig::new("python3", argv)
}

fn temp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "mcpx-{}-{}-{name}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn modern_stdio_drains_verbose_stderr_and_closes_gracefully() {
    let marker = temp("eof");
    let mut remote = Client::new()
        .unwrap()
        .connect_stdio(
            fixture(&["--verbose", "--eof-marker", marker.to_str().unwrap()]),
            Dialect::Modern,
        )
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    let CallOutcome::Completed(response) = remote
        .call_json(&binding, json!({"message":"stdio"}))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(response.raw["structuredContent"]["message"], "stdio");
    assert!(
        remote
            .drain_events()
            .events
            .iter()
            .any(|e| e.kind == "progress" && e.operation_id > 0)
    );
    remote.close().await.unwrap();
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "closed");
    std::fs::remove_file(marker).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn auto_stdio_detects_legacy_without_replaying_tool_calls() {
    let mut remote = Client::new()
        .unwrap()
        .connect_stdio(fixture(&["--legacy"]), Dialect::Auto)
        .await
        .unwrap();
    assert_eq!(remote.machine().dialect(), Dialect::Legacy);
    let binding = remote.tool("echo").unwrap();
    assert!(
        remote
            .call_json(&binding, json!({"message":"legacy"}))
            .await
            .is_ok()
    );
    remote.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn silent_probe_restarts_process_before_legacy_handshake() {
    let counter = temp("starts");
    let mut remote = Client::new()
        .unwrap()
        .connect_stdio(
            fixture(&["--silent-probe", "--start-count", counter.to_str().unwrap()]),
            Dialect::Auto,
        )
        .await
        .unwrap();
    assert_eq!(remote.machine().dialect(), Dialect::Legacy);
    assert_eq!(std::fs::read_to_string(&counter).unwrap(), "2");
    remote.close().await.unwrap();
    std::fs::remove_file(counter).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn process_arguments_are_literal_and_environment_is_configurable() {
    let literal = "$(echo should-not-run); * $HOME";
    let mut config = fixture(&[literal]);
    config
        .env
        .insert("MCPX_FIXTURE_VALUE".into(), "configured".into());
    config.working_directory = Some(std::env::temp_dir());
    let mut remote = Client::new()
        .unwrap()
        .connect_stdio(config, Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("inspect").unwrap();
    let CallOutcome::Completed(response) = remote.call_json(&binding, json!({})).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(response.raw["structuredContent"]["literal"], literal);
    assert_eq!(
        response.raw["structuredContent"]["environment"],
        "configured"
    );
    remote.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_stdout_closes_transport_and_never_becomes_a_result() {
    let mut remote = Client::new()
        .unwrap()
        .connect_stdio(fixture(&["--invalid"]), Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    assert_eq!(
        remote
            .call_json(&binding, json!({"message":"x"}))
            .await
            .err()
            .unwrap()
            .code,
        "ProtocolError"
    );
    assert_eq!(
        remote
            .call_json(&binding, json!({"message":"x"}))
            .await
            .err()
            .unwrap()
            .code,
        "TransportClosed"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn timeout_terminates_the_process_without_retrying() {
    let mut remote = Client::new()
        .unwrap()
        .with_timeout(Duration::from_millis(100))
        .connect_stdio(fixture(&["--delay", "2"]), Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    assert_eq!(
        remote
            .call_json(&binding, json!({"message":"x"}))
            .await
            .err()
            .unwrap()
            .code,
        "Timeout"
    );
    assert_eq!(
        remote
            .call_json(&binding, json!({"message":"x"}))
            .await
            .err()
            .unwrap()
            .code,
        "TransportClosed"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn library_cancellation_notifies_stdio_and_closes_without_forced_shutdown() {
    use std::{cell::RefCell, rc::Rc};
    for dialect in [Dialect::Modern, Dialect::Legacy] {
        let marker = temp("cancel-eof");
        let log = temp("cancel-log");
        let handle: Rc<RefCell<Option<mcpx_native::CancellationHandle>>> =
            Rc::new(RefCell::new(None));
        let callback = handle.clone();
        let mut remote = Client::new()
            .unwrap()
            .with_event_handler(move |event| {
                let mcpx_native::EventStreamItem::Event(event) = event else {
                    panic!("unexpected core loss in fixture")
                };
                if event.kind == "progress"
                    && let Some(handle) = callback.borrow().as_ref()
                {
                    handle.cancel();
                }
            })
            .connect_stdio(
                fixture(&[
                    "--async-calls",
                    "--delay",
                    "60",
                    "--eof-marker",
                    marker.to_str().unwrap(),
                    "--requests-log",
                    log.to_str().unwrap(),
                ]),
                dialect,
            )
            .await
            .unwrap();
        *handle.borrow_mut() = Some(remote.cancellation_handle());
        let binding = remote.tool("echo").unwrap();
        assert_eq!(
            remote
                .call_json(&binding, json!({"message":"cancel"}))
                .await
                .unwrap_err()
                .code,
            "Cancelled"
        );
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "closed");
        let requests: Vec<serde_json::Value> = std::fs::read_to_string(&log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let call = requests
            .iter()
            .find(|request| request["method"] == "tools/call")
            .unwrap();
        let cancellation = requests
            .iter()
            .find(|request| request["method"] == "notifications/cancelled")
            .unwrap();
        assert_eq!(cancellation["requestId"], call["id"]);
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["method"] == "tools/call")
                .count(),
            1
        );
        std::fs::remove_file(marker).unwrap();
        std::fs::remove_file(log).unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn slow_event_consumers_receive_lag_counts_without_losing_call_outcomes() {
    use mcpx_native::EventStreamItem;
    let (client, mut events) = Client::new().unwrap().with_event_stream(4).unwrap();
    let mut remote = client
        .connect_stdio(fixture(&[]), Dialect::Modern)
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    for _ in 0..2 {
        remote
            .call_json(&binding, json!({"message":"ok"}))
            .await
            .unwrap();
    }
    assert_eq!(
        remote
            .call_json(&binding, json!({"message":"blocked"}))
            .await
            .unwrap_err()
            .code,
        "ConsumerLagged"
    );
    assert!(matches!(
        events.try_next(),
        Some(EventStreamItem::Lagged { dropped: 2 })
    ));
    let mut terminal = vec![];
    while let Some(EventStreamItem::Event(event)) = events.try_next() {
        assert!(event.is_terminal());
        terminal.push(event.kind);
    }
    assert_eq!(terminal, ["catalog_updated", "completed", "completed"]);
    remote.close().await.unwrap();
    assert!(events.next().await.is_none());
}

#[tokio::test]
async fn dropped_reconnect_cleanup_is_retryable_and_final_close_cannot_reopen() {
    let starts = temp("explicit-reconnect-starts");
    let log = temp("explicit-reconnect-log");
    let (client, mut events) = Client::new()
        .unwrap()
        .with_timeout(Duration::from_secs(2))
        .with_event_stream(64)
        .unwrap();
    let mut remote = client
        .connect_stdio(
            fixture(&[
                "--start-count",
                starts.to_str().unwrap(),
                "--requests-log",
                log.to_str().unwrap(),
                "--eof-delay",
                "0.3",
            ]),
            Dialect::Auto,
        )
        .await
        .unwrap();
    let old = remote.tool("echo").unwrap();
    let args = old.args_from(json!({"message":"before"})).unwrap();
    remote.call(&old, &args).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), remote.reconnect())
            .await
            .is_err()
    );
    assert!(remote.is_suspended());
    assert_eq!(
        remote.call(&old, &args).await.unwrap_err().code,
        "TransportClosed"
    );
    remote.reconnect().await.unwrap();
    assert!(!remote.is_suspended());
    assert_eq!(
        remote.call(&old, &args).await.unwrap_err().code,
        "BindingStale"
    );
    let fresh = remote.tool("echo").unwrap();
    remote
        .call_json(&fresh, json!({"message":"after"}))
        .await
        .unwrap();
    while events.try_next().is_some() {}
    assert!(
        tokio::time::timeout(Duration::from_millis(20), events.next())
            .await
            .is_err(),
        "transport recovery must not end the lifetime event stream"
    );
    remote.close().await.unwrap();
    assert_eq!(
        remote.reconnect().await.unwrap_err().code,
        "TransportClosed"
    );
    while events.next().await.is_some() {}
    assert_eq!(std::fs::read_to_string(&starts).unwrap(), "2");
    let records: Vec<serde_json::Value> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        records
            .iter()
            .filter(|record| record["method"] == "tools/call")
            .count(),
        2
    );
    let ids: Vec<_> = records
        .iter()
        .filter_map(|record| record["id"].as_u64())
        .collect();
    assert_eq!(
        ids.len(),
        ids.iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    );
    std::fs::remove_file(starts).unwrap();
    std::fs::remove_file(log).unwrap();
}

#[tokio::test]
async fn auto_reconnect_retries_preferred_protocol_after_a_legacy_session() {
    let starts = temp("auto-reconnect-preference");
    let mut remote = Client::new()
        .unwrap()
        .connect_stdio(
            fixture(&[
                "--legacy-first-launch",
                "--start-count",
                starts.to_str().unwrap(),
            ]),
            Dialect::Auto,
        )
        .await
        .unwrap();
    assert_eq!(remote.machine().dialect(), Dialect::Legacy);
    remote.reconnect().await.unwrap();
    assert_eq!(remote.machine().dialect(), Dialect::Modern);
    let binding = remote.tool("echo").unwrap();
    remote
        .call_json(&binding, json!({"message":"modern-after-legacy"}))
        .await
        .unwrap();
    remote.close().await.unwrap();
    assert_eq!(std::fs::read_to_string(&starts).unwrap(), "2");
    std::fs::remove_file(starts).unwrap();
}

#[tokio::test]
async fn same_identity_reconnect_keeps_registered_schema_references_usable() {
    let registry=mcpx_core::SchemaRegistry::new([
        ("https://schemas.mcpx.test/common.json".into(),json!({"$defs":{"message":{"type":"string","minLength":1}}})),
        ("https://schemas.mcpx.test/result.json".into(),json!({"type":"object","properties":{"message":{"$ref":"https://schemas.mcpx.test/common.json#/$defs/message"}},"required":["message"]})),
    ]).unwrap();
    let mut remote = Client::new()
        .unwrap()
        .with_schema_registry(registry)
        .connect_stdio(fixture(&["--schema-refs"]), Dialect::Modern)
        .await
        .unwrap();
    for epoch in 0..2 {
        if epoch > 0 {
            remote.reconnect().await.unwrap();
        }
        let binding = remote.tool("echo").unwrap();
        assert_eq!(
            binding.args_from(json!({"message":5})).err().unwrap().code,
            "ValidationFailed"
        );
        let CallOutcome::Completed(response) = remote
            .call_json(&binding, json!({"message":"registered"}))
            .await
            .unwrap()
        else {
            panic!()
        };
        assert!(matches!(
            response.output_validation,
            mcpx_core::OutputValidation::Valid
        ));
        assert_eq!(remote.schema_registry_stats().resources, 2);
    }
    remote.close().await.unwrap();
}
