use mcpx_core::CallOutcome;
use mcpx_native::{Execution, ProtocolMode, QueueOptions, QueuedRemote, ServerConfig};
use rmcp::ServiceExt;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::AtomicUsize},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[path = "support/sdk_server.rs"]
mod sdk_server;

// This same executable runs as an SDK subprocess, without a test harness on stdout.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--server") {
        let server = sdk_server::SdkServer(Arc::new(AtomicUsize::new(0)))
            .serve(rmcp::transport::stdio())
            .await
            .unwrap();
        let _ = server.waiting().await;
        std::fs::write(&args[2], "closed").unwrap();
        return;
    }
    println!("running 1 test");
    for (index, protocol) in [
        ProtocolMode::Modern,
        ProtocolMode::Legacy,
        ProtocolMode::Auto,
    ]
    .into_iter()
    .enumerate()
    {
        let marker = std::env::temp_dir().join(format!(
            "mcpx-sdk-stdio-{}-{index}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = ServerConfig::Stdio {
            program: std::env::current_exe().unwrap().to_string_lossy().into(),
            args: vec!["--server".into(), marker.to_string_lossy().into()],
            env: BTreeMap::new(),
            working_directory: None,
            protocol,
        };
        let remote = QueuedRemote::connect(config, QueueOptions::default())
            .await
            .unwrap();
        let binding = remote.tool("echo").await.unwrap();
        assert!(binding.submit_json(json!({"message":2})).is_err());
        let mut calls: Vec<
            std::pin::Pin<
                Box<dyn std::future::Future<Output = mcpx_core::Result<CallOutcome>> + Send>,
            >,
        > = Vec::new();
        for index in 0..16 {
            if index % 3 == 0 {
                calls.push(Box::pin(
                    binding
                        .submit_json(json!({"message":format!("SDK stdio {index}")}))
                        .unwrap(),
                ));
            } else {
                let call = if index % 3 == 1 {
                    remote.execute("echo", &format!("--message 'SDK stdio {index}'"))
                } else {
                    remote.execute_argv("echo", &["--message".into(), format!("SDK stdio {index}")])
                }
                .unwrap();
                calls.push(Box::pin(async move {
                    call.await.map(|value| match value {
                        Execution::Outcome(value) => value,
                        _ => panic!(),
                    })
                }));
            }
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            for (index, call) in calls.into_iter().enumerate() {
                let CallOutcome::Completed(response) = call.await.unwrap() else {
                    panic!()
                };
                assert_eq!(
                    response.raw["structuredContent"]["message"],
                    format!("SDK stdio {index}")
                );
            }
        })
        .await
        .unwrap();
        if index != 1 {
            remote.set_elicitation(true, false).unwrap().await.unwrap();
            let mut inputs = Vec::new();
            for _ in 0..16 {
                inputs.push(remote.execute("echo", "--message interactive").unwrap());
            }
            let mut resumes = Vec::new();
            for input in inputs {
                let Execution::Outcome(CallOutcome::NeedsInput(handle)) = input.await.unwrap()
                else {
                    panic!()
                };
                resumes.push(
                    remote
                        .resume(
                            &handle,
                            json!({"confirm":{"action":"accept","content":{"yes":true}}}),
                        )
                        .unwrap(),
                );
            }
            for call in resumes {
                assert!(matches!(call.await.unwrap(), CallOutcome::Completed(_)));
            }
        }
        remote.close().await.unwrap();
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "closed");
        std::fs::remove_file(marker).unwrap();
    }
    for (index, protocol) in [
        ProtocolMode::Modern,
        ProtocolMode::Legacy,
        ProtocolMode::Auto,
    ]
    .into_iter()
    .enumerate()
    {
        let marker = std::env::temp_dir().join(format!(
            "mcpx-sdk-stdio-notifications-{}-{index}",
            std::process::id()
        ));
        let config = ServerConfig::Stdio {
            program: std::env::current_exe().unwrap().to_string_lossy().into(),
            args: vec!["--server".into(), marker.to_string_lossy().into()],
            env: BTreeMap::new(),
            working_directory: None,
            protocol,
        };
        let remote = QueuedRemote::connect(config, QueueOptions::default())
            .await
            .unwrap();
        for _ in 0..2 {
            remote.start_notifications().unwrap().await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if remote
                        .notification_status()
                        .unwrap()
                        .await
                        .unwrap()
                        .is_some_and(|status| {
                            status.phase == mcpx_core::NotificationPhase::Listening
                        })
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let mut watcher = remote.subscribe_catalog().unwrap();
                let Execution::Outcome(CallOutcome::Completed(result)) = remote
                    .execute("echo", "--message 'SDK subscription change'")
                    .unwrap()
                    .await
                    .unwrap()
                else {
                    panic!()
                };
                assert_eq!(
                    result.raw["structuredContent"]["message"],
                    "SDK subscription change"
                );
                loop {
                    if watcher.changed().await.unwrap().invalidated {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(
                remote
                    .stop_notifications()
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap()
                    .phase,
                mcpx_core::NotificationPhase::Cancelled
            );
        }
        remote.close().await.unwrap();
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "closed");
        std::fs::remove_file(marker).unwrap();
    }
    println!("test official_sdk_stdio_concurrency_notifications_recovery_and_graceful_eof ... ok");
    println!("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out");
}
