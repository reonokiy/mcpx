//! Fixture-driven notification lifecycle example. The companion
//! `scripts/smoke-wasi-notifications.mjs` coordinates its JSON stage messages.

#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{CallOutcome, Client, Credential, Dialect, HostRuntime, NotificationPhase};
    use mcpx_wasi::{WasiClientExt, WasiDriver, run_blocking};
    use serde_json::json;
    use std::{
        cell::Cell,
        io::Write,
        rc::Rc,
        task::Poll,
        time::{Duration, Instant},
    };

    fn stage(name: &str) {
        println!("{}", json!({"stage":name}));
        std::io::stdout().flush().unwrap();
    }
    fn until(remote: &mut Client<WasiDriver>, phase: NotificationPhase) {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            remote
                .poll_notifications(Duration::from_millis(25))
                .unwrap();
            if remote
                .notification_status()
                .is_some_and(|s| s.phase == phase)
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "notification phase timed out: expected {phase:?}, actual {:?}",
                remote.notification_status()
            );
        }
    }
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().expect("endpoint");
    let protocol = args.next().expect("protocol");
    let scenario = args.next().unwrap_or_else(|| "normal".into());
    let legacy = protocol.ends_with("legacy");
    let runtime = HostRuntime::with_dialect(
        endpoint,
        if protocol.starts_with("auto") {
            Dialect::Auto
        } else if legacy {
            Dialect::Legacy
        } else {
            Dialect::Modern
        },
    )?;
    let armed = Rc::new(Cell::new(false));
    let observed = armed.clone();
    let short = matches!(
        scenario.as_str(),
        "noack" | "noheaders" | "lifetime" | "business-timeout"
    );
    let driver = WasiDriver::new(
        Credential::bearer("notify-snapshot".into(), "wasi-notify".into())?,
        Duration::from_millis(if short { 300 } else { 5000 }),
    )?
    .with_http_notifications(scenario != "manual")
    .with_event_handler(move |batch| {
        if observed.get()
            && batch
                .events
                .iter()
                .any(|event| event.kind == "catalog_stale")
        {
            observed.set(false);
            stage("during-call");
        }
    });
    let mut remote = Client::new(driver, runtime);
    let result = run_blocking(async {
        let binding = remote.fresh_tool("echo").await?;
        if matches!(
            scenario.as_str(),
            "noack" | "noheaders" | "reject" | "badmime"
        ) {
            until(&mut remote, NotificationPhase::Failed);
            let status = remote.notification_status().unwrap();
            assert_eq!(
                status.error_code.as_deref(),
                Some(match scenario.as_str() {
                    "noack" | "noheaders" => "Timeout",
                    "badmime" => "ProtocolError",
                    _ => "CapabilityUnavailable",
                })
            );
            let call = remote
                .call_json(&binding, json!({"message":"after-notification-failure"}))
                .await?;
            assert!(matches!(call, CallOutcome::Completed(_)));
            assert_eq!(
                remote.notification_status().unwrap().resource_id,
                status.resource_id
            );
            println!("{}", json!({"result":"failure-isolated","status":status}));
            return Ok(());
        }
        if scenario == "manual" {
            assert!(remote.notification_status().is_none());
            remote.start_notifications()?;
        }
        until(&mut remote, NotificationPhase::Listening);
        let first = remote.notification_status().unwrap();
        assert_eq!(remote.start_notifications()?.resource_id, first.resource_id);
        if scenario == "context" {
            remote
                .host_runtime_mut()
                .accept_authorization("foreign-context")?;
            remote.poll_notifications(Duration::ZERO)?;
            assert_eq!(
                remote.start_notifications().unwrap_err().code,
                "BindingStale"
            );
            assert_eq!(
                remote.notification_status().unwrap().phase,
                NotificationPhase::Cancelled
            );
            println!("{}", json!({"result":"context-rejected"}));
            return Ok(());
        }
        if scenario == "oversize" {
            stage("oversize");
            until(&mut remote, NotificationPhase::Failed);
            assert_eq!(
                remote.notification_status().unwrap().error_code.as_deref(),
                Some("LimitExceeded")
            );
            let fresh = remote.fresh_tool("echo").await?;
            remote
                .call_json(&fresh, json!({"message":"after-oversize"}))
                .await?;
            remote.start_notifications()?;
            until(&mut remote, NotificationPhase::Listening);
            println!("{}", json!({"result":"oversize-isolated"}));
            return Ok(());
        }
        if scenario == "lifetime" {
            let end = Instant::now() + Duration::from_millis(750);
            while Instant::now() < end {
                remote.poll_notifications(Duration::from_millis(100))?;
            }
            assert_eq!(
                remote.notification_status().unwrap().phase,
                NotificationPhase::Listening
            );
        }
        let mut watcher = remote.subscribe_catalog()?;
        stage("idle");
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            remote.poll_notifications(Duration::from_millis(25))?;
            if let Poll::Ready(Some(change)) = watcher.try_changed()
                && change.invalidated
            {
                break;
            }
            assert!(Instant::now() < deadline, "idle invalidation missing");
        }
        let binding = remote.fresh_tool("echo").await?;
        armed.set(true);
        stage("held");
        let call = remote
            .call_json(&binding, json!({"message":"held-0"}))
            .await;
        assert!(
            !armed.get(),
            "ordinary HTTP wait did not service notifications"
        );
        if scenario == "business-timeout" {
            assert_eq!(call.unwrap_err().code, "Timeout");
            assert_eq!(
                remote.notification_status().unwrap().phase,
                NotificationPhase::Listening
            );
            stage("timed-out");
            let fresh = remote.fresh_tool("echo").await?;
            remote
                .call_json(&fresh, json!({"message":"after-business-timeout"}))
                .await?;
            println!("{}", json!({"result":"business-timeout-isolated"}));
            return Ok(());
        }
        assert!(matches!(call?, CallOutcome::Completed(_)));
        stage("eof");
        until(
            &mut remote,
            if legacy {
                NotificationPhase::Failed
            } else {
                NotificationPhase::Closed
            },
        );
        let ended = remote.notification_status().unwrap();
        if legacy {
            assert_eq!(
                ended.error_code.as_deref(),
                Some(if scenario == "reader-failure" {
                    "ProtocolError"
                } else {
                    "SubscriptionDisconnected"
                })
            );
            for _ in 0..4 {
                remote.poll_notifications(Duration::from_millis(10))?;
            }
        }
        let fresh = remote.fresh_tool("echo").await?;
        let call = remote
            .call_json(&fresh, json!({"message":"before-reply-ack"}))
            .await?;
        assert!(matches!(call, CallOutcome::Completed(_)));
        stage("reply-business");
        if scenario == "writer-timeout" {
            let until = Instant::now() + Duration::from_millis(650);
            while Instant::now() < until {
                remote.poll_notifications(Duration::from_millis(25))?;
            }
            stage("reply-timeout");
        }
        remote.poll_notifications(Duration::from_millis(50))?;
        assert_eq!(
            remote.notification_status().unwrap().resource_id,
            ended.resource_id
        );
        remote.start_notifications()?;
        until(&mut remote, NotificationPhase::Listening);
        if !legacy {
            stage("bad");
            until(&mut remote, NotificationPhase::Failed);
            assert_eq!(
                remote.notification_status().unwrap().error_code.as_deref(),
                Some("ProtocolError")
            );
        }
        remote.stop_notifications()?;
        remote.start_notifications()?;
        until(&mut remote, NotificationPhase::Listening);
        remote.stop_notifications()?;
        assert_eq!(
            remote.notification_status().unwrap().phase,
            NotificationPhase::Cancelled
        );
        let before = remote.notification_status().unwrap().resource_id;
        remote.fresh_tool("echo").await?;
        assert_eq!(remote.notification_status().unwrap().resource_id, before);
        assert!(
            remote
                .poll_notifications(Duration::from_secs(86401))
                .is_err()
        );
        println!(
            "{}",
            json!({"result":"notification-flow","legacy":legacy,"status":remote.notification_status()})
        );
        Ok(())
    });
    let closed = run_blocking(remote.close());
    result?;
    closed?;
    assert_eq!(
        remote.poll_notifications(Duration::ZERO).unwrap_err().code,
        "TransportClosed"
    );
    Ok(())
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("Build this example with --target wasm32-wasip2");
}
