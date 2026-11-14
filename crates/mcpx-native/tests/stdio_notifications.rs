use mcpx_core::{
    CallOutcome, Credential, CredentialFuture, CredentialProvider, CredentialRequest,
    NotificationPhase,
};
use mcpx_native::{Client, Execution, ProtocolMode, QueueOptions, QueuedRemote, ServerConfig};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;

struct Fixture {
    directory: PathBuf,
    legacy: bool,
    protocol: ProtocolMode,
    mode: &'static str,
}
impl Fixture {
    fn new(protocol: ProtocolMode, legacy: bool, mode: &'static str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let directory = std::env::temp_dir().join(format!(
            "mcpx-stdio-notify-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("commands"), "").unwrap();
        Self {
            directory,
            legacy,
            protocol,
            mode,
        }
    }
    fn config(&self) -> ServerConfig {
        let mut args = vec![
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../tests/fixtures/stdio_notifications.py")
                .to_str()
                .unwrap()
                .into(),
            "--directory".into(),
            self.directory.to_str().unwrap().into(),
            "--mode".into(),
            self.mode.into(),
        ];
        if self.legacy {
            args.push("--legacy".into());
        }
        ServerConfig::Stdio {
            program: "python3".into(),
            args,
            env: Default::default(),
            working_directory: None,
            protocol: self.protocol,
        }
    }
    fn records(&self) -> Vec<Value> {
        std::fs::read_to_string(self.directory.join("requests"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }
    fn requests(&self, method: &str) -> Vec<Value> {
        self.records()
            .into_iter()
            .filter_map(|value| {
                value
                    .get("request")
                    .filter(|r| r["method"] == method)
                    .cloned()
            })
            .collect()
    }
    fn command(&self, value: Value) {
        use std::io::Write;
        writeln!(
            std::fs::OpenOptions::new()
                .append(true)
                .open(self.directory.join("commands"))
                .unwrap(),
            "{value}"
        )
        .unwrap();
    }
    async fn wait(&self, condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn closed(&self) {
        self.wait(|| self.records().iter().any(|r| r["closed"] == true))
            .await;
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn options() -> QueueOptions {
    QueueOptions {
        stdio_notifications: true,
        ..Default::default()
    }
}
async fn phase(
    remote: &QueuedRemote,
    expected: NotificationPhase,
) -> mcpx_core::NotificationStatus {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = remote.notification_status().unwrap().await.unwrap()
                && status.phase == expected
            {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
fn message(value: CallOutcome) -> Value {
    let CallOutcome::Completed(value) = value else {
        panic!()
    };
    value.raw["structuredContent"]["message"].clone()
}
fn execution(value: Execution) -> CallOutcome {
    let Execution::Outcome(value) = value else {
        panic!()
    };
    value
}
fn modes() -> [(ProtocolMode, bool); 4] {
    [
        (ProtocolMode::Modern, false),
        (ProtocolMode::Legacy, true),
        (ProtocolMode::Auto, false),
        (ProtocolMode::Auto, true),
    ]
}

#[tokio::test]
async fn manual_stop_uses_reserved_capacity_and_explicit_restart_keeps_the_same_process() {
    for (protocol, legacy) in modes() {
        let fixture = Fixture::new(protocol, legacy, "normal");
        let remote = QueuedRemote::connect(
            fixture.config(),
            QueueOptions {
                capacity: 16,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(
            remote
                .notification_status()
                .unwrap()
                .await
                .unwrap()
                .is_none()
        );
        let first = remote
            .start_notifications()
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        phase(&remote, NotificationPhase::Listening).await;
        let mut calls = Vec::new();
        for index in 0..16 {
            calls.push(
                remote
                    .execute("echo", &format!("--message held-{index}"))
                    .unwrap(),
            );
        }
        fixture
            .wait(|| fixture.requests("tools/call").len() == 16)
            .await;
        assert_eq!(
            remote
                .execute("echo", "--message overflow")
                .err()
                .unwrap()
                .code,
            "QueueFull"
        );
        let stopped =
            tokio::time::timeout(Duration::from_secs(1), remote.stop_notifications().unwrap())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(stopped.phase, NotificationPhase::Cancelled);
        let status = tokio::time::timeout(
            Duration::from_secs(1),
            remote.notification_status().unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(status.resource_id, first.resource_id);
        fixture.command(json!({"kind":"release"}));
        for call in calls {
            call.await.unwrap();
        }
        let second = remote
            .start_notifications()
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        assert_ne!(second.resource_id, first.resource_id);
        phase(&remote, NotificationPhase::Listening).await;
        assert_eq!(
            remote
                .start_notifications()
                .unwrap()
                .await
                .unwrap()
                .unwrap()
                .resource_id,
            second.resource_id
        );
        assert_eq!(
            fixture
                .records()
                .iter()
                .filter(|r| r["start"] == true)
                .count(),
            1
        );
        remote.close().await.unwrap();
        fixture.closed().await;
        assert_eq!(
            fixture.requests("subscriptions/listen").len(),
            if legacy { 0 } else { 2 }
        );
        assert_eq!(
            fixture.requests("notifications/cancelled").len(),
            if legacy { 0 } else { 2 }
        );
    }
}

#[tokio::test]
async fn stop_supersedes_queued_start_before_business_completion_and_newer_start_remains_eligible()
{
    let fixture = Fixture::new(ProtocolMode::Modern, false, "normal");
    let remote = QueuedRemote::connect(
        fixture.config(),
        QueueOptions {
            capacity: 18,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut calls = Vec::new();
    for index in 0..16 {
        calls.push(
            remote
                .execute("echo", &format!("--message held-{index}"))
                .unwrap(),
        );
    }
    fixture
        .wait(|| fixture.requests("tools/call").len() == 16)
        .await;
    let old = remote.start_notifications().unwrap();
    assert!(
        remote
            .stop_notifications()
            .unwrap()
            .await
            .unwrap()
            .is_none()
    );
    let error = tokio::time::timeout(Duration::from_secs(1), old)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, "Cancelled");
    assert_eq!(
        error.execution_certainty,
        mcpx_core::ExecutionCertainty::NotSent
    );
    let new = remote.start_notifications().unwrap();
    assert!(fixture.requests("subscriptions/listen").is_empty());
    fixture.command(json!({"kind":"release"}));
    for call in calls {
        call.await.unwrap();
    }
    new.await.unwrap();
    phase(&remote, NotificationPhase::Listening).await;
    remote.close().await.unwrap();
    fixture.closed().await;
    assert_eq!(fixture.requests("subscriptions/listen").len(), 1);
    assert_eq!(fixture.requests("tools/call").len(), 16);
}

#[tokio::test]
async fn subscriptions_are_opt_in_and_idle_changes_invalidate_before_seventeenth_call() {
    for (protocol, legacy) in modes() {
        let fixture = Fixture::new(protocol, legacy, "normal");
        let remote = QueuedRemote::connect(fixture.config(), options())
            .await
            .unwrap();
        phase(&remote, NotificationPhase::Listening).await;
        let old = remote.tool("echo").await.unwrap();
        let mut calls = Vec::new();
        for index in 0..16 {
            calls.push(
                old.submit_json(json!({"message":format!("held-{index}")}))
                    .unwrap(),
            );
        }
        let last = remote.execute("echo", "--message 42").unwrap();
        fixture
            .wait(|| fixture.requests("tools/call").len() == 16)
            .await;
        let mut watcher = remote.subscribe_catalog().unwrap();
        fixture.command(json!({"kind":"change"}));
        assert!(
            tokio::time::timeout(Duration::from_secs(3), watcher.changed())
                .await
                .unwrap()
                .unwrap()
                .invalidated
        );
        assert_eq!(fixture.requests("tools/call").len(), 16);
        fixture.command(json!({"kind":"release"}));
        for (index, call) in calls.into_iter().enumerate() {
            assert_eq!(message(call.await.unwrap()), format!("held-{index}"));
        }
        assert_eq!(message(execution(last.await.unwrap())), 42);
        let error = old
            .submit_json(json!({"message":"stale"}))
            .unwrap()
            .await
            .unwrap_err();
        assert_eq!(error.code, "BindingStale");
        remote.close().await.unwrap();
        fixture.closed().await;
        assert_eq!(
            fixture.requests("subscriptions/listen").len(),
            usize::from(!legacy)
        );
        assert_eq!(
            fixture.requests("notifications/cancelled").len(),
            usize::from(!legacy)
        );
        assert_eq!(fixture.requests("tools/call").len(), 17);
        assert_eq!(fixture.requests("tools/list").len(), 2);
    }
    let fixture = Fixture::new(ProtocolMode::Modern, false, "normal");
    let remote = QueuedRemote::connect(fixture.config(), QueueOptions::default())
        .await
        .unwrap();
    assert!(
        remote
            .notification_status()
            .unwrap()
            .await
            .unwrap()
            .is_none()
    );
    remote.close().await.unwrap();
    fixture.closed().await;
    assert!(fixture.requests("subscriptions/listen").is_empty());
}

#[tokio::test]
async fn stop_during_start_metadata_waits_for_safe_boundary_without_closing_the_shared_process() {
    let fixture = Fixture::new(ProtocolMode::Modern, false, "metadata-hold");
    let remote = QueuedRemote::connect(fixture.config(), QueueOptions::default())
        .await
        .unwrap();
    remote.start_notifications().unwrap().await.unwrap();
    phase(&remote, NotificationPhase::Listening).await;
    remote.stop_notifications().unwrap().await.unwrap();
    let start = remote.start_notifications().unwrap();
    fixture
        .wait(|| fixture.requests("tools/list").len() == 2)
        .await;
    let stop = remote.stop_notifications().unwrap();
    let status = remote
        .notification_status_with_timeout(Duration::from_millis(20))
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), status)
            .await
            .unwrap()
            .unwrap_err()
            .code,
        "Timeout"
    );
    fixture.command(json!({"kind":"release_metadata"}));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), start)
            .await
            .unwrap()
            .unwrap_err()
            .code,
        "Cancelled"
    );
    assert_eq!(
        stop.await.unwrap().unwrap().phase,
        NotificationPhase::Cancelled
    );
    assert_eq!(
        message(execution(
            remote
                .execute("echo", "--message alive")
                .unwrap()
                .await
                .unwrap()
        )),
        "alive"
    );
    assert_eq!(
        fixture
            .records()
            .iter()
            .filter(|r| r["start"] == true)
            .count(),
        1
    );
    assert_eq!(fixture.requests("subscriptions/listen").len(), 1);
    remote.close().await.unwrap();
    fixture.closed().await;
}

#[tokio::test]
async fn missing_bad_and_rejected_acks_leave_the_process_usable_without_retry() {
    for mode in ["noack", "badack", "reject"] {
        let fixture = Fixture::new(ProtocolMode::Modern, false, mode);
        let remote = QueuedRemote::spawn_with(fixture.config(), options(), || {
            Ok(Client::new()?.with_timeout(Duration::from_millis(200)))
        })
        .await
        .unwrap();
        let status = phase(&remote, NotificationPhase::Failed).await;
        assert_eq!(
            status.error_code.as_deref(),
            Some(match mode {
                "noack" => "Timeout",
                "badack" => "ProtocolError",
                _ => "RemoteProtocolError",
            })
        );
        assert_eq!(
            message(execution(
                remote
                    .execute("echo", "--message good")
                    .unwrap()
                    .await
                    .unwrap()
            )),
            "good"
        );
        remote.close().await.unwrap();
        fixture.closed().await;
        assert_eq!(fixture.requests("subscriptions/listen").len(), 1);
        assert_eq!(
            fixture.requests("notifications/cancelled").len(),
            usize::from(mode != "reject")
        );
        assert_eq!(fixture.requests("tools/call").len(), 1);
    }
}

#[tokio::test]
async fn graceful_end_does_not_cancel_a_completed_subscription() {
    let fixture = Fixture::new(ProtocolMode::Modern, false, "normal");
    let remote = QueuedRemote::connect(fixture.config(), options())
        .await
        .unwrap();
    phase(&remote, NotificationPhase::Listening).await;
    fixture.command(json!({"kind":"graceful"}));
    phase(&remote, NotificationPhase::Closed).await;
    assert_eq!(
        message(execution(
            remote
                .execute("echo", "--message good")
                .unwrap()
                .await
                .unwrap()
        )),
        "good"
    );
    remote.close().await.unwrap();
    fixture.closed().await;
    assert_eq!(fixture.requests("subscriptions/listen").len(), 1);
    assert!(fixture.requests("notifications/cancelled").is_empty());
}

struct Provider {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    wait: Arc<AtomicBool>,
    bob: Arc<AtomicBool>,
}
impl CredentialProvider for Provider {
    fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(async move {
            if self.wait.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Credential::scoped(
                if self.bob.load(Ordering::SeqCst) {
                    "bob"
                } else {
                    "alice"
                }
                .into(),
            )
        })
    }
}

#[tokio::test]
async fn provider_wait_keeps_subscription_readable_and_identity_replaces_it_once() {
    let fixture = Fixture::new(ProtocolMode::Modern, false, "normal");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let wait = Arc::new(AtomicBool::new(false));
    let bob = Arc::new(AtomicBool::new(false));
    let provider = Provider {
        entered: entered.clone(),
        release: release.clone(),
        wait: wait.clone(),
        bob: bob.clone(),
    };
    let remote = QueuedRemote::spawn_with(fixture.config(), options(), move || {
        Ok(Client::new()?.with_credential_provider(provider))
    })
    .await
    .unwrap();
    let first = phase(&remote, NotificationPhase::Listening).await;
    let mut watcher = remote.subscribe_catalog().unwrap();
    wait.store(true, Ordering::SeqCst);
    let command = remote.execute("echo", "--message 42").unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    fixture.command(json!({"kind":"change"}));
    assert!(
        tokio::time::timeout(Duration::from_secs(3), watcher.changed())
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
    release.notify_one();
    assert_eq!(message(execution(command.await.unwrap())), 42);
    bob.store(true, Ordering::SeqCst);
    assert_eq!(
        message(execution(
            remote
                .execute("echo", "--message 43")
                .unwrap()
                .await
                .unwrap()
        )),
        43
    );
    let second = phase(&remote, NotificationPhase::Listening).await;
    assert_ne!(first.rpc_id, second.rpc_id);
    let mut watcher = remote.subscribe_catalog().unwrap();
    fixture.command(json!({"kind":"late","rpc":first.rpc_id.unwrap()}));
    fixture
        .wait(|| {
            fixture
                .records()
                .iter()
                .any(|r| r.pointer("/command/kind").is_some_and(|k| k == "late"))
        })
        .await;
    remote.notification_status().unwrap().await.unwrap();
    assert!(watcher.try_changed().is_pending());
    assert_eq!(
        phase(&remote, NotificationPhase::Listening).await.rpc_id,
        second.rpc_id
    );
    remote.close().await.unwrap();
    fixture.closed().await;
    assert_eq!(fixture.requests("subscriptions/listen").len(), 2);
    assert_eq!(fixture.requests("notifications/cancelled").len(), 2);
}

#[tokio::test]
async fn ack_deadline_and_cancellation_progress_while_provider_is_pending() {
    let fixture = Fixture::new(ProtocolMode::Modern, false, "noack");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let wait = Arc::new(AtomicBool::new(false));
    let provider = Provider {
        entered: entered.clone(),
        release: release.clone(),
        wait: wait.clone(),
        bob: Arc::new(AtomicBool::new(false)),
    };
    let remote = QueuedRemote::spawn_with(fixture.config(), options(), move || {
        Ok(Client::new()?
            .with_timeout(Duration::from_millis(250))
            .with_credential_provider(provider))
    })
    .await
    .unwrap();
    phase(&remote, NotificationPhase::Opening).await;
    let mut watcher = remote.subscribe_catalog().unwrap();
    wait.store(true, Ordering::SeqCst);
    let command = remote.execute("echo", "--message good").unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), watcher.changed())
            .await
            .unwrap()
            .unwrap()
            .invalidated
    );
    fixture
        .wait(|| fixture.requests("notifications/cancelled").len() == 1)
        .await;
    assert!(fixture.requests("tools/call").is_empty());
    // Leave the provider pending until its independent ordinary deadline ends.
    let error = command.await.err().unwrap();
    assert_eq!(error.code, "Timeout");
    assert_eq!(
        error.execution_certainty,
        mcpx_core::ExecutionCertainty::NotSent
    );
    assert_eq!(
        message(execution(
            remote
                .execute("echo", "--message recovered")
                .unwrap()
                .await
                .unwrap()
        )),
        "recovered"
    );
    assert_eq!(
        phase(&remote, NotificationPhase::Failed)
            .await
            .error_code
            .as_deref(),
        Some("Timeout")
    );
    remote.close().await.unwrap();
    fixture.closed().await;
    assert_eq!(fixture.requests("subscriptions/listen").len(), 1);
}

#[tokio::test]
async fn recovery_replaces_the_subscription_without_replaying_old_work() {
    let fixture = Fixture::new(ProtocolMode::Modern, false, "normal");
    let remote = QueuedRemote::spawn_with(fixture.config(), options(), || {
        Client::new()?.with_stdio_reconnect(mcpx_native::StdioReconnectPolicy {
            max_attempts: 2,
            backoff: Duration::from_millis(10),
            timeout: Duration::from_secs(3),
        })
    })
    .await
    .unwrap();
    let first = phase(&remote, NotificationPhase::Listening).await;
    let old = remote.tool("echo").await.unwrap();
    let call = old.submit_json(json!({"message":"held-lost"})).unwrap();
    fixture
        .wait(|| fixture.requests("tools/call").len() == 1)
        .await;
    fixture.command(json!({"kind":"exit"}));
    assert!(call.await.is_err());
    fixture
        .wait(|| {
            fixture
                .records()
                .iter()
                .filter(|r| r["start"] == true)
                .count()
                == 2
        })
        .await;
    let second = phase(&remote, NotificationPhase::Listening).await;
    assert_ne!(first.rpc_id, second.rpc_id);
    assert_eq!(
        message(execution(
            remote
                .execute("echo", "--message recovered")
                .unwrap()
                .await
                .unwrap()
        )),
        "recovered"
    );
    remote.close().await.unwrap();
    fixture.closed().await;
    assert_eq!(
        fixture
            .records()
            .iter()
            .filter(|r| r["start"] == true)
            .count(),
        2
    );
    assert_eq!(fixture.requests("tools/call").len(), 2);
    assert_eq!(fixture.requests("subscriptions/listen").len(), 2);
}
