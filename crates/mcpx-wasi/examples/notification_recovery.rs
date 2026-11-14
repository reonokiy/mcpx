//! Real WASIp2 HTTP recovery scenarios driven by smoke-wasi-recovery.mjs.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{
        Client, Credential, CredentialFuture, CredentialProvider, CredentialRequest, Dialect,
        ExecutionCertainty, HostRuntime, NotificationPhase, NotificationStatus, Result,
    };
    use mcpx_wasi::{
        NotificationRecoveryPolicy, WasiClientExt, WasiDriver, WasiQueue, WasiQueueOptions,
        run_blocking,
    };
    use serde_json::json;
    use std::{
        cell::Cell,
        future::poll_fn,
        io::Write,
        rc::Rc,
        task::Poll,
        time::{Duration, Instant},
    };
    #[derive(Default)]
    struct Shared {
        second: Cell<bool>,
        identity: Cell<bool>,
        blocked: Cell<bool>,
        calls: Cell<usize>,
        drops: Cell<usize>,
    }
    struct Provider(Rc<Shared>);
    impl Drop for Provider {
        fn drop(&mut self) {
            self.0.drops.set(self.0.drops.get() + 1);
        }
    }
    impl CredentialProvider for Provider {
        fn resolve<'a>(&'a mut self, _: CredentialRequest<'a>) -> CredentialFuture<'a> {
            self.0.calls.set(self.0.calls.get() + 1);
            Box::pin(poll_fn(move |_| {
                if self.0.blocked.get() {
                    Poll::Pending
                } else {
                    Poll::Ready(Credential::bearer(
                        if self.0.second.get() {
                            "notify-second"
                        } else {
                            "notify-snapshot"
                        }
                        .into(),
                        if self.0.identity.get() {
                            "new-identity"
                        } else {
                            "wasi-recovery"
                        }
                        .into(),
                    ))
                }
            }))
        }
    }
    fn stage(name: &str) {
        println!("{}", json!({"stage":name}));
        std::io::stdout().flush().unwrap();
    }
    fn handshake(name: &str) {
        stage(name);
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        assert_eq!(line, "go\n");
    }
    fn handoff(queue: &mut WasiQueue, name: &str) -> Result<()> {
        stage(name);
        let input = wasip2::cli::stdin::get_stdin();
        let mut bytes = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(8);
        while !bytes.ends_with(b"\n") {
            queue.poll(Duration::from_millis(5))?;
            bytes.extend(input.read(16).unwrap());
            assert!(Instant::now() < deadline, "handoff timeout {name}");
        }
        assert_eq!(bytes, b"go\n");
        Ok(())
    }
    fn until(
        queue: &mut WasiQueue,
        predicate: impl Fn(&NotificationStatus) -> bool,
    ) -> Result<NotificationStatus> {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            queue.poll(Duration::from_millis(10))?;
            if let Some(status) = queue.notification_status()
                && predicate(&status)
            {
                return Ok(status);
            }
            assert!(
                Instant::now() < deadline,
                "notification timeout: {:?}",
                queue.notification_status()
            );
        }
    }
    fn drive(queue: &mut WasiQueue, ms: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_millis(ms);
        while Instant::now() < deadline {
            queue.poll(Duration::from_millis(5))?;
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().unwrap();
    let protocol = args.next().unwrap();
    let mode = args.next().unwrap();
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
    let automatic = matches!(
        mode.as_str(),
        "automatic"
            | "window"
            | "drain-window"
            | "healthy"
            | "opening"
            | "401"
            | "404"
            | "405"
            | "reset"
            | "broken"
            | "writer-failure"
            | "auto-provider"
            | "policy-only"
            | "server-retry"
    ) || !legacy;
    let policy = automatic.then_some(NotificationRecoveryPolicy {
        max_attempts: if mode == "healthy" { 1 } else { 2 },
        backoff: Duration::from_millis(20),
        timeout: Duration::from_millis(match mode.as_str() {
            "window" | "drain-window" | "opening" => 100,
            "healthy" => 120,
            _ => 800,
        }),
    });
    let driver = WasiDriver::new(
        Credential::bearer("notify-snapshot".into(), "wasi-recovery".into())?,
        Duration::from_secs(5),
    )?
    .with_http_notifications(mode != "policy-only")
    .with_notification_recovery(policy)?;
    if mode == "client" || mode == "client-timeout" {
        let mut client = Client::new(driver, runtime);
        let result = (|| -> Result<()> {
            run_blocking(client.fresh_tool("echo"))?;
            let deadline = Instant::now() + Duration::from_secs(5);
            while client
                .notification_status()
                .is_none_or(|s| s.phase != NotificationPhase::Listening)
            {
                client.poll_notifications(Duration::from_millis(5))?;
                assert!(Instant::now() < deadline);
            }
            let first = client.notification_status().unwrap();
            handshake("eof");
            while client.notification_status().is_none_or(|s| {
                s.phase
                    != if legacy {
                        NotificationPhase::Failed
                    } else {
                        NotificationPhase::Closed
                    }
            }) {
                client.poll_notifications(Duration::from_millis(5))?;
                assert!(Instant::now() < deadline);
            }
            if !legacy {
                assert_eq!(
                    client
                        .resume_notifications(first.resource_id)
                        .unwrap_err()
                        .code,
                    "ResumeUnavailable"
                );
            } else if mode == "client-timeout" {
                let error = client
                    .resume_notifications_with_timeout(first.resource_id, Duration::from_millis(25))
                    .unwrap_err();
                assert_eq!(error.code, "Timeout");
                assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
            } else {
                stage("resume-waiting");
                let opening = client.resume_notifications(first.resource_id)?;
                assert_ne!(opening.resource_id, first.resource_id);
                while client
                    .notification_status()
                    .is_none_or(|s| s.phase != NotificationPhase::Listening)
                {
                    client.poll_notifications(Duration::from_millis(5))?;
                    assert!(Instant::now() < deadline);
                }
            }
            Ok(())
        })();
        let closed = run_blocking(client.close());
        result?;
        closed?;
        println!("{}", json!({"result":"client"}));
        return Ok(());
    }
    let shared = Rc::<Shared>::default();
    let mut queue = WasiQueue::new(
        driver,
        runtime,
        WasiQueueOptions {
            capacity: 16,
            ..Default::default()
        },
    )?;
    queue.set_credential_provider(Provider(shared.clone()), None)?;
    if mode == "reactor" || mode == "owner-drop" {
        queue.fresh_tool("echo")?;
        let first = until(&mut queue, |s| s.phase == NotificationPhase::Listening)?;
        handoff(&mut queue, "eof")?;
        until(&mut queue, |s| s.phase == NotificationPhase::Failed)?;
        if mode == "owner-drop" {
            let mut resume = queue.resume_notifications(first.resource_id)?;
            drop(queue);
            assert_eq!(shared.drops.get(), 1);
            assert_eq!(
                resume.try_result().unwrap().unwrap_err().code,
                "TransportClosed"
            );
        } else {
            let mut reactor = queue.into_reactor();
            let remote = reactor.remote();
            let resume = remote.resume_notifications(first.resource_id)?;
            let opening = reactor.run(resume)?;
            assert_ne!(opening.resource_id, first.resource_id);
            let deadline = Instant::now() + Duration::from_secs(5);
            while remote
                .notification_status()?
                .is_none_or(|s| s.phase != NotificationPhase::Listening)
            {
                reactor.poll(Duration::from_millis(5))?;
                assert!(Instant::now() < deadline);
            }
            reactor.close()?;
            drop(reactor);
            assert_eq!(remote.stats().unwrap_err().code, "TransportClosed");
            assert_eq!(shared.drops.get(), 1);
        }
        println!("{}", json!({"result":mode}));
        return Ok(());
    }
    let result = (|| -> Result<()> {
        queue.fresh_tool("echo")?;
        if mode == "policy-only" {
            assert!(queue.notification_status().is_none());
            return Ok(());
        }
        if mode == "automatic" {
            handoff(&mut queue, "exhausted")?;
            let failed = queue.notification_status().unwrap();
            assert_eq!(failed.phase, NotificationPhase::Failed);
            let mut call = queue.execute("echo", "--message after-exhaustion")?;
            queue.wait(&mut call)?;
            let mut resume = queue.resume_notifications(failed.resource_id)?;
            queue.wait(&mut resume)?;
            handoff(&mut queue, "override")?;
            queue.stop_notifications()?;
            return Ok(());
        }
        let first = until(&mut queue, |s| s.phase == NotificationPhase::Listening)?;
        if !legacy {
            handoff(&mut queue, "eof")?;
            until(&mut queue, |s| s.phase == NotificationPhase::Closed)?;
            let mut resume = queue.resume_notifications(first.resource_id)?;
            assert_eq!(
                queue.wait(&mut resume).unwrap_err().code,
                "ResumeUnavailable"
            );
            return Ok(());
        }
        if mode == "manual" || mode == "provider" || mode == "auto-provider" {
            shared.second.set(true);
            let mut refresh = queue.refresh()?;
            queue.wait(&mut refresh)?;
        }
        let mut calls = Vec::new();
        if mode == "manual" {
            for n in 0..16 {
                calls.push(queue.execute("echo", &format!("--message held-{n}"))?);
            }
        }
        let mut pending_provider = if mode == "provider" || mode == "auto-provider" {
            shared.blocked.set(true);
            let operation = queue.refresh()?;
            while !queue.stats().resolving {
                queue.poll(Duration::from_millis(5))?;
            }
            Some(operation)
        } else {
            None
        };
        handoff(&mut queue, if mode == "manual" { "full" } else { "eof" })?;
        if mode == "auto-provider" {
            let before = shared.calls.get();
            until(&mut queue, |s| {
                s.resource_id != first.resource_id && s.phase == NotificationPhase::Listening
            })?;
            assert_eq!(shared.calls.get(), before);
            queue.stop_notifications()?;
        } else if matches!(
            mode.as_str(),
            "window"
                | "drain-window"
                | "healthy"
                | "opening"
                | "401"
                | "404"
                | "405"
                | "reset"
                | "broken"
                | "writer-failure"
                | "disabled"
                | "server-retry"
        ) {
            match mode.as_str() {
                "server-retry" => {
                    until(&mut queue, |s| {
                        s.resource_id != first.resource_id
                            && s.phase == NotificationPhase::Listening
                    })?;
                    queue.stop_notifications()?;
                }
                "healthy" => {
                    until(&mut queue, |s| {
                        s.resource_id != first.resource_id
                            && s.phase == NotificationPhase::Listening
                    })?;
                    let second = queue.notification_status().unwrap().resource_id;
                    handoff(&mut queue, "healthy")?;
                    until(&mut queue, |s| {
                        s.resource_id != second && s.phase == NotificationPhase::Listening
                    })?;
                    handoff(&mut queue, "short-eof")?;
                    until(&mut queue, |s| s.phase == NotificationPhase::Failed)?;
                    drive(&mut queue, 100)?;
                }
                "opening" => {
                    until(&mut queue, |s| s.error_code.as_deref() == Some("Timeout"))?;
                }
                "401" | "404" | "405" => {
                    until(&mut queue, |s| {
                        s.resource_id != first.resource_id && s.phase == NotificationPhase::Failed
                    })?;
                    drive(&mut queue, 100)?;
                }
                "drain-window" => {
                    until(&mut queue, |s| s.phase == NotificationPhase::Failed)?;
                    handoff(&mut queue, "drain-window")?;
                    drive(&mut queue, 150)?;
                }
                "writer-failure" => {
                    until(&mut queue, |s| s.phase == NotificationPhase::Failed)?;
                    drive(&mut queue, 600)?;
                    let mut resume = queue.resume_notifications(first.resource_id)?;
                    assert_eq!(
                        queue.wait(&mut resume).unwrap_err().code,
                        "ResumeUnavailable"
                    );
                }
                "reset" | "broken" => {
                    until(&mut queue, |s| s.phase == NotificationPhase::Failed)?;
                    drive(&mut queue, 100)?;
                    let mut resume = queue.resume_notifications(first.resource_id)?;
                    assert_eq!(
                        queue.wait(&mut resume).unwrap_err().code,
                        "ResumeUnavailable"
                    );
                }
                _ => {
                    until(&mut queue, |s| s.phase == NotificationPhase::Failed)?;
                    drive(&mut queue, 350)?;
                }
            }
        } else {
            until(&mut queue, |s| s.phase == NotificationPhase::Failed)?;
            if mode == "timeout" {
                let mut resume = queue.resume_notifications(first.resource_id)?;
                queue.set_timeout(&resume, Duration::from_millis(25))?;
                let error = queue.wait(&mut resume).unwrap_err();
                assert_eq!(error.code, "Timeout");
                assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
                let mut cancelled = queue.resume_notifications(first.resource_id)?;
                cancelled.cancel();
                assert_eq!(queue.wait(&mut cancelled).unwrap_err().code, "Cancelled");
                let mut stopped = queue.resume_notifications(first.resource_id)?;
                queue.stop_notifications()?;
                assert_eq!(queue.wait(&mut stopped).unwrap_err().code, "Cancelled");
            } else if mode == "identity" {
                let mut resume = queue.resume_notifications(first.resource_id)?;
                shared.second.set(true);
                shared.identity.set(true);
                let mut refresh = queue.refresh()?;
                queue.wait(&mut refresh)?;
                assert_eq!(queue.wait(&mut resume).unwrap_err().code, "Cancelled");
                until(&mut queue, |s| s.phase == NotificationPhase::Listening)?;
            } else if mode == "drop" {
                let mut resume = queue.resume_notifications(first.resource_id)?;
                queue.close()?;
                assert_eq!(shared.drops.get(), 1);
                assert_eq!(
                    resume.try_result().unwrap().unwrap_err().code,
                    "TransportClosed"
                );
            } else {
                let before = shared.calls.get();
                let mut resume = queue.resume_notifications(first.resource_id)?;
                assert_eq!(
                    queue
                        .resume_notifications(first.resource_id)
                        .unwrap_err()
                        .code,
                    "QueueFull"
                );
                assert_eq!(
                    queue.notification_status().unwrap().phase,
                    NotificationPhase::Failed
                );
                handoff(&mut queue, "resume-waiting")?;
                let opening = queue.wait(&mut resume)?;
                assert_ne!(opening.resource_id, first.resource_id);
                until(&mut queue, |s| s.phase == NotificationPhase::Listening)?;
                assert_eq!(shared.calls.get(), before);
                handoff(&mut queue, "resumed")?;
            }
        }
        for mut call in calls {
            queue.wait(&mut call)?;
        }
        if let Some(mut pending) = pending_provider.take() {
            pending.cancel();
            assert_eq!(queue.wait(&mut pending).unwrap_err().code, "Cancelled");
            shared.blocked.set(false);
        }
        Ok(())
    })();
    let events = queue.drain_operation_events();
    let mut terminals = std::collections::BTreeSet::new();
    for event in &events.events {
        if matches!(
            event.kind,
            mcpx_core::OperationEventKind::Completed
                | mcpx_core::OperationEventKind::NeedsInput
                | mcpx_core::OperationEventKind::Failed
                | mcpx_core::OperationEventKind::Cancelled
        ) {
            assert!(
                terminals.insert(event.queue_operation_id),
                "duplicate lifecycle terminal"
            );
        }
    }
    assert!(
        events
            .events
            .iter()
            .filter(|event| matches!(
                event.kind,
                mcpx_core::OperationEventKind::Failed | mcpx_core::OperationEventKind::Cancelled
            ))
            .all(|event| event.payload["execution_certainty"] == "not_sent" || mode == "manual")
    );
    let closed = queue.close();
    result?;
    closed?;
    assert_eq!(shared.drops.get(), 1);
    println!("{}", json!({"result":"queue","stats":queue.stats()}));
    Ok(())
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("Build for wasm32-wasip2");
}
