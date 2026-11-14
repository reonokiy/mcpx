//! Fixture-driven standalone concurrent WASI client. The companion Node smoke
//! coordinates barriers; no real service is called by these example scenarios.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{
        CallOutcome, Credential, Dialect, ExecutionCertainty, HostRuntime, OperationEventKind,
    };
    use mcpx_wasi::{Execution, WasiDriver, WasiQueue, WasiQueueOptions};
    use serde::Serialize;
    use serde_json::json;
    use std::{cell::Cell, io::Write, rc::Rc, time::Duration};
    fn stage(name: &str) {
        println!("{}", json!({"stage":name}));
        std::io::stdout().flush().unwrap();
    }
    fn handshake(name: &str) {
        stage(name);
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "go");
    }
    fn handshake_driving(queue: &mut WasiQueue, name: &str) -> mcpx_core::Result<()> {
        stage(name);
        let input = wasip2::cli::stdin::get_stdin();
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\n") {
            queue.poll(Duration::from_millis(5))?;
            bytes.extend(input.read(16).expect("fixture input"));
        }
        assert_eq!(bytes, b"go\n");
        Ok(())
    }
    fn message(value: CallOutcome) -> String {
        let CallOutcome::Completed(value) = value else {
            panic!("expected completed call");
        };
        value.raw["structuredContent"]["message"]
            .as_str()
            .unwrap()
            .into()
    }
    fn execute(value: Execution) -> CallOutcome {
        let Execution::Outcome(value) = value else {
            panic!("expected call");
        };
        value
    }
    #[derive(Serialize)]
    struct Echo {
        message: String,
    }
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().unwrap();
    let protocol = args.next().unwrap();
    let scenario = args.next().unwrap_or_else(|| "parallel".into());
    let runtime = HostRuntime::with_dialect(
        endpoint.clone(),
        if protocol.starts_with("auto") {
            Dialect::Auto
        } else if protocol == "legacy" {
            Dialect::Legacy
        } else {
            Dialect::Modern
        },
    )?;
    let armed = Rc::new(Cell::new(false));
    let observed = armed.clone();
    let driver = WasiDriver::new(
        Credential::bearer("notify-snapshot".into(), "wasi-queue".into())?,
        Duration::from_secs(5),
    )?
    .with_http_notifications(scenario == "parallel")
    .with_event_handler(move |batch| {
        if observed.get() && batch.events.iter().any(|e| e.kind == "catalog_stale") {
            observed.set(false);
            stage("changed");
        }
    });
    let mut queue = WasiQueue::new(
        driver,
        runtime,
        WasiQueueOptions {
            capacity: if scenario == "budgets" { 2 } else { 19 },
            max_bytes: if scenario == "budgets" {
                1024
            } else {
                8 * 1024 * 1024
            },
            ..Default::default()
        },
    )?;
    if scenario == "drop" {
        let binding = queue.fresh_tool("echo")?;
        let mut first = queue.submit_json(&binding, json!({"message":"held-drop-1"}))?;
        let mut second = queue.submit_json(&binding, json!({"message":"held-drop-2"}))?;
        let mut good = queue.submit_json(&binding, json!({"message":"retained-result"}))?;
        for _ in 0..20 {
            queue.poll(Duration::ZERO)?;
            std::thread::sleep(Duration::from_millis(2));
        }
        handshake_driving(&mut queue, "active")?;
        while !good.is_finished() {
            queue.poll(Duration::from_millis(10))?;
        }
        drop(queue);
        assert_eq!(
            first.try_result().unwrap().unwrap_err().code,
            "TransportClosed"
        );
        assert_eq!(
            second.try_result().unwrap().unwrap_err().code,
            "TransportClosed"
        );
        assert_eq!(message(good.try_result().unwrap()?), "retained-result");
        println!("{}", json!({"result":"dropped"}));
        return Ok(());
    }
    let result = (|| -> mcpx_core::Result<()> {
        if scenario.starts_with("metadata") {
            handshake("constructed");
            let mut binding = queue.bind("echo")?;
            handshake("submitted");
            if scenario == "metadata-timeout" {
                queue.set_timeout(&binding, Duration::from_millis(300))?;
            }
            for _ in 0..20 {
                queue.poll(Duration::ZERO)?;
                std::thread::sleep(Duration::from_millis(2));
            }
            handshake_driving(&mut queue, "metadata-pending")?;
            if scenario == "metadata-cancel" {
                binding.cancel();
            }
            if scenario == "metadata" {
                queue.wait(&mut binding)?;
            } else {
                let error = queue.wait(&mut binding).err().unwrap();
                assert_eq!(
                    error.code,
                    if scenario == "metadata-cancel" {
                        "Cancelled"
                    } else {
                        "Timeout"
                    }
                );
                assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
                stage("metadata-failed");
                queue.fresh_tool("echo")?;
            }
            println!("{}", json!({"result":"metadata","stats":queue.stats()}));
            return Ok(());
        }
        let binding = queue.fresh_tool("echo")?;
        assert_eq!(
            queue
                .submit_json(&binding, json!({"message":3}))
                .unwrap_err()
                .code,
            "ValidationFailed"
        );
        let mut help = queue.execute("echo", "--help")?;
        assert!(matches!(queue.wait(&mut help)?, Execution::Help(_)));
        let mut completion = queue.complete_line("ec", 2)?;
        assert_eq!(queue.wait(&mut completion)?.candidates[0].value, "echo");
        if scenario == "budgets" {
            let unsupported = WasiQueue::new(
                WasiDriver::new(Credential::anonymous(), Duration::from_secs(5))?,
                HostRuntime::stdio(Dialect::Modern),
                WasiQueueOptions::default(),
            );
            assert_eq!(unsupported.err().unwrap().code, "CapabilityUnavailable");
            let mut busy = HostRuntime::auto(endpoint.clone())?;
            busy.discover()?;
            let busy = WasiQueue::new(
                WasiDriver::new(Credential::anonymous(), Duration::from_secs(5))?,
                busy,
                WasiQueueOptions::default(),
            );
            assert_eq!(busy.err().unwrap().code, "Busy");
            assert_eq!(
                queue
                    .submit_json(&binding, json!({"message":"é".repeat(450)}))
                    .unwrap_err()
                    .code,
                "QueueFull"
            );
            let mut accepted = queue.submit_json(&binding, json!({"message":"a".repeat(450)}))?;
            let mut foreign = WasiQueue::new(
                WasiDriver::new(Credential::anonymous(), Duration::from_secs(5))?,
                HostRuntime::auto(endpoint.clone())?,
                WasiQueueOptions::default(),
            )?;
            assert_eq!(
                foreign.wait(&mut accepted).unwrap_err().code,
                "InvalidHandle"
            );
            assert_eq!(
                foreign
                    .set_timeout(&accepted, Duration::from_millis(1))
                    .unwrap_err()
                    .code,
                "InvalidHandle"
            );
            assert_eq!(
                foreign
                    .submit_json(&binding, json!({"message":"never-foreign"}))
                    .unwrap_err()
                    .code,
                "BindingStale"
            );
            foreign.close()?;
            accepted.cancel();
            queue.poll(Duration::ZERO)?;
            assert_eq!(queue.wait(&mut accepted).unwrap_err().code, "Cancelled");
            struct MustNotSerialize;
            impl Serialize for MustNotSerialize {
                fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                    Err(serde::ser::Error::custom("serializer must not run"))
                }
            }
            let mut first = queue.submit_json(&binding, json!({"message":"never-budget-1"}))?;
            let mut second = queue.submit_json(&binding, json!({"message":"never-budget-2"}))?;
            assert_eq!(
                queue
                    .submit_typed(&binding, MustNotSerialize)
                    .unwrap_err()
                    .code,
                "QueueFull"
            );
            first.cancel();
            second.cancel();
            queue.poll(Duration::ZERO)?;
            assert_eq!(queue.wait(&mut first).unwrap_err().code, "Cancelled");
            assert_eq!(queue.wait(&mut second).unwrap_err().code, "Cancelled");
            assert_eq!(queue.stats().input_bytes, 0);
        } else if scenario == "resume" {
            let mut configure = queue.set_elicitation(true, false)?;
            queue.wait(&mut configure)?;
            let mut initial = Vec::new();
            for _ in 0..16 {
                initial.push(queue.submit_json(&binding, json!({"message":"interactive"}))?);
            }
            let mut handles = Vec::new();
            for operation in &mut initial {
                let CallOutcome::NeedsInput(handle) = queue.wait(operation)? else {
                    panic!("expected input");
                };
                handles.push(handle);
            }
            let responses = json!({"confirm":{"action":"accept","content":{"yes":true}}});
            let mut cancelled = queue.resume(&handles[0], responses.clone())?;
            cancelled.cancel();
            queue.poll(Duration::ZERO)?;
            assert_eq!(queue.wait(&mut cancelled).unwrap_err().code, "Cancelled");
            let mut resumed = Vec::new();
            for handle in &handles {
                resumed.push(queue.resume(handle, responses.clone())?);
            }
            let mut duplicate = queue.resume(&handles[0], responses)?;
            stage("resume-barrier");
            for operation in &mut resumed {
                assert_eq!(message(queue.wait(operation)?), "interactive");
            }
            let error = queue.wait(&mut duplicate).unwrap_err();
            assert_eq!(error.code, "InvalidContinuation");
            assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
            assert_eq!(queue.stats().input_bytes, 0);
        } else if scenario == "parallel" {
            while queue
                .notification_status()
                .is_none_or(|s| s.phase != mcpx_core::NotificationPhase::Listening)
            {
                queue.poll(Duration::from_millis(25))?;
            }
            let mut calls = Vec::new();
            let mut commands = Vec::new();
            let mut expected_calls = Vec::new();
            let mut expected_commands = Vec::new();
            for n in 0..16 {
                let text = format!("held-{n}");
                match n % 4 {
                    0 => {
                        calls.push(queue.submit_typed(
                            &binding,
                            Echo {
                                message: text.clone(),
                            },
                        )?);
                        expected_calls.push(text);
                    }
                    1 => {
                        calls.push(queue.submit_json(&binding, json!({"message":text}))?);
                        expected_calls.push(text);
                    }
                    2 => {
                        commands.push(queue.execute("echo", &format!("--message '{text}'"))?);
                        expected_commands.push(text);
                    }
                    _ => {
                        commands
                            .push(queue.execute_argv("echo", &["--message".into(), text.clone()])?);
                        expected_commands.push(text);
                    }
                }
            }
            let mut seventeenth = queue.submit_raw(&binding, r#"{"message":"after-barrier"}"#)?;
            let mut cancelled =
                queue.submit_json(&binding, json!({"message":"never-cancelled"}))?;
            let mut expired = queue.submit_json(&binding, json!({"message":"never-expired"}))?;
            queue.set_timeout(&expired, Duration::from_millis(15))?;
            assert_eq!(
                queue
                    .submit_json(&binding, json!({"message":"never-full"}))
                    .unwrap_err()
                    .code,
                "QueueFull"
            );
            cancelled.cancel();
            armed.set(true);
            stage("barrier");
            for (operation, text) in calls.iter_mut().zip(expected_calls) {
                assert_eq!(message(queue.wait(operation)?), text);
            }
            for (operation, text) in commands.iter_mut().zip(expected_commands) {
                assert_eq!(message(execute(queue.wait(operation)?)), text);
            }
            assert!(!armed.get());
            assert_eq!(message(queue.wait(&mut seventeenth)?), "after-barrier");
            for (operation, code) in [(&mut cancelled, "Cancelled"), (&mut expired, "Timeout")] {
                let error = queue.wait(operation).unwrap_err();
                assert_eq!(error.code, code);
                assert_eq!(error.execution_certainty, ExecutionCertainty::NotSent);
                assert_eq!(error.queue_operation_id, Some(operation.id()));
            }
            let events = queue.drain_operation_events();
            for id in calls
                .iter()
                .map(|o| o.id())
                .chain(commands.iter().map(|o| o.id()))
                .chain([seventeenth.id(), cancelled.id(), expired.id()])
            {
                assert_eq!(
                    events
                        .events
                        .iter()
                        .filter(|e| e.queue_operation_id == id
                            && matches!(
                                e.kind,
                                OperationEventKind::Completed
                                    | OperationEventKind::Failed
                                    | OperationEventKind::Cancelled
                            ))
                        .count(),
                    1
                );
            }
            assert_eq!(queue.stats().active, 0);
            assert_eq!(queue.stats().queued, 0);
            assert_eq!(queue.stats().input_bytes, 0);
        } else if scenario == "isolation" {
            let mut cancelled = queue.submit_json(&binding, json!({"message":"held-cancel"}))?;
            let mut expired = queue.submit_json(&binding, json!({"message":"held-timeout"}))?;
            let mut broken = queue.submit_json(&binding, json!({"message":"broken-response"}))?;
            let mut good = queue.submit_json(&binding, json!({"message":"good"}))?;
            for _ in 0..20 {
                queue.poll(Duration::ZERO)?;
                std::thread::sleep(Duration::from_millis(2));
            }
            handshake_driving(&mut queue, "active")?;
            assert_eq!(message(queue.wait(&mut good)?), "good");
            assert_eq!(queue.wait(&mut broken).unwrap_err().code, "ProtocolError");
            cancelled.cancel();
            queue.set_timeout(&expired, Duration::from_millis(200))?;
            assert_eq!(queue.wait(&mut cancelled).unwrap_err().code, "Cancelled");
            assert_eq!(queue.wait(&mut expired).unwrap_err().code, "Timeout");
            let mut after = queue.submit_json(&binding, json!({"message":"after-isolation"}))?;
            assert_eq!(message(queue.wait(&mut after)?), "after-isolation");
        }
        println!("{}", json!({"result":scenario,"stats":queue.stats()}));
        Ok(())
    })();
    let closed = queue.close();
    result?;
    closed
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("Build this example with --target wasm32-wasip2");
}
