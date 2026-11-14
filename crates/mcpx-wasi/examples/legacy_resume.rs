//! Explicit low-level legacy listener recovery. The host freezes credentials,
//! drains capability replies and owns each HTTP body; no business POST is retried.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{
        CallOutcome, Client, Credential, Dialect, Driver, Error, HostRuntime, NotificationRequest,
        Result,
    };
    use mcpx_wasi::{WasiDriver, run_blocking};
    use std::{collections::BTreeMap, time::Duration};
    use wasip2::{
        http::{
            outgoing_handler,
            types::{
                Fields, IncomingResponse, Method, OutgoingBody, OutgoingRequest, RequestOptions,
                Scheme,
            },
        },
        io::streams::StreamError,
    };

    fn host_error() -> Error {
        Error::local("HostError", "example HTTP exchange failed")
    }
    fn request(
        endpoint: &str,
        fields: &BTreeMap<String, String>,
        method: Method,
        bytes: &[u8],
    ) -> IncomingResponse {
        let url = url::Url::parse(endpoint).unwrap();
        let scheme = match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            _ => panic!("HTTP(S) endpoint required"),
        };
        let is_get = matches!(method, Method::Get);
        let mut fields: Vec<_> = fields
            .iter()
            .filter(|(name, _)| is_get || name.as_str() != "last-event-id")
            .map(|(name, value)| (name.clone(), value.as_bytes().to_vec()))
            .collect();
        fields.push(("authorization".into(), b"Bearer resume-frozen".to_vec()));
        if !is_get {
            fields.retain(|(name, _)| name != "content-type" && name != "accept");
            fields.push(("content-type".into(), b"application/json".to_vec()));
            fields.push((
                "accept".into(),
                b"application/json, text/event-stream".to_vec(),
            ));
        }
        let outgoing = OutgoingRequest::new(Fields::from_list(&fields).unwrap());
        outgoing.set_method(&method).unwrap();
        outgoing.set_scheme(Some(&scheme)).unwrap();
        outgoing
            .set_authority(Some(
                &url[url::Position::BeforeHost..url::Position::AfterPort],
            ))
            .unwrap();
        outgoing
            .set_path_with_query(Some(
                &url[url::Position::BeforePath..url::Position::AfterQuery],
            ))
            .unwrap();
        let body = outgoing.body().unwrap();
        // A GET may receive a complete response before a needless empty-body
        // flush. Finish empty bodies before dispatch, as the product driver does.
        let body = if bytes.is_empty() {
            OutgoingBody::finish(body, None).unwrap();
            None
        } else {
            Some(body)
        };
        let options = RequestOptions::new();
        options.set_connect_timeout(Some(5_000_000_000)).unwrap();
        options.set_first_byte_timeout(Some(5_000_000_000)).unwrap();
        options
            .set_between_bytes_timeout(Some(5_000_000_000))
            .unwrap();
        let response = outgoing_handler::handle(outgoing, Some(options)).unwrap();
        if let Some(body) = body {
            let stream = body.write().unwrap();
            stream.blocking_write_and_flush(bytes).unwrap();
            drop(stream);
            OutgoingBody::finish(body, None).unwrap();
        }
        let pollable = response.subscribe();
        pollable.block();
        drop(pollable);
        response.get().unwrap().unwrap().unwrap()
    }
    fn tick(remote: &mut Client<WasiDriver>) -> Result<()> {
        let now = remote.driver().now_ms();
        remote.host_runtime_mut().advance_time(now)
    }
    fn read(
        remote: &mut Client<WasiDriver>,
        plan: &NotificationRequest,
        expected: &str,
    ) -> Result<()> {
        let response = request(
            &plan.url,
            &plan.headers,
            if plan.method == "GET" {
                Method::Get
            } else {
                Method::Post
            },
            &plan
                .body
                .as_ref()
                .map(serde_json::to_vec)
                .transpose()
                .unwrap()
                .unwrap_or_default(),
        );
        let mime = String::from_utf8(response.headers().get("content-type")[0].clone()).unwrap();
        tick(remote)?;
        remote.host_runtime_mut().begin_notifications(
            plan.resource_id,
            response.status(),
            &mime,
        )?;
        let body = response.consume().unwrap();
        let stream = body.stream().unwrap();
        loop {
            match stream.blocking_read(65536) {
                Ok(bytes) => {
                    if bytes.is_empty() {
                        let ready = stream.subscribe();
                        ready.block();
                        continue;
                    }
                    tick(remote)?;
                    remote
                        .host_runtime_mut()
                        .push_notifications(plan.resource_id, &bytes)?;
                    for reply in remote
                        .host_runtime_mut()
                        .notification_replies(plan.resource_id)?
                    {
                        assert_eq!(
                            request(
                                &plan.url,
                                &plan.headers,
                                Method::Post,
                                &serde_json::to_vec(&reply).unwrap()
                            )
                            .status(),
                            202
                        );
                    }
                }
                Err(StreamError::Closed) => break,
                Err(_) => return Err(host_error()),
            }
        }
        drop(stream);
        drop(body);
        drop(response);
        tick(remote)?;
        assert_eq!(
            remote
                .host_runtime_mut()
                .finish_notifications(plan.resource_id)
                .unwrap_err()
                .code,
            expected
        );
        Ok(())
    }
    run_blocking(async {
        let mut args = std::env::args().skip(1);
        let endpoint = args.next().unwrap();
        let legacy = args.next().as_deref() == Some("legacy");
        let auto = args.next().as_deref() == Some("auto");
        let runtime = HostRuntime::with_dialect(
            endpoint,
            if auto {
                Dialect::Auto
            } else if legacy {
                Dialect::Legacy
            } else {
                Dialect::Modern
            },
        )?;
        let driver = WasiDriver::new(
            Credential::bearer("resume-frozen".into(), "resume-scope".into())?,
            Duration::from_secs(5),
        )?;
        let mut remote = Client::new(driver, runtime);
        remote.fresh_tool("echo").await?;
        let first = remote.host_runtime_mut().start_notifications()?;
        read(
            &mut remote,
            &first,
            if legacy {
                "ProtocolError"
            } else {
                "SubscriptionDisconnected"
            },
        )?;
        if legacy {
            let info = remote
                .host_runtime_mut()
                .notification_resume(first.resource_id)?;
            assert!(info.retry_after_ms <= 25);
            if info.retry_after_ms != 0 {
                assert_eq!(
                    remote
                        .host_runtime_mut()
                        .resume_notifications(first.resource_id)
                        .unwrap_err()
                        .code,
                    "RetryLater"
                );
            }
            let binding = remote.fresh_tool("echo").await?;
            let CallOutcome::Completed(response) = remote
                .call_json(&binding, serde_json::json!({"message":"independent"}))
                .await?
            else {
                panic!()
            };
            assert_eq!(response.raw["structuredContent"]["message"], "independent");
            loop {
                tick(&mut remote)?;
                let delay = remote
                    .host_runtime_mut()
                    .notification_resume(first.resource_id)?
                    .retry_after_ms;
                if delay == 0 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(delay));
            }
            let second = remote
                .host_runtime_mut()
                .resume_notifications(first.resource_id)?;
            assert_eq!(second.headers["last-event-id"], "游标/é");
            assert_ne!(first.resource_id, second.resource_id);
            assert_eq!(
                remote
                    .host_runtime_mut()
                    .abort_notifications(first.resource_id, false)
                    .unwrap_err()
                    .code,
                "InvalidHandle"
            );
            read(&mut remote, &second, "SubscriptionDisconnected")?;
            let third = remote
                .host_runtime_mut()
                .resume_notifications(second.resource_id)?;
            assert_eq!(third.headers["last-event-id"], "cursor-two");
            read(&mut remote, &third, "SubscriptionDisconnected")?;
            assert_eq!(
                remote
                    .host_runtime_mut()
                    .resume_notifications(third.resource_id)
                    .unwrap_err()
                    .code,
                "ResumeUnavailable"
            );
        } else {
            assert_eq!(
                remote
                    .host_runtime_mut()
                    .resume_notifications(first.resource_id)
                    .unwrap_err()
                    .code,
                "ResumeUnavailable"
            );
        }
        remote.close().await?;
        println!("PASS legacy resume");
        Ok(())
    })
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("Build with --target wasm32-wasip2");
}
