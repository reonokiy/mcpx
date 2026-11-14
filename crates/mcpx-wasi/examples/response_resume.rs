//! Explicit shared-core business response recovery, without a queue policy.
#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_core::{
        Action, CallOutcome, Client, Credential, Dialect, Driver, Error, HostRuntime, Result,
    };
    use mcpx_wasi::{WasiDriver, run_blocking};
    use std::{collections::BTreeMap, time::Duration};
    use wasip2::{
        clocks::monotonic_clock,
        http::{
            outgoing_handler,
            types::{
                Fields, IncomingResponse, Method, OutgoingBody, OutgoingRequest, RequestOptions,
                Scheme,
            },
        },
        io::streams::StreamError,
    };
    fn request(
        endpoint: &str,
        fields: &BTreeMap<String, String>,
        get: bool,
        bytes: &[u8],
    ) -> IncomingResponse {
        let url = url::Url::parse(endpoint).unwrap();
        let mut fields: Vec<_> = fields
            .iter()
            .filter(|(k, _)| get || !k.eq_ignore_ascii_case("last-event-id"))
            .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
            .collect();
        fields.push(("authorization".into(), b"Bearer response-frozen".to_vec()));
        if !get {
            fields.retain(|(k, _)| k != "content-type" && k != "accept");
            fields.push(("content-type".into(), b"application/json".to_vec()));
            fields.push((
                "accept".into(),
                b"application/json, text/event-stream".to_vec(),
            ));
        }
        let outgoing = OutgoingRequest::new(Fields::from_list(&fields).unwrap());
        outgoing
            .set_method(&if get { Method::Get } else { Method::Post })
            .unwrap();
        outgoing
            .set_scheme(Some(&if url.scheme() == "https" {
                Scheme::Https
            } else {
                Scheme::Http
            }))
            .unwrap();
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
        let future = outgoing_handler::handle(outgoing, Some(options)).unwrap();
        if let Some(body) = body {
            let stream = body.write().unwrap();
            stream.blocking_write_and_flush(bytes).unwrap();
            drop(stream);
            OutgoingBody::finish(body, None).unwrap();
        }
        future.subscribe().block();
        future.get().unwrap().unwrap().unwrap()
    }
    fn tick(client: &mut Client<WasiDriver>) -> Result<()> {
        let now = client.driver().now_ms();
        client.host_runtime_mut().advance_time(now)
    }
    fn read(
        client: &mut Client<WasiDriver>,
        id: u64,
        url: &str,
        fields: &BTreeMap<String, String>,
        get: bool,
        bytes: &[u8],
        legacy: bool,
    ) -> Result<bool> {
        let response = request(url, fields, get, bytes);
        tick(client)?;
        let headers = response
            .headers()
            .entries()
            .into_iter()
            .map(|(name, bytes)| (name, String::from_utf8(bytes).unwrap()));
        client
            .host_runtime_mut()
            .begin_response_headers(id, response.status(), headers)?;
        let body = response.consume().unwrap();
        let stream = body.stream().unwrap();
        let mut complete = false;
        loop {
            match stream.blocking_read(65536) {
                Ok(bytes) if bytes.is_empty() => {}
                Ok(bytes) => {
                    tick(client)?;
                    if client.host_runtime_mut().push_response_for(id, &bytes)? {
                        complete = true;
                        break;
                    }
                }
                Err(StreamError::Closed) => break,
                Err(_) => return Err(Error::local("HostError", "fixture response failed")),
            }
        }
        drop(stream);
        drop(body);
        drop(response);
        tick(client)?;
        if !legacy {
            assert!(client.host_runtime_mut().finish_response_for(id).is_err());
            return Ok(false);
        }
        if !complete {
            client.host_runtime_mut().interrupt_response_for(id)?;
            assert_eq!(
                client
                    .host_runtime_mut()
                    .response_resume(id)
                    .unwrap_err()
                    .code,
                "Busy"
            );
        }
        for reply in client.host_runtime_mut().response_replies(id)? {
            assert_eq!(
                request(url, fields, false, &serde_json::to_vec(&reply).unwrap()).status(),
                202
            );
        }
        Ok(complete)
    }
    let mut args = std::env::args().skip(1);
    let endpoint = args.next().unwrap();
    let legacy = args.next().as_deref() == Some("legacy");
    let auto = args.next().as_deref() == Some("auto");
    let runtime = HostRuntime::with_dialect(
        endpoint.clone(),
        if auto {
            Dialect::Auto
        } else if legacy {
            Dialect::Legacy
        } else {
            Dialect::Modern
        },
    )?;
    let driver = WasiDriver::new(
        Credential::bearer("response-frozen".into(), "response-scope".into())?,
        Duration::from_secs(5),
    )?;
    let mut client = Client::new(driver, runtime);
    let result = (|| -> Result<()> {
        let binding = run_blocking(client.fresh_tool("echo"))?;
        let args = binding.args_from(serde_json::json!({"message":"resumable"}))?;
        let operation = client
            .host_runtime_mut()
            .call_concurrent_bound(&binding, &args)?;
        let mut actions = client.host_runtime_mut().take_actions();
        assert_eq!(actions.len(), 1);
        let Action::Http {
            id,
            rpc_id,
            url,
            headers,
            body,
            ..
        } = actions.remove(0)
        else {
            panic!()
        };
        if !legacy {
            assert_eq!(
                client
                    .host_runtime_mut()
                    .enable_response_resumption(id)
                    .unwrap_err()
                    .code,
                "ResumeUnavailable"
            );
            read(
                &mut client,
                id,
                &url,
                &headers,
                false,
                &serde_json::to_vec(&body).unwrap(),
                false,
            )?;
            assert!(
                client
                    .take_concurrent_completion(operation)
                    .unwrap()
                    .is_err()
            );
            return Ok(());
        }
        client.host_runtime_mut().enable_response_resumption(id)?;
        assert!(!read(
            &mut client,
            id,
            &url,
            &headers,
            false,
            &serde_json::to_vec(&body).unwrap(),
            true
        )?);
        let mut current = id;
        for attempt in 0..3 {
            loop {
                tick(&mut client)?;
                let info = client.host_runtime_mut().response_resume(current)?;
                if info.retry_after_ms == 0 {
                    break;
                }
                monotonic_clock::subscribe_duration(info.retry_after_ms * 1_000_000).block();
            }
            let plan = client.host_runtime_mut().resume_response(current)?;
            assert_ne!(plan.action_id, current);
            assert_eq!(Some(plan.rpc_id), rpc_id);
            assert_eq!(plan.operation_id, operation);
            assert_eq!(plan.method, "GET");
            assert!(plan.body.is_none());
            assert_eq!(
                client
                    .host_runtime_mut()
                    .response_resume(current)
                    .unwrap_err()
                    .code,
                "InvalidHandle"
            );
            current = plan.action_id;
            if read(
                &mut client,
                current,
                &plan.url,
                &plan.headers,
                true,
                &[],
                true,
            )? {
                break;
            }
            assert!(attempt < 2);
        }
        let CallOutcome::Completed(response) =
            client.take_concurrent_completion(operation).unwrap()?
        else {
            panic!()
        };
        assert_eq!(response.operation_id, Some(operation));
        assert_eq!(
            response.raw["structuredContent"]["value"].as_u64(),
            Some(u64::MAX)
        );
        assert!(client.host_runtime_mut().take_actions().is_empty());
        Ok(())
    })();
    let closed = run_blocking(client.close());
    result?;
    closed?;
    println!("PASS response resume");
    Ok(())
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("Build for wasm32-wasip2");
}
