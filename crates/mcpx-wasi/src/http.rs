use mcpx_core::{
    Action, Credential, Driver, DriverContext, Error, EventBatch, HostRuntime, Result,
};
use std::{collections::BTreeMap, time::Duration};
use wasip2::{
    clocks::monotonic_clock,
    http::{
        outgoing_handler,
        types::{
            self, Fields, IncomingResponse, Method, OutgoingBody, OutgoingRequest, RequestOptions,
            Scheme,
        },
    },
    io::{
        poll::{Pollable, poll},
        streams::StreamError,
    },
};

mod exchange;
mod notifications;
mod queue;
mod reconnect;
mod responses;
pub use notifications::WasiClientExt;
pub use queue::{WasiQueue, WasiQueueOptions, WasiQueueStats, WasiReactor, WasiRemote};

struct Session {
    url: String,
    id: String,
    credential: Credential,
}
struct Cancellation {
    url: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
    credential: Credential,
}

pub struct WasiDriver {
    credential: Credential,
    origin: u64,
    timeout_ns: u64,
    deadline: Option<u64>,
    session: Option<Session>,
    cancellation: Option<Cancellation>,
    events: Option<Box<dyn FnMut(EventBatch)>>,
    closed: bool,
    http_notifications: bool,
    notifications: Option<notifications::NotificationIo>,
    recovery: notifications::Recovery,
    response_recovery: Option<crate::ResponseRecoveryPolicy>,
    reconnecting: bool,
    reconnect_deadline: Option<u64>,
}
fn host_error() -> Error {
    Error::new(
        "HttpError",
        "WASI HTTP I/O failed; execution may be unknown",
    )
}
fn timeout() -> Error {
    Error::new(
        "Timeout",
        "operation deadline exceeded; execution may be unknown",
    )
}
fn http_error(error: types::ErrorCode) -> Error {
    use types::ErrorCode::*;
    match error {
        DnsTimeout
        | ConnectionTimeout
        | ConnectionReadTimeout
        | ConnectionWriteTimeout
        | HttpResponseTimeout => timeout(),
        _ => host_error(),
    }
}
fn ensure(deadline: u64) -> Result<()> {
    if monotonic_clock::now() >= deadline {
        Err(timeout())
    } else {
        Ok(())
    }
}
impl WasiDriver {
    pub fn new(credential: Credential, timeout: Duration) -> Result<Self> {
        let timeout_ns = u64::try_from(timeout.as_nanos())
            .map_err(|_| Error::new("ConfigurationError", "timeout exceeds 24 hours"))?;
        if !(1_000_000..=86_400_000_000_000).contains(&timeout_ns)
            || credential.token().is_some_and(|token| token.len() > 65536)
        {
            return Err(Error::new(
                "ConfigurationError",
                "timeout must be 1 ms..24 hours and bearer at most 64 KiB",
            ));
        }
        Ok(Self {
            credential,
            origin: monotonic_clock::now(),
            timeout_ns,
            deadline: None,
            session: None,
            cancellation: None,
            events: None,
            closed: false,
            http_notifications: false,
            notifications: None,
            recovery: notifications::Recovery::default(),
            response_recovery: None,
            reconnecting: false,
            reconnect_deadline: None,
        })
    }
    pub fn with_event_handler(mut self, handler: impl FnMut(EventBatch) + 'static) -> Self {
        self.events = Some(Box::new(handler));
        self
    }
    fn remember_session(&mut self, url: &str, id: &str, credential: &Credential) {
        if self
            .session
            .as_ref()
            .is_none_or(|old| old.url != url || old.id != id)
        {
            self.session = Some(Session {
                url: url.into(),
                id: id.into(),
                credential: credential.clone(),
            });
        }
    }
    fn deadline(&self) -> Result<u64> {
        self.deadline
            .ok_or_else(|| Error::new("ProtocolError", "WASI driver was not prepared"))
    }
    #[allow(clippy::too_many_arguments)]
    fn request(
        &mut self,
        method: Method,
        url: &str,
        headers: &BTreeMap<String, String>,
        body: Vec<u8>,
        credential: &Credential,
        deadline: u64,
        mut runtime: Option<&mut HostRuntime>,
    ) -> Result<IncomingResponse> {
        let mut exchange =
            exchange::Exchange::new(method, url, headers, body, credential, deadline, false)?;
        loop {
            ensure(deadline)?;
            match exchange.step()? {
                exchange::Step::Progress => {}
                exchange::Step::Pending => self.wait_with_notifications(
                    &exchange.subscribe(),
                    deadline,
                    runtime.as_deref_mut(),
                )?,
                exchange::Step::Response(response) => return Ok(response),
            }
        }
    }
    fn perform(&mut self, action: Action, runtime: &mut HostRuntime) -> Result<()> {
        if self.response_recovery.is_some() && responses::eligible(&action) {
            return self.perform_resumable(action, runtime);
        }
        let Action::Http {
            id,
            url,
            headers,
            body,
            cancellation,
            ..
        } = action
        else {
            return match action {
                Action::Cancel { .. } => Ok(()),
                _ => Err(Error::new(
                    "CapabilityUnavailable",
                    "WASI driver supports HTTP only",
                )),
            };
        };
        let deadline = self.deadline()?;
        self.cancellation = cancellation.map(|value| Cancellation {
            url: url.clone(),
            headers: headers.clone(),
            body: serde_json::to_vec(&value).expect("JSON value"),
            credential: self.credential.clone(),
        });
        let credential = self.credential.clone();
        let response = self.request(
            Method::Post,
            &url,
            &headers,
            serde_json::to_vec(&body).expect("JSON value"),
            &credential,
            deadline,
            Some(runtime),
        )?;
        runtime.advance_time(self.now_ms())?;
        let fields = response
            .headers()
            .entries()
            .into_iter()
            .map(|(name, value)| {
                String::from_utf8(value)
                    .map(|value| (name, value))
                    .map_err(|_| Error::new("InvalidResponse", "HTTP header is not UTF-8"))
            })
            .collect::<Result<Vec<_>>>()?;
        let done = runtime.begin_response_headers(id, response.status(), fields)?;
        if headers
            .get("mcp-protocol-version")
            .is_some_and(|v| v == "2025-11-25")
            && let Some(id) = runtime.machine().session_id()
        {
            self.remember_session(&url, id, &credential);
        }
        self.collect_events(runtime);
        if done {
            self.cancellation = None;
            return Ok(());
        }
        let incoming = response.consume().map_err(|_| host_error())?;
        let stream = incoming.stream().map_err(|_| host_error())?;
        loop {
            ensure(deadline)?;
            match stream.read(65536) {
                Ok(bytes) if bytes.is_empty() => {
                    self.wait_with_notifications(&stream.subscribe(), deadline, Some(runtime))?
                }
                Ok(bytes) => {
                    runtime.advance_time(self.now_ms())?;
                    let done = runtime.push_response_for(id, &bytes)?;
                    self.collect_events(runtime);
                    if done {
                        break;
                    }
                }
                Err(StreamError::Closed) => {
                    runtime.advance_time(self.now_ms())?;
                    runtime.finish_response_for(id)?;
                    self.collect_events(runtime);
                    break;
                }
                Err(_) => return Err(host_error()),
            }
        }
        // InputStream is a child resource and must drop before IncomingBody.
        drop(stream);
        drop(incoming);
        self.cancellation = None;
        Ok(())
    }
}
impl Driver for WasiDriver {
    fn supports_reconnect(&self) -> bool {
        !self.closed
    }
    async fn reconnect(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        self.reopen_session(runtime)
    }
    async fn prepare(&mut self) -> Result<DriverContext> {
        if self.closed {
            return Err(Error::new("TransportClosed", "WASI driver is closed"));
        }
        let deadline = monotonic_clock::now()
            .checked_add(self.timeout_ns)
            .ok_or_else(timeout)?;
        let deadline = self
            .reconnect_deadline
            .map_or(deadline, |limit| limit.min(deadline));
        ensure(deadline)?;
        self.deadline = Some(deadline);
        Ok(DriverContext {
            auth_context_id: self.credential.auth_context_id().into(),
        })
    }
    fn now_ms(&self) -> u64 {
        (monotonic_clock::now() - self.origin) / 1_000_000
    }
    async fn execute(&mut self, action: Action, runtime: &mut HostRuntime) -> Result<()> {
        let result = self.perform(action, runtime);
        if self.reconnect_deadline.is_some() && result.is_ok() && !runtime.busy() {
            self.reconnecting = false;
        }
        result
    }
    async fn abort(&mut self, runtime: &mut HostRuntime, cause: &Error) {
        if (matches!(cause.code.as_str(), "Timeout" | "Cancelled")
            || self.response_recovery.is_some() && cause.code == "ResponseRecoveryExhausted")
            && let Some(cancel) = self.cancellation.take()
        {
            let _ = self.request(
                Method::Post,
                &cancel.url,
                &cancel.headers,
                cancel.body,
                &cancel.credential,
                monotonic_clock::now().saturating_add(200_000_000),
                Some(runtime),
            );
        }
        self.cancellation = None;
        runtime.take_actions();
        self.collect_events(runtime);
    }
    fn finish(&mut self) {
        self.deadline = None;
        self.cancellation = None;
        self.reconnect_deadline = None;
    }
    fn collect_events(&mut self, runtime: &mut HostRuntime) {
        self.service_notifications(runtime);
        self.deliver_events(runtime);
    }
    async fn close(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        self.closed = true;
        self.reconnecting = true;
        self.reconnect_deadline = None;
        self.stop_notifications(runtime)?;
        self.credential = Credential::anonymous();
        self.events = None;
        self.close_session()?;
        Ok(())
    }
}
impl WasiDriver {
    fn deliver_events(&mut self, runtime: &mut HostRuntime) {
        if let Some(handler) = &mut self.events {
            let batch = runtime.take_events();
            if !batch.events.is_empty() || batch.dropped > 0 {
                handler(batch);
            }
        }
    }
    fn close_session(&mut self) -> Result<()> {
        self.close_session_until(None)
    }
    fn close_session_until(&mut self, root: Option<u64>) -> Result<()> {
        if let Some(session) = &self.session {
            let headers = BTreeMap::from([
                ("mcp-session-id".into(), session.id.clone()),
                ("mcp-protocol-version".into(), "2025-11-25".into()),
            ]);
            let url = session.url.clone();
            let credential = session.credential.clone();
            let response = self
                .request(
                    Method::Delete,
                    &url,
                    &headers,
                    Vec::new(),
                    &credential,
                    root.unwrap_or(u64::MAX)
                        .min(monotonic_clock::now().saturating_add(500_000_000)),
                    None,
                )
                .map_err(|_| {
                    if root.is_some_and(|d| monotonic_clock::now() >= d) {
                        timeout()
                    } else {
                        Error::new("SessionCleanupFailed", "could not close MCP session")
                    }
                })?;
            if !(200..300).contains(&response.status()) && !matches!(response.status(), 404 | 405) {
                return Err(Error::new(
                    "SessionCleanupFailed",
                    "could not close MCP session",
                ));
            }
            self.session = None;
        }
        Ok(())
    }
}
