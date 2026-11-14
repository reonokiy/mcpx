use crate::{EventHandler, stdio::ProcessDriver};
use futures_util::StreamExt;
use mcpx_core::{
    Action, Credential, CredentialProvider, CredentialRequest, Dialect, Driver, DriverContext,
    Error, HostRuntime, Result,
};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
mod session_recovery;

#[derive(Clone, Debug)]
pub struct CancellationHandle(pub(crate) CancellationToken);
impl CancellationHandle {
    pub fn cancel(&self) {
        self.0.cancel();
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}

pub struct NativeDriver {
    pub(crate) response_recovery: Option<crate::ResponseRecoveryPolicy>,
    response_io: Option<crate::response_recovery::ResponseIo>,
    pub(crate) http: reqwest::Client,
    pub(crate) endpoint: String,
    pub(crate) process: Option<ProcessDriver>,
    pub(crate) timeout: Duration,
    pub(crate) provider: Option<Box<dyn CredentialProvider>>,
    pub(crate) events: Option<Rc<RefCell<EventHandler>>>,
    event_stream: Option<Rc<crate::events::EventProducer>>,
    credential: Credential,
    active_credential: Credential,
    origin: Instant,
    deadline: tokio::time::Instant,
    cancellation: CancellationToken,
    active: Option<Action>,
    closed: bool,
    final_closed: bool,
    cleanup_complete: bool,
    stdio_config: Option<crate::StdioConfig>,
    recovery_deadline: Option<tokio::time::Instant>,
    pub(crate) reconnect_policy: Option<crate::StdioReconnectPolicy>,
    pub(crate) http_session_recovery: Option<crate::HttpSessionRecoveryPolicy>,
    pub(crate) session_recovery_state: Option<crate::session_recovery::HttpRecoveryState>,
    pub(crate) mutable_session_recovery: bool,
    last_tool_result: Option<u64>,
    credential_reference: Option<String>,
    concurrent: BTreeMap<u64, Credential>,
    session_credential: Option<(String, Credential)>,
}

impl NativeDriver {
    async fn exchange_response(
        &mut self,
        action: Action,
        runtime: &mut HostRuntime,
        policy: crate::ResponseRecoveryPolicy,
    ) -> Result<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
        let wake = std::sync::Arc::new(tokio::sync::Notify::new());
        let token = self.cancellation.clone();
        let deadline = self.deadline.into_std();
        let origin = self.origin;
        self.response_io = Some(crate::response_recovery::ResponseIo::new(
            runtime,
            action,
            self.http.clone(),
            self.active_credential.clone(),
            policy,
            deadline,
            token.clone(),
            CancellationToken::new(),
            sender,
            wake.clone(),
        )?);
        let _abort_on_drop = self.response_io.as_ref().unwrap().abort_on_drop();
        let result = loop {
            let io = self.response_io.as_mut().unwrap();
            if let Err(error) = io.service(runtime, origin).await {
                break Err(error);
            }
            if io.is_terminal() && io.drained() {
                break Ok(());
            }
            let terminal = io.is_terminal();
            let next = io.wake_deadline().unwrap_or(deadline);
            self.deliver_events(runtime);
            tokio::select! {biased;
                message=receiver.recv(),if !terminal=>{
                    let Some(message)=message else{break Err(Error::new("TransportClosed","response reader stopped"));};
                    let io=self.response_io.as_mut().unwrap();
                    if message.id!=io.id {continue;}
                    if let Err(error)=io.feed(runtime,message.input,origin){break Err(error);}
                    if !runtime.machine().has_action(io.id) {io.terminal(true);}
                }
                _=token.cancelled(),if !terminal=>break Err(Error::new("Cancelled","operation cancelled; execution may be unknown")),
                _=tokio::time::sleep_until(next.into())=>{},
                _=wake.notified()=>{},
            }
        };
        if let Some(mut io) = self.response_io.take() {
            io.close().await;
        }
        result
    }
    pub(crate) fn new(
        client: crate::Client,
        endpoint: String,
        process: Option<ProcessDriver>,
    ) -> Self {
        let stdio_config = process.as_ref().map(|process| process.config.clone());
        Self {
            response_recovery: client.response_recovery,
            response_io: None,
            reconnect_policy: client.reconnect,
            http_session_recovery: client.http_session_recovery,
            stdio_config,
            final_closed: false,
            cleanup_complete: false,
            recovery_deadline: None,
            http: client.http,
            endpoint,
            process,
            timeout: client.timeout,
            provider: client.provider,
            events: client.events.map(|handler| Rc::new(RefCell::new(handler))),
            event_stream: client.event_stream.map(Rc::new),
            credential_reference: client.credential_reference,
            credential: Credential::anonymous(),
            active_credential: Credential::anonymous(),
            origin: Instant::now(),
            deadline: tokio::time::Instant::now(),
            cancellation: client.cancellation,
            active: None,
            concurrent: BTreeMap::new(),
            session_credential: None,
            session_recovery_state: None,
            mutable_session_recovery: false,
            last_tool_result: None,
            closed: false,
        }
    }
    async fn reopen_io(&mut self) -> Result<()> {
        if let Some(mut io) = self.response_io.take() {
            io.close().await;
        }
        if let Some(process) = self.process.as_mut() {
            process.close().await?;
        }
        self.process = None;
        self.close_session().await?;
        if let Some(config) = self.stdio_config.clone() {
            self.process = Some(ProcessDriver::spawn(config)?);
        }
        Ok(())
    }
    pub fn cancellation_handle(&self) -> CancellationHandle {
        CancellationHandle(self.cancellation.clone())
    }
    pub(crate) fn clock_origin(&self) -> Instant {
        self.origin
    }
    pub(crate) fn notification_snapshot(&self) -> (reqwest::Client, Credential, Duration) {
        (self.http.clone(), self.credential.clone(), self.timeout)
    }
    pub(crate) fn prepared_deadline(&self) -> Instant {
        self.deadline.into_std()
    }
    pub(crate) fn http_snapshot(
        &self,
        operation_id: u64,
    ) -> Result<(reqwest::Client, Credential, Instant)> {
        let credential = self.concurrent.get(&operation_id).ok_or_else(|| {
            Error::new("InvalidOperation", "missing prepared credential snapshot")
        })?;
        Ok((
            self.http.clone(),
            credential.clone(),
            self.deadline.into_std(),
        ))
    }
    pub(crate) fn deliver_events(&mut self, runtime: &mut HostRuntime) {
        let received = runtime.machine().last_tool_result();
        if self.last_tool_result != received {
            self.last_tool_result = received;
            if !runtime.machine().http_session_expired() {
                self.session_recovery_state = None;
            }
        }
        for operation in self.event_dispatch().publish(runtime) {
            self.concurrent.remove(&operation);
        }
    }
    pub(crate) fn event_dispatch(&self) -> EventDispatch {
        EventDispatch {
            handler: self.events.clone(),
            stream: self.event_stream.clone(),
        }
    }
    async fn close_session(&mut self) -> Result<()> {
        let Some((session, credential)) = &self.session_credential else {
            return Ok(());
        };
        let mut request = self
            .http
            .delete(&self.endpoint)
            .header("mcp-session-id", session)
            .header("mcp-protocol-version", mcpx_protocol::LEGACY_VERSION);
        if let Some(token) = credential.token() {
            request = request.bearer_auth(token);
        }
        let response = tokio::time::timeout(Duration::from_millis(500), request.send())
            .await
            .map_err(|_| Error::new("Timeout", "session shutdown exceeded deadline"))?
            .map_err(|_| Error::new("TransportError", "session shutdown failed"))?;
        if !response.status().is_success() && !matches!(response.status().as_u16(), 404 | 405) {
            return Err(Error::new("HttpError", "remote rejected session shutdown"));
        }
        self.session_credential = None;
        Ok(())
    }
    async fn exchange(&mut self, action: Action, runtime: &mut HostRuntime) -> Result<()> {
        match action {
            Action::Cancel { .. } => Ok(()),
            Action::Stream { id, body, .. } => {
                self.process
                    .as_mut()
                    .ok_or_else(|| Error::new("TransportClosed", "stdio transport is closed"))?
                    .write(&body)
                    .await?;
                if body.get("id").is_none() {
                    return runtime.complete_write(id);
                }
                while runtime.machine().has_action(id) {
                    let message = if runtime.machine().is_auto_probe() {
                        match tokio::time::timeout(
                            Duration::from_secs(1),
                            self.process.as_mut().unwrap().read(),
                        )
                        .await
                        {
                            Ok(result) => result?,
                            Err(_) => {
                                let config = self.process.as_ref().unwrap().config.clone();
                                self.process.as_mut().unwrap().close().await?;
                                self.process = Some(ProcessDriver::spawn(config)?);
                                runtime.probe_timeout()?;
                                return Ok(());
                            }
                        }
                    } else {
                        self.process.as_mut().unwrap().read().await?
                    };
                    runtime.advance_time(self.now_ms())?;
                    if let Some(reply) = runtime.stream_input(message)? {
                        self.process.as_mut().unwrap().write(&reply).await?;
                    }
                    self.deliver_events(runtime);
                }
                Ok(())
            }
            Action::Http {
                id,
                url,
                headers,
                body,
                ..
            } => {
                let initializing =
                    body.get("method").and_then(serde_json::Value::as_str) == Some("initialize");
                if initializing {
                    self.close_session().await?;
                }
                let mut request = self.http.post(url).body(body.to_string());
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                if let Some(token) = self.active_credential.token() {
                    request = request.bearer_auth(token);
                }
                let response = request.send().await.map_err(|_| {
                    Error::new(
                        "TransportError",
                        "HTTP request failed; execution may be unknown",
                    )
                })?;
                let status = response.status().as_u16();
                let headers: BTreeMap<_, _> = response
                    .headers()
                    .iter()
                    .filter_map(|(name, value)| {
                        value
                            .to_str()
                            .ok()
                            .map(|value| (name.to_string(), value.to_owned()))
                    })
                    .collect();
                if initializing && let Some(session) = headers.get("mcp-session-id") {
                    self.session_credential =
                        Some((session.clone(), self.active_credential.clone()));
                }
                let headers = serde_json::to_string(&headers).expect("serializable headers");
                runtime.advance_time(self.now_ms())?;
                if runtime.begin_response(id, status, &headers)? {
                    return Ok(());
                }
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let bytes = chunk.map_err(|_| {
                        Error::new(
                            "TransportError",
                            "HTTP response interrupted; execution may be unknown",
                        )
                    })?;
                    runtime.advance_time(self.now_ms())?;
                    let complete = runtime.push_response(&bytes)?;
                    self.deliver_events(runtime);
                    if complete {
                        return Ok(());
                    }
                }
                runtime.advance_time(self.now_ms())?;
                runtime.finish_response()
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct EventDispatch {
    handler: Option<Rc<RefCell<EventHandler>>>,
    stream: Option<Rc<crate::events::EventProducer>>,
}
impl EventDispatch {
    pub(crate) fn publish(&self, runtime: &mut HostRuntime) -> Vec<u64> {
        let mut finished = Vec::new();
        if self.handler.is_some() || self.stream.is_some() {
            let batch = runtime.take_events();
            if batch.dropped > 0 {
                if let Some(handler) = &self.handler {
                    (handler.borrow_mut())(&crate::EventStreamItem::Lagged {
                        dropped: batch.dropped,
                    });
                }
                if let Some(stream) = &self.stream {
                    stream.report_dropped(batch.dropped);
                }
            }
            for event in batch.events {
                if event.is_terminal() {
                    finished.push(event.operation_id);
                }
                let item = crate::EventStreamItem::Event(event);
                if let Some(handler) = &self.handler {
                    (handler.borrow_mut())(&item);
                }
                if let Some(stream) = &self.stream
                    && let crate::EventStreamItem::Event(event) = item
                {
                    stream.push(event);
                }
            }
        }
        finished
    }
}

impl Driver for NativeDriver {
    fn supports_session_recovery(&self) -> bool {
        self.mutable_session_recovery
            && self.http_session_recovery.is_some()
            && self.process.is_none()
            && !self.final_closed
            && !self.cancellation.is_cancelled()
    }
    async fn recover_session(
        &mut self,
        runtime: &mut HostRuntime,
        observed_at_ms: u64,
        prepared: bool,
    ) -> Result<DriverContext> {
        self.renew_session(runtime, observed_at_ms, prepared).await
    }
    fn supports_reconnect(&self) -> bool {
        !self.final_closed
    }
    async fn reconnect(&mut self, runtime: &mut HostRuntime) -> Result<()> {
        self.last_tool_result = runtime.machine().last_tool_result();
        if self.final_closed {
            return Err(Error::local(
                "TransportClosed",
                "remote is permanently closed",
            ));
        }
        self.closed = true;
        self.active = None;
        self.concurrent.clear();
        let deadline = tokio::time::Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| {
                Error::local(
                    "ConfigurationError",
                    "reconnect timeout exceeds clock range",
                )
            })?;
        self.recovery_deadline = Some(deadline);
        let token = self.cancellation.clone();
        let result = tokio::select! {biased;
            _=token.cancelled()=>Err(Error::local("Cancelled","reconnect cancelled")),
            _=tokio::time::sleep_until(deadline)=>Err(Error::local("Timeout","reconnect deadline exceeded")),
            result=self.reopen_io()=>result,
        };
        if result.is_ok() {
            self.closed = false;
        }
        result
    }

    async fn prepare(&mut self) -> Result<DriverContext> {
        if self.closed {
            return Err(Error::new("TransportClosed", "transport is closed"));
        }
        self.deadline = tokio::time::Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| {
                Error::new(
                    "ConfigurationError",
                    "timeout exceeds the supported monotonic clock range",
                )
            })?;
        if let Some(deadline) = self.recovery_deadline {
            self.deadline = self.deadline.min(deadline);
        }
        if self.cancellation.is_cancelled() {
            return Err(Error::new(
                "Cancelled",
                "operation cancelled before sending",
            ));
        }
        if let Some(stream) = &self.event_stream {
            if self.concurrent.is_empty() {
                stream.reserve_operation()?;
            } else {
                stream.reserve_operations(self.concurrent.len() + 1)?;
            }
        }
        let token = self.cancellation.clone();
        self.credential = if let Some(provider) = &mut self.provider {
            tokio::select! {
                biased;
                credential = provider.resolve(CredentialRequest {endpoint: &self.endpoint, reference: self.credential_reference.as_deref()}) => credential?,
                _ = token.cancelled() => return Err(Error::new("Cancelled", "credential resolution cancelled")),
                _ = tokio::time::sleep_until(self.deadline) => return Err(Error::new("Timeout", "credential resolution exceeded deadline")),
            }
        } else {
            Credential::anonymous()
        };
        if self.process.is_some() && self.credential.token().is_some() {
            return Err(Error::new(
                "CapabilityUnavailable",
                "bearer credentials apply only to HTTP transports",
            ));
        }
        Ok(DriverContext {
            auth_context_id: self.credential.auth_context_id().into(),
        })
    }
    fn now_ms(&self) -> u64 {
        self.origin.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
    }
    async fn execute(&mut self, action: Action, runtime: &mut HostRuntime) -> Result<()> {
        if tokio::time::Instant::now() >= self.deadline {
            return Err(Error::new(
                "Timeout",
                "operation deadline exceeded before sending",
            ));
        }
        if self.cancellation.is_cancelled() {
            return Err(Error::new(
                "Cancelled",
                "operation cancelled before sending",
            ));
        }
        let action_id = match &action {
            Action::Http { id, .. } | Action::Stream { id, .. } | Action::Cancel { id } => *id,
        };
        self.active_credential = runtime
            .machine()
            .action_operation(action_id)
            .and_then(|operation| self.concurrent.get(&operation))
            .unwrap_or(&self.credential)
            .clone();
        self.active = Some(action.clone());
        let token = self.cancellation.clone();
        let deadline = self.deadline;
        let response_policy = self
            .response_recovery
            .filter(|_| crate::response_recovery::eligible(&action));
        let result = if let Some(policy) = response_policy {
            self.exchange_response(action, runtime, policy).await
        } else {
            tokio::select! {
                biased;
                result = self.exchange(action, runtime) => result,
                _ = token.cancelled() => Err(Error::new("Cancelled", "operation cancelled; execution may be unknown")),
                _ = tokio::time::sleep_until(deadline) => Err(Error::new("Timeout", "operation deadline exceeded; execution may be unknown")),
            }
        };
        if result.is_ok() {
            self.active = None;
        }
        self.deliver_events(runtime);
        result
    }
    async fn abort(&mut self, runtime: &mut HostRuntime, cause: &Error) {
        if let Some(mut io) = self.response_io.take() {
            io.close().await;
        }
        if (matches!(cause.code.as_str(), "Cancelled" | "Timeout")
            || self.response_recovery.is_some() && cause.code == "ResponseRecoveryExhausted")
            && let Some(action) = self.active.take()
        {
            let notification = match &action {
                Action::Http { cancellation, .. } | Action::Stream { cancellation, .. } => {
                    cancellation.clone()
                }
                _ => None,
            };
            if let Some(notification) = notification {
                if let Some(process) = &mut self.process {
                    let _ = tokio::time::timeout(
                        Duration::from_millis(100),
                        process.write(&notification),
                    )
                    .await;
                } else if runtime.machine().dialect() == Dialect::Legacy
                    && let Action::Http { url, headers, .. } = action
                {
                    let mut request = self.http.post(url).body(notification.to_string());
                    for (name, value) in headers {
                        request = request.header(name, value);
                    }
                    if let Some(token) = self.active_credential.token() {
                        request = request.bearer_auth(token);
                    }
                    let _ = tokio::time::timeout(Duration::from_millis(200), request.send()).await;
                }
            }
        }
        if let Some(process) = self.process.as_mut() {
            self.closed = true;
            if process.close().await.is_ok() {
                self.process = None;
            }
        }
        self.deliver_events(runtime);
    }
    fn finish(&mut self) {
        self.recovery_deadline = None;
        self.active = None;
        self.cancellation = CancellationToken::new();
    }
    fn concurrent_started(&mut self, operation_id: u64) {
        self.concurrent
            .insert(operation_id, self.credential.clone());
    }
    fn concurrent_finished(&mut self, operation_id: u64) {
        self.concurrent.remove(&operation_id);
    }
    fn collect_events(&mut self, runtime: &mut HostRuntime) {
        self.deliver_events(runtime);
    }
    async fn close(&mut self, _runtime: &mut HostRuntime) -> Result<()> {
        if let Some(mut io) = self.response_io.take() {
            io.close().await;
        }
        if let Some(stream) = &self.event_stream {
            stream.close();
        }
        self.final_closed = true;
        self.closed = true;
        self.stdio_config = None;
        self.credential_reference = None;
        self.provider = None;
        self.credential = Credential::anonymous();
        self.active_credential = Credential::anonymous();
        self.concurrent.clear();
        if self.cleanup_complete {
            return Ok(());
        }
        if let Some(process) = self.process.as_mut() {
            process.close().await?;
        }
        self.process = None;
        self.close_session().await?;
        self.cleanup_complete = true;
        self.events = None;
        Ok(())
    }
}

#[cfg(test)]
mod event_tests {
    use super::*;
    use crate::EventStreamItem;
    use serde_json::json;

    #[test]
    fn callback_and_stream_report_core_loss_and_buffer_loss_exactly_once() {
        let mut runtime = HostRuntime::new("http://localhost/mcp".into(), false).unwrap();
        for progress in 0..500 {
            runtime.stream_input(json!({"jsonrpc":"2.0","method":"notifications/message","params":{"data":progress}})).unwrap();
        }
        let captured = Rc::new(RefCell::new(Vec::new()));
        let copy = captured.clone();
        let (producer, mut receiver) = crate::events::channel(mcpx_core::EventLimits {
            capacity: 4,
            ..Default::default()
        })
        .unwrap();
        let handler: EventHandler = Box::new(move |item| copy.borrow_mut().push(item.clone()));
        let dispatch = EventDispatch {
            handler: Some(Rc::new(RefCell::new(handler))),
            stream: Some(Rc::new(producer)),
        };
        assert!(dispatch.publish(&mut runtime).is_empty());
        let captured = captured.borrow();
        assert!(matches!(
            captured[0],
            EventStreamItem::Lagged { dropped: 244 }
        ));
        assert_eq!(captured.len(), 257);
        assert!(matches!(
            receiver.try_next(),
            Some(EventStreamItem::Lagged { dropped: 496 })
        ));
        let mut retained = 0;
        while let Some(item) = receiver.try_next() {
            if matches!(item, EventStreamItem::Event(_)) {
                retained += 1;
            }
        }
        assert_eq!(retained, 4);
        assert_eq!(runtime.take_events().dropped, 0);
    }
}
