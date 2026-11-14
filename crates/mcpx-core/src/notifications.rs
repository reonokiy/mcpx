use crate::{Dialect, Error, HostRuntime, Result, ids::IdKind};
use mcpx_protocol::{ResponseDecoder, SseMetadata};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

const SUBSCRIPTION_ID: &str = "io.modelcontextprotocol/subscriptionId";
const ACK: &str = "notifications/subscriptions/acknowledged";
const TOOLS_CHANGED: &str = "notifications/tools/list_changed";
pub const DEFAULT_NOTIFICATION_RETRY_MS: u64 = 1000;

/// A possible legacy notification GET resume, without exposing the opaque cursor.
#[derive(Clone, Debug, Serialize)]
pub struct NotificationResume {
    pub resource_id: u64,
    pub retry_after_ms: u64,
}

/// A separate long-lived I/O resource, not a tool action or business operation.
#[derive(Clone, Serialize)]
pub struct NotificationRequest {
    pub resource_id: u64,
    pub rpc_id: Option<u64>,
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
}
impl std::fmt::Debug for NotificationRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotificationRequest")
            .field("resource_id", &self.resource_id)
            .field("rpc_id", &self.rpc_id)
            .field("method", &self.method)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationPhase {
    Opening,
    Listening,
    Closed,
    Failed,
    Cancelled,
}
#[derive(Clone, Debug, Serialize)]
pub struct NotificationStatus {
    pub resource_id: u64,
    pub rpc_id: Option<u64>,
    pub phase: NotificationPhase,
    pub error_code: Option<String>,
}
pub(crate) struct NotificationState {
    status: NotificationStatus,
    decoder: Option<ResponseDecoder>,
    replies: Vec<Value>,
    legacy: bool,
    session: Option<String>,
    headers: bool,
    stdio: bool,
    opening_deadline: Option<u64>,
    cursor: SseMetadata,
    resumable: bool,
    disconnected_at: u64,
}
impl NotificationState {
    fn active(&self) -> bool {
        matches!(
            self.status.phase,
            NotificationPhase::Opening | NotificationPhase::Listening
        )
    }
    fn end(&mut self, phase: NotificationPhase, code: Option<&str>) {
        self.resumable = false;
        self.status.phase = phase;
        self.status.error_code = code.map(String::from);
        self.decoder = None;
        if !(self.legacy && phase == NotificationPhase::Failed) {
            self.replies.clear();
        }
    }
    fn message(&mut self, message: &Value) -> Result<bool> {
        let bad = || Error::local("ProtocolError", "invalid notification stream message");
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(bad());
        }
        if self.legacy {
            if message.get("method").and_then(Value::as_str).is_none()
                || message.get("result").is_some()
                || message.get("error").is_some()
            {
                return Err(bad());
            }
            if let Some(id) = message.get("id") {
                if !(id.as_str().is_some_and(|id| id.len() <= 256)
                    || id
                        .as_i64()
                        .is_some_and(|id| id.unsigned_abs() <= crate::MAX_HOST_ID))
                {
                    return Err(bad());
                }
                if self.replies.len() == 16 {
                    return Err(Error::local(
                        "LimitExceeded",
                        "notification reply budget exceeded",
                    ));
                }
                self.replies.push(json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32601,"message":"Client capability not supported"}}));
                return Ok(false);
            }
            return Ok(true);
        }
        let rpc = self.status.rpc_id.expect("modern subscription RPC");
        if message.get("id").is_some() {
            if message.get("id").and_then(Value::as_u64) != Some(rpc)
                || message.get("method").is_some()
                || message.get("result").is_some() == message.get("error").is_some()
            {
                return Err(bad());
            }
            if let Some(error) = message.get("error") {
                if !error.get("code").is_some_and(Value::is_i64)
                    || !error.get("message").is_some_and(Value::is_string)
                {
                    return Err(bad());
                }
                return Err(Error::local(
                    "RemoteProtocolError",
                    "remote rejected the notification subscription",
                ));
            }
            let result = &message["result"];
            if self.status.phase != NotificationPhase::Listening
                || result.get("resultType").and_then(Value::as_str) != Some("complete")
                || result
                    .get("_meta")
                    .and_then(|meta| meta.get(SUBSCRIPTION_ID))
                    .and_then(Value::as_u64)
                    != Some(rpc)
            {
                return Err(bad());
            }
            self.end(NotificationPhase::Closed, None);
            return Ok(false);
        }
        if message.get("result").is_some()
            || message.get("error").is_some()
            || !message.get("params").is_some_and(Value::is_object)
            || message["params"]
                .get("_meta")
                .and_then(|meta| meta.get(SUBSCRIPTION_ID))
                .and_then(Value::as_u64)
                != Some(rpc)
        {
            return Err(bad());
        }
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(bad)?;
        if self.status.phase == NotificationPhase::Opening {
            if method != ACK {
                return Err(bad());
            }
            let filter = message["params"]
                .get("notifications")
                .and_then(Value::as_object)
                .ok_or_else(bad)?;
            for name in ["promptsListChanged", "resourcesListChanged"] {
                if filter
                    .get(name)
                    .is_some_and(|value| value != &Value::Bool(false))
                {
                    return Err(bad());
                }
            }
            if filter
                .get("resourceSubscriptions")
                .is_some_and(|value| value.as_array().is_none_or(|items| !items.is_empty()))
            {
                return Err(bad());
            }
            match filter.get("toolsListChanged") {
                Some(Value::Bool(true)) => self.status.phase = NotificationPhase::Listening,
                None | Some(Value::Bool(false)) => {
                    return Err(Error::local(
                        "CapabilityUnavailable",
                        "server did not acknowledge tool-list notifications",
                    ));
                }
                _ => return Err(bad()),
            }
            return Ok(false);
        }
        if method != TOOLS_CHANGED {
            return Err(bad());
        }
        Ok(true)
    }
}

impl HostRuntime {
    /// Inspect an interrupted legacy HTTP listener. The host must keep the
    /// original credential snapshot/session, drain accepted replies and advance
    /// the monotonic clock. Modern HTTP and stdio never use Last-Event-ID.
    pub fn notification_resume(&self, id: u64) -> Result<NotificationResume> {
        let state = self
            .notifications
            .as_ref()
            .filter(|s| s.status.resource_id == id)
            .ok_or_else(|| Error::local("InvalidHandle", "notification resource is unavailable"))?;
        let unavailable = || {
            Error::local(
                "ResumeUnavailable",
                "notification stream has no usable legacy resume checkpoint",
            )
        };
        if state.stdio
            || !state.legacy
            || !state.resumable
            || state.status.phase != NotificationPhase::Failed
        {
            return Err(unavailable());
        }
        let cursor = state.cursor.last_event_id().ok_or_else(unavailable)?;
        // Header APIs cannot preserve OWS or controls exactly. Never transform
        // an opaque cursor (for example by applying MCP parameter encoding).
        if cursor.trim_matches([' ', '\t']) != cursor
            || cursor.bytes().any(|b| b < 32 && b != b'\t' || b == 127)
        {
            return Err(unavailable());
        }
        if !state.replies.is_empty() {
            return Err(Error::local(
                "Busy",
                "drain accepted notification replies before resuming",
            ));
        }
        let due = state
            .disconnected_at
            .checked_add(
                state
                    .cursor
                    .retry_ms()
                    .unwrap_or(DEFAULT_NOTIFICATION_RETRY_MS),
            )
            .filter(|due| *due <= crate::MAX_HOST_ID)
            .ok_or_else(|| {
                Error::local(
                    "LimitExceeded",
                    "notification retry exceeds the supported monotonic clock range",
                )
            })?;
        Ok(NotificationResume {
            resource_id: id,
            retry_after_ms: due.saturating_sub(self.machine.now_ms()),
        })
    }
    /// Plan a GET resume after its retry delay. This allocates a fresh resource
    /// ID to reject old reader input and never replays a request POST.
    pub fn resume_notifications(&mut self, id: u64) -> Result<NotificationRequest> {
        let resume = self.notification_resume(id)?;
        if resume.retry_after_ms != 0 {
            return Err(Error::local(
                "RetryLater",
                "notification retry interval has not elapsed",
            ));
        }
        let cursor = self.notifications.as_ref().unwrap().cursor.clone();
        let mut request = self.start_notifications()?;
        request.headers.insert(
            "last-event-id".into(),
            cursor.last_event_id().unwrap().into(),
        );
        self.notifications.as_mut().unwrap().cursor = cursor;
        Ok(request)
    }
    /// Open one explicit HTTP tool-catalog notification resource. The host owns
    /// the I/O and a frozen credential snapshot. No tool call is sent or replayed.
    pub fn start_notifications(&mut self) -> Result<NotificationRequest> {
        self.start_notification_resource(false)
    }
    /// Plan one shared-stdio catalog listener. Modern peers receive `body` as
    /// a JSON-RPC request; legacy peers need no additional wire message.
    /// Feed framed messages through `stream_input`, not the HTTP/SSE methods.
    pub fn start_stdio_notifications(&mut self) -> Result<NotificationRequest> {
        self.start_notification_resource(true)
    }
    fn start_notification_resource(&mut self, stdio: bool) -> Result<NotificationRequest> {
        if self
            .notifications
            .as_ref()
            .is_some_and(|state| state.active() || !state.replies.is_empty())
        {
            return Err(Error::local(
                "Busy",
                "finish or release the previous notification resource first",
            ));
        }
        IdKind::Notification.available(self.next_notification)?;
        let rpc_id = self.machine.notification_rpc(stdio)?;
        let resource_id = IdKind::Notification.allocate(&mut self.next_notification)?;
        let legacy = self.machine.dialect() == Dialect::Legacy;
        let mut headers = BTreeMap::from([
            (
                "accept".into(),
                if legacy {
                    "text/event-stream"
                } else {
                    "application/json, text/event-stream"
                }
                .into(),
            ),
            (
                "mcp-protocol-version".into(),
                self.machine.dialect().version().into(),
            ),
        ]);
        let body = if legacy {
            if let Some(session) = self.machine.session_id() {
                headers.insert("mcp-session-id".into(), session.into());
            }
            None
        } else {
            headers.insert("content-type".into(), "application/json".into());
            headers.insert("mcp-method".into(), "subscriptions/listen".into());
            Some(self.machine.dialect().request(
                rpc_id.unwrap(),
                "subscriptions/listen",
                json!({"notifications":{"toolsListChanged":true}}),
            ))
        };
        self.notifications = Some(NotificationState {
            status: NotificationStatus {
                resource_id,
                rpc_id,
                phase: if stdio && legacy {
                    NotificationPhase::Listening
                } else {
                    NotificationPhase::Opening
                },
                error_code: None,
            },
            decoder: None,
            replies: Vec::new(),
            legacy,
            session: headers.get("mcp-session-id").cloned(),
            headers: stdio,
            stdio,
            opening_deadline: None,
            cursor: SseMetadata::default(),
            resumable: false,
            disconnected_at: 0,
        });
        Ok(NotificationRequest {
            resource_id,
            rpc_id,
            method: if stdio {
                "STDIO"
            } else if legacy {
                "GET"
            } else {
                "POST"
            }
            .into(),
            url: self.endpoint().into(),
            headers: if stdio { BTreeMap::new() } else { headers },
            body,
        })
    }
    pub(crate) fn stdio_notification_message(&mut self, message: &Value) -> bool {
        let Some(state) = &mut self.notifications else {
            return false;
        };
        if !state.stdio || state.legacy {
            return false;
        }
        let rpc = state.status.rpc_id.expect("modern subscription RPC");
        let method = message.get("method").and_then(Value::as_str);
        let scope = message.pointer("/params/_meta/io.modelcontextprotocol~1subscriptionId");
        let response = message.get("id").and_then(Value::as_u64) == Some(rpc);
        if !response && method != Some(ACK) && method != Some(TOOLS_CHANGED) && scope.is_none() {
            return false;
        }
        // Messages for replaced subscriptions share the same channel. Their old
        // allocated IDs must never invalidate a new listener or a business call.
        if scope
            .and_then(Value::as_u64)
            .is_some_and(|id| id != rpc && self.machine.is_past_rpc(id))
        {
            return true;
        }
        if !state.active() {
            return true;
        }
        let id = state.status.resource_id;
        let result = state.message(message).and_then(|changed| {
            if changed {
                self.machine.notify(message)
            } else {
                Ok(())
            }
        });
        if let Err(error) = result {
            self.notification_failed(id, error);
        }
        true
    }
    /// Tighten the opening/ACK deadline on the host's monotonic millisecond
    /// clock. `advance_time` checks it even during a serialized stream request.
    pub fn set_notification_deadline(&mut self, id: u64, deadline_ms: u64) -> Result<()> {
        let state = self.notification(id)?;
        if state.status.phase != NotificationPhase::Opening {
            return Err(Error::local("InvalidHandle", "notification is not opening"));
        }
        state.opening_deadline = Some(
            state
                .opening_deadline
                .map_or(deadline_ms, |old| old.min(deadline_ms)),
        );
        self.expire_notification_deadline();
        Ok(())
    }
    pub(crate) fn expire_notification_deadline(&mut self) {
        if let Some(id) = self
            .notifications
            .as_ref()
            .filter(|s| {
                s.status.phase == NotificationPhase::Opening
                    && s.opening_deadline
                        .is_some_and(|deadline| self.machine.now_ms() >= deadline)
            })
            .map(|s| s.status.resource_id)
        {
            self.notification_failed(
                id,
                Error::local("Timeout", "notification acknowledgment deadline exceeded"),
            );
        }
    }
    pub fn notification_status(&self) -> Option<NotificationStatus> {
        self.notifications
            .as_ref()
            .map(|state| state.status.clone())
    }
    fn notification(&mut self, id: u64) -> Result<&mut NotificationState> {
        self.notifications
            .as_mut()
            .filter(|state| state.status.resource_id == id && state.active())
            .ok_or_else(|| Error::local("InvalidHandle", "notification resource is not active"))
    }
    fn notification_failed(&mut self, id: u64, error: Error) -> Error {
        if let Some(state) = self
            .notifications
            .as_mut()
            .filter(|state| state.status.resource_id == id)
        {
            if let Some(cursor) = state
                .decoder
                .as_ref()
                .and_then(ResponseDecoder::sse_metadata)
            {
                state.cursor = cursor.clone();
            }
            state.end(NotificationPhase::Failed, Some(&error.code));
            state.disconnected_at = self.machine.now_ms();
            self.machine.notification_gap();
            if error.code == "SessionExpired" {
                self.machine.expire_http_session(state.session.as_deref());
            }
        }
        error
    }
    pub(crate) fn cancel_notification_context(&mut self, code: &str) {
        if let Some(state) = self.notifications.as_mut() {
            state.end(NotificationPhase::Cancelled, Some(code));
            state.cursor = SseMetadata::default();
        }
    }
    pub fn begin_notifications(&mut self, id: u64, status: u16, content_type: &str) -> Result<()> {
        let state = self.notification(id)?;
        if state.headers {
            return Err(Error::local(
                "InvalidHandle",
                "notification response already started",
            ));
        }
        let result = (|| {
            if !(100..=599).contains(&status) {
                return Err(Error::local(
                    "InvalidResponse",
                    "invalid notification HTTP status",
                ));
            }
            match status {
                401 | 403 => {
                    return Err(Error::local(
                        "AuthenticationRequired",
                        "notification authorization was rejected",
                    ));
                }
                404 if state.legacy => {
                    return Err(Error::local(
                        "SessionExpired",
                        "notification session is unavailable",
                    ));
                }
                404 | 405 => {
                    return Err(Error::local(
                        "CapabilityUnavailable",
                        "notification endpoint is unavailable",
                    ));
                }
                200 => {}
                _ => {
                    return Err(Error::local(
                        "HttpError",
                        "notification endpoint returned an unexpected status",
                    ));
                }
            }
            if content_type.len() > 128
                || content_type
                    .bytes()
                    .any(|byte| byte == 127 || byte < 32 && byte != b'\t')
                || !content_type
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .eq_ignore_ascii_case("text/event-stream")
            {
                return Err(Error::local(
                    "ProtocolError",
                    "notification response must be SSE",
                ));
            }
            let decoder = ResponseDecoder::new(content_type).expect("validated SSE MIME");
            state.decoder = Some(if state.legacy {
                decoder.track_sse_metadata(state.cursor.clone())
            } else {
                decoder
            });
            state.headers = true;
            if state.legacy {
                state.status.phase = NotificationPhase::Listening;
            }
            Ok(())
        })();
        result.map_err(|error| self.notification_failed(id, error))
    }
    /// Returns true after a validated modern graceful-close result. The host
    /// should cancel its body reader; EOF is not required after that result.
    pub fn push_notifications(&mut self, id: u64, bytes: &[u8]) -> Result<bool> {
        self.notification(id)?;
        let state = self.notifications.as_mut().unwrap();
        let result = (|| {
            if bytes.len() > 65536 {
                return Err(Error::local(
                    "LimitExceeded",
                    "notification chunk exceeds 64 KiB",
                ));
            }
            let messages = state
                .decoder
                .as_mut()
                .ok_or_else(|| Error::local("InvalidHandle", "notification headers are required"))?
                .push(bytes)
                .map_err(|error| {
                    Error::local(
                        if matches!(error, mcpx_protocol::DecodeError::Limit) {
                            "LimitExceeded"
                        } else {
                            "ProtocolError"
                        },
                        "invalid or oversized notification SSE",
                    )
                })?;
            for message in messages {
                if !state.active() {
                    return Err(Error::local(
                        "ProtocolError",
                        "message after notification stream completion",
                    ));
                }
                if state.message(&message)? {
                    self.machine.notify(&message)?;
                }
            }
            Ok(!state.active())
        })();
        let result = result.map_err(|error| self.notification_failed(id, error));
        if matches!(result, Ok(true)) {
            self.machine.notification_gap();
        }
        result
    }
    pub fn finish_notifications(&mut self, id: u64) -> Result<()> {
        let state = self.notification(id)?;
        let decoder = state
            .decoder
            .take()
            .ok_or_else(|| Error::local("InvalidHandle", "notification headers are required"))?;
        if let Some(cursor) = decoder.sse_metadata() {
            state.cursor = cursor.clone();
        }
        let resumable = state.legacy && !state.stdio;
        let error = if decoder.finish().is_err() {
            Error::local("ProtocolError", "incomplete notification SSE")
        } else {
            Error::local(
                "SubscriptionDisconnected",
                "notification stream ended without a graceful result",
            )
        };
        let error = self.notification_failed(id, error);
        self.notifications.as_mut().unwrap().resumable = resumable;
        Err(error)
    }
    pub fn stop_notifications(&mut self, id: u64) -> Result<()> {
        let state = self
            .notifications
            .as_mut()
            .filter(|state| state.status.resource_id == id)
            .ok_or_else(|| Error::local("InvalidHandle", "notification resource is unavailable"))?;
        if state.active() {
            state.end(NotificationPhase::Cancelled, None);
            self.machine.notification_gap();
        } else {
            state.replies.clear();
        }
        state.resumable = false;
        state.cursor = SseMetadata::default();
        Ok(())
    }
    pub fn abort_notifications(&mut self, id: u64, timeout: bool) -> Result<()> {
        self.notification(id)?;
        let resumable = self.notifications.as_ref().is_some_and(|s| {
            s.legacy && !s.stdio && (s.headers || s.cursor.last_event_id().is_some())
        });
        self.notification_failed(
            id,
            Error::local(
                if timeout { "Timeout" } else { "TransportError" },
                "notification I/O ended",
            ),
        );
        self.notifications.as_mut().unwrap().resumable = resumable;
        Ok(())
    }
    pub fn notification_replies(&mut self, id: u64) -> Result<Vec<Value>> {
        let state = self
            .notifications
            .as_mut()
            .filter(|state| {
                state.status.resource_id == id
                    && (state.active()
                        || state.legacy && state.status.phase == NotificationPhase::Failed)
            })
            .ok_or_else(|| Error::local("InvalidHandle", "notification replies are unavailable"))?;
        Ok(std::mem::take(&mut state.replies))
    }
}
