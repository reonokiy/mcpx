use crate::{Error, HostRuntime, Result};
use mcpx_protocol::{ResponseDecoder, SseMetadata};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// A suspended original call, without exposing the opaque event cursor.
#[derive(Clone, Debug, Serialize)]
pub struct ResponseResume {
    pub action_id: u64,
    pub rpc_id: u64,
    pub operation_id: u64,
    pub retry_after_ms: u64,
}
/// A GET for the same business operation/RPC, with a fresh host action ID.
/// The host retains its original authorization and cancellation snapshot.
#[derive(Clone, Serialize)]
pub struct ResponseResumeRequest {
    pub action_id: u64,
    pub rpc_id: u64,
    pub operation_id: u64,
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
}
impl std::fmt::Debug for ResponseResumeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseResumeRequest")
            .field("action_id", &self.action_id)
            .field("rpc_id", &self.rpc_id)
            .field("operation_id", &self.operation_id)
            .field("method", &self.method)
            .finish_non_exhaustive()
    }
}
enum Phase {
    Awaiting { resumed: bool },
    Open,
    Interrupted,
    Completed,
}
pub(crate) struct Stream {
    rpc: u64,
    operation: u64,
    headers: BTreeMap<String, String>,
    cursor: SseMetadata,
    phase: Phase,
    ended: u64,
    replies: Vec<Value>,
}
fn unavailable() -> Error {
    Error::local(
        "ResumeUnavailable",
        "response has no usable legacy stream checkpoint",
    )
}
fn safe_cursor(cursor: &SseMetadata) -> Result<&str> {
    let id = cursor.last_event_id().ok_or_else(unavailable)?;
    if id.trim_matches([' ', '\t']) != id || id.bytes().any(|b| b < 32 && b != b'\t' || b == 127) {
        return Err(unavailable());
    }
    Ok(id)
}
impl HostRuntime {
    /// Opt in before opening the original HTTP response. Ordinary EOF handling
    /// remains terminal; hosts explicitly interrupt instead of finishing it.
    /// At most 16 trackers (including retained terminal replies) are admitted.
    pub fn enable_response_resumption(&mut self, id: u64) -> Result<()> {
        let (operation, rpc, headers) = self.machine.response_resume_route(id)?;
        if self.responses.contains_key(&id) || self.response_resumptions.contains_key(&id) {
            return Err(Error::local(
                "Busy",
                "response stream is already tracked or open",
            ));
        }
        if self.response_resumptions.len() >= 16 {
            return Err(Error::local(
                "LimitExceeded",
                "response resume tracker limit exceeded",
            ));
        }
        self.response_resumptions.insert(
            id,
            Stream {
                rpc,
                operation,
                headers,
                cursor: SseMetadata::default(),
                phase: Phase::Awaiting { resumed: false },
                ended: 0,
                replies: Vec::new(),
            },
        );
        Ok(())
    }
    pub(crate) fn check_response_stream_input(&self, id: u64) -> Result<()> {
        if self
            .response_resumptions
            .get(&id)
            .is_some_and(|s| matches!(s.phase, Phase::Interrupted | Phase::Completed))
        {
            return Err(Error::local(
                "InvalidHandle",
                "response stream is interrupted or completed",
            ));
        }
        Ok(())
    }
    pub(crate) fn prepare_resumable_decoder(
        &mut self,
        id: u64,
        status: u16,
        headers: &BTreeMap<String, String>,
        decoder: ResponseDecoder,
    ) -> Result<ResponseDecoder> {
        let Some(stream) = self.response_resumptions.get_mut(&id) else {
            return Ok(decoder);
        };
        let mime = headers
            .get("content-type")
            .map(String::as_str)
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        let sse = mime.eq_ignore_ascii_case("text/event-stream");
        if matches!(stream.phase, Phase::Awaiting { resumed: true }) && (status != 200 || !sse) {
            let error = Error::local("ProtocolError", "resumed GET must return HTTP 200 SSE");
            self.response_resumptions.remove(&id);
            let error = error.with_context(
                self.machine
                    .action_error_context(id)
                    .expect("active response"),
            );
            self.machine.fail_request(id, error.clone())?;
            return Err(error);
        }
        if status != 200 || !sse {
            self.response_resumptions.remove(&id);
            return Ok(decoder);
        }
        stream.phase = Phase::Open;
        Ok(decoder.track_sse_metadata(stream.cursor.clone()))
    }
    pub(crate) fn check_resumed_response_status(&mut self, id: u64, status: u16) -> Result<()> {
        let Some(stream) = self.response_resumptions.get(&id) else {
            return Ok(());
        };
        if !matches!(stream.phase, Phase::Awaiting { resumed: true }) || status == 200 {
            return Ok(());
        }
        let error = match status {
            401 | 403 => Error::local("AuthFailed", "resumed response authorization was rejected"),
            404 if stream.headers.contains_key("mcp-session-id") => {
                Error::local("SessionExpired", "response session is unavailable")
            }
            200..=299 => Error::local("ProtocolError", "resumed GET requires HTTP 200 SSE"),
            _ => Error::local(
                "HttpError",
                format!("resumed response HTTP status {status}"),
            ),
        }
        .with_context(
            self.machine
                .action_error_context(id)
                .expect("active response"),
        );
        if error.code == "SessionExpired" {
            self.machine
                .expire_http_session(stream.headers.get("mcp-session-id").map(String::as_str));
        }
        self.response_resumptions.remove(&id);
        self.machine.fail_request(id, error.clone())?;
        Err(error)
    }
    pub(crate) fn complete_resumable_response(&mut self, id: u64) {
        if let Some(stream) = self.response_resumptions.get_mut(&id) {
            stream.phase = Phase::Completed;
        }
    }
    pub(crate) fn queue_response_reply(&mut self, id: u64, reply: Value) -> Result<()> {
        let stream = self
            .response_resumptions
            .get_mut(&id)
            .expect("tracked stream");
        if stream.replies.len() >= 16 {
            return Err(Error::local(
                "LimitExceeded",
                "response capability reply queue exceeded",
            ));
        }
        stream.replies.push(reply);
        Ok(())
    }
    pub(crate) fn validate_response_server_request(&self, message: &Value) -> Result<()> {
        crate::check_json(message, 0)?;
        let id = message.get("id").expect("request ID");
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || !message.get("method").is_some_and(Value::is_string)
            || message.get("result").is_some()
            || message.get("error").is_some()
            || message
                .get("params")
                .is_some_and(|params| !params.is_object())
            || !(id.as_str().is_some_and(|id| id.len() <= 4096)
                || id
                    .as_i64()
                    .is_some_and(|id| id.unsigned_abs() <= crate::MAX_HOST_ID))
        {
            return Err(Error::local(
                "ProtocolError",
                "invalid legacy response server request",
            ));
        }
        Ok(())
    }
    /// Drain accepted legacy capability errors using the original POST
    /// headers/credentials (never Last-Event-ID). Await host writes before resume.
    /// Reading after completion releases the terminal tracker, even if empty.
    pub fn response_replies(&mut self, id: u64) -> Result<Vec<Value>> {
        let stream = self
            .response_resumptions
            .get_mut(&id)
            .ok_or_else(|| Error::local("InvalidHandle", "response tracker is unavailable"))?;
        let replies = std::mem::take(&mut stream.replies);
        if matches!(stream.phase, Phase::Completed) {
            self.response_resumptions.remove(&id);
        }
        Ok(replies)
    }
    /// Discard a terminal tracker/replies. Active calls must first be failed or
    /// cancelled through their ordinary operation API, which also clears it.
    pub fn release_response_resumption(&mut self, id: u64) -> Result<()> {
        if self.machine.has_action(id) {
            return Err(Error::local("Busy", "response operation is still active"));
        }
        self.response_resumptions
            .remove(&id)
            .ok_or_else(|| Error::local("InvalidHandle", "response tracker is unavailable"))?;
        Ok(())
    }
    /// Close an interrupted reader without terminating or replaying the call.
    /// A partial final frame is discarded after the last complete SSE cursor.
    /// Errors leave the original response intact for ordinary finish/fail.
    pub fn interrupt_response_for(&mut self, id: u64) -> Result<()> {
        let stream = self.response_resumptions.get(&id).ok_or_else(unavailable)?;
        self.machine.response_resume_route(id)?;
        let cursor = match stream.phase {
            Phase::Open => self
                .responses
                .get(&id)
                .and_then(|response| response.decoder.sse_metadata())
                .ok_or_else(unavailable)?
                .clone(),
            Phase::Awaiting { resumed: true } => stream.cursor.clone(),
            _ => return Err(unavailable()),
        };
        safe_cursor(&cursor)?;
        let stream = self.response_resumptions.get_mut(&id).unwrap();
        stream.cursor = cursor;
        stream.ended = self.machine.now_ms();
        stream.phase = Phase::Interrupted;
        self.responses.remove(&id);
        Ok(())
    }
    /// Read the retry floor without disclosing the cursor. Busy means accepted
    /// server requests still need draining, not a reason to replay the POST.
    pub fn response_resume(&self, id: u64) -> Result<ResponseResume> {
        let stream = self
            .response_resumptions
            .get(&id)
            .ok_or_else(|| Error::local("InvalidHandle", "response tracker is unavailable"))?;
        self.machine.response_resume_route(id)?;
        if !matches!(stream.phase, Phase::Interrupted) {
            return Err(unavailable());
        }
        safe_cursor(&stream.cursor)?;
        if !stream.replies.is_empty() {
            return Err(Error::local(
                "Busy",
                "drain accepted response replies before resuming",
            ));
        }
        let due = stream
            .ended
            .checked_add(stream.cursor.retry_ms().unwrap_or(1000))
            .filter(|due| *due <= crate::MAX_HOST_ID)
            .ok_or_else(|| {
                Error::local(
                    "LimitExceeded",
                    "response retry exceeds supported monotonic clock range",
                )
            })?;
        Ok(ResponseResume {
            action_id: id,
            rpc_id: stream.rpc,
            operation_id: stream.operation,
            retry_after_ms: due.saturating_sub(self.machine.now_ms()),
        })
    }
    /// Report exhaustion of the host's explicit GET attempt policy. Only a
    /// suspended, reply-drained original call can be failed this way.
    pub fn exhaust_response_resumption(&mut self, id: u64) -> Result<()> {
        self.response_resume(id)?;
        self.fail_request(
            id,
            Error::new(
                "ResponseRecoveryExhausted",
                "response GET attempts exhausted; original tool execution may be unknown",
            ),
        )
    }
    /// Plan only a GET with a fresh stream handle. No JSON-RPC message is
    /// created; operation/RPC IDs, call state and deadline ownership are retained.
    pub fn resume_response(&mut self, id: u64) -> Result<ResponseResumeRequest> {
        let info = self.response_resume(id)?;
        if info.retry_after_ms != 0 {
            return Err(Error::local(
                "RetryLater",
                "response retry interval has not elapsed",
            ));
        }
        let stream = self.response_resumptions.get(&id).unwrap();
        let mut headers = stream.headers.clone();
        headers.insert("last-event-id".into(), safe_cursor(&stream.cursor)?.into());
        let next = self.machine.resume_response_action(id)?;
        let mut stream = self.response_resumptions.remove(&id).unwrap();
        stream.phase = Phase::Awaiting { resumed: true };
        self.response_resumptions.insert(next, stream);
        Ok(ResponseResumeRequest {
            action_id: next,
            rpc_id: info.rpc_id,
            operation_id: info.operation_id,
            method: "GET".into(),
            url: self.endpoint().into(),
            headers,
            body: None,
        })
    }
}
