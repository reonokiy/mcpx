use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
mod headers;
pub use headers::{HeaderError, parameter_headers, validate_header_annotations};

pub const LEGACY_VERSION: &str = "2025-11-25";
pub const MODERN_VERSION: &str = "2026-07-28";
pub const MAX_BODY: usize = 16 * 1024 * 1024;
pub const MAX_EVENT: usize = 8 * 1024 * 1024;
pub const MAX_EVENT_ID: usize = 4096;

/// Optional SSE resumption metadata. Opaque event IDs are omitted from Debug.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SseMetadata {
    last_event_id: Option<String>,
    retry_ms: Option<u64>,
}
impl SseMetadata {
    pub fn last_event_id(&self) -> Option<&str> {
        self.last_event_id.as_deref()
    }
    pub fn retry_ms(&self) -> Option<u64> {
        self.retry_ms
    }
}
impl std::fmt::Debug for SseMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseMetadata")
            .field("has_event_id", &self.last_event_id.is_some())
            .field("retry_ms", &self.retry_ms)
            .finish()
    }
}
struct MetadataState {
    committed: SseMetadata,
    pending_id: Option<String>,
}

pub fn encode_header(value: &str) -> String {
    use base64::Engine;
    if value.trim() == value
        && value.bytes().all(|c| (0x20..=0x7e).contains(&c))
        && !(value.starts_with("=?base64?") && value.ends_with("?="))
    {
        value.into()
    } else {
        format!(
            "=?base64?{}?=",
            base64::engine::general_purpose::STANDARD.encode(value)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolDescriptor {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub input_schema: Value,
    #[serde(default)]
    pub output_schema: Option<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Dialect {
    Auto,
    Legacy,
    Modern,
}

impl Dialect {
    pub fn version(self) -> &'static str {
        match self {
            Self::Legacy => LEGACY_VERSION,
            Self::Modern | Self::Auto => MODERN_VERSION,
        }
    }

    pub fn request(self, id: u64, method: &str, mut params: Value) -> Value {
        if self != Self::Legacy {
            let metadata = json!({
                "io.modelcontextprotocol/protocolVersion": MODERN_VERSION,
                "io.modelcontextprotocol/clientInfo": {"name":"mcpx", "version": env!("CARGO_PKG_VERSION")},
                "io.modelcontextprotocol/clientCapabilities": {}
            });
            if !params["_meta"].is_object() {
                params["_meta"] = json!({});
            }
            params["_meta"]
                .as_object_mut()
                .expect("object metadata")
                .extend(metadata.as_object().unwrap().clone());
        }
        json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params})
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("response exceeds configured byte limit")]
    Limit,
    #[error("invalid UTF-8 in event stream")]
    Utf8,
    #[error("invalid JSON response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported response content type")]
    ContentType,
    #[error("incomplete SSE event at end of response")]
    Incomplete,
}

/// Byte-oriented framing keeps UTF-8 characters intact across network chunks.
pub struct ResponseDecoder {
    sse: bool,
    buffer: Vec<u8>,
    data: Vec<u8>,
    event_bytes: usize,
    after_cr: bool,
    first_line: bool,
    metadata: Option<MetadataState>,
}

impl ResponseDecoder {
    pub fn new(content_type: &str) -> Result<Self, DecodeError> {
        let mime = content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        let sse = match mime.as_str() {
            "text/event-stream" => true,
            "application/json" => false,
            _ => return Err(DecodeError::ContentType),
        };
        Ok(Self {
            sse,
            buffer: vec![],
            data: vec![],
            event_bytes: 0,
            after_cr: false,
            first_line: true,
            metadata: None,
        })
    }

    /// Opt in to bounded SSE IDs/retry fields. A prior checkpoint seeds a new
    /// connection, so events without an ID inherit the original cursor.
    pub fn track_sse_metadata(mut self, previous: SseMetadata) -> Self {
        if self.sse {
            self.metadata = Some(MetadataState {
                pending_id: previous.last_event_id.clone(),
                committed: previous,
            });
        }
        self
    }
    pub fn sse_metadata(&self) -> Option<&SseMetadata> {
        self.metadata.as_ref().map(|m| &m.committed)
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Value>, DecodeError> {
        let mut messages = Vec::new();
        if !self.sse {
            if self.buffer.len().saturating_add(bytes.len()) > MAX_BODY {
                return Err(DecodeError::Limit);
            }
            self.buffer.extend_from_slice(bytes);
            return Ok(messages);
        }
        for &byte in bytes {
            if self.after_cr {
                self.after_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            self.event_bytes += 1;
            if self.event_bytes > MAX_EVENT {
                return Err(DecodeError::Limit);
            }
            if matches!(byte, b'\r' | b'\n') {
                self.after_cr = byte == b'\r';
                let mut line = std::mem::take(&mut self.buffer);
                if self.first_line {
                    self.first_line = false;
                    if line.starts_with(&[0xef, 0xbb, 0xbf]) {
                        line.drain(..3);
                    }
                }
                if line.is_empty() {
                    if !self.data.is_empty() {
                        self.data.pop();
                        // MCP SSE priming events carry an empty data field and no JSON message.
                        if !self.data.is_empty() {
                            messages.push(serde_json::from_slice(&self.data)?);
                        }
                        self.data.clear();
                    }
                    if let Some(metadata) = &mut self.metadata {
                        metadata.committed.last_event_id = metadata.pending_id.clone();
                    }
                    self.event_bytes = 0;
                } else {
                    std::str::from_utf8(&line).map_err(|_| DecodeError::Utf8)?;
                    let (field, value) = line
                        .iter()
                        .position(|&b| b == b':')
                        .map_or((line.as_slice(), &b""[..]), |pos| {
                            (&line[..pos], &line[pos + 1..])
                        });
                    let value = value.strip_prefix(b" ").unwrap_or(value);
                    if field == b"data" {
                        self.data.extend_from_slice(value);
                        self.data.push(b'\n');
                    } else if let Some(metadata) = &mut self.metadata {
                        if field == b"id" && !value.contains(&0) {
                            // Reset/oversized IDs cannot be used for a bounded
                            // resume. Never silently retain an older cursor.
                            metadata.pending_id =
                                (!value.is_empty() && value.len() <= MAX_EVENT_ID).then(|| {
                                    String::from_utf8(value.to_vec()).expect("validated UTF-8")
                                });
                        } else if field == b"retry"
                            && !value.is_empty()
                            && value.iter().all(u8::is_ascii_digit)
                        {
                            metadata.committed.retry_ms = Some(value.iter().fold(0_u64, |n, b| {
                                n.saturating_mul(10).saturating_add(u64::from(b - b'0'))
                            }));
                        }
                    }
                }
            } else {
                self.buffer.push(byte);
            }
        }
        Ok(messages)
    }

    pub fn finish(self) -> Result<Vec<Value>, DecodeError> {
        if !self.sse {
            return Ok(vec![serde_json::from_slice(&self.buffer)?]);
        }
        if !self.buffer.is_empty() || !self.data.is_empty() {
            return Err(DecodeError::Incomplete);
        }
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_encoding_handles_whitespace_unicode_controls_and_sentinels() {
        assert_eq!(encode_header("echo"), "echo");
        for value in [" padded ", "a\nb", "\u{4f60}\u{597d}", "=?base64?literal?="] {
            use base64::Engine;
            let header = encode_header(value);
            let encoded = header
                .strip_prefix("=?base64?")
                .unwrap()
                .strip_suffix("?=")
                .unwrap();
            assert_eq!(
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .unwrap(),
                value.as_bytes()
            );
        }
    }

    #[test]
    fn sse_is_invariant_under_every_chunk_boundary() {
        let wire = ": keepalive\r\n\r\ndata: {\"jsonrpc\":\"2.0\",\r\ndata: \"id\":1,\"result\":\"你好\"}\r\n\r\n".as_bytes();
        for split in 0..=wire.len() {
            let mut decoder = ResponseDecoder::new("text/event-stream; charset=utf-8").unwrap();
            let mut messages = decoder.push(&wire[..split]).unwrap();
            messages.extend(decoder.push(&wire[split..]).unwrap());
            messages.extend(decoder.finish().unwrap());
            assert_eq!(
                messages,
                vec![json!({"jsonrpc":"2.0","id":1,"result":"你好"})]
            );
        }
    }

    #[test]
    fn json_and_incomplete_sse_are_distinct() {
        let mut decoder = ResponseDecoder::new("application/json").unwrap();
        assert!(decoder.push(br#"{"result":1}"#).unwrap().is_empty());
        assert_eq!(decoder.finish().unwrap(), vec![json!({"result":1})]);
        let mut decoder = ResponseDecoder::new("text/event-stream").unwrap();
        decoder.push(b"data: {}\n").unwrap();
        assert!(matches!(decoder.finish(), Err(DecodeError::Incomplete)));
    }

    #[test]
    fn sse_priming_events_do_not_become_empty_json_responses() {
        let wire = b"id: 0\nretry: 3000\ndata: \n\ndata: {\"id\":1,\"result\":{}}\n\n";
        for split in 0..=wire.len() {
            let mut decoder = ResponseDecoder::new("text/event-stream").unwrap();
            let mut messages = decoder.push(&wire[..split]).unwrap();
            messages.extend(decoder.push(&wire[split..]).unwrap());
            messages.extend(decoder.finish().unwrap());
            assert_eq!(messages, vec![json!({"id":1,"result":{}})]);
        }
    }

    #[test]
    fn sse_accepts_bom_and_bare_cr_line_endings() {
        let mut decoder = ResponseDecoder::new("Text/Event-Stream").unwrap();
        let wire = b"\xef\xbb\xbfdata: {\"id\":1}\r\r";
        let mut messages = vec![];
        for byte in wire {
            messages.extend(decoder.push(&[*byte]).unwrap());
        }
        messages.extend(decoder.finish().unwrap());
        assert_eq!(messages, vec![json!({"id":1})]);
    }

    #[test]
    fn sse_checkpoints_are_chunk_invariant_and_commit_only_complete_events() {
        let wire = "\u{feff}id: 游标/é\r\nretry: 25\r\ndata:\r\n\r\nid: bad\0id\ndata: {}\n\nid: uncommitted\ndata: {".as_bytes();
        for split in 0..=wire.len() {
            let mut decoder = ResponseDecoder::new("text/event-stream")
                .unwrap()
                .track_sse_metadata(SseMetadata::default());
            let mut messages = decoder.push(&wire[..split]).unwrap();
            messages.extend(decoder.push(&wire[split..]).unwrap());
            assert_eq!(messages, vec![json!({})]);
            let metadata = decoder.sse_metadata().unwrap();
            assert_eq!(metadata.last_event_id(), Some("游标/é"));
            assert_eq!(metadata.retry_ms(), Some(25));
            assert!(!format!("{metadata:?}").contains("游标"));
            assert!(matches!(decoder.finish(), Err(DecodeError::Incomplete)));
        }
    }

    #[test]
    fn sse_cursor_reset_inheritance_budget_and_retry_integer_rules() {
        let mut decoder = ResponseDecoder::new("text/event-stream")
            .unwrap()
            .track_sse_metadata(SseMetadata::default());
        decoder.push(b"id: prior\nretry: 00025\n\n").unwrap();
        for bad in ["", "+1", "1x", "１２", " 2"] {
            decoder
                .push(format!("retry: {bad}\n\n").as_bytes())
                .unwrap();
            assert_eq!(decoder.sse_metadata().unwrap().retry_ms(), Some(25));
        }
        let mut resumed = ResponseDecoder::new("text/event-stream")
            .unwrap()
            .track_sse_metadata(decoder.sse_metadata().unwrap().clone());
        resumed.push(b"data: {}\n\n").unwrap();
        assert_eq!(
            resumed.sse_metadata().unwrap().last_event_id(),
            Some("prior")
        );
        resumed
            .push(format!("id: {}\n\n", "x".repeat(MAX_EVENT_ID + 1)).as_bytes())
            .unwrap();
        assert_eq!(resumed.sse_metadata().unwrap().last_event_id(), None);
        resumed
            .push(b"id: next\n\nid\n\nretry: 999999999999999999999999999999\n")
            .unwrap();
        assert_eq!(resumed.sse_metadata().unwrap().last_event_id(), None);
        assert_eq!(resumed.sse_metadata().unwrap().retry_ms(), Some(u64::MAX));
        resumed.push(b"retry: 0\n\n").unwrap();
        assert_eq!(resumed.sse_metadata().unwrap().retry_ms(), Some(0));
    }

    #[test]
    fn sse_metadata_is_opt_in_and_json_never_has_a_resume_cursor() {
        let mut ordinary = ResponseDecoder::new("text/event-stream").unwrap();
        ordinary
            .push(b"id: cursor\nretry: 1\ndata: {}\n\n")
            .unwrap();
        assert!(ordinary.sse_metadata().is_none());
        let mut json = ResponseDecoder::new("application/json")
            .unwrap()
            .track_sse_metadata(SseMetadata::default());
        json.push(b"{}").unwrap();
        assert!(json.sse_metadata().is_none());
        assert_eq!(json.finish().unwrap(), vec![json!({})]);
    }
}
