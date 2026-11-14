use mcpx_core::Error;
use serde_json::Value;

pub(crate) enum Input {
    NotificationHeaders(u16, String, std::time::Instant),
    NotificationChunk(Vec<u8>, std::time::Instant),
    NotificationEof,
    NotificationFailed(bool),
    Headers(u16, String),
    Chunk(Vec<u8>),
    Eof(std::time::Instant),
    Failed(Error, std::time::Instant),
    Stdio(Value),
    StdioFailed(Error),
}
pub(crate) struct Message {
    pub id: u64,
    pub input: Input,
}
