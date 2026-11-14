use serde::{Deserialize, Serialize};

/// Evidence about this attempt, never a guarantee about earlier calls or resumes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionCertainty {
    NotSent,
    #[default]
    Unknown,
    ResultReceived,
    NeedsInput,
}

/// Guidance for an explicit caller decision. No variant authorizes replay.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryAdvice {
    #[default]
    None,
    CorrectInput,
    RefreshBinding,
    RefreshAuthentication,
    ExplicitPolicy,
    InspectExecution,
    InspectResult,
    ResumeInput,
}

/// A snapshot survives removal of an action by a decoder or cancellation.
#[derive(Debug, Clone, Copy)]
pub struct ErrorContext {
    pub operation_id: Option<u64>,
    pub queue_operation_id: Option<u64>,
    pub execution_certainty: ExecutionCertainty,
}
impl ErrorContext {
    pub const fn not_sent() -> Self {
        Self {
            operation_id: None,
            queue_operation_id: None,
            execution_certainty: ExecutionCertainty::NotSent,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub instance_path: String,
    pub schema_path: String,
}

#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
#[error("{code}: {message}")]
pub struct Error {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub issues: Vec<ValidationIssue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_operation_id: Option<u64>,
    #[serde(default)]
    pub execution_certainty: ExecutionCertainty,
    #[serde(default)]
    pub retry_advice: RetryAdvice,
}

impl Error {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            issues: vec![],
            operation_id: None,
            queue_operation_id: None,
            execution_certainty: ExecutionCertainty::Unknown,
            retry_advice: RetryAdvice::None,
        }
    }
    /// A failure known to precede dispatch of this tool attempt.
    pub fn local(code: &str, message: impl Into<String>) -> Self {
        Self::new(code, message).with_context(ErrorContext::not_sent())
    }
    pub fn with_context(mut self, context: ErrorContext) -> Self {
        self.operation_id = context.operation_id;
        self.queue_operation_id = context.queue_operation_id;
        self.execution_certainty = context.execution_certainty;
        self.retry_advice = match context.execution_certainty {
            ExecutionCertainty::Unknown => RetryAdvice::InspectExecution,
            ExecutionCertainty::ResultReceived => RetryAdvice::InspectResult,
            ExecutionCertainty::NeedsInput => RetryAdvice::ResumeInput,
            ExecutionCertainty::NotSent => match self.code.as_str() {
                "BindingStale" | "BindingExpired" => RetryAdvice::RefreshBinding,
                "AuthenticationRequired"
                | "AuthorizationFailed"
                | "CredentialMissing"
                | "Unauthorized"
                | "AuthenticationFailed"
                | "AuthFailed"
                | "CredentialUnavailable" => RetryAdvice::RefreshAuthentication,
                "ValidationFailed"
                | "InvalidJson"
                | "InvalidInput"
                | "InvalidArguments"
                | "NumberOutOfRange"
                | "SerializationFailed"
                | "UnknownArgument"
                | "MissingArgument"
                | "InvalidCommand"
                | "TokenizeError"
                | "CommandParseError"
                | "CommandError" => RetryAdvice::CorrectInput,
                "Timeout" | "TransportError" | "HostError" | "QueueFull" | "Busy"
                | "SessionExpired" => RetryAdvice::ExplicitPolicy,
                _ => RetryAdvice::None,
            },
        };
        self
    }
    pub fn with_queue_operation(mut self, id: u64) -> Self {
        self.queue_operation_id = Some(id);
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
