mod binding;
mod catalog_watch;
mod client;
mod continuation;
mod credential;
mod error;
mod event_queue;
mod ids;
mod machine;
mod notifications;
mod operation_events;
mod policy;
mod response;
mod response_resume;
mod runtime;
mod schema;
mod serde_args;
mod validator_cache;
pub use catalog_watch::{
    CatalogChange, CatalogChanged, CatalogRevision, CatalogRevisionSource, CatalogWatcher,
    MAX_CATALOG_WATCHERS,
};
pub use client::{CheckedCallError, Client, Driver, DriverContext, drive_host};
pub use continuation::Continuation;
pub use credential::{Credential, CredentialFuture, CredentialProvider, CredentialRequest};
pub use event_queue::{EventBatch, EventLimits, EventStats, MAX_EVENT_RECORD_BYTES};
pub use ids::MAX_HOST_ID;
pub use notifications::{
    DEFAULT_NOTIFICATION_RETRY_MS, NotificationPhase, NotificationRequest, NotificationResume,
    NotificationStatus,
};
pub use policy::CatalogStatus;
pub use response_resume::{ResponseResume, ResponseResumeRequest};
pub use runtime::HostRuntime;
pub use schema::{
    MAX_REGEX_AST_DEPTH, MAX_REGEX_BACKTRACKS, MAX_REGEX_COMPILED_BYTES, MAX_REGEX_DFA_BYTES,
    MAX_REGEX_REPEAT, MAX_REGEX_STATIC_WORK, MAX_REGEX_VM_INSTRUCTIONS, MAX_SCHEMA_BUNDLE_BYTES,
    MAX_SCHEMA_BYTES, MAX_SCHEMA_NODES, MAX_SCHEMA_PATTERN_BYTES, MAX_SCHEMA_PATTERN_TOTAL_BYTES,
    MAX_SCHEMA_PATTERNS, MAX_SCHEMA_RESOURCE_BYTES, MAX_SCHEMA_RESOURCES, SCHEMA_VOCABULARIES,
    SchemaRegistry, SchemaRegistryStats, SchemaResource,
};
pub use validator_cache::{
    DEFAULT_VALIDATOR_CACHE_CAPACITY, MAX_VALIDATOR_CACHE_CAPACITY, SCHEMA_POLICY_VERSION,
    ValidatorCacheStats,
};

pub use binding::{ArgsBuilder, BoundArgs, Catalog, ToolBinding, ToolSchema};
pub use error::{Error, ErrorContext, ExecutionCertainty, Result, RetryAdvice, ValidationIssue};
pub use machine::{Action, CallOutcome, Event, Machine, OutputValidation, ToolResponse};
pub use mcpx_protocol::{Dialect, ToolDescriptor};
pub use operation_events::{
    OperationEvent, OperationEventBatch, OperationEventKind, OperationEvents, QueueScope,
};

pub fn parse_json(text: &str) -> Result<serde_json::Value> {
    if text.len() > mcpx_protocol::MAX_BODY {
        return Err(Error::local(
            "LimitExceeded",
            "JSON input exceeds byte limit",
        ));
    }
    let value = serde_json::from_str(text)
        .map_err(|_| Error::local("InvalidJson", "invalid JSON input"))?;
    check_json(&value, 0)?;
    Ok(value)
}

fn check_json(value: &serde_json::Value, depth: usize) -> Result<()> {
    if depth > 64 {
        return Err(Error::local(
            "LimitExceeded",
            "JSON nesting exceeds 64 levels",
        ));
    }
    match value {
        serde_json::Value::Number(number) => {
            let text = number.as_str();
            let floating = text.contains(['.', 'e', 'E']);
            if (!floating && number.as_i64().is_none() && number.as_u64().is_none())
                || number.as_f64().is_none_or(|number| !number.is_finite())
            {
                return Err(Error::local(
                    "NumberOutOfRange",
                    "number exceeds supported numeric range",
                ));
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                check_json(value, depth + 1)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                check_json(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub use serde_args::MAX_ARGUMENT_BYTES;
use serde_args::serialize_value;
