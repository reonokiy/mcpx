use crate::{BoundArgs, Error, Result, ToolBinding, ToolDescriptor};
use serde::Serialize;
use serde_json::{Map, Value};
use std::{collections::BTreeMap, fmt, sync::Arc};

pub(crate) const MAX_CONTINUATIONS: usize = 32;
pub(crate) const MAX_CONTINUATION_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const CONTINUATION_TTL_MS: u64 = 300_000;
pub(crate) const MAX_ROUNDS: u8 = 16;

/// A single-use, remote-local handle. The server state is echoed only by `resume`.
#[derive(Clone, Serialize)]
pub struct Continuation {
    pub(crate) operation_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) queue_operation_id: Option<u64>,
    pub(crate) id: u64,
    pub(crate) expires_at_ms: u64,
    #[serde(serialize_with = "serialize_raw")]
    pub(crate) raw: Arc<Value>,
    #[serde(skip)]
    pub(crate) owner: Arc<()>,
}
fn serialize_raw<S: serde::Serializer>(
    raw: &Arc<Value>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    raw.as_ref().serialize(serializer)
}
impl Continuation {
    pub fn operation_id(&self) -> u64 {
        self.operation_id
    }
    pub fn queue_operation_id(&self) -> Option<u64> {
        self.queue_operation_id
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn raw(&self) -> &Value {
        &self.raw
    }
    pub fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }
}
impl fmt::Debug for Continuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Continuation")
            .field("id", &self.id)
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

pub(crate) struct CallState {
    pub binding: ToolBinding,
    pub args: BoundArgs,
    pub rounds: u8,
    pub deadline: Option<u64>,
}
pub(crate) struct SavedContinuation {
    pub call: CallState,
    pub handle: Continuation,
    pub requests: BTreeMap<String, InputRequest>,
    pub bytes: usize,
}
pub(crate) enum InputRequest {
    Form(ToolBinding),
    Url,
}

pub(crate) fn input_requests(
    result: &Value,
    form: bool,
    url: bool,
    catalog: &mut crate::Catalog,
) -> Result<BTreeMap<String, InputRequest>> {
    if result.get("requestState").is_some_and(|v| !v.is_string()) {
        return Err(Error::local(
            "ProtocolError",
            "requestState must be a string",
        ));
    }
    let requests = match result.get("inputRequests") {
        Some(Value::Object(requests)) => requests,
        Some(_) => {
            return Err(Error::local(
                "ProtocolError",
                "inputRequests must be an object",
            ));
        }
        None if result.get("requestState").is_some() => return Ok(BTreeMap::new()),
        None => {
            return Err(Error::local(
                "ProtocolError",
                "input_required has no inputs or state",
            ));
        }
    };
    if requests.len() > 16 {
        return Err(Error::local("LimitExceeded", "too many input requests"));
    }
    let mut compiled = BTreeMap::new();
    for (id, request) in requests {
        if id.is_empty() || id.len() > 1024 {
            return Err(Error::local(
                "ProtocolError",
                "invalid input request identifier",
            ));
        }
        let params = &request["params"];
        if request["method"] != "elicitation/create" {
            return Err(Error::local(
                "CapabilityUnsupported",
                "input request capability is not enabled",
            ));
        }
        if !params["message"].is_string() {
            return Err(Error::local(
                "ProtocolError",
                "elicitation message is missing",
            ));
        }
        let mode = match params.get("mode") {
            None => "form",
            Some(Value::String(mode)) => mode.as_str(),
            Some(_) => {
                return Err(Error::local(
                    "ProtocolError",
                    "elicitation mode must be a string",
                ));
            }
        };
        let input = match mode {
            "form" if form => {
                let binding = catalog.compile_local_binding(ToolDescriptor {
                    name: id.clone(),
                    description: None,
                    input_schema: params["requestedSchema"].clone(),
                    output_schema: None,
                    extra: BTreeMap::new(),
                })?;
                InputRequest::Form(binding)
            }
            "url" if url => {
                if !params["url"].is_string() || !params["elicitationId"].is_string() {
                    return Err(Error::local(
                        "ProtocolError",
                        "URL elicitation fields are missing",
                    ));
                }
                InputRequest::Url
            }
            _ => {
                return Err(Error::local(
                    "CapabilityUnsupported",
                    "elicitation mode is not enabled",
                ));
            }
        };
        compiled.insert(id.clone(), input);
    }
    Ok(compiled)
}

impl SavedContinuation {
    pub fn responses(&self, value: Value) -> Result<Map<String, Value>> {
        crate::check_json(&value, 0)?;
        if value.to_string().len() > 1024 * 1024 {
            return Err(Error::local(
                "LimitExceeded",
                "input responses exceed 1 MiB",
            ));
        }
        let responses = value
            .as_object()
            .ok_or_else(|| Error::local("ValidationFailed", "input responses must be an object"))?;
        if responses.len() != self.requests.len()
            || self.requests.keys().any(|id| !responses.contains_key(id))
        {
            return Err(Error::local(
                "ValidationFailed",
                "provide exactly the requested input response identifiers",
            ));
        }
        for (id, request) in &self.requests {
            let response = &responses[id];
            if !response.is_object()
                || response
                    .as_object()
                    .unwrap()
                    .keys()
                    .any(|key| !matches!(key.as_str(), "action" | "content" | "_meta"))
            {
                return Err(Error::local(
                    "ValidationFailed",
                    "invalid elicitation response fields",
                ));
            }
            match response["action"].as_str() {
                Some("accept") => match request {
                    InputRequest::Form(binding) => {
                        binding.validate_json(response["content"].clone())?;
                    }
                    InputRequest::Url if response.get("content").is_some() => {
                        return Err(Error::local(
                            "ValidationFailed",
                            "URL response must not include form content",
                        ));
                    }
                    InputRequest::Url => {}
                },
                Some("decline" | "cancel") if response.get("content").is_none() => {}
                _ => {
                    return Err(Error::local(
                        "ValidationFailed",
                        "invalid elicitation action or content",
                    ));
                }
            }
        }
        Ok(responses.clone())
    }
}
