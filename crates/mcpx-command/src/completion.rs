use crate::{CommandSpec, clean, lex};
use mcpx_core::{Error, Result, ToolDescriptor};
use serde::Serialize;
use std::collections::BTreeSet;

pub const MAX_COMPLETION_INPUT_BYTES: usize = 64 * 1024;
pub const MAX_COMPLETION_CANDIDATES: usize = 128;
pub const MAX_COMPLETION_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateKind {
    Tool,
    Option,
    Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct CompletionCandidate {
    pub value: String,
    /// Replaces the full token, with quoting for a command line or literal text for argv.
    pub replacement: String,
    pub description: String,
    pub kind: CandidateKind,
}

#[derive(Clone, Debug, Serialize)]
pub struct Completion {
    pub token: String,
    /// UTF-8 byte range in the original line, including quotes and any suffix after the cursor.
    pub start: usize,
    pub end: usize,
    pub candidates: Vec<CompletionCandidate>,
    pub truncated: bool,
}

struct Builder {
    result: Completion,
    bytes: usize,
    line: bool,
}
impl Builder {
    fn new(token: String, start: usize, end: usize, line: bool) -> Result<Self> {
        let result = Completion {
            token,
            start,
            end,
            candidates: Vec::new(),
            truncated: false,
        };
        let bytes = serde_json::to_vec(&result).expect("completion JSON").len();
        if bytes > MAX_COMPLETION_BYTES {
            return Err(limit());
        }
        Ok(Self {
            result,
            bytes,
            line,
        })
    }
    fn push(&mut self, value: &str, description: &str, kind: CandidateKind) {
        let candidate = CompletionCandidate {
            value: value.into(),
            replacement: if self.line {
                quote_token(value)
            } else {
                value.into()
            },
            description: short(&clean(&short(description, 1024)), 1024),
            kind,
        };
        let bytes = serde_json::to_vec(&candidate)
            .expect("candidate JSON")
            .len()
            + 1;
        if self.result.candidates.len() == MAX_COMPLETION_CANDIDATES
            || self.bytes + bytes > MAX_COMPLETION_BYTES
        {
            self.result.truncated = true;
        } else {
            self.bytes += bytes;
            self.result.candidates.push(candidate);
        }
    }
}
fn limit() -> Error {
    Error::local(
        "LimitExceeded",
        "completion input or output exceeds its budget",
    )
}
fn short(text: &str, max: usize) -> String {
    let mut end = max.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].into()
}
fn quote_token(value: &str) -> String {
    if !value.is_empty()
        && !value
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '\'' | '"' | '\\'))
    {
        return value.into();
    }
    if !value.contains('\'') {
        return format!("'{value}'");
    }
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Completes a line beginning with a tool name. This never binds or calls a tool.
/// Unclosed quotes and a trailing escape are accepted while editing.
pub fn complete_line<'a>(
    tools: impl IntoIterator<Item = &'a ToolDescriptor>,
    line: &str,
    cursor: usize,
) -> Result<Completion> {
    if line.len() > MAX_COMPLETION_INPUT_BYTES {
        return Err(limit());
    }
    if cursor > line.len() || !line.is_char_boundary(cursor) {
        return Err(Error::local(
            "InvalidCursor",
            "completion cursor must be a UTF-8 byte boundary",
        ));
    }
    let words = lex(line, false)?;
    if words.len() > 4096 {
        return Err(limit());
    }
    let current = words
        .iter()
        .position(|word| word.start <= cursor && cursor <= word.end);
    let index =
        current.unwrap_or_else(|| words.iter().take_while(|word| word.end < cursor).count());
    let (start, end, prefix) = if let Some(index) = current {
        let word = &words[index];
        let prefix = lex(&line[word.start..cursor], false)?
            .into_iter()
            .next()
            .map(|w| w.value)
            .unwrap_or_default();
        (word.start, word.end, prefix)
    } else {
        (cursor, cursor, String::new())
    };
    let mut builder = Builder::new(prefix.clone(), start, end, true)?;
    if index == 0 {
        for tool in tools {
            if tool.name.starts_with(&prefix) {
                builder.push(
                    &tool.name,
                    tool.description.as_deref().unwrap_or(""),
                    CandidateKind::Tool,
                );
            }
        }
    } else {
        let name = &words[0].value;
        let tool = tools
            .into_iter()
            .find(|tool| &tool.name == name)
            .ok_or_else(|| {
                Error::local(
                    "ToolNotFound",
                    "completion tool is not in the current catalog",
                )
            })?;
        let args: Vec<_> = words[1..index]
            .iter()
            .map(|word| word.value.clone())
            .collect();
        CommandSpec::from_descriptor(tool).fill(&args, &prefix, &mut builder);
    }
    Ok(builder.result)
}

/// Complete logical argv words without reparsing them as command-line syntax.
/// `completed` starts with the tool name; `current` is the word being edited.
pub fn complete_argv<'a>(
    tools: impl IntoIterator<Item = &'a ToolDescriptor>,
    completed: &[String],
    current: &str,
) -> Result<Completion> {
    if completed.len() > 4096
        || completed.iter().fold(current.len(), |n, word| {
            n.saturating_add(word.len()).saturating_add(1)
        }) > MAX_COMPLETION_INPUT_BYTES
    {
        return Err(limit());
    }
    let Some((name, arguments)) = completed.split_first() else {
        let mut builder = Builder::new(current.into(), 0, current.len(), false)?;
        for tool in tools {
            if tool.name.starts_with(current) {
                builder.push(
                    &tool.name,
                    tool.description.as_deref().unwrap_or(""),
                    CandidateKind::Tool,
                );
            }
        }
        return Ok(builder.result);
    };
    let tool = tools
        .into_iter()
        .find(|tool| &tool.name == name)
        .ok_or_else(|| {
            Error::local(
                "ToolNotFound",
                "completion tool is not in the current catalog",
            )
        })?;
    CommandSpec::from_descriptor(tool).complete_argv(arguments, current)
}

impl CommandSpec {
    /// `completed` excludes the tool name; `current` is one logical, partially typed argv value.
    pub fn complete_argv(&self, completed: &[String], current: &str) -> Result<Completion> {
        if completed.len() > 4096
            || completed.iter().fold(current.len(), |n, s| {
                n.saturating_add(s.len()).saturating_add(1)
            }) > MAX_COMPLETION_INPUT_BYTES
        {
            return Err(limit());
        }
        let mut builder = Builder::new(current.into(), 0, current.len(), false)?;
        self.fill(completed, current, &mut builder);
        Ok(builder.result)
    }
    fn fill(&self, completed: &[String], current: &str, builder: &mut Builder) {
        let mut used = BTreeSet::new();
        let mut pending = None;
        let mut raw = false;
        let mut business = false;
        for word in completed {
            if pending.take().is_some() {
                continue;
            }
            let (flag, inline) = word
                .split_once('=')
                .map_or((word.as_str(), None), |(a, b)| (a, Some(b)));
            if flag == "--help" || flag == "-h" || flag == "--" {
                return;
            }
            if flag == "--args" {
                if raw || business {
                    return;
                }
                raw = true;
            } else if self.parameters.iter().any(|p| p.flag == flag) {
                if raw {
                    return;
                }
                business = true;
            } else {
                return;
            }
            if !used.insert(flag.to_owned()) {
                return;
            }
            if inline.is_none() {
                pending = Some(flag.to_owned());
            }
        }
        if let Some(flag) = pending {
            self.values(&flag, current, None, builder);
            return;
        }
        if let Some((flag, prefix)) = current.split_once('=') {
            if !raw && !used.contains(flag) {
                self.values(flag, prefix, Some(flag), builder);
            }
            return;
        }
        for flag in ["--args", "--help"] {
            if flag == "--args" && (raw || business) {
                continue;
            }
            if flag.starts_with(current) && !used.contains(flag) {
                builder.push(
                    flag,
                    if flag == "--args" {
                        "Complete JSON argument object"
                    } else {
                        "Show tool help"
                    },
                    CandidateKind::Option,
                );
            }
        }
        if !raw {
            for parameter in &self.parameters {
                if !used.contains(&parameter.flag) && parameter.flag.starts_with(current) {
                    builder.push(
                        &parameter.flag,
                        &parameter.description,
                        CandidateKind::Option,
                    );
                }
            }
        }
    }
    fn values(&self, flag: &str, prefix: &str, inline: Option<&str>, builder: &mut Builder) {
        let Some(parameter) = self.parameters.iter().find(|p| p.flag == flag) else {
            return;
        };
        for value in &parameter.values {
            if value.starts_with(prefix) {
                let candidate =
                    inline.map_or_else(|| value.clone(), |flag| format!("{flag}={value}"));
                builder.push(&candidate, &parameter.description, CandidateKind::Value);
            }
        }
    }
}
