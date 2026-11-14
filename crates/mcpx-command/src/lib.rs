use mcpx_core::{BoundArgs, Error, Result, ToolBinding};
use serde_json::{Map, Value};
mod client;
mod completion;
mod spec;
pub use completion::{
    CandidateKind, Completion, CompletionCandidate, MAX_COMPLETION_BYTES,
    MAX_COMPLETION_CANDIDATES, MAX_COMPLETION_INPUT_BYTES, complete_argv, complete_line,
};
pub use spec::{CommandSpec, ParameterSpec};
mod runtime;
pub use client::{ClientCommandExt, Execution};
pub use runtime::{CommandAdmission, CommandRuntimeExt, HostRuntime};

pub enum PreparedCommand {
    Help(String),
    Call(BoundArgs),
}

pub fn tokenize(input: &str) -> Result<Vec<String>> {
    Ok(lex(input, true)?
        .into_iter()
        .map(|word| word.value)
        .collect())
}

struct Lexeme {
    value: String,
    start: usize,
    end: usize,
}
fn lex(input: &str, strict: bool) -> Result<Vec<Lexeme>> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut token = String::new();
    let mut quote = None;
    let mut started = false;
    let mut chars = input.char_indices().peekable();
    while let Some((offset, ch)) = chars.next() {
        if !started && !ch.is_whitespace() {
            start = offset;
        }
        match (quote, ch) {
            (Some('\''), '\'') | (Some('"'), '"') => quote = None,
            (Some('\''), _) => token.push(ch),
            (Some('"'), '\\') => {
                if chars.peek().is_some_and(|(_, ch)| matches!(ch, '"' | '\\')) {
                    token.push(chars.next().unwrap().1);
                } else {
                    token.push(ch);
                }
            }
            (Some('"'), _) => token.push(ch),
            (None, '\'' | '"') => {
                quote = Some(ch);
                started = true;
            }
            (None, '\\') => {
                match chars.next() {
                    Some((_, ch)) => token.push(ch),
                    None if strict => {
                        return Err(Error::local(
                            "TokenizeError",
                            format!("trailing escape at byte {offset}"),
                        ));
                    }
                    None => {}
                }
                started = true;
            }
            (None, ch) if ch.is_whitespace() => {
                if started {
                    args.push(Lexeme {
                        value: std::mem::take(&mut token),
                        start,
                        end: offset,
                    });
                    started = false;
                }
            }
            (None, _) => {
                token.push(ch);
                started = true;
            }
            _ => unreachable!(),
        }
    }
    if strict && quote.is_some() {
        return Err(Error::local("TokenizeError", "unclosed quote"));
    }
    if started {
        args.push(Lexeme {
            value: token,
            start,
            end: input.len(),
        });
    }
    Ok(args)
}

fn safe_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('-')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
}

fn clean(text: &str) -> String {
    text.chars()
        .flat_map(|c| {
            if c.is_control() {
                c.escape_default().collect::<Vec<_>>()
            } else {
                vec![c]
            }
        })
        .collect()
}

fn projected_type(schema: &Value) -> Option<&str> {
    if ["$ref", "oneOf", "anyOf", "allOf", "if"]
        .iter()
        .any(|key| schema.get(key).is_some())
    {
        return None;
    }
    schema.get("type").and_then(Value::as_str).filter(|kind| {
        matches!(
            *kind,
            "string" | "integer" | "number" | "boolean" | "object" | "array"
        )
    })
}

pub fn prepare(binding: &ToolBinding, argv: &[String]) -> Result<PreparedCommand> {
    let spec = CommandSpec::from_binding(binding);
    let command = spec.command();
    let matches = match command.try_get_matches_from(argv) {
        Ok(matches) => matches,
        Err(error) if error.kind() == clap::error::ErrorKind::DisplayHelp => {
            return Ok(PreparedCommand::Help(error.to_string()));
        }
        Err(_) => {
            return Err(Error::local(
                "CommandParseError",
                "invalid arguments; use --help for supported options",
            ));
        }
    };
    let mut fields = Map::new();
    for parameter in &spec.parameters {
        let name = &parameter.name;
        let kind = parameter.value_kind.as_str();
        if let Some(text) = matches.get_one::<String>(name) {
            let value = match kind {
                "string" => Value::String(text.clone()),
                "integer" => {
                    if let Ok(value) = text.parse::<i64>() {
                        Value::from(value)
                    } else if let Ok(value) = text.parse::<u64>() {
                        Value::from(value)
                    } else {
                        return Err(Error::local(
                            "InvalidArgumentType",
                            format!("{name} requires an integer"),
                        ));
                    }
                }
                "number" => match mcpx_core::parse_json(text) {
                    Ok(value) if value.is_number() => value,
                    Err(error) if error.code == "NumberOutOfRange" => return Err(error),
                    _ => {
                        return Err(Error::local(
                            "InvalidArgumentType",
                            format!("{name} requires a finite JSON number"),
                        ));
                    }
                },
                "boolean" => match text.as_str() {
                    "true" => Value::Bool(true),
                    "false" => Value::Bool(false),
                    _ => {
                        return Err(Error::local(
                            "InvalidArgumentType",
                            format!("{name} requires true or false"),
                        ));
                    }
                },
                _ => mcpx_core::parse_json(text)?,
            };
            fields.insert(name.clone(), value);
        }
    }
    let value = if let Some(text) = matches.get_one::<String>("args") {
        if !fields.is_empty() {
            return Err(Error::local(
                "ArgumentConflict",
                "--args cannot be mixed with tool flags",
            ));
        }
        mcpx_core::parse_json(text)?
    } else {
        Value::Object(fields)
    };
    Ok(PreparedCommand::Call(binding.validate_json(value)?))
}

pub fn prepare_string(binding: &ToolBinding, input: &str) -> Result<PreparedCommand> {
    prepare(binding, &tokenize(input)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tokenizer_preserves_empty_values_and_does_not_expand_shell() {
        assert_eq!(
            tokenize(r#"a '' "" 'x y' "x\"z" $HOME $(date)"#).unwrap(),
            vec!["a", "", "", "x y", "x\"z", "$HOME", "$(date)"]
        );
        assert!(tokenize("'open").is_err());
        assert!(tokenize("trailing\\").is_err());
    }
}
