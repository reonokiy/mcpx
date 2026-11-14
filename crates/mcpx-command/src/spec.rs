use crate::{clean, clean_help, projected_type, safe_name};
use clap::{Arg, ArgAction, Command};
use mcpx_core::{ToolBinding, ToolDescriptor};
use serde::Serialize;
use serde_json::Value;

/// The flag projection of a remote descriptor. Validation still uses the original Schema.
#[derive(Clone, Debug, Serialize)]
pub struct CommandSpec {
    pub tool: String,
    pub description: String,
    pub parameters: Vec<ParameterSpec>,
    pub json_only: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ParameterSpec {
    pub name: String,
    pub flag: String,
    pub description: String,
    pub value_kind: String,
    pub required: bool,
    /// Logical argv values, without command-line quoting. Integers remain decimal text.
    pub values: Vec<String>,
}

impl CommandSpec {
    pub fn from_binding(binding: &ToolBinding) -> Self {
        Self::from_descriptor(binding.descriptor())
    }

    pub fn from_descriptor(tool: &ToolDescriptor) -> Self {
        let mut spec = Self {
            tool: tool.name.clone(),
            description: clean_help(tool.description.as_deref().unwrap_or("")),
            parameters: Vec::new(),
            json_only: Vec::new(),
        };
        let required = tool.input_schema.get("required").and_then(Value::as_array);
        for (name, schema) in tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            let kind = projected_type(schema).filter(|_| {
                safe_name(name) && !matches!(name.as_str(), "args" | "args-file" | "help")
            });
            let Some(kind) = kind else {
                spec.json_only.push(name.clone());
                continue;
            };
            let mut values = Vec::new();
            if let Some(options) = schema.get("enum").and_then(Value::as_array) {
                for option in options {
                    let value = match (kind, option) {
                        ("string", Value::String(value)) => Some(value.clone()),
                        ("boolean", Value::Bool(_)) => Some(option.to_string()),
                        ("integer", Value::Number(n))
                            if n.as_i64().is_some() || n.as_u64().is_some() =>
                        {
                            Some(option.to_string())
                        }
                        ("number", Value::Number(_)) => Some(option.to_string()),
                        _ => None,
                    };
                    if let Some(value) = value {
                        values.push(value);
                    }
                }
                values.sort();
                values.dedup();
            } else if kind == "boolean" {
                values = vec!["false".into(), "true".into()];
            }
            spec.parameters.push(ParameterSpec {
                name: name.clone(),
                flag: format!("--{name}"),
                description: clean_help(
                    schema
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or(kind),
                ),
                value_kind: kind.into(),
                required: required
                    .is_some_and(|items| items.iter().any(|v| v.as_str() == Some(name))),
                values,
            });
        }
        spec.parameters.sort_by(|a, b| a.name.cmp(&b.name));
        spec.json_only.sort();
        spec
    }

    pub fn help(&self) -> String {
        self.command().render_help().to_string()
    }

    pub(crate) fn command(&self) -> Command {
        let mut command = Command::new(clean(&self.tool))
            .no_binary_name(true)
            .about(self.description.clone())
            .arg(
                Arg::new("args")
                    .long("args")
                    .action(ArgAction::Set)
                    .help("Complete JSON argument object"),
            );
        for parameter in &self.parameters {
            command = command.arg(
                Arg::new(parameter.name.clone())
                    .long(parameter.name.clone())
                    .action(ArgAction::Set)
                    .allow_hyphen_values(true)
                    .help(parameter.description.clone()),
            );
        }
        if !self.json_only.is_empty() {
            command = command.after_help(format!(
                "Use --args for: {}",
                self.json_only
                    .iter()
                    .map(|v| clean(v))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        command
    }
}
