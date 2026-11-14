use crate::encode_header;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, thiserror::Error)]
#[error("invalid x-mcp-header annotation or unsupported header parameter value")]
pub struct HeaderError;

struct Mapping {
    path: Vec<String>,
    name: String,
    kind: String,
}

fn mappings(schema: &Value) -> Result<Vec<Mapping>, HeaderError> {
    fn walk(
        schema: &Value,
        path: &mut Vec<String>,
        reachable: bool,
        seen: &mut BTreeSet<String>,
        output: &mut Vec<Mapping>,
    ) -> Result<(), HeaderError> {
        let Some(object) = schema.as_object() else {
            return Ok(());
        };
        if let Some(name) = object.get("x-mcp-header") {
            let name = name.as_str().ok_or(HeaderError)?;
            let token = !name.is_empty()
                && name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c));
            let kind = object
                .get("type")
                .and_then(Value::as_str)
                .ok_or(HeaderError)?;
            if !reachable
                || path.is_empty()
                || !token
                || !matches!(kind, "integer" | "boolean" | "string")
                || !seen.insert(name.to_ascii_lowercase())
            {
                return Err(HeaderError);
            }
            output.push(Mapping {
                path: path.clone(),
                name: format!("mcp-param-{}", name.to_ascii_lowercase()),
                kind: kind.into(),
            });
        }
        for keyword in [
            "properties",
            "$defs",
            "patternProperties",
            "dependentSchemas",
        ] {
            if let Some(properties) = object.get(keyword).and_then(Value::as_object) {
                for (name, child) in properties {
                    path.push(name.clone());
                    walk(
                        child,
                        path,
                        reachable && keyword == "properties",
                        seen,
                        output,
                    )?;
                    path.pop();
                }
            }
        }
        for keyword in ["oneOf", "anyOf", "allOf", "prefixItems"] {
            if let Some(children) = object.get(keyword).and_then(Value::as_array) {
                for child in children {
                    walk(child, path, false, seen, output)?;
                }
            }
        }
        for keyword in [
            "if",
            "then",
            "else",
            "not",
            "items",
            "contains",
            "additionalProperties",
            "unevaluatedProperties",
            "unevaluatedItems",
            "propertyNames",
            "contentSchema",
        ] {
            if let Some(child) = object.get(keyword) {
                walk(child, path, false, seen, output)?;
            }
        }
        Ok(())
    }
    let mut result = vec![];
    walk(schema, &mut vec![], true, &mut BTreeSet::new(), &mut result)?;
    Ok(result)
}

pub fn validate_header_annotations(schema: &Value) -> Result<(), HeaderError> {
    mappings(schema).map(|_| ())
}

pub fn parameter_headers(
    schema: &Value,
    arguments: &Value,
) -> Result<BTreeMap<String, String>, HeaderError> {
    let mut headers = BTreeMap::new();
    for mapping in mappings(schema)? {
        let mut value = arguments;
        let mut missing = false;
        for key in &mapping.path {
            if let Some(next) = value.get(key) {
                value = next;
            } else {
                missing = true;
                break;
            }
        }
        if missing || value.is_null() {
            continue;
        }
        let text = match mapping.kind.as_str() {
            "string" => value.as_str().ok_or(HeaderError)?.to_owned(),
            "boolean" => value.as_bool().ok_or(HeaderError)?.to_string(),
            "integer" => {
                let value = value.as_f64().ok_or(HeaderError)?;
                if !value.is_finite()
                    || value.fract() != 0.0
                    || value.abs() > 9_007_199_254_740_991.0
                {
                    return Err(HeaderError);
                }
                format!("{value:.0}")
            }
            _ => return Err(HeaderError),
        };
        headers.insert(mapping.name, encode_header(&text));
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_headers_preserve_exact_paths_and_omit_missing_or_null() {
        let schema = json!({"type":"object","properties":{"scope":{"type":"object","properties":{"region":{"type":"string","x-mcp-header":"Region"}}},"limit":{"type":"integer","x-mcp-header":"Limit"},"flag":{"type":"boolean","x-mcp-header":"Flag"}}});
        let headers = parameter_headers(
            &schema,
            &json!({"scope":{"region":" padded "},"limit":42,"flag":true}),
        )
        .unwrap();
        assert_eq!(headers["mcp-param-region"], encode_header(" padded "));
        assert_eq!(headers["mcp-param-limit"], "42");
        assert_eq!(headers["mcp-param-flag"], "true");
        assert!(
            parameter_headers(&schema, &json!({"flag":null}))
                .unwrap()
                .is_empty()
        );
        assert!(parameter_headers(&schema, &json!({"limit":9007199254740992_u64})).is_err());
    }

    #[test]
    fn rejects_composition_annotations_duplicate_names_and_invalid_tokens() {
        for schema in [
            json!({"oneOf":[{"properties":{"x":{"type":"string","x-mcp-header":"X"}}}]}),
            json!({"properties":{"a":{"type":"string","x-mcp-header":"X"},"b":{"type":"string","x-mcp-header":"x"}}}),
            json!({"properties":{"a":{"type":"string","x-mcp-header":"X\r\nInjected"}}}),
            json!({"properties":{"a":{"type":"number","x-mcp-header":"X"}}}),
        ] {
            assert!(validate_header_annotations(&schema).is_err());
        }
        assert!(
            validate_header_annotations(&json!({"default":{"x-mcp-header":"not an annotation"}}))
                .is_ok()
        );
    }
}
