use mcpx_command::{PreparedCommand, prepare, prepare_string};
use mcpx_core::{ToolBinding, ToolDescriptor};
use serde_json::json;

fn tool() -> ToolBinding {
    let descriptor: ToolDescriptor = serde_json::from_value(json!({"name":"search", "inputSchema":{
        "type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"},"enabled":{"type":"boolean"}},
        "required":["query"],"additionalProperties":false
    }})).unwrap();
    ToolBinding::new(descriptor).unwrap()
}

#[test]
fn strings_argv_builder_and_json_produce_the_same_arguments() {
    let tool = tool();
    let expected = tool
        .args()
        .set("query", "hello world")
        .unwrap()
        .set("limit", 10)
        .unwrap()
        .finish()
        .unwrap();
    let PreparedCommand::Call(from_string) =
        prepare_string(&tool, "--query 'hello world' --limit 10").unwrap()
    else {
        panic!()
    };
    let argv: Vec<String> = ["--query", "hello world", "--limit", "10"]
        .map(String::from)
        .into();
    let PreparedCommand::Call(from_argv) = prepare(&tool, &argv).unwrap() else {
        panic!()
    };
    let PreparedCommand::Call(from_json) =
        prepare_string(&tool, r#"--args '{"query":"hello world","limit":10}'"#).unwrap()
    else {
        panic!()
    };
    for args in [from_string, from_argv, from_json] {
        assert_eq!(args.value(), expected.value());
    }
}

#[test]
fn help_does_not_require_business_arguments_and_inputs_do_not_silently_coerce() {
    let tool = tool();
    assert!(matches!(
        prepare_string(&tool, "--help").unwrap(),
        PreparedCommand::Help(_)
    ));
    assert!(prepare_string(&tool, "--query x --enabled yes").is_err());
    assert!(prepare_string(&tool, "--query x --limit 18446744073709551616").is_err());
    assert!(prepare_string(&tool, r#"--query x --args '{"query":"y"}'"#).is_err());
    assert!(prepare_string(&tool, "--query x --query y").is_err());
}

#[test]
fn help_preserves_description_paragraphs_without_exposing_terminal_controls() {
    let descriptor: ToolDescriptor = serde_json::from_value(json!({
        "name": "search",
        "description": "First paragraph.\r\n\r\nSecond paragraph.\nEscape: \u{1b}[31m literal: \\n",
        "inputSchema": {"type":"object", "properties": {
            "query": {"type":"string", "description":"First line.\nSecond line.\u{7}"}
        }}
    }))
    .unwrap();
    let binding = ToolBinding::new(descriptor).unwrap();
    let PreparedCommand::Help(help) = prepare_string(&binding, "--help").unwrap() else {
        panic!()
    };
    assert!(
        help.contains("First paragraph.\n\nSecond paragraph."),
        "{help}"
    );
    assert!(!help.contains("First paragraph.\\r\\n"));
    assert!(!help.contains('\u{1b}'));
    assert!(!help.contains('\u{7}'));
    assert!(help.contains("\\u{1b}[31m"), "{help}");
    assert!(help.contains("literal: \\n"), "{help}");
    let spec = mcpx_command::CommandSpec::from_binding(&binding);
    assert_eq!(
        spec.parameters[0].description,
        "First line.\nSecond line.\\u{7}"
    );
}
