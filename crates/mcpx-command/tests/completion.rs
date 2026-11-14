use mcpx_command::{
    CommandRuntimeExt, CommandSpec, HostRuntime, PreparedCommand, complete_line, prepare, tokenize,
};
use mcpx_core::{ToolBinding, ToolDescriptor};
use serde_json::json;

fn descriptor() -> ToolDescriptor {
    serde_json::from_value(json!({"name":"search","description":"Search tools","inputSchema":{
        "type":"object","properties":{
            "query":{"type":"string","description":"A query","enum":["hello world","has'quote\\and\"double","雪 花","$HOME","$(date)",""]},
            "enabled":{"type":"boolean"},
            "limit":{"type":"integer","enum":[-1,18446744073709551615_u64]},
            "filter":{"type":"object"},
            "choice":{"type":["string","null"]},
            "args-file":{"type":"string"},"help":{"type":"string"},"odd name":{"type":"string"}
        },"required":["query"],"additionalProperties":false
    }})).unwrap()
}
fn complete(line: &str) -> mcpx_command::Completion {
    complete_line([&descriptor()], line, line.len()).unwrap()
}
fn values(c: &mcpx_command::Completion) -> Vec<&str> {
    c.candidates.iter().map(|v| v.value.as_str()).collect()
}

#[test]
fn catalog_argv_completion_keeps_logical_values_without_retokenizing_or_binding() {
    let tool = descriptor();
    let tools = mcpx_command::complete_argv([&tool], &[], "sea").unwrap();
    assert_eq!(values(&tools), ["search"]);
    let values =
        mcpx_command::complete_argv([&tool], &["search".into(), "--query".into()], "hello")
            .unwrap();
    assert_eq!(values.candidates[0].replacement, "hello world");
    let exact =
        mcpx_command::complete_argv([&tool], &["search".into(), "--limit".into()], "184467")
            .unwrap();
    assert_eq!(exact.candidates[0].value, "18446744073709551615");
    let used = mcpx_command::complete_argv(
        [&tool],
        &["search".into(), "--query".into(), "hello world".into()],
        "--",
    )
    .unwrap();
    assert!(
        !used
            .candidates
            .iter()
            .any(|candidate| candidate.value == "--query")
    );
    assert_eq!(
        mcpx_command::complete_argv([&tool], &["missing".into()], "")
            .unwrap_err()
            .code,
        "ToolNotFound"
    );
}

#[test]
fn catalog_argv_completion_rejects_size_and_count_before_processing_descriptors() {
    let tool = descriptor();
    assert_eq!(
        mcpx_command::complete_argv([&tool], &[], &"x".repeat(65537))
            .unwrap_err()
            .code,
        "LimitExceeded"
    );
    assert_eq!(
        mcpx_command::complete_argv([&tool], &vec![String::new(); 4097], "")
            .unwrap_err()
            .code,
        "LimitExceeded"
    );
}

#[test]
fn help_completion_and_parser_share_the_same_safe_projection() {
    let binding = ToolBinding::new(descriptor()).unwrap();
    let spec = CommandSpec::from_binding(&binding);
    let help = spec.help();
    for parameter in &spec.parameters {
        assert!(help.contains(&parameter.flag));
    }
    assert_eq!(spec.json_only, ["args-file", "choice", "help", "odd name"]);
    assert!(help.contains("Use --args for:"));
    assert!(
        spec.parameters
            .iter()
            .find(|p| p.name == "query")
            .unwrap()
            .required
    );
    let c = spec
        .complete_argv(&["--query".into(), "hello world".into()], "--")
        .unwrap();
    assert!(!values(&c).contains(&"--query") && !values(&c).contains(&"--args"));
    assert!(values(&c).contains(&"--enabled"));
    let PreparedCommand::Call(args) = prepare(
        &binding,
        &[
            "--args".into(),
            r#"{"query":"hello world","args-file":"literal"}"#.into(),
        ],
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(args.value()["args-file"], "literal");
}

#[test]
fn tool_and_option_prefixes_provide_current_token_and_descriptions() {
    let tool = complete("sea");
    assert_eq!(values(&tool), ["search"]);
    assert_eq!((tool.token.as_str(), tool.start, tool.end), ("sea", 0, 3));
    assert_eq!(tool.candidates[0].description, "Search tools");
    let c = complete("search --qu");
    assert_eq!(values(&c), ["--query"]);
    assert_eq!(c.candidates[0].description, "A query");
    assert!(complete("search --unknown x ").candidates.is_empty());
}

#[test]
fn middle_of_a_quoted_token_replaces_its_suffix_and_round_trips_to_valid_args() {
    let line = "search --query \"helZZ\" --enabled true";
    let c = complete_line([&descriptor()], line, "search --query \"hel".len()).unwrap();
    assert_eq!(values(&c), ["hello world"]);
    assert_eq!(&line[c.start..c.end], "\"helZZ\"");
    let edited = format!(
        "{}{}{}",
        &line[..c.start],
        c.candidates[0].replacement,
        &line[c.end..]
    );
    let words = tokenize(&edited).unwrap();
    let PreparedCommand::Call(args) =
        prepare(&ToolBinding::new(descriptor()).unwrap(), &words[1..]).unwrap()
    else {
        panic!()
    };
    assert_eq!(args.value(), &json!({"query":"hello world","enabled":true}));
}

#[test]
fn incomplete_quotes_empty_values_and_shell_literals_round_trip_without_expansion() {
    for line in ["search --query '", "search --query \"", "search --query \\"] {
        let c = complete(line);
        assert!(c.candidates.len() >= 6);
        for candidate in c.candidates {
            let edited = format!(
                "{}{}{}",
                &line[..c.start],
                candidate.replacement,
                &line[c.end..]
            );
            assert_eq!(
                tokenize(&edited).unwrap(),
                ["search", "--query", &candidate.value]
            );
        }
    }
}

#[test]
fn boolean_and_inline_uint64_enums_keep_exact_values() {
    assert_eq!(values(&complete("search --enabled t")), ["true"]);
    assert_eq!(
        values(&complete("search --enabled=")),
        ["--enabled=false", "--enabled=true"]
    );
    assert_eq!(
        values(&complete("search --limit=18")),
        ["--limit=18446744073709551615"]
    );
    assert_eq!(values(&complete("search --limit -")), ["-1"]);
    let spec = CommandSpec::from_descriptor(&descriptor());
    let c = spec.complete_argv(&["--query".into()], "hello").unwrap();
    assert_eq!(c.candidates[0].replacement, "hello world"); // Already tokenized, no quotes.
}

#[test]
fn number_flags_preserve_exact_integer_candidates_and_reject_overflow_before_validation() {
    let tool: ToolDescriptor = serde_json::from_value(json!({"name":"price","inputSchema":{
        "type":"object","properties":{"amount":{"type":"number","enum":[18446744073709551615_u64,0.25]}}
    }})).unwrap();
    let binding = ToolBinding::new(tool.clone()).unwrap();
    let c = complete_line([&tool], "price --amount 18", 17).unwrap();
    assert_eq!(values(&c), ["18446744073709551615"]);
    let words = tokenize(&format!("price --amount {}", c.candidates[0].replacement)).unwrap();
    let PreparedCommand::Call(args) = prepare(&binding, &words[1..]).unwrap() else {
        panic!()
    };
    assert_eq!(args.value()["amount"].as_u64(), Some(u64::MAX));
    assert!(
        serde_json::to_string(args.value())
            .unwrap()
            .contains("18446744073709551615")
    );
    for text in ["18446744073709551616", "1e309"] {
        assert_eq!(
            prepare(&binding, &["--amount".into(), text.into()])
                .err()
                .unwrap()
                .code,
            "NumberOutOfRange"
        );
    }
    for text in ["NaN", "Infinity", "true", "\"2\""] {
        assert_eq!(
            prepare(&binding, &["--amount".into(), text.into()])
                .err()
                .unwrap()
                .code,
            "InvalidArgumentType"
        );
    }
}

#[test]
fn raw_json_mode_does_not_offer_conflicting_business_flags_or_read_files() {
    assert!(complete("search --args ").candidates.is_empty());
    assert_eq!(values(&complete("search --args '{}' --")), ["--help"]);
    assert!(complete("search --filter ").candidates.is_empty());
    assert!(complete("search --args-file /tmp/ ").candidates.is_empty());
    assert_eq!(
        complete_line([&descriptor()], "missing --", 10)
            .unwrap_err()
            .code,
        "ToolNotFound"
    );
}

#[test]
fn cursors_are_utf8_byte_offsets_and_input_and_output_are_bounded() {
    let line = "search --query 雪";
    assert_eq!(
        complete_line([&descriptor()], line, line.len() - 1)
            .unwrap_err()
            .code,
        "InvalidCursor"
    );
    assert_eq!(values(&complete(line)), ["雪 花"]);
    assert_eq!(
        complete_line([&descriptor()], &"x".repeat(65537), 0)
            .unwrap_err()
            .code,
        "LimitExceeded"
    );
    let tools: Vec<_> = (0..300)
        .map(|i| {
            let mut t = descriptor();
            t.name = format!("tool-{i:03}");
            t
        })
        .collect();
    let c = complete_line(&tools, "", 0).unwrap();
    assert_eq!(c.candidates.len(), 128);
    assert!(c.truncated);
    assert!(serde_json::to_vec(&c).unwrap().len() <= 65536);
    let mut large = descriptor();
    large.input_schema["properties"]["query"]["enum"] = json!(["a".repeat(70000), "b"]);
    let c = complete_line([&large], "search --query ", 15).unwrap();
    assert!(c.truncated);
    assert_eq!(values(&c), ["b"]);
    assert!(serde_json::to_vec(&c).unwrap().len() <= 65536);
}

#[test]
fn cold_runtime_requires_explicit_discovery_and_emits_no_actions() {
    let mut runtime = HostRuntime::new("https://mcp.example/endpoint".into(), false).unwrap();
    assert_eq!(
        runtime.complete_line("", 0).unwrap_err().code,
        "CatalogStale"
    );
    assert_eq!(
        runtime.command_spec("search").unwrap_err().code,
        "CatalogStale"
    );
    assert!(runtime.take_actions().is_empty());
}
