use mcpx_native::{Config, ServerConfig};

#[test]
fn configuration_rejects_unknown_fields_versions_and_reserved_aliases() {
    assert!(Config::parse("schema_version = 2").is_err());
    assert!(Config::parse("unexpected = true").is_err());
    assert!(Config::parse("[servers.call]\ntransport='http'\nurl='http://localhost'").is_err());
    assert!(Config::parse("[servers.complete]\ntransport='http'\nurl='http://localhost'").is_err());
    assert!(
        Config::parse("[servers.demo]\ntransport='http'\nurl='http://localhost'\nprotcol='auto'")
            .is_err()
    );
}

#[test]
fn http_and_stdio_servers_share_the_same_named_configuration() {
    let config = Config::parse(
        r#"
schema_version = 1
[servers.remote]
transport = "http"
url = "https://example.com/mcp"
credential = "env:MCP_TOKEN"
[servers.local]
transport = "stdio"
program = "python3"
args = ["server.py"]
protocol = "2025-11-25"
[servers.local.env]
MODE = "read-only"
"#,
    )
    .unwrap();
    assert_eq!(config.server("remote").unwrap().transport(), "http");
    let ServerConfig::Stdio { env, .. } = config.server("local").unwrap() else {
        panic!()
    };
    assert_eq!(env["MODE"], "read-only");
    assert!(config.server("missing").is_err());
}

#[test]
fn native_adapter_uses_portable_validation_and_redacts_debug_values() {
    let text = "[servers.local]\ntransport='stdio'\nprogram='not-started'\nargs=['secret-argument']\nenv={TOKEN='secret-environment'}";
    let config = Config::parse(text).unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains("secret-argument") && !debug.contains("secret-environment"));
    let portable = mcpx_config::Config::parse(text).unwrap();
    assert_eq!(
        config.server("local").unwrap().transport(),
        portable.server("local").unwrap().transport()
    );
    assert_eq!(
        Config::parse(&" ".repeat(mcpx_config::MAX_CONFIG_BYTES + 1))
            .unwrap_err()
            .code,
        "LimitExceeded"
    );
}
