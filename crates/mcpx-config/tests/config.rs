use mcpx_config::{Config, MAX_CONFIG_BYTES, ServerConfig};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "mcpx-config-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn common_model_rejects_unknown_fields_versions_and_reserved_aliases() {
    for text in [
        "schema_version=2",
        "unexpected=true",
        "[servers.call]\ntransport='http'\nurl='http://localhost'",
        "[servers.completions]\ntransport='http'\nurl='http://localhost'",
        "[servers.__complete]\ntransport='http'\nurl='http://localhost'",
        "[servers.x]\ntransport='http'\nurl='http://localhost'\nprotcol='auto'",
        "[servers.x]\ntransport='http'\nurl='http://localhost'\nprotocol='future'",
        "[servers.x]\ntransport='stdio'\nprogram='none'\ncredential='env:SOURCE'",
    ] {
        assert!(
            Config::parse(text).is_err(),
            "accepted invalid configuration"
        );
    }
    let config = Config::parse(
        "[servers.x]\ntransport='http'\nurl='https://example.com/mcp'\ncredential='env:SOURCE'",
    )
    .unwrap();
    let ServerConfig::Http {
        protocol,
        credential,
        ..
    } = config.server("x").unwrap()
    else {
        panic!()
    };
    assert!(matches!((*protocol).into(), mcpx_core::Dialect::Auto));
    assert_eq!(credential.as_deref(), Some("env:SOURCE"));
    assert_eq!(
        config.server("missing").err().unwrap().code,
        "RemoteNotFound"
    );
}
#[test]
fn configuration_debug_does_not_disclose_endpoint_query_args_env_or_references() {
    let config=Config::parse("[servers.x]\ntransport='http'\nurl='https://example.com/mcp?token=endpoint-secret'\ncredential='reference-secret'\n[servers.local]\ntransport='stdio'\nprogram='program-secret'\nargs=['argument-secret']\nenv={ACCESS_TOKEN='environment-secret'}").unwrap();
    let debug = format!("{config:?}");
    assert!(debug.contains("http") && debug.contains("stdio"));
    for secret in [
        "endpoint-secret",
        "reference-secret",
        "program-secret",
        "argument-secret",
        "environment-secret",
    ] {
        assert!(!debug.contains(secret));
    }
}
#[test]
fn text_and_file_sources_share_a_bound_and_utf8_errors_do_not_echo_input() {
    let directory = Directory::new();
    let path = directory.0.join("config.toml");
    let text = format!("#{}", "x".repeat(MAX_CONFIG_BYTES));
    assert_eq!(Config::parse(&text).unwrap_err().code, "LimitExceeded");
    std::fs::write(&path, text).unwrap();
    assert_eq!(Config::load(&path).unwrap_err().code, "LimitExceeded");
    std::fs::write(&path, b"secret-config\xff").unwrap();
    let error = Config::load(&path).unwrap_err();
    assert_eq!(error.code, "ConfigurationError");
    assert!(!error.message.contains("secret-config"));
}
#[test]
fn relative_process_working_directories_follow_the_config_file_parent() {
    let directory = Directory::new();
    let nested = directory.0.join("nested");
    std::fs::create_dir(&nested).unwrap();
    let path = nested.join("servers.toml");
    std::fs::write(
        &path,
        "[servers.local]\ntransport='stdio'\nprogram='not-launched'\nworking_directory='child'",
    )
    .unwrap();
    let config = Config::load(&path).unwrap();
    let ServerConfig::Stdio {
        working_directory, ..
    } = config.server("local").unwrap()
    else {
        panic!()
    };
    assert_eq!(
        working_directory.as_ref().unwrap(),
        &std::fs::canonicalize(nested).unwrap().join("child")
    );
}

#[test]
fn unresolved_file_loading_preserves_paths_and_the_existing_input_limits() {
    let directory = Directory::new();
    let path = directory.0.join("config.toml");
    std::fs::write(
        &path,
        "[servers.local]\ntransport='stdio'\nprogram='not-launched'\nworking_directory='child'",
    )
    .unwrap();
    let config = Config::load_unresolved(&path).unwrap();
    let ServerConfig::Stdio {
        working_directory, ..
    } = config.server("local").unwrap()
    else {
        panic!()
    };
    assert_eq!(
        working_directory.as_deref(),
        Some(std::path::Path::new("child"))
    );
    std::fs::write(&path, b"private-config\xff").unwrap();
    let error = Config::load_unresolved(&path).unwrap_err();
    assert_eq!(error.code, "ConfigurationError");
    assert!(!error.message.contains("private-config"));
    std::fs::write(&path, "#".repeat(MAX_CONFIG_BYTES + 1)).unwrap();
    assert_eq!(
        Config::load_unresolved(&path).unwrap_err().code,
        "LimitExceeded"
    );
}

#[cfg(unix)]
#[test]
fn resolved_file_loading_keeps_the_physical_parent_of_a_symlinked_config() {
    let directory = Directory::new();
    let physical = directory.0.join("physical");
    let logical = directory.0.join("logical");
    std::fs::create_dir(&physical).unwrap();
    std::fs::create_dir(&logical).unwrap();
    let target = physical.join("servers.toml");
    let link = logical.join("alias.toml");
    std::fs::write(
        &target,
        "[servers.local]\ntransport='stdio'\nprogram='not-launched'\nworking_directory='child'",
    )
    .unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let config = Config::load(&link).unwrap();
    let ServerConfig::Stdio {
        working_directory, ..
    } = config.server("local").unwrap()
    else {
        panic!()
    };
    assert_eq!(
        working_directory.as_deref(),
        Some(
            std::fs::canonicalize(physical)
                .unwrap()
                .join("child")
                .as_path()
        )
    );
}
