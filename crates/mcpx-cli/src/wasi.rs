use crate::shared::{args_file, print_json, safe};
use mcpx::wasi::{WasiDriver, run_blocking};
use mcpx::{ClientCommandExt, Credential, Dialect, Error, HostRuntime, command::Execution};
use serde_json::json;

pub(crate) fn main() -> std::process::ExitCode {
    use std::io::Write;
    let code = crate::shared::status(run_blocking(run()));
    // wasi:cli/run's result and the older exit import carry only success/error.
    // The explicit host extension preserves mcpx's detailed CLI status codes.
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    wasip2::cli::exit::exit_with_code(code);
    std::process::ExitCode::from(code)
}
async fn run() -> mcpx::Result<u8> {
    let Some(mut invocation) = crate::routing::parse(true)? else {
        return Ok(0);
    };
    if matches!(
        invocation.route,
        crate::routing::Route::Auth { .. }
            | crate::routing::Route::Add { .. }
            | crate::routing::Route::UseProfile { .. }
    ) {
        return Err(Error::local(
            "CapabilityUnavailable",
            "OAuth account management and configuration writes require the native CLI; WASI hosts can inject credentials",
        ));
    }
    let output = invocation.output.as_str();
    let configuration = if invocation.url.is_none() {
        // Stdio routes are listed but rejected before dispatch on this host.
        // Do not require canonicalization for their unused working directories.
        Some({
            let mut config = mcpx::config::Config::load_unresolved(&crate::shared::file_path(
                &invocation.config,
            )?)?;
            if matches!(invocation.route, crate::routing::Route::Profiles) {
                crate::shared::print_profiles(&config, invocation.profile.as_deref(), output);
                return Ok(0);
            }
            let profile = invocation
                .profile
                .get_or_insert_with(|| config.default_profile().to_owned());
            config.select_profile(profile)?;
            config
        })
    } else {
        None
    };
    if matches!(invocation.route, crate::routing::Route::Servers) {
        crate::shared::print_servers(
            configuration
                .as_ref()
                .unwrap()
                .servers
                .iter()
                .map(|(name, server)| (name.as_str(), server.transport())),
            output,
        );
        return Ok(0);
    }
    let (endpoint, configured_protocol, reference) = if let Some(url) = &invocation.url {
        (url.as_str(), Dialect::Auto, None)
    } else {
        match configuration
            .as_ref()
            .unwrap()
            .server(invocation.alias().unwrap())?
        {
            mcpx::config::ServerConfig::Http {
                url,
                protocol,
                credential,
            } => (url.as_str(), (*protocol).into(), credential.as_deref()),
            mcpx::config::ServerConfig::Stdio { .. } => {
                return Err(Error::local(
                    "CapabilityUnavailable",
                    "WASI CLI cannot launch child processes; select an HTTP server",
                ));
            }
        }
    };
    let tool = invocation.tool();
    let listing = matches!(
        invocation.route,
        crate::routing::Route::Tools { .. } | crate::routing::Route::ServerHelp { .. }
    );
    let completion = match &invocation.route {
        crate::routing::Route::Complete { line, cursor, .. } => Some((line, *cursor)),
        _ => None,
    };
    let arguments = args_file(invocation.arguments().to_vec())?;
    let registry = invocation
        .schema_registry
        .as_deref()
        .map(crate::shared::schema_registry)
        .transpose()?;
    if reference.is_some_and(|reference| reference.is_empty() || reference.len() > 1024) {
        return Err(Error::local(
            "ConfigurationError",
            "credential reference must contain 1..1024 bytes",
        ));
    }
    let credential = if let Some(variable) = &invocation.token_env {
        read_credential(variable)?
    } else if let Some(reference) = reference {
        let variable = reference
            .strip_prefix("env:")
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                Error::local(
                    "CredentialUnavailable",
                    "named credential reference requires a credential provider",
                )
            })?;
        read_credential(variable)?
    } else {
        Credential::anonymous()
    };
    let ndjson = output == "ndjson-events";
    let driver =
        WasiDriver::new(credential, invocation.timeout)?.with_event_handler(move |batch| {
            if ndjson {
                if batch.dropped > 0 {
                    println!("{}", json!({"type":"lagged","dropped":batch.dropped}));
                }
                for event in batch.events {
                    println!("{}", json!({"type":"event","event":event}));
                }
            }
        });
    let mut runtime = HostRuntime::with_dialect(
        endpoint.into(),
        invocation.selected.unwrap_or(configured_protocol),
    )?;
    if let Some(registry) = registry {
        runtime.set_schema_registry(registry)?;
    }
    let mut remote = mcpx::Client::new(driver, runtime);
    let result = async {
        if let crate::routing::Route::Shell {
            direct_root,
            words,
            current,
            bash_current,
            ..
        } = &invocation.route
        {
            let value = remote
                .with_fresh_catalog(|machine| {
                    mcpx::command::complete_argv(machine.catalog().tools(), words, current)
                })
                .await?;
            crate::shell::emit_completion(value, *direct_root, current, bash_current.as_ref());
            return Ok(0);
        }
        remote.refresh().await?;
        if let Some((line, cursor)) = completion {
            let value = remote.complete_line(line, cursor).await?;
            match output {
                "text" => {
                    for c in &value.candidates {
                        println!("{}\t{}", safe(&c.value), safe(&c.description));
                    }
                }
                "ndjson-events" => println!("{}", json!({"type":"completion","completion":value})),
                _ => print_json(&value),
            }
            return Ok(0);
        }
        if listing {
            let tools: Vec<_> = remote.machine().catalog().tools().collect();
            if matches!(invocation.route, crate::routing::Route::ServerHelp { .. }) {
                crate::shared::print_server_help(&invocation, &tools);
                return Ok(0);
            }
            match output {
                "text" => {
                    for tool in tools {
                        println!(
                            "{}\t{}",
                            safe(&tool.name),
                            safe(tool.description.as_deref().unwrap_or(""))
                        );
                    }
                }
                "ndjson-events" => println!("{}", json!({"type":"catalog","tools":tools})),
                _ => print_json(&tools),
            }
            return Ok(0);
        }
        match remote.execute_argv(tool, &arguments).await? {
            Execution::Help(text) => {
                print!("{text}");
                Ok(0)
            }
            Execution::Outcome(value) => Ok(crate::shared::outcome(value, output)),
        }
    }
    .await;
    let shutdown = remote.close().await;
    let code = result?;
    shutdown?;
    Ok(code)
}

fn read_credential(variable: &str) -> mcpx::Result<Credential> {
    Credential::bearer(
        std::env::var(variable).map_err(|_| {
            Error::local(
                "CredentialUnavailable",
                "token environment variable is unavailable",
            )
        })?,
        "wasi-cli".into(),
    )
}
