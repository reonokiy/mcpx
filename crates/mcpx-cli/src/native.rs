use mcpx::native::{Client, Config, Execution};
use mcpx::{Dialect, Error};
use serde_json::json;
use std::process::ExitCode;

use crate::shared::{args_file, print_json, safe};

#[tokio::main(flavor = "current_thread")]
pub(crate) async fn main() -> ExitCode {
    ExitCode::from(crate::shared::status(run().await))
}

async fn run() -> mcpx::Result<u8> {
    let Some(mut invocation) = crate::routing::parse(false)? else {
        return Ok(0);
    };
    if let crate::routing::Route::UseProfile { name } = &invocation.route {
        return crate::config_write::use_profile(&invocation, name);
    }
    if let crate::routing::Route::Add { .. } = &invocation.route {
        return crate::config_write::add(&invocation);
    }
    let output = invocation.output.as_str();
    let configuration = if invocation.url.is_none() {
        Some({
            let mut config = Config::load(&invocation.config)?;
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
    let url = invocation.url.as_ref();
    let alias = invocation.alias();
    let configured = configuration
        .as_ref()
        .map(|config| config.server(alias.unwrap()))
        .transpose()?;
    let endpoint = if let Some(url) = url {
        Some(url.as_str())
    } else {
        match configured {
            Some(mcpx::native::ServerConfig::Http { url, .. }) => Some(url.as_str()),
            _ => None,
        }
    };
    let profile = invocation.profile.as_deref().unwrap_or("default");
    if let crate::routing::Route::Auth {
        action,
        client_id,
        scopes,
        no_browser,
        store,
        redirect_port,
        ..
    } = &invocation.route
    {
        let endpoint = endpoint.ok_or_else(|| {
            Error::local(
                "CapabilityUnavailable",
                "OAuth is supported for HTTP servers; stdio uses environment credentials",
            )
        })?;
        let operation = async {
            match action.as_str() {
                "login" => {
                    mcpx::native::oauth::login(
                        endpoint,
                        mcpx::native::oauth::LoginOptions {
                            client_id: client_id.clone(),
                            scopes: scopes.clone(),
                            no_browser: *no_browser,
                            callback_stdin: *no_browser,
                            redirect_port: *redirect_port,
                            profile: Some(profile.into()),
                            store: match store.as_str() {
                                "file" => mcpx::native::oauth::StoreKind::File,
                                "keyring" => mcpx::native::oauth::StoreKind::Keyring,
                                _ => mcpx::native::oauth::StoreKind::Auto,
                            },
                        },
                        |url| {
                            eprintln!("Open this URL to authorize mcpx:\n{}", safe(url));
                            if *no_browser { eprintln!("After authorizing, paste the complete callback URL here and press Enter (or use the loopback redirect)."); }
                        },
                        invocation.timeout,
                    )
                    .await?;
                    if let Some(path) =
                        mcpx::native::oauth::TokenStore::for_profile(endpoint, profile)?
                            .file_storage_path()?
                    {
                        eprintln!(
                            "Credentials saved in private file: {}",
                            safe(&path.display().to_string())
                        );
                    }
                    println!("Logged in.");
                }
                "status" => {
                    let state = mcpx::native::oauth::status_profile(endpoint, profile).await?;
                    if output == "text" {
                        println!(
                            "{}",
                            if state["logged_in"] == true {
                                "Logged in."
                            } else {
                                "Not logged in."
                            }
                        );
                    } else {
                        print_json(&state);
                    }
                }
                "logout" => {
                    mcpx::native::oauth::logout_profile(endpoint, profile).await?;
                    println!("Logged out.");
                }
                _ => unreachable!(),
            }
            Ok::<u8, Error>(0)
        };
        return tokio::select! {result=operation=>result,_=tokio::signal::ctrl_c()=>Err(Error::local("Cancelled","OAuth operation cancelled"))};
    }
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
    let mut client = Client::new()?.with_timeout(invocation.timeout);
    if let Some(file) = &invocation.schema_registry {
        client = client.with_schema_registry(crate::shared::schema_registry(file)?);
    }
    if output == "ndjson-events" {
        client = client.with_event_handler(|item| match item {
            mcpx::native::EventStreamItem::Event(event) => {
                println!("{}", json!({"type":"event","event":event}))
            }
            mcpx::native::EventStreamItem::Lagged { dropped } => {
                println!("{}", json!({"type":"lagged","dropped":dropped}))
            }
        });
    }
    if let Some(name) = &invocation.token_env {
        client = client.with_bearer(std::env::var(name).map_err(|_| {
            Error::local(
                "CredentialUnavailable",
                "token environment variable is unavailable",
            )
        })?);
    }
    if invocation.token_env.is_none() {
        let reference = match configured {
            Some(mcpx::native::ServerConfig::Http { credential, .. }) => credential.as_deref(),
            _ => None,
        };
        if let Some(endpoint) = endpoint
            && (reference.is_some_and(|r| r == "oauth" || r.starts_with("oauth:"))
                || reference.is_none()
                    && mcpx::native::oauth::OAuthCredentialProvider::has_profile(
                        endpoint, profile,
                    )?)
        {
            client = client.with_credential_provider(
                mcpx::native::oauth::OAuthCredentialProvider::for_profile(endpoint, profile)?,
            );
        }
    }
    let selected = invocation.selected;
    let connect = async {
        if let Some(url) = url {
            client.connect(url, selected.unwrap_or(Dialect::Auto)).await
        } else {
            configured.unwrap().connect(client, selected).await
        }
    };
    let mut remote = tokio::select! {
        result = connect => result?,
        _ = tokio::signal::ctrl_c() => return Err(Error::local("Cancelled", "operation cancelled")),
    };
    let result: mcpx::Result<u8> = async {
        if let crate::routing::Route::Shell {words, current, bash_current, direct_root, ..} = &invocation.route {
            let value = tokio::select! {
                result = remote.with_fresh_catalog(|machine| mcpx::command::complete_argv(machine.catalog().tools(), words, current)) => result?,
                _ = tokio::signal::ctrl_c() => return Err(Error::new("Cancelled", "completion cancelled")),
            };
            crate::shell::emit_completion(value, *direct_root, current, bash_current.as_ref());
            return Ok(0);
        }
        if let Some((line, cursor)) = &completion {
            let value = tokio::select! {
                result = remote.complete_line(line, *cursor) => result?,
                _ = tokio::signal::ctrl_c() => return Err(Error::new("Cancelled", "completion cancelled")),
            };
            match output {
                "text" => for candidate in &value.candidates { println!("{}\t{}", safe(&candidate.value), safe(&candidate.description)); },
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
            if output == "text" {crate::shared::print_tools(&tools);}
            else if output == "ndjson-events" {println!("{}", json!({"type":"catalog","tools":tools}));}
            else {print_json(&tools);}
            return Ok(0);
        }
        let execution = tokio::select! {
            result = remote.execute_argv(tool, &arguments) => result?,
            _ = tokio::signal::ctrl_c() => return Err(Error::new("Cancelled", "operation cancelled; execution may be unknown")),
        };
        match execution {
            Execution::Help(text) => {print!("{text}"); Ok(0)}
            Execution::Outcome(outcome) => Ok(crate::shared::outcome(outcome, output)),
        }
    }.await;
    let result = result.map_err(|error| {
        if error.code == "Cancelled" && error.operation_id.is_none() {
            let context = remote
                .machine()
                .active_action_id()
                .and_then(|id| remote.machine().action_error_context(id))
                .unwrap_or_else(mcpx::ErrorContext::not_sent);
            error.with_context(context)
        } else {
            error
        }
    });
    let shutdown = remote.close().await;
    let code = result?;
    shutdown?;
    Ok(code)
}
