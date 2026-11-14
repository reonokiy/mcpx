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
    let Some(invocation) = crate::routing::parse(false)? else {
        return Ok(0);
    };
    let output = invocation.output.as_str();
    let configuration = if invocation.url.is_none() {
        Some(Config::load(&invocation.config)?)
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
    let tool = invocation.tool();
    let listing = matches!(invocation.route, crate::routing::Route::Tools { .. });
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
            if output == "text" {for tool in tools {println!("{}\t{}", safe(&tool.name), safe(tool.description.as_deref().unwrap_or("")));}}
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
