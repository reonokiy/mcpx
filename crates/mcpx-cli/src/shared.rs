use mcpx::{CallOutcome, Dialect, Error, OutputValidation};
use serde_json::{Value, json};
use std::{
    io::Read,
    path::{Path, PathBuf},
};

pub(crate) fn file_path(path: &Path) -> mcpx::Result<PathBuf> {
    #[cfg(all(target_os = "wasi", target_env = "p2"))]
    if path.is_relative()
        && let Some(cwd) = wasip2::cli::environment::initial_cwd()
    {
        let cwd = Path::new(&cwd);
        if !cwd.is_absolute() {
            return Err(Error::local(
                "ConfigurationError",
                "WASI host working directory must be absolute",
            ));
        }
        return Ok(cwd.join(path));
    }
    Ok(path.to_owned())
}

pub(crate) fn protocol(value: &str) -> Dialect {
    match value {
        "2025-11-25" => Dialect::Legacy,
        "2026-07-28" => Dialect::Modern,
        _ => Dialect::Auto,
    }
}

pub(crate) fn safe(text: &str) -> String {
    text.chars()
        .flat_map(|c| {
            if c.is_control() && !matches!(c, '\n' | '\t') {
                c.escape_default().collect::<Vec<_>>()
            } else {
                vec![c]
            }
        })
        .collect()
}

pub(crate) fn print_json(value: &impl serde::Serialize) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("serializable output")
    );
}

pub(crate) fn print_servers<'a>(
    servers: impl Iterator<Item = (&'a str, &'static str)>,
    output: &str,
) {
    let servers: Vec<_> = servers
        .map(|(name, transport)| json!({"name":name,"transport":transport}))
        .collect();
    if output == "text" {
        for server in servers {
            println!(
                "{}\t{}",
                safe(server["name"].as_str().unwrap()),
                safe(server["transport"].as_str().unwrap())
            );
        }
    } else {
        print_json(&servers);
    }
}

pub(crate) fn args_file(mut argv: Vec<String>) -> mcpx::Result<Vec<String>> {
    let mut found = false;
    let mut index = 0;
    while index < argv.len() {
        let path = if argv[index] == "--args-file" {
            if index + 1 >= argv.len() {
                return Err(Error::local(
                    "CommandParseError",
                    "--args-file needs a path",
                ));
            }
            Some((argv[index + 1].clone(), 2))
        } else {
            argv[index]
                .strip_prefix("--args-file=")
                .map(|path| (path.to_owned(), 1))
        };
        if let Some((path, count)) = path {
            if found {
                return Err(Error::local(
                    "ArgumentConflict",
                    "--args-file may only be specified once",
                ));
            }
            found = true;
            let mut bytes = Vec::new();
            let source: Box<dyn Read> = if path == "-" {
                Box::new(std::io::stdin())
            } else {
                Box::new(
                    std::fs::File::open(file_path(Path::new(&path))?).map_err(|_| {
                        Error::local("ConfigurationError", "cannot open argument file")
                    })?,
                )
            };
            source
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| Error::local("ConfigurationError", "cannot read argument file"))?;
            if bytes.len() > 1024 * 1024 {
                return Err(Error::local("LimitExceeded", "argument file exceeds 1 MiB"));
            }
            let text = String::from_utf8(bytes).map_err(|_| {
                Error::local("InvalidJson", "argument file must contain UTF-8 JSON")
            })?;
            mcpx::parse_json(&text)?;
            argv.splice(index..index + count, ["--args".into(), text]);
            index += 2;
        } else {
            index += 1;
        }
    }
    Ok(argv)
}

pub(crate) fn status(result: mcpx::Result<u8>) -> u8 {
    match result {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{}", serde_json::to_string(&error).unwrap());
            match error.code.as_str() {
                "ValidationFailed"
                | "ValidationLimitExceeded"
                | "CommandParseError"
                | "InvalidJson"
                | "ArgumentConflict"
                | "InvalidArgumentType"
                | "NumberOutOfRange"
                | "InvalidCursor"
                | "TokenizeError" => 2,
                "ConfigurationError"
                | "InvalidEndpoint"
                | "ToolNotFound"
                | "SchemaUnsupported"
                | "SchemaReferenceUnavailable"
                | "RemoteNotFound"
                | "InvalidAlias"
                | "ProcessStartFailed"
                | "CapabilityUnavailable" => 3,
                "AuthFailed"
                | "CredentialUnavailable"
                | "LoginRequired"
                | "TokenStoreUnavailable"
                | "OAuthMetadataInvalid"
                | "OAuthDiscoveryFailed"
                | "OAuthPkceRequired"
                | "OAuthRegistrationRequired"
                | "OAuthRegistrationFailed"
                | "OAuthClientInvalid"
                | "OAuthCallbackInvalid"
                | "OAuthCallbackUnavailable"
                | "OAuthConsentDenied"
                | "OAuthTokenFailed"
                | "OAuthTransportError" => 4,
                "Timeout" => 6,
                "Cancelled" => 130,
                _ => 5,
            }
        }
    }
}

pub(crate) fn outcome(outcome: CallOutcome, output: &str) -> u8 {
    match output {
        "json" => print_json(&outcome),
        "ndjson-events" => println!("{}", json!({"type":"result","outcome":outcome})),
        _ => match &outcome {
            CallOutcome::NeedsInput(_) => eprintln!("Tool needs additional input"),
            CallOutcome::Completed(response) => {
                for content in response
                    .raw
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(text) = content.get("text").and_then(Value::as_str) {
                        println!("{}", safe(text));
                    } else {
                        println!(
                            "[{} content]",
                            safe(
                                content
                                    .get("type")
                                    .and_then(Value::as_str)
                                    .unwrap_or("unknown")
                            )
                        );
                    }
                }
            }
        },
    }
    match outcome {
        CallOutcome::NeedsInput(_) => 7,
        CallOutcome::Completed(response)
            if response.is_error
                || matches!(
                    response.output_validation,
                    OutputValidation::Invalid | OutputValidation::Indeterminate
                ) =>
        {
            1
        }
        _ => 0,
    }
}

pub(crate) fn schema_registry(file: &str) -> mcpx::Result<mcpx::SchemaRegistry> {
    let source = std::fs::File::open(file_path(Path::new(file))?)
        .map_err(|_| Error::local("ConfigurationError", "cannot open schema registry file"))?;
    let mut bytes = Vec::new();
    source
        .take(mcpx::MAX_SCHEMA_BUNDLE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::local("ConfigurationError", "cannot read schema registry file"))?;
    if bytes.len() > mcpx::MAX_SCHEMA_BUNDLE_BYTES {
        return Err(Error::local(
            "LimitExceeded",
            "schema bundle exceeds 16 MiB",
        ));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| Error::local("ConfigurationError", "schema bundle must be UTF-8 JSON"))?;
    mcpx::SchemaRegistry::from_json(text)
}
