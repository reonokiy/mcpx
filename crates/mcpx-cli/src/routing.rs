use crate::shared::protocol;
use clap::{Arg, Command};
use mcpx::{Dialect, Error, Result};
use std::{path::PathBuf, time::Duration};

pub enum Route {
    Shell {
        direct_root: bool,
        alias: Option<String>,
        words: Vec<String>,
        current: String,
        bash_current: Option<crate::shell::BashWord>,
    },
    Servers,
    Tools {
        alias: Option<String>,
    },
    Complete {
        alias: Option<String>,
        line: String,
        cursor: usize,
    },
    Call {
        alias: Option<String>,
        tool: String,
        arguments: Vec<String>,
    },
}
pub struct Invocation {
    pub url: Option<String>,
    pub config: PathBuf,
    pub selected: Option<Dialect>,
    pub output: String,
    pub timeout: Duration,
    pub token_env: Option<String>,
    pub schema_registry: Option<String>,
    pub route: Route,
}
impl Invocation {
    pub fn alias(&self) -> Option<&str> {
        match &self.route {
            Route::Servers => None,
            Route::Shell { alias, .. }
            | Route::Tools { alias }
            | Route::Complete { alias, .. }
            | Route::Call { alias, .. } => alias.as_deref(),
        }
    }
    pub fn arguments(&self) -> &[String] {
        match &self.route {
            Route::Call { arguments, .. } => arguments,
            _ => &[],
        }
    }
    pub fn tool(&self) -> &str {
        match &self.route {
            Route::Call { tool, .. } => tool,
            _ => "",
        }
    }
}

pub fn parse(is_wasi: bool) -> Result<Option<Invocation>> {
    let help = "mcpx --config mcpx.toml SERVER TOOL [OPTIONS]\nmcpx tools SERVER\nmcpx servers list\nmcpx call SERVER TOOL --args JSON\nmcpx complete SERVER --line 'TOOL --par' [--cursor UTF8_BYTES]\nmcpx --url ENDPOINT TOOL [OPTIONS]\nmcpx completions bash|zsh|fish\nGlobal options precede the route; use TOOL --help for tool parameters.";
    let help = if is_wasi {
        format!(
            "{help}\nWASI HTTP: files and environment require host grants; child-process transport is unavailable."
        )
    } else {
        help.into()
    };
    let matches = Command::new("mcpx")
        .version(env!("CARGO_PKG_VERSION"))
        .about(if is_wasi {
            "Bind and call remote MCP tools using WASI HTTP"
        } else {
            "Bind and call remote MCP tools without generated code"
        })
        .arg(Arg::new("url").long("url").value_name("ENDPOINT"))
        .arg(
            Arg::new("config")
                .long("config")
                .value_name("FILE")
                .conflicts_with("url"),
        )
        .arg(Arg::new("protocol").long("protocol").value_parser([
            "auto",
            "2026-07-28",
            "2025-11-25",
        ]))
        .arg(
            Arg::new("output")
                .long("output")
                .default_value("text")
                .value_parser(["text", "json", "ndjson-events"]),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .default_value("60")
                .value_parser(
                    clap::value_parser!(u64).range(1..=if is_wasi { 86400 } else { u64::MAX }),
                ),
        )
        .arg(
            Arg::new("token-env")
                .long("token-env")
                .help("Environment variable containing a bearer token"),
        )
        .arg(
            Arg::new("schema-registry")
                .long("schema-registry")
                .value_name("FILE")
                .help("Preloaded uri/schema_json resource records; no automatic retrieval"),
        )
        .allow_external_subcommands(true)
        .external_subcommand_value_parser(clap::value_parser!(String))
        .after_help(help)
        .try_get_matches();
    let matches = match matches {
        Ok(value) => value,
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) =>
        {
            print!("{error}");
            return Ok(None);
        }
        Err(_) => {
            return Err(Error::local(
                "CommandParseError",
                "invalid command; use --help",
            ));
        }
    };
    let (route, trailing) = matches.subcommand().ok_or_else(|| {
        Error::local(
            "CommandParseError",
            "a server, tool or management command is required",
        )
    })?;
    let argv: Vec<String> = trailing
        .get_many::<String>("")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    if route == "completions" {
        crate::shell::print_script(&argv)?;
        return Ok(None);
    }
    if route == "__complete" {
        return crate::shell::request(&argv);
    }
    let url = matches.get_one::<String>("url").cloned();
    let route = if route == "complete" {
        let mut command = Command::new("complete")
            .no_binary_name(true)
            .arg(
                Arg::new("line")
                    .long("line")
                    .required(true)
                    .allow_hyphen_values(true),
            )
            .arg(
                Arg::new("cursor")
                    .long("cursor")
                    .value_parser(clap::value_parser!(u64)),
            );
        if url.is_none() {
            command = command.arg(Arg::new("server").required(true));
        }
        let parsed = match command.try_get_matches_from(&argv) {
            Ok(value) => value,
            Err(error) if error.kind() == clap::error::ErrorKind::DisplayHelp => {
                print!("{error}");
                return Ok(None);
            }
            Err(_) => {
                return Err(Error::local(
                    "CommandParseError",
                    "expected complete SERVER --line LINE [--cursor UTF8_BYTES]; omit SERVER with --url",
                ));
            }
        };
        let line = parsed.get_one::<String>("line").unwrap().clone();
        let cursor = parsed
            .get_one::<u64>("cursor")
            .copied()
            .unwrap_or(line.len() as u64);
        if line.len() > mcpx::command::MAX_COMPLETION_INPUT_BYTES {
            return Err(Error::local(
                "LimitExceeded",
                "completion input exceeds 64 KiB",
            ));
        }
        if cursor > line.len() as u64 || !line.is_char_boundary(cursor as usize) {
            return Err(Error::local(
                "InvalidCursor",
                "completion cursor must be a UTF-8 byte boundary",
            ));
        }
        let alias = if url.is_none() {
            parsed.get_one::<String>("server").cloned()
        } else {
            None
        };
        Route::Complete {
            alias,
            line,
            cursor: cursor as usize,
        }
    } else if url.is_none() && route == "servers" {
        if argv != ["list"] {
            return Err(Error::local("CommandParseError", "expected servers list"));
        }
        Route::Servers
    } else if route == "tools" {
        let alias = if url.is_some() {
            if !argv.is_empty() {
                return Err(Error::local(
                    "CommandParseError",
                    "tools takes no arguments with --url",
                ));
            }
            None
        } else {
            if argv.len() != 1 {
                return Err(Error::local("CommandParseError", "expected tools SERVER"));
            }
            Some(argv[0].clone())
        };
        Route::Tools { alias }
    } else if route == "call" {
        if url.is_some() {
            let (tool, arguments) = argv.split_first().ok_or_else(|| {
                Error::local("CommandParseError", "expected call TOOL with --url")
            })?;
            Route::Call {
                alias: None,
                tool: tool.clone(),
                arguments: arguments.into(),
            }
        } else {
            if argv.len() < 2 {
                return Err(Error::local(
                    "CommandParseError",
                    "expected call SERVER TOOL",
                ));
            }
            Route::Call {
                alias: Some(argv[0].clone()),
                tool: argv[1].clone(),
                arguments: argv[2..].into(),
            }
        }
    } else if url.is_some() {
        Route::Call {
            alias: None,
            tool: route.into(),
            arguments: argv,
        }
    } else {
        let (tool, arguments) = argv
            .split_first()
            .ok_or_else(|| Error::local("CommandParseError", "expected SERVER TOOL"))?;
        Route::Call {
            alias: Some(route.into()),
            tool: tool.clone(),
            arguments: arguments.into(),
        }
    };
    Ok(Some(Invocation {
        url,
        config: matches
            .get_one::<String>("config")
            .map(PathBuf::from)
            .unwrap_or_else(|| "mcpx.toml".into()),
        selected: matches.get_one::<String>("protocol").map(|v| protocol(v)),
        output: matches.get_one::<String>("output").unwrap().clone(),
        timeout: Duration::from_secs(*matches.get_one::<u64>("timeout").unwrap()),
        token_env: matches.get_one::<String>("token-env").cloned(),
        schema_registry: matches.get_one::<String>("schema-registry").cloned(),
        route,
    }))
}
