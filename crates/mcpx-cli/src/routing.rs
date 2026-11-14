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
    Profiles,
    #[cfg_attr(all(target_os = "wasi", target_env = "p2"), allow(dead_code))]
    UseProfile {
        name: String,
    },
    #[cfg_attr(all(target_os = "wasi", target_env = "p2"), allow(dead_code))]
    Add {
        name: String,
        url: Option<String>,
        command: Vec<String>,
        credential: Option<String>,
    },
    // Native UI fields are parsed on WASI only to return a clear capability error.
    #[cfg_attr(all(target_os = "wasi", target_env = "p2"), allow(dead_code))]
    Auth {
        alias: Option<String>,
        action: String,
        client_id: Option<String>,
        scopes: Vec<String>,
        no_browser: bool,
        store: String,
        redirect_port: Option<u16>,
    },
    ServerHelp {
        alias: Option<String>,
    },
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
    pub profile: Option<String>,
    pub schema_registry: Option<String>,
    pub route: Route,
}
impl Invocation {
    pub fn alias(&self) -> Option<&str> {
        match &self.route {
            Route::Servers | Route::Profiles | Route::UseProfile { .. } | Route::Add { .. } => None,
            Route::Auth { alias, .. } => alias.as_deref(),
            Route::Shell { alias, .. }
            | Route::ServerHelp { alias }
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

/// Native defaults follow the nearest project configuration, from cwd to root.
pub(crate) fn default_config() -> PathBuf {
    #[cfg(not(all(target_os = "wasi", target_env = "p2")))]
    if let Ok(directory) = std::env::current_dir() {
        for ancestor in directory.ancestors() {
            let candidate = ancestor.join("mcpx.toml");
            if candidate.is_file() {
                return candidate;
            }
        }
    }
    "mcpx.toml".into()
}

fn cli_command(is_wasi: bool) -> Command {
    let help = if is_wasi {
        "Use SERVER TOOL or --url ENDPOINT TOOL for shorthand calls.\nWASI HTTP requires host grants for files and environment."
    } else {
        "Use SERVER TOOL or --url ENDPOINT TOOL for shorthand calls. Use call --help for explicit calls."
    };
    Command::new("mcpx")
        .arg_required_else_help(true)
        .version(env!("CARGO_PKG_VERSION"))
        .about(if is_wasi {
            "Bind and call remote MCP tools using WASI HTTP"
        } else {
            "Bind and call remote MCP tools without generated code"
        })
        .arg(Arg::new("url").long("url").value_name("ENDPOINT"))
        .arg(
            Arg::new("profile")
                .long("profile")
                .global(true)
                .value_name("PROFILE")
                .help("Configuration and OAuth profile (overrides saved default)"),
        )
        .arg(
            Arg::new("config")
                .long("config")
                .value_name("FILE")
                .help("Override automatic mcpx.toml discovery from the current directory upward")
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
        .subcommand(
            Command::new("tools")
                .about("List a server's tools")
                .arg(Arg::new("server").value_name("SERVER")),
        )
        .subcommand(
            Command::new("servers")
                .about("Manage configured servers")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(Command::new("list").about("List configured servers"))
                .subcommand(
                    Command::new("add")
                        .about("Save a server in the selected configuration profile")
                        .arg(Arg::new("name").required(true))
                        .arg(
                            Arg::new("endpoint")
                                .long("url")
                                .conflicts_with("command")
                                .required_unless_present("command"),
                        )
                        .arg(
                            Arg::new("credential")
                                .long("credential")
                                .requires("endpoint"),
                        )
                        .arg(
                            Arg::new("command")
                                .num_args(1..)
                                .trailing_var_arg(true)
                                .required_unless_present("endpoint"),
                        ),
                ),
        )
        .subcommand(
            Command::new("profiles")
                .about("List profiles or choose the saved default")
                .arg_required_else_help(true)
                .subcommand_required(true)
                .subcommand(Command::new("list").about("List configuration profiles"))
                .subcommand(
                    Command::new("use")
                        .about("Save the default profile in this configuration")
                        .arg(Arg::new("name").required(true)),
                ),
        )
        .subcommand(
            Command::new("auth")
                .about("Manage OAuth accounts")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommands(["login", "status", "logout"].map(|action| {
                    Command::new(action)
                        .about(match action {
                            "login" => "Sign in to a server",
                            "status" => "Show account status",
                            _ => "Sign out of a server",
                        })
                        .arg(Arg::new("server").value_name("SERVER"))
                        .arg(Arg::new("client-id").long("client-id"))
                        .arg(
                            Arg::new("redirect-port")
                                .long("redirect-port")
                                .value_parser(clap::value_parser!(u16)),
                        )
                        .arg(
                            Arg::new("scope")
                                .long("scope")
                                .action(clap::ArgAction::Append),
                        )
                        .arg(
                            Arg::new("no-browser")
                                .long("no-browser")
                                .action(clap::ArgAction::SetTrue),
                        )
                        .arg(
                            Arg::new("store")
                                .long("store")
                                .help("Credential storage: auto tries keyring then private files (default: auto)")
                                .value_parser(["auto", "keyring", "file"]),
                        )
                })),
        )
        .subcommand(
            Command::new("call")
                .about("Call a tool: call SERVER TOOL [OPTIONS]; omit SERVER with --url")
                .arg(
                    Arg::new("arguments")
                        .value_name("SERVER_OR_TOOL")
                        .num_args(1..)
                        .trailing_var_arg(true),
                ),
        )
        .subcommand(
            Command::new("complete")
                .about("Complete a tool command line")
                .arg(Arg::new("server").value_name("SERVER"))
                .arg(
                    Arg::new("line")
                        .long("line")
                        .required(true)
                        .allow_hyphen_values(true),
                )
                .arg(
                    Arg::new("cursor")
                        .long("cursor")
                        .value_name("UTF8_BYTES")
                        .value_parser(clap::value_parser!(u64)),
                ),
        )
        .subcommand(
            Command::new("completions")
                .about("Print a shell completion script")
                .arg(
                    Arg::new("shell")
                        .required(true)
                        .value_parser(["bash", "zsh", "fish"]),
                ),
        )
        .allow_external_subcommands(true)
        .external_subcommand_value_parser(clap::value_parser!(String))
        .after_help(help)
}

/// Render the deepest recognized command without loading configuration or connecting.
pub(crate) fn usage_help(is_wasi: bool) -> String {
    let mut command = cli_command(is_wasi);
    command.build();
    let args: Vec<String> = std::env::args().skip(1).collect();
    command_help(&mut command, &args)
}

fn command_help(command: &mut Command, args: &[String]) -> String {
    let mut index = 0;
    while let Some(word) = args.get(index) {
        if word.starts_with('-') {
            let flag = word.split('=').next().unwrap();
            let takes_value = command.get_arguments().any(|arg| {
                (arg.get_long()
                    .is_some_and(|name| flag == format!("--{name}"))
                    || arg
                        .get_short()
                        .is_some_and(|name| flag == format!("-{name}")))
                    && arg.get_action().takes_values()
            });
            index += if takes_value && !word.contains('=') {
                2
            } else {
                1
            };
            continue;
        }
        if let Some(child) = command.find_subcommand_mut(word) {
            return command_help(child, &args[index + 1..]);
        }
        break;
    }
    command.render_long_help().to_string()
}

pub fn parse(is_wasi: bool) -> Result<Option<Invocation>> {
    let mut command = cli_command(is_wasi);
    let matches = command.clone().try_get_matches();
    let matches = match matches {
        Ok(value) => value,
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp
                    | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
                    | clap::error::ErrorKind::DisplayVersion
            ) =>
        {
            print!("{error}");
            return Ok(None);
        }
        Err(error) => {
            return Err(Error::local(
                "CliUsage",
                format!("{error}\n{}", usage_help(is_wasi)),
            ));
        }
    };
    let (route, trailing) = matches.subcommand().ok_or_else(|| {
        Error::local(
            "CommandParseError",
            "a server, tool or management command is required",
        )
    })?;
    // Keep dynamic tool arguments opaque; management commands are validated by clap.
    let values = |m: &clap::ArgMatches, id: &str| -> Vec<String> {
        m.get_raw(id)
            .map(|v| v.map(|s| s.to_string_lossy().into_owned()).collect())
            .unwrap_or_default()
    };
    let argv: Vec<String> = match route {
        "profiles" => {
            if matches.get_one::<String>("url").is_some() {
                return Err(Error::local(
                    "CliUsage",
                    "profiles commands require a configuration file; omit --url\nUsage: mcpx profiles <list|use NAME>",
                ));
            }
            let (action, m) = trailing.subcommand().unwrap();
            let route = if action == "use" {
                Route::UseProfile {
                    name: m.get_one::<String>("name").unwrap().clone(),
                }
            } else {
                Route::Profiles
            };
            return Ok(Some(invocation(&matches, route)));
        }
        "servers" => {
            if let Some(("add", m)) = trailing.subcommand() {
                let route = Route::Add {
                    name: m.get_one::<String>("name").unwrap().clone(),
                    url: m.get_one::<String>("endpoint").cloned(),
                    credential: m.get_one::<String>("credential").cloned(),
                    command: values(m, "command"),
                };
                return Ok(Some(invocation(&matches, route)));
            }
            vec!["list".into()]
        }
        "tools" => values(trailing, "server"),
        "call" => values(trailing, "arguments"),
        "completions" => values(trailing, "shell"),
        "complete" => {
            let mut args = values(trailing, "server");
            for key in ["line", "cursor"] {
                for value in values(trailing, key) {
                    args.extend([format!("--{key}"), value]);
                }
            }
            args
        }
        "auth" => {
            let (action, m) = trailing.subcommand().unwrap();
            let mut args = vec![action.into()];
            args.extend(values(m, "server"));
            for key in ["client-id", "redirect-port", "scope", "store"] {
                for value in values(m, key) {
                    args.extend([format!("--{key}"), value]);
                }
            }
            if m.get_flag("no-browser") {
                args.push("--no-browser".into());
            }
            args
        }
        _ => values(trailing, ""),
    };
    if route == "completions" {
        crate::shell::print_script(&argv)?;
        return Ok(None);
    }
    if route == "__complete" {
        return crate::shell::request(&argv);
    }
    let url = matches.get_one::<String>("url").cloned();
    let route = if route == "auth" {
        let parsed = Command::new("auth")
            .no_binary_name(true)
            .arg(
                Arg::new("action")
                    .required(true)
                    .value_parser(["login", "status", "logout"]),
            )
            .arg(Arg::new("server").required(url.is_none()))
            .arg(Arg::new("client-id").long("client-id"))
            .arg(
                Arg::new("redirect-port")
                    .long("redirect-port")
                    .value_parser(clap::value_parser!(u16)),
            )
            .arg(
                Arg::new("scope")
                    .long("scope")
                    .action(clap::ArgAction::Append),
            )
            .arg(
                Arg::new("no-browser")
                    .long("no-browser")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("store")
                    .long("store")
                    .help(
                        "Credential storage: auto tries keyring then private files (default: auto)",
                    )
                    .default_value("auto")
                    .value_parser(["auto", "keyring", "file"]),
            )
            .try_get_matches_from(&argv);
        let parsed = match parsed {
            Ok(value) => value,
            Err(e) if e.kind() == clap::error::ErrorKind::DisplayHelp => {
                print!("{e}");
                return Ok(None);
            }
            Err(_) => {
                return Err(Error::local(
                    "CommandParseError",
                    "expected auth login|status|logout SERVER; omit SERVER with --url",
                ));
            }
        };
        if url.is_some() && parsed.get_one::<String>("server").is_some() {
            return Err(Error::local("CommandParseError", "omit SERVER with --url"));
        }
        Route::Auth {
            alias: parsed.get_one::<String>("server").cloned(),
            action: parsed.get_one::<String>("action").unwrap().clone(),
            client_id: parsed.get_one::<String>("client-id").cloned(),
            scopes: parsed
                .get_many::<String>("scope")
                .map(|v| {
                    v.flat_map(|s| s.split_whitespace().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
            redirect_port: parsed.get_one::<u16>("redirect-port").copied(),
            no_browser: parsed.get_flag("no-browser"),
            store: parsed.get_one::<String>("store").unwrap().clone(),
        }
    } else if route == "complete" {
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
        if matches.get_one::<String>("config").is_none()
            && !crate::shared::file_path(&default_config())?.is_file()
        {
            return Err(Error::local(
                "CliUsage",
                format!(
                    "error: unknown command '{route}'\n\n{}",
                    command.render_long_help()
                ),
            ));
        }
        if argv.len() == 1 && matches!(argv[0].as_str(), "-h" | "--help") {
            Route::ServerHelp {
                alias: Some(route.into()),
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
        }
    };
    Ok(Some(invocation(&matches, route)))
}

fn invocation(matches: &clap::ArgMatches, route: Route) -> Invocation {
    Invocation {
        url: matches.get_one::<String>("url").cloned(),
        config: matches
            .get_one::<String>("config")
            .map(PathBuf::from)
            .unwrap_or_else(default_config),
        selected: matches.get_one::<String>("protocol").map(|v| protocol(v)),
        output: matches.get_one::<String>("output").unwrap().clone(),
        timeout: Duration::from_secs(*matches.get_one::<u64>("timeout").unwrap()),
        token_env: matches.get_one::<String>("token-env").cloned(),
        profile: matches.get_one::<String>("profile").cloned(),
        schema_registry: matches.get_one::<String>("schema-registry").cloned(),
        route,
    }
}
