use crate::routing::{Invocation, Route};
use clap::{Arg, Command};
use mcpx::{Error, Result};
use std::{collections::BTreeSet, path::Path, time::Duration};

pub struct BashWord {
    pub raw: String,
    pub fragment: String,
}

const OPTIONS: &[&str] = &[
    "--config",
    "--url",
    "--protocol",
    "--output",
    "--timeout",
    "--token-env",
    "--profile",
    "--schema-registry",
    "--help",
    "--version",
];
const ROUTES: &[&str] = &["servers", "tools", "call", "complete", "completions"];

pub fn print_script(argv: &[String]) -> Result<()> {
    if argv == ["--help"] || argv == ["-h"] {
        println!(
            "Usage: mcpx completions bash|zsh|fish\nPrint a generic dynamic completion script; no server is contacted."
        );
        return Ok(());
    }
    let Some(shell) = argv.first().filter(|_| argv.len() == 1) else {
        return Err(Error::local(
            "CommandParseError",
            "expected completions bash|zsh|fish",
        ));
    };
    let script = match shell.as_str() {
        "bash" => include_str!("../shells/mcpx.bash"),
        "zsh" => include_str!("../shells/_mcpx"),
        "fish" => include_str!("../shells/mcpx.fish"),
        _ => {
            return Err(Error::local(
                "CommandParseError",
                "unsupported completion shell",
            ));
        }
    };
    print!("{script}");
    Ok(())
}

// Editing grammar only: never evaluate expansions, substitutions or redirections.
// Simple pipeline/control separators select the active command, not an executor.
fn words(line: &str, fish: bool) -> Option<(Vec<String>, String, Option<char>)> {
    if line.len() > mcpx::command::MAX_COMPLETION_INPUT_BYTES {
        return None;
    }
    let mut words = Vec::new();
    let mut word = String::new();
    let mut started = false;
    let mut quote = None;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match (quote, ch) {
            (Some('\''), '\'') | (Some('"'), '"') => quote = None,
            (Some('\''), '\\')
                if fish && chars.peek().is_some_and(|ch| matches!(ch, '\\' | '\'')) =>
            {
                word.push(chars.next().unwrap())
            }
            (Some('\''), _) => word.push(ch),
            (Some('"'), '\\') => match chars.peek() {
                Some('$' | '`' | '"' | '\\') => word.push(chars.next().unwrap()),
                Some('\n') => {
                    chars.next();
                }
                _ => word.push(ch),
            },
            (Some('"') | None, '$' | '`') => return None,
            (Some('"'), _) => word.push(ch),
            (None, '\'' | '"') => {
                quote = Some(ch);
                started = true;
            }
            (None, '\\') => {
                if let Some(ch) = chars.next()
                    && ch != '\n'
                {
                    word.push(ch);
                    started = true;
                }
            }
            (None, '|' | ';' | '&' | '\n') => {
                words.clear();
                word.clear();
                started = false;
            }
            (None, '<' | '>' | '(' | ')') => return None,
            (None, '#') if !started => return None,
            (None, '~') if !started => return None,
            (None, ' ' | '\t') => {
                if started {
                    words.push(std::mem::take(&mut word));
                    started = false;
                }
                if words.len() > 4096 {
                    return None;
                }
            }
            (None, _) => {
                word.push(ch);
                started = true;
            }
            _ => return None,
        }
    }
    Some((words, word, quote))
}

pub fn request(argv: &[String]) -> Result<Option<Invocation>> {
    let parsed = Command::new("__complete")
        .no_binary_name(true)
        .arg(
            Arg::new("shell")
                .required(true)
                .value_parser(["bash", "zsh", "fish"]),
        )
        .arg(
            Arg::new("line")
                .long("line")
                .required_unless_present("word")
                .conflicts_with("word")
                .allow_hyphen_values(true),
        )
        .arg(
            Arg::new("word")
                .long("word")
                .action(clap::ArgAction::Append)
                .allow_hyphen_values(true),
        )
        .arg(
            Arg::new("current")
                .long("current")
                .required_if_eq("shell", "bash")
                .allow_hyphen_values(true),
        )
        .arg(
            Arg::new("fragment")
                .long("fragment")
                .required_if_eq("shell", "bash")
                .allow_hyphen_values(true),
        )
        .try_get_matches_from(argv)
        .map_err(|_| Error::local("CommandParseError", "invalid shell completion request"))?;
    for name in ["line", "current", "fragment"] {
        if parsed
            .get_one::<String>(name)
            .is_some_and(|value| value.len() > mcpx::command::MAX_COMPLETION_INPUT_BYTES)
        {
            return Ok(None);
        }
    }
    if let Some(words) = parsed.get_many::<String>("word") {
        if parsed.get_one::<String>("shell").unwrap() == "bash" {
            return Err(Error::local(
                "CommandParseError",
                "bash completion requires line context",
            ));
        }
        let current = parsed
            .get_one::<String>("current")
            .cloned()
            .unwrap_or_default();
        if words.len() > 4096
            || words.clone().fold(current.len(), |n, word| {
                n.saturating_add(word.len()).saturating_add(1)
            }) > mcpx::command::MAX_COMPLETION_INPUT_BYTES
            || words
                .clone()
                .any(|word| word.contains(['$', '`']) || word.starts_with('~'))
        {
            return Ok(None);
        }
        let words: Vec<_> = words.cloned().collect();
        return plan(words, current, None);
    }
    let line = parsed.get_one::<String>("line").unwrap();
    let bash_current = (parsed.get_one::<String>("shell").unwrap() == "bash").then(|| BashWord {
        raw: parsed
            .get_one::<String>("current")
            .cloned()
            .unwrap_or_default(),
        fragment: parsed
            .get_one::<String>("fragment")
            .cloned()
            .unwrap_or_default(),
    });
    let Some((completed, current, _)) =
        words(line, parsed.get_one::<String>("shell").unwrap() == "fish")
    else {
        return Ok(None);
    };
    let Some(invocation) = plan(completed, current, bash_current)? else {
        return Ok(None);
    };
    Ok(Some(invocation))
}

fn local(
    candidates: impl IntoIterator<Item = String>,
    current: &str,
    bash_current: Option<&BashWord>,
) {
    emit(
        candidates
            .into_iter()
            .filter(|value| value.starts_with(current)),
        current,
        bash_current,
    );
}

fn aliases(path: &Path) -> Vec<String> {
    let Ok(path) = crate::shared::file_path(path) else {
        return Vec::new();
    };
    mcpx::config::Config::load_unresolved(&path)
        .map(|config| config.servers.into_keys().collect())
        .unwrap_or_default()
}

fn values(flag: &str, current: &str) -> Vec<String> {
    match flag {
        "--protocol" => ["auto", "2026-07-28", "2025-11-25"]
            .map(String::from)
            .into(),
        "--output" => ["text", "json", "ndjson-events"].map(String::from).into(),
        "--config" | "--schema-registry" => {
            let path = Path::new(current);
            let (base, prefix) = if current.ends_with('/') {
                (path, "")
            } else {
                (
                    path.parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or_else(|| Path::new(".")),
                    path.file_name().and_then(|v| v.to_str()).unwrap_or(""),
                )
            };
            let Ok(base) = crate::shared::file_path(base) else {
                return Vec::new();
            };
            std::fs::read_dir(base)
                .into_iter()
                .flatten()
                .take(4096)
                .flatten()
                .filter_map(|entry| {
                    let name = entry.file_name().into_string().ok()?;
                    if !name.starts_with(prefix) {
                        return None;
                    }
                    let mut value = current[..current.len() - prefix.len()].to_owned();
                    value.push_str(&name);
                    if entry.file_type().ok()?.is_dir() {
                        value.push('/');
                    }
                    Some(value)
                })
                .take(mcpx::command::MAX_COMPLETION_CANDIDATES)
                .collect()
        }
        _ => Vec::new(),
    }
}

fn plan(
    mut completed: Vec<String>,
    current: String,
    bash_current: Option<BashWord>,
) -> Result<Option<Invocation>> {
    if completed.first().is_some_and(|word| word == "command") {
        completed.remove(0);
    }
    let Some(program) = completed.first() else {
        return Ok(None);
    };
    if Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        != Some("mcpx")
    {
        return Ok(None);
    }
    let mut invocation = Invocation {
        config: "mcpx.toml".into(),
        url: None,
        selected: None,
        output: "shell".into(),
        timeout: Duration::from_secs(2),
        token_env: None,
        profile: None,
        schema_registry: None,
        route: Route::Servers,
    };
    let mut seen = BTreeSet::new();
    let mut index = 1;
    while let Some(word) = completed.get(index).filter(|word| word.starts_with('-')) {
        let (flag, inline) = word
            .split_once('=')
            .map_or((word.as_str(), None), |(flag, value)| (flag, Some(value)));
        if !OPTIONS.contains(&flag) || matches!(flag, "--help" | "--version") || !seen.insert(flag)
        {
            return Ok(None);
        }
        index += 1;
        let value = if let Some(value) = inline {
            value
        } else if let Some(value) = completed.get(index) {
            index += 1;
            value
        } else {
            local(values(flag, &current), &current, bash_current.as_ref());
            return Ok(None);
        };
        match flag {
            "--config" => invocation.config = value.into(),
            "--url" => invocation.url = Some(value.into()),
            "--protocol" if ["auto", "2026-07-28", "2025-11-25"].contains(&value) => {
                invocation.selected = Some(crate::shared::protocol(value))
            }
            "--output" if ["text", "json", "ndjson-events"].contains(&value) => {}
            "--timeout" => {
                let Ok(seconds) = value.parse::<u64>() else {
                    return Ok(None);
                };
                if seconds == 0 {
                    return Ok(None);
                }
                invocation.timeout = Duration::from_secs(seconds.min(2));
            }
            "--token-env" => invocation.token_env = Some(value.into()),
            "--profile" => invocation.profile = Some(value.into()),
            "--schema-registry" => invocation.schema_registry = Some(value.into()),
            _ => return Ok(None),
        }
    }
    if seen.contains("--url") && seen.contains("--config") {
        return Ok(None);
    }
    if index == completed.len() && current.starts_with('-') {
        if let Some((flag, prefix)) = current.split_once('=') {
            local(
                values(flag, prefix)
                    .into_iter()
                    .map(|value| format!("{flag}={value}")),
                &current,
                bash_current.as_ref(),
            );
        } else {
            local(
                OPTIONS
                    .iter()
                    .filter(|flag| !seen.contains(**flag))
                    .map(|flag| (*flag).into()),
                &current,
                bash_current.as_ref(),
            );
        }
        return Ok(None);
    }
    let remaining = &completed[index..];
    let Some((route, rest)) = remaining.split_first() else {
        if invocation.url.is_none() {
            local(
                ROUTES
                    .iter()
                    .map(|v| (*v).into())
                    .chain(aliases(&invocation.config)),
                &current,
                bash_current.as_ref(),
            );
            return Ok(None);
        }
        invocation.route = Route::Shell {
            direct_root: true,
            alias: None,
            words: Vec::new(),
            current,
            bash_current,
        };
        return Ok(Some(invocation));
    };
    match route.as_str() {
        "servers" if invocation.url.is_none() => {
            if rest.is_empty() {
                local(["list".into()], &current, bash_current.as_ref());
            }
            return Ok(None);
        }
        "completions" => {
            if rest.is_empty() {
                local(
                    ["bash".into(), "zsh".into(), "fish".into()],
                    &current,
                    bash_current.as_ref(),
                );
            }
            return Ok(None);
        }
        "tools" | "complete" if rest.is_empty() && invocation.url.is_none() => {
            local(aliases(&invocation.config), &current, bash_current.as_ref());
            return Ok(None);
        }
        "tools" | "__complete" | "complete" => return Ok(None),
        _ => {}
    }
    let named = invocation.url.is_none();
    let (alias, args) = if route == "call" {
        if named {
            let Some((alias, args)) = rest.split_first() else {
                local(aliases(&invocation.config), &current, bash_current.as_ref());
                return Ok(None);
            };
            (Some(alias.clone()), args.to_vec())
        } else {
            (None, rest.to_vec())
        }
    } else if named {
        (Some(route.clone()), rest.to_vec())
    } else {
        (None, remaining.to_vec())
    };
    invocation.route = Route::Shell {
        direct_root: false,
        alias,
        words: args,
        current,
        bash_current,
    };
    Ok(Some(invocation))
}

pub fn emit(
    candidates: impl IntoIterator<Item = String>,
    current: &str,
    bash_current: Option<&BashWord>,
) {
    // Bash may split a logical word at '=' or ':'. Keep its already-inserted prefix.
    let fragment = bash_current
        .filter(|word| word.raw.len() < mcpx::command::MAX_COMPLETION_INPUT_BYTES)
        .and_then(|word| words(&format!("x{}", word.raw), false))
        .map(|(_, word, quote)| (word.strip_prefix('x').unwrap_or(&word).to_owned(), quote));
    let retained = bash_current
        .and_then(|word| current.strip_suffix(word.fragment.as_str()))
        .or_else(|| {
            fragment
                .as_ref()
                .and_then(|(fragment, _)| current.strip_suffix(fragment.as_str()))
        })
        .unwrap_or("");
    let mut bytes = 0;
    let mut count = 0;
    for value in candidates {
        if value.is_empty() || value.chars().any(char::is_control) {
            continue;
        }
        let value = value.strip_prefix(retained).unwrap_or(&value);
        let value = if bash_current.is_some() {
            bash_escape(value, fragment.as_ref().and_then(|(_, quote)| *quote))
        } else {
            value.to_owned()
        };
        if count == mcpx::command::MAX_COMPLETION_CANDIDATES
            || bytes + value.len() + 1 > mcpx::command::MAX_COMPLETION_BYTES
        {
            break;
        }
        println!("{value}");
        bytes += value.len() + 1;
        count += 1;
    }
}

pub fn emit_completion(
    completion: mcpx::command::Completion,
    direct_root: bool,
    current: &str,
    bash_current: Option<&BashWord>,
) {
    const DIRECT_ROUTES: &[&str] = &["tools", "call", "complete", "completions"];
    let remote = completion
        .candidates
        .into_iter()
        .map(|candidate| candidate.value)
        .filter(|value| {
            !direct_root || (!DIRECT_ROUTES.contains(&value.as_str()) && value != "__complete")
        });
    let local = DIRECT_ROUTES
        .iter()
        .filter(|route| direct_root && route.starts_with(current))
        .map(|route| (*route).to_owned());
    emit(local.chain(remote), current, bash_current);
}

fn bash_escape(value: &str, quote: Option<char>) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match quote {
            Some('\'') if ch == '\'' => escaped.push_str("'\\''"),
            Some('\'') => escaped.push(ch),
            Some('"') => {
                if ch == '!' {
                    escaped.push_str("\"'!'\"");
                    continue;
                }
                if matches!(ch, '$' | '`' | '"' | '\\') {
                    escaped.push('\\');
                }
                escaped.push(ch);
            }
            _ => {
                if !(ch.is_alphanumeric()
                    || (!ch.is_ascii() && !ch.is_whitespace())
                    || "_/-.,:=+@%".contains(ch))
                {
                    escaped.push('\\');
                }
                escaped.push(ch);
            }
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn editing_lexer_keeps_argv_quotes_empty_values_and_active_command_boundaries() {
        let (done, current, quote) =
            words("echo previous | mcpx demo echo --mode 'two w", false).unwrap();
        assert_eq!(done, ["mcpx", "demo", "echo", "--mode"]);
        assert_eq!((current.as_str(), quote), ("two w", Some('\'')));
        let (done, current, quote) = words(r#"mcpx '' "a\$b" 雪　工"#, false).unwrap();
        assert_eq!(done, ["mcpx", "", "a$b"]);
        assert_eq!((current.as_str(), quote), ("雪　工", None));
        assert_eq!(words("false && mcpx ", false).unwrap().0, ["mcpx"]);
        assert_eq!(
            words(r#"mcpx demo echo --mode 'quote\'"#, true).unwrap().1,
            "quote'"
        );
    }

    #[test]
    fn editing_lexer_rejects_evaluation_redirection_and_unbounded_inputs() {
        for line in [
            "mcpx $(touch marker)",
            "mcpx `touch marker`",
            "mcpx \"$TOKEN\"",
            "mcpx demo > out",
            "mcpx (other)",
            "mcpx # comment",
        ] {
            assert!(
                words(line, false).is_none(),
                "accepted unsupported editing context"
            );
        }
        assert!(
            words(
                &"x".repeat(mcpx::command::MAX_COMPLETION_INPUT_BYTES + 1),
                false
            )
            .is_none()
        );
        assert!(words(&"x ".repeat(4097), false).is_none());
        assert_eq!(
            words("mcpx '$(literal)' ", false).unwrap().0,
            ["mcpx", "$(literal)"]
        );
    }

    #[test]
    fn bash_escape_preserves_literal_values_in_each_quote_context() {
        assert_eq!(bash_escape("two words", None), "two\\ words");
        assert_eq!(bash_escape("$(x);*", None), "\\$\\(x\\)\\;\\*");
        assert_eq!(bash_escape("a'b", Some('\'')), "a'\\''b");
        assert_eq!(bash_escape("$x`y`\\\"", Some('"')), "\\$x\\`y\\`\\\\\\\"");
        assert_eq!(bash_escape("雪だるま", None), "雪だるま");
    }
}
