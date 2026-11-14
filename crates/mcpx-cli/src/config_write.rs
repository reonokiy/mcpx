use crate::routing::{Invocation, Route};
use mcpx::{Error, Result};
use std::{fs, io::Write, path::Path};
use toml_edit::{Array, DocumentMut, Item, Table, value};

fn write_error() -> Error {
    Error::local("ConfigurationError", "cannot update mcpx configuration")
}

fn table<'a>(parent: &'a mut Table, key: &str) -> Result<&'a mut Table> {
    let item = parent.entry(key).or_insert_with(|| {
        let mut table = Table::new();
        table.set_implicit(true);
        Item::Table(table)
    });
    if !item.is_table() {
        *item = Item::Table(
            std::mem::take(item)
                .into_table()
                .map_err(|_| write_error())?,
        );
    }
    item.as_table_mut().ok_or_else(write_error)
}

pub(crate) fn add(invocation: &Invocation) -> Result<u8> {
    let Route::Add {
        name,
        url,
        command,
        credential,
    } = &invocation.route
    else {
        unreachable!()
    };
    if invocation.url.is_some() {
        return Err(Error::local(
            "CommandParseError",
            "use servers add NAME --url ENDPOINT, without a global --url",
        ));
    }

    let path = &invocation.config;
    let original = match fs::read_to_string(path) {
        Ok(text) => {
            mcpx::config::Config::parse(&text)?;
            text
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(_) => return Err(write_error()),
    };
    let mut document = original.parse::<DocumentMut>().map_err(|_| write_error())?;
    let profile = invocation
        .profile
        .as_deref()
        .or_else(|| document.get("default_profile").and_then(Item::as_str))
        .unwrap_or("default")
        .to_owned();
    let profile = profile.as_str();
    if document.get("schema_version").is_none() {
        document["schema_version"] = value(1);
    }
    // Move the original table rather than rewriting it, preserving its comments.
    if let Some(legacy) = document.remove("servers") {
        table(table(document.as_table_mut(), "profiles")?, "default")?["servers"] = legacy;
    }
    if document
        .get("profiles")
        .and_then(|p| p.get(profile))
        .and_then(|p| p.get("servers"))
        .and_then(|p| p.get(name))
        .is_some()
    {
        return Err(Error::local(
            "ArgumentConflict",
            "server already exists in this profile",
        ));
    }
    let mut server = Table::new();
    if let Some(endpoint) = url {
        mcpx::HostRuntime::auto(endpoint.clone())?;
        server["transport"] = value("http");
        server["url"] = value(endpoint);
        if let Some(reference) = credential {
            server["credential"] = value(reference);
        }
    } else {
        let program = command.first().ok_or_else(|| {
            Error::local("CommandParseError", "a server URL or command is required")
        })?;
        server["transport"] = value("stdio");
        server["program"] = value(program);
        let mut arguments = Array::new();
        for argument in &command[1..] {
            arguments.push(argument.as_str());
        }
        server["args"] = value(arguments);
    }
    server["protocol"] = value(match invocation.selected {
        Some(mcpx::Dialect::Modern) => "2026-07-28",
        Some(mcpx::Dialect::Legacy) => "2025-11-25",
        _ => "auto",
    });
    table(
        table(table(document.as_table_mut(), "profiles")?, profile)?,
        "servers",
    )?[name] = Item::Table(server);
    let text = document.to_string();
    mcpx::config::Config::parse(&text)?;
    persist(path, &text)?;
    if invocation.output == "text" {
        println!("Added {name} to profile {profile} in {}", path.display());
    } else {
        crate::shared::print_json(
            &serde_json::json!({"server":name,"profile":profile,"config":path}),
        );
    }
    Ok(0)
}

fn persist(path: &Path, text: &str) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|_| write_error())?;
    if let Ok(metadata) = fs::metadata(path) {
        temporary
            .as_file()
            .set_permissions(metadata.permissions())
            .map_err(|_| write_error())?;
    }
    temporary
        .write_all(text.as_bytes())
        .map_err(|_| write_error())?;
    temporary.as_file().sync_all().map_err(|_| write_error())?;
    temporary.persist(path).map_err(|_| write_error())?;
    Ok(())
}

pub(crate) fn use_profile(invocation: &Invocation, name: &str) -> Result<u8> {
    if invocation.url.is_some() {
        return Err(Error::local(
            "CommandParseError",
            "profiles use requires a configuration file",
        ));
    }
    let mut config = mcpx::config::Config::load_unresolved(&invocation.config)?;
    config.select_profile(name)?;
    let original = fs::read_to_string(&invocation.config).map_err(|_| write_error())?;
    let mut document = original.parse::<DocumentMut>().map_err(|_| write_error())?;
    document["default_profile"] = value(name);
    let text = document.to_string();
    mcpx::config::Config::parse(&text)?;
    persist(&invocation.config, &text)?;
    if invocation.output == "text" {
        println!("Default profile: {name}");
    } else {
        crate::shared::print_json(
            &serde_json::json!({"default_profile":name,"config":invocation.config}),
        );
    }
    Ok(0)
}
