//! Shared named-server configuration. Parsing performs no I/O; `load` explicitly
//! reads a bounded file through the caller's filesystem capabilities.
use mcpx_core::{Dialect, Error, Result};
use serde::{Deserialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub const MAX_CONFIG_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub enum ProtocolMode {
    #[default]
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "2026-07-28")]
    Modern,
    #[serde(rename = "2025-11-25")]
    Legacy,
}
impl From<ProtocolMode> for Dialect {
    fn from(mode: ProtocolMode) -> Self {
        match mode {
            ProtocolMode::Auto => Self::Auto,
            ProtocolMode::Modern => Self::Modern,
            ProtocolMode::Legacy => Self::Legacy,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(tag = "transport", rename_all = "lowercase", deny_unknown_fields)]
pub enum ServerConfig {
    Http {
        url: String,
        #[serde(default)]
        protocol: ProtocolMode,
        #[serde(default)]
        credential: Option<String>,
    },
    Stdio {
        program: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default)]
        working_directory: Option<PathBuf>,
        #[serde(default)]
        protocol: ProtocolMode,
    },
}
impl ServerConfig {
    pub fn transport(&self) -> &'static str {
        match self {
            Self::Http { .. } => "http",
            Self::Stdio { .. } => "stdio",
        }
    }
}
impl std::fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerConfig")
            .field("transport", &self.transport())
            .finish_non_exhaustive()
    }
}

/// Path resolution hook for platform adapters that retain their own server API.
pub trait ServerPaths {
    fn working_directory_mut(&mut self) -> Option<&mut PathBuf>;
}
impl ServerPaths for ServerConfig {
    fn working_directory_mut(&mut self) -> Option<&mut PathBuf> {
        match self {
            Self::Stdio {
                working_directory, ..
            } => working_directory.as_mut(),
            _ => None,
        }
    }
}

fn version() -> u32 {
    1
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, bound(deserialize = "S: Deserialize<'de>"))]
pub struct Configuration<S> {
    #[serde(default = "version")]
    pub schema_version: u32,
    #[serde(default)]
    pub servers: BTreeMap<String, S>,
}
pub type Config = Configuration<ServerConfig>;
impl<S: DeserializeOwned + ServerPaths> Configuration<S> {
    pub fn parse(text: &str) -> Result<Self> {
        if text.len() > MAX_CONFIG_BYTES {
            return Err(Error::local("LimitExceeded", "configuration exceeds 1 MiB"));
        }
        let config: Self = toml::from_str(text)
            .map_err(|_| Error::local("ConfigurationError", "invalid mcpx TOML configuration"))?;
        if config.schema_version != 1 {
            return Err(Error::local(
                "ConfigurationError",
                "unsupported configuration version",
            ));
        }
        for alias in config.servers.keys() {
            validate_alias(alias)?;
        }
        Ok(config)
    }
    /// Read a bounded UTF-8 file without resolving stdio working directories.
    /// Hosts that cannot launch processes need no path canonicalization capability.
    pub fn load_unresolved(path: &Path) -> Result<Self> {
        use std::io::Read;
        let file = std::fs::File::open(path)
            .map_err(|_| Error::local("ConfigurationError", "cannot open mcpx configuration"))?;
        let mut bytes = Vec::new();
        file.take(MAX_CONFIG_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::local("ConfigurationError", "cannot read mcpx configuration"))?;
        if bytes.len() > MAX_CONFIG_BYTES {
            return Err(Error::local("LimitExceeded", "configuration exceeds 1 MiB"));
        }
        Self::parse(
            std::str::from_utf8(&bytes)
                .map_err(|_| Error::local("ConfigurationError", "configuration is not UTF-8"))?,
        )
    }
    pub fn load(path: &Path) -> Result<Self> {
        let mut config = Self::load_unresolved(path)?;
        // Resolving a working directory is relevant only for descriptors that
        // have one; HTTP-only files need no additional path/cwd capabilities.
        if config.servers.values_mut().any(|server| {
            server
                .working_directory_mut()
                .is_some_and(|path| path.is_relative())
        }) {
            let canonical = std::fs::canonicalize(path).map_err(|_| {
                Error::local("ConfigurationError", "cannot resolve configuration path")
            })?;
            let base = canonical.parent().ok_or_else(|| {
                Error::local(
                    "ConfigurationError",
                    "configuration has no parent directory",
                )
            })?;
            for server in config.servers.values_mut() {
                if let Some(directory) = server.working_directory_mut()
                    && directory.is_relative()
                {
                    *directory = base.join(&directory);
                }
            }
        }
        Ok(config)
    }
    pub fn server(&self, alias: &str) -> Result<&S> {
        self.servers
            .get(alias)
            .ok_or_else(|| Error::local("RemoteNotFound", "server alias is not configured"))
    }
}

pub fn validate_alias(alias: &str) -> Result<()> {
    if alias.is_empty()
        || matches!(
            alias,
            "tools" | "call" | "servers" | "complete" | "completions" | "__complete"
        )
        || !alias
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        return Err(Error::local(
            "InvalidAlias",
            "server alias is invalid or reserved",
        ));
    }
    Ok(())
}
