use crate::{Client, Remote, StdioConfig};
pub use mcpx_config::ProtocolMode;
use mcpx_core::{Dialect, Result};
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf};
pub type Config = mcpx_config::Configuration<ServerConfig>;

// Preserve the native constructors/connect method while the wire format is
// deserialized exclusively by the common portable definition.
#[derive(Clone, Deserialize)]
#[serde(from = "mcpx_config::ServerConfig")]
pub enum ServerConfig {
    Http {
        url: String,
        protocol: ProtocolMode,
        credential: Option<String>,
    },
    Stdio {
        program: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        working_directory: Option<PathBuf>,
        protocol: ProtocolMode,
    },
}
impl From<mcpx_config::ServerConfig> for ServerConfig {
    fn from(value: mcpx_config::ServerConfig) -> Self {
        match value {
            mcpx_config::ServerConfig::Http {
                url,
                protocol,
                credential,
            } => Self::Http {
                url,
                protocol,
                credential,
            },
            mcpx_config::ServerConfig::Stdio {
                program,
                args,
                env,
                working_directory,
                protocol,
            } => Self::Stdio {
                program,
                args,
                env,
                working_directory,
                protocol,
            },
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
impl mcpx_config::ServerPaths for ServerConfig {
    fn working_directory_mut(&mut self) -> Option<&mut PathBuf> {
        match self {
            Self::Stdio {
                working_directory, ..
            } => working_directory.as_mut(),
            _ => None,
        }
    }
}
impl ServerConfig {
    pub fn transport(&self) -> &'static str {
        match self {
            Self::Http { .. } => "http",
            Self::Stdio { .. } => "stdio",
        }
    }
    pub async fn connect(
        &self,
        mut client: Client,
        override_protocol: Option<Dialect>,
    ) -> Result<Remote> {
        match self {
            Self::Http {
                url,
                protocol,
                credential,
            } => {
                if let Some(reference) = credential {
                    client = client.with_credential_reference(reference.clone());
                }
                client
                    .connect(url, override_protocol.unwrap_or((*protocol).into()))
                    .await
            }
            Self::Stdio {
                program,
                args,
                env,
                working_directory,
                protocol,
            } => {
                client
                    .connect_stdio(
                        StdioConfig {
                            program: program.clone(),
                            args: args.clone(),
                            env: env.clone(),
                            working_directory: working_directory.clone(),
                        },
                        override_protocol.unwrap_or((*protocol).into()),
                    )
                    .await
            }
        }
    }
}
