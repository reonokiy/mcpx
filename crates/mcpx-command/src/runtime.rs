use crate::{PreparedCommand, prepare};
pub use mcpx_core::HostRuntime;
use mcpx_core::Result;

/// A local command result or an operation whose I/O is owned by the host.
pub enum CommandAdmission {
    Help(String),
    Operation(u64),
}
impl CommandAdmission {
    pub fn to_json(&self) -> String {
        match self {
            Self::Help(text) => serde_json::json!({"kind":"help","value":text}),
            Self::Operation(id) => serde_json::json!({"kind":"operation","value":id}),
        }
        .to_string()
    }
}

pub trait CommandRuntimeExt {
    /// A local snapshot only; asynchronous hosts must apply authorization/TTL preflight first.
    fn complete_line(&self, line: &str, cursor: usize) -> Result<crate::Completion>;
    fn command_spec(&self, name: &str) -> Result<crate::CommandSpec>;
    fn command(&mut self, name: &str, argv: &[String]) -> Result<Option<String>>;
    fn command_concurrent(&mut self, name: &str, argv: &[String]) -> Result<CommandAdmission>;
    fn command_line(&mut self, line: &str) -> Result<Option<String>> {
        let argv = crate::tokenize(line)?;
        let (name, args) = argv
            .split_first()
            .ok_or_else(|| mcpx_core::Error::local("CommandError", "expected a tool name"))?;
        self.command(name, args)
    }
    fn command_line_concurrent(&mut self, line: &str) -> Result<CommandAdmission> {
        let argv = crate::tokenize(line)?;
        let (name, args) = argv
            .split_first()
            .ok_or_else(|| mcpx_core::Error::local("CommandError", "expected a tool name"))?;
        self.command_concurrent(name, args)
    }
}

impl CommandRuntimeExt for HostRuntime {
    fn complete_line(&self, line: &str, cursor: usize) -> Result<crate::Completion> {
        if self.catalog_needs_refresh_at(self.machine().now_ms()) {
            return Err(mcpx_core::Error::local(
                "CatalogStale",
                "refresh the authorized catalog before completing",
            ));
        }
        crate::complete_line(self.machine().catalog().tools(), line, cursor)
    }
    fn command_spec(&self, name: &str) -> Result<crate::CommandSpec> {
        if self.catalog_needs_refresh_at(self.machine().now_ms()) {
            return Err(mcpx_core::Error::local(
                "CatalogStale",
                "refresh the authorized catalog before reading command metadata",
            ));
        }
        let descriptor = self
            .machine()
            .catalog()
            .tools()
            .find(|tool| tool.name == name)
            .ok_or_else(|| {
                mcpx_core::Error::local("ToolNotFound", "tool is not in the current catalog")
            })?;
        Ok(crate::CommandSpec::from_descriptor(descriptor))
    }
    fn command(&mut self, name: &str, argv: &[String]) -> Result<Option<String>> {
        let binding = self.tool_binding(name)?;
        match prepare(&binding, argv)? {
            PreparedCommand::Help(text) => Ok(Some(text)),
            PreparedCommand::Call(args) => {
                self.call_bound(&binding, &args)?;
                Ok(None)
            }
        }
    }
    fn command_concurrent(&mut self, name: &str, argv: &[String]) -> Result<CommandAdmission> {
        let binding = self.tool_binding(name)?;
        match prepare(&binding, argv)? {
            PreparedCommand::Help(text) => Ok(CommandAdmission::Help(text)),
            PreparedCommand::Call(args) => self
                .call_concurrent_bound(&binding, &args)
                .map(CommandAdmission::Operation),
        }
    }
}
