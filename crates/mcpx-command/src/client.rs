use crate::{PreparedCommand, prepare, tokenize};
use mcpx_core::{CallOutcome, Client, Driver, DriverContext, Result};
use std::ops::ControlFlow;

pub enum Execution {
    Help(String),
    Outcome(CallOutcome),
}

#[allow(async_fn_in_trait)]
pub trait ClientCommandExt {
    async fn complete_line(&mut self, line: &str, cursor: usize) -> Result<crate::Completion>;
    async fn command_spec(&mut self, tool: &str) -> Result<crate::CommandSpec>;
    async fn execute(&mut self, tool: &str, command: &str) -> Result<Execution>;
    async fn execute_argv(&mut self, tool: &str, argv: &[String]) -> Result<Execution>;
    async fn admit_prepared_command(
        &mut self,
        context: DriverContext,
        tool: &str,
        argv: &[String],
    ) -> Result<ControlFlow<String, u64>>;
}

impl<D: Driver> ClientCommandExt for Client<D> {
    async fn complete_line(&mut self, line: &str, cursor: usize) -> Result<crate::Completion> {
        // Validate the editing request before resolving credentials or doing discovery.
        if line.len() > crate::MAX_COMPLETION_INPUT_BYTES {
            return Err(mcpx_core::Error::local(
                "LimitExceeded",
                "completion input exceeds 64 KiB",
            ));
        }
        if cursor > line.len() || !line.is_char_boundary(cursor) {
            return Err(mcpx_core::Error::local(
                "InvalidCursor",
                "completion cursor must be a UTF-8 byte boundary",
            ));
        }
        self.with_fresh_catalog(|machine| {
            crate::complete_line(machine.catalog().tools(), line, cursor)
        })
        .await
    }
    async fn command_spec(&mut self, tool: &str) -> Result<crate::CommandSpec> {
        self.with_fresh_catalog(|machine| {
            let descriptor = machine
                .catalog()
                .tools()
                .find(|item| item.name == tool)
                .ok_or_else(|| {
                    mcpx_core::Error::local("ToolNotFound", "tool is not in the current catalog")
                })?;
            Ok(crate::CommandSpec::from_descriptor(descriptor))
        })
        .await
    }
    async fn execute(&mut self, tool: &str, command: &str) -> Result<Execution> {
        self.execute_argv(tool, &tokenize(command)?).await
    }
    async fn execute_argv(&mut self, tool: &str, argv: &[String]) -> Result<Execution> {
        match self
            .call_tool_with(tool, |binding| prepared(binding, argv))
            .await?
        {
            ControlFlow::Break(text) => Ok(Execution::Help(text)),
            ControlFlow::Continue(outcome) => Ok(Execution::Outcome(outcome)),
        }
    }
    async fn admit_prepared_command(
        &mut self,
        context: DriverContext,
        tool: &str,
        argv: &[String],
    ) -> Result<ControlFlow<String, u64>> {
        self.admit_prepared_tool(context, tool, |binding| prepared(binding, argv))
            .await
    }
}

fn prepared(
    binding: &mcpx_core::ToolBinding,
    argv: &[String],
) -> Result<ControlFlow<String, mcpx_core::BoundArgs>> {
    Ok(match prepare(binding, argv)? {
        PreparedCommand::Help(text) => ControlFlow::Break(text),
        PreparedCommand::Call(args) => ControlFlow::Continue(args),
    })
}
