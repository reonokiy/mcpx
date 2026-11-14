//! Runtime MCP bindings. Remote tool schemas never require generated Rust code.
#[cfg(feature = "command")]
pub use mcpx_command as command;
#[cfg(feature = "command")]
pub use mcpx_command::ClientCommandExt;
#[cfg(feature = "config")]
pub use mcpx_config as config;
pub use mcpx_core::*;
#[cfg(feature = "native")]
pub use mcpx_native as native;
pub use mcpx_protocol as protocol;
#[cfg(feature = "wasi")]
pub use mcpx_wasi as wasi;
