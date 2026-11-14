# mcpx

![mcpx header](docs/assets/banner.svg)

**Turn MCP tools into CLI commands and runtime API calls.**

`mcpx` is a general-purpose CLI and embeddable library for the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). Connect to MCP servers, discover their tools at runtime, and invoke them through one consistent command syntax or library API—across native Rust, browser WebAssembly, and WASI. No tool-specific client generation is required.

**[Documentation](docs/) · [Examples](examples/) · [Releases](https://github.com/reonokiy/mcpx/releases) · [Implementation status](docs/implementation-status.md)**

## Why mcpx?

- **A universal MCP CLI**: Discover tools and call them as `mcpx SERVER TOOL --flag value`, with schema-derived help, flags, and shell completion.
- **An embeddable library**: Use the same runtime bindings with typed Rust arguments, JSON, command strings, or argv instead of spawning a process.
- **Multiple environments**: Run the native CLI or Rust library locally, embed command-style invocation in browser Web Workers, or use the WASI Component API and standalone CLI.
- **Runtime discovery and validation**: Tool definitions come from the server, and arguments are validated against their input schemas before invocation.
- **No generated tool clients**: Add a server configuration and use its tools without generating a new CLI, Rust SDK, or TypeScript SDK for each service.

## Use it as a CLI

Download a prebuilt binary from [Releases](https://github.com/reonokiy/mcpx/releases), or run the CLI from this repository:

```bash
# List the tools exposed by the configured stdio MCP server.
cargo run -p mcpx-cli -- --config mcpx.example.toml tools demo

# Tool flags and help are derived from the server's schema.
cargo run -p mcpx-cli -- --config mcpx.example.toml demo echo --help

# Invoke the remote MCP tool like a local command.
cargo run -p mcpx-cli -- --config mcpx.example.toml demo echo --message "hello"
```

The included [example configuration](mcpx.example.toml) defines the `demo` server. After installing the `mcpx` binary, the invocation becomes:

```bash
mcpx --config mcpx.example.toml demo echo --message "hello"
```

Prefer structured input? Use the same tool through the explicit call route:

```bash
mcpx --config mcpx.example.toml call demo echo --args '{"message":"hello"}'
```

Direct HTTP endpoints work without a configuration file:

```bash
mcpx --url https://example.com/mcp tools
mcpx --url https://example.com/mcp TOOL --help
```

HTTP and stdio transports are available in the native CLI. Named server configuration, OAuth, text/JSON/NDJSON output, argument files, and dynamic Bash/Zsh/Fish completion are also supported.

## Use it as a library

Embed the same discovery, validation, command parsing, and invocation engine in an application. Prefer native tool bindings and argument builders in application code; CLI-style execution is also available when accepting command strings from users:

```rust,no_run
use mcpx::{native::Client, Dialect};

async fn example() -> mcpx::Result<()> {
    let mut remote = Client::new()?
        .connect("https://example.com/mcp", Dialect::Modern)
        .await?;

    // Native library API: bind the tool, build arguments, and call it directly.
    let search = remote.fresh_tool("search_repositories").await?;
    let args = search.args()
        .set("query", "rust")?
        .set("limit", 10)?
        .finish()?;
    let outcome = remote.call(&search, &args).await?;

    // Optional command interface: execute the same tool using CLI-style input.
    let command_outcome = remote
        .execute("search_repositories", "--query rust --limit 10")
        .await?;

    Ok(())
}
```

Typed Rust arguments, JSON objects, and argv are available as well. The caller owns the async runtime; no tool-specific Rust types are generated.

| Environment | Library and command interface |
| --- | --- |
| **Native** | Rust library and native CLI, with HTTP and stdio transports |
| **Browser** | JavaScript / Web Worker bindings powered by WebAssembly, with CLI-style commands over HTTP |
| **WASI** | Typed Component API and standalone WASIp2 CLI over `wasi:http` |

Explore the [browser CLI example](examples/web-worker/) and [WASI Component example](examples/wasi-component/) for complete embedding setups.

## Documentation & support

- [Technical design](docs/technical-design.md) and [implementation status](docs/implementation-status.md)
- Found a bug? [Open an issue](https://github.com/reonokiy/mcpx/issues).
- Have a question or feedback? Start a [discussion](https://github.com/reonokiy/mcpx/discussions).

---

*mcpx is licensed under the [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) license.*
