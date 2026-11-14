# mcpx

Runtime MCP tool bindings in Rust, with native CLI, browser WebAssembly, a WASI Component API and a standalone WASI CLI. Remote tools are discovered through MCP and validated against their input schemas. No tool-specific source generation is required.

**Test release: [v0.1.0-alpha.2](https://github.com/reonokiy/mcpx/releases/tag/v0.1.0-alpha.2).** Download the native CLI, standalone WASI CLI, Web/Component bundle, or Rust workspace source from the release assets. This alpha is intended for early testing; APIs may change. The CLI supports named HTTP/stdio servers and direct HTTP endpoints.

For Linux x86_64 with glibc 2.39 or newer:

```bash
gh release download v0.1.0-alpha.2 --repo reonokiy/mcpx --pattern '*x86_64-unknown-linux-gnu.tar.gz'
tar -xzf mcpx-v0.1.0-alpha.2-x86_64-unknown-linux-gnu.tar.gz
./mcpx-v0.1.0-alpha.2-x86_64-unknown-linux-gnu/mcpx --version
```

Release assets are built and uploaded by [GitHub Actions](https://github.com/reonokiy/mcpx/actions/workflows/release.yml). macOS and Windows native builds are included alongside Linux; choose the target triple matching your system. Use the source archive to build on other native platforms. The full implementation plan and remaining work are documented in [the technical design](docs/technical-design.md) and [implementation status](docs/implementation-status.md).

## Implemented

- Remote catalogs, atomic pagination, lazy Schema compilation, a configurable validator LRU and immutable tool bindings.
- Runtime argument builders, JSON objects, command strings and argv with shared validation.
- Shared CommandSpec for parsing/help and live tool, flag, boolean and enum completion on every host.
- Input validation with JSON Schema Draft 2020-12, conditional schemas, internal references and host-preloaded external resources.
- Automatic version probing plus explicit `2026-07-28` discovery and `2025-11-25` initialization over HTTP and stdio.
- Modern HTTP routing headers, safe value encoding, parameter header mapping and invalid-annotation filtering.
- JSON and incremental SSE responses, progress records and output validation.
- Native Rust HTTP client and CLI; no process-wide runtime is created by the library.
- Native child-process transport with bounded message framing, independent stderr draining and explicit graceful close.
- TOML server configuration, argument files, text/JSON/NDJSON output and native Ctrl-C cleanup.
- Browser Wasm bindings, fetch driver and an asynchronous JS wrapper.
- WASI Component with a host-driven I/O API, plus a standalone WASIp2 CLI/HTTP driver exercised in Wasmtime.
- A platform-independent asynchronous `Client<Driver>`; native HTTP/stdio use the same host runtime as the Wasm entry points.
- Pluggable asynchronous credential providers, authorization-scoped bindings and catalog TTL refresh in the Rust client.
- Operation-scoped cancellation handles, checked calls preserving raw failure results, and bounded asynchronous native event streams.

## Native CLI

Rust 1.100.0 and the Wasm targets are pinned in `rust-toolchain.toml`. Rustup installs missing components when running Cargo from this directory.

```bash
cargo run -p mcpx-cli -- --help
cargo run -p mcpx-cli -- --url https://example.com/mcp tools
cargo run -p mcpx-cli -- --url https://example.com/mcp search_repositories --help
cargo run -p mcpx-cli -- --url https://example.com/mcp search_repositories --query rust
cargo run -p mcpx-cli -- --url https://example.com/mcp search_repositories --args '{"query":"rust"}'
```

The default protocol is `auto`; an override such as `--protocol 2025-11-25` goes before the route. `--token-env VARIABLE_NAME` reads a bearer token in the native entry point; it is never included in the catalog or default diagnostics. Output defaults to text; `--output json` returns an outcome envelope and `--output ndjson-events` streams progress and a final record. Business failures and invalid or indeterminate structured output produce a nonzero exit code.

Named servers are read from `mcpx.toml` in the working directory or from `--config FILE`. See [the example configuration](mcpx.example.toml). The default credential resolver supports `env:VARIABLE_NAME`; embedding applications can supply a `CredentialProvider` for other named references. Native and WASI use the same configuration schema and CLI routes. Parsing rejects unknown fields, unsupported versions and reserved aliases; both text and file sources are limited to 1 MiB. Listing servers is offline and prints only alias/transport metadata. Default Debug output also omits endpoint, credential, process arguments and environment values.

```bash
cargo run -p mcpx-cli -- --config mcpx.example.toml servers list
cargo run -p mcpx-cli -- --config mcpx.example.toml tools demo
cargo run -p mcpx-cli -- --config mcpx.example.toml demo echo --message hello
cargo run -p mcpx-cli -- --config mcpx.example.toml call demo echo --args-file request.json
```

`--args-file -` reads JSON from stdin. Global options precede the server/tool route, so remote tool flags do not collide with CLI output or protocol options. Ctrl-C returns exit code 130 and releases an active child process; a timed-out stdio connection is closed rather than reused with a potentially partial response.

Management names `auth`, `tools`, `call`, `servers`, `complete`, `completions` and `__complete` are reserved server aliases. Use `call SERVER TOOL` to address a remote tool with a management name, or `--url ENDPOINT call TOOL` for a direct endpoint.

### OAuth login (native CLI)

For a protected HTTP server, configure OAuth instead of an environment token:

```toml
[servers.private]
transport = "http"
url = "https://your-server.example/mcp"
credential = "oauth"
```

```bash
mcpx auth login private
mcpx private echo --message hello
mcpx auth status private
mcpx auth logout private
```

Login opens the system browser and receives the authorization code on a loopback
callback. Credentials are saved to the OS credential store by default. The CLI
silently refreshes expiring tokens and saves refresh-token rotations; independent
CLI processes serialize refresh for the same resource. Refresh preserves the
binding context unless granted scopes change. Logout removes local credentials
and requests revocation when the authorization server advertises it. An already
failed tool request is never replayed after authentication.

Discovery follows MCP protected-resource metadata, OAuth/OIDC server metadata and
PKCE S256. Pre-registered public clients (`--client-id ID`), CIMD and legacy DCR are
supported. The official CIMD document is hosted at
<https://reonokiy.github.io/mcpx/oauth/native.json> and uses
`http://127.0.0.1:48721/callback`. Use `--redirect-port PORT` for a pre-registered or
DCR client; port `0` requests an ephemeral port. The official CIMD needs its fixed
port. `--scope SCOPE` adds required permissions; `--no-browser` prints the URL for
manual opening. The global `--timeout` bounds the entire login.

On a host without a usable OS credential store, opt into private-file storage
explicitly: `mcpx auth login private --store file`. Files are atomically replaced
and created with mode 0600 on Unix, beneath the platform's per-user state directory;
`MCPX_AUTH_DIR` overrides that directory. File storage is unencrypted. No plaintext
fallback happens automatically. `--output json auth status private` reports expiry
and scopes without tokens. Direct URLs also work with
`mcpx --url https://your-server.example/mcp auth login`; saved grants are then
reused for that endpoint. Explicit `--token-env` and configured `env:` credentials
retain precedence.

Use `--profile ACCOUNT` before the route to keep separate accounts for the same
server, for example `mcpx --profile alice auth login private` and
`mcpx --profile bob private echo --message hello`. Persistent records and refresh
locks are keyed by profile plus canonical MCP resource. A configuration may use
`credential = "oauth:alice"`; an explicit `--profile` overrides it.

Rust embedders can use `mcpx::native::oauth::OAuthCredentialProvider`, or select
`credential = "oauth"` in a native server configuration. `resolve` never opens a
browser; missing/invalid refresh grants return `LoginRequired`. The shared
`mcpx-auth` crate contains the authorization protocol and a host HTTP trait.
For multi-user applications, create one Provider/Remote per authenticated user.
`OAuthCredentialProvider::from_grant(grant)` keeps that user's tokens in memory;
cloning it shares only that user's refresh state. `for_profile(endpoint, account)`
selects isolated persistent state. Different users have independent refresh locks,
authorization contexts, catalogs, typed bindings and request credentials; the HTTP
connection pool may be reused without setting a global Authorization header.
`forget().await` clears only that identity's shared state. The application must
select profiles from its trusted authentication context, not an untrusted request
parameter. A binding from one Remote cannot be used as another user's binding.
Interactive OAuth in browser/Worker and WASI hosts is not included in this alpha.

### Dynamic command completion

```bash
cargo run -p mcpx-cli -- --config mcpx.toml --output json complete demo --line 'echo --m'
cargo run -p mcpx-cli -- --url https://example.com/mcp --output json complete --line 'echo --mode tw'
cargo run -p mcpx-cli -- complete --help
```

The line starts with a tool name, without `mcpx` or a server alias. `--cursor` optionally selects a UTF-8 byte offset; the default is the line's end. Only the selected server is discovered. Completion uses its current authorized catalog and never sends `tools/call`. HTTP and stdio both work. A new CLI process discovers its catalog again; no persistent or cross-process cache is implied. Completion help is available without loading a configuration.

The JSON result contains `token` (the decoded prefix), `start`/`end` (the entire token's UTF-8 byte range), `candidates` and `truncated`. A candidate has `value`, `replacement`, `description` and `kind` (`tool`, `option` or `value`). Replace the whole range, including any quoted suffix after the cursor, with `replacement`. Unclosed quotes and trailing escapes are accepted while editing; executing a command still requires valid syntax. The candidate's `value` is a literal logical argv token; `replacement` is quoted for mcpx's tokenizer. Shell adapters can quote `value` using their shell's rules.

Candidates cover tool names, available flags, explicit boolean values and primitive Schema enums, including `--flag=prefix`. Used flags are omitted; `--args` and business flags remain mutually exclusive. Object/array and ambiguous Schema branches use their existing JSON paths. File paths, environment variables and command substitutions are not expanded by completion. Each request accepts at most 64 KiB and 4096 tokens; the result has at most 128 candidates and 64 KiB of JSON. `truncated` reports omitted candidates; type a longer prefix to narrow them.

Number flags use the shared JSON numeric parser: an integer literal within i64/u64 remains exact even when its Schema type is `number`. Out-of-range integers and non-finite numbers are rejected before dispatch. Numeric enum candidates remain strings throughout the host bridge.

`mcpx::command::CommandSpec::from_binding()` / `from_descriptor()` expose the same projection used by help and parsing: parameter names/flags, descriptions, required status, value kinds and enum candidates, plus `json_only` property names. Reserved properties such as `args`, `args-file` and `help`, or names that cannot become flags, stay callable through `--args`. Suggestions do not replace full Schema validation.

Rust `Remote::command_spec(tool).await` and `complete_line(line, cursor).await` resolve credentials once and refresh an expired catalog before reading it. The core `ClientCommandExt` provides the same methods; the queued native methods return cancellable `QueuedOperation`s and act as catalog barriers. Pure `complete_line(descriptors, line, cursor)` and `CommandSpec::complete_argv(completed, current)` support embedding without I/O; the latter accepts already-tokenized arguments, excluding the tool name, and returns literal replacements. A saved CommandSpec is a metadata snapshot; the caller manages its freshness.

Direct JS and WorkerRemote provide `await remote.commandSpec(tool)` and `await remote.completeLine(line, cursor, options)`. They use ordinary deadline/cancellation and authorization/TTL preflight; an omitted cursor means the end. Browser selections use UTF-16 positions, so convert `line.slice(0, selectionStart)` to UTF-8 bytes before passing the cursor. The browser CLI example supports Tab and Complete, including selectable candidates.

Low-level Wasm exports `command_spec` and `complete_line` with JSON results. WIT uses typed `command-spec`, `parameter-spec`, `command-completion` and candidate records; jco exposes `commandSpec` / `completeLine`. These local reads return `CatalogStale` until the host discovers or refreshes its authorized catalog. The fixed `componentRuntime()` adapter gives them the shared JS shape. These interfaces describe remote data at runtime; no tool SDK is generated.

### Shell completion

With an executable named `mcpx` on PATH, load a generic adapter in the corresponding shell:

```bash
# Bash
source <(mcpx completions bash)
# Zsh (after compinit)
source <(mcpx completions zsh)
# Fish
mcpx completions fish | source
```

`completions SHELL` prints a fixed script offline, without loading configuration or resolving credentials. Tool names, flags and enum values come from the selected server's authorized catalog when you press Tab. These adapters do not generate a tool-specific SDK or send `tools/call`. A completion starts a new CLI process; a configured stdio server may start for discovery and is closed afterward. MCP operations use a timeout capped at two seconds each; this is not a whole-process or synchronous validation CPU limit. No persistent catalog cache is used.

Root aliases, management commands and global option values are completed locally. Only explicit `--config` and `--schema-registry` paths use directory listing, limited to 4096 scanned entries and 128 matches. WASI supports the same adapters through an executable `mcpx` launcher that grants its required HTTP/filesystem capabilities; a shell alias or function alone does not suffice because adapters use `command mcpx`.

Candidates are inserted as literal arguments using each shell's quoting rules. The editing grammar handles ordinary quotes, escapes and simple pipeline separators, without evaluating expressions. Unsupported expansions, substitutions, redirections and tilde expansion return no candidates. Zsh also conservatively rejects completed words containing dollar signs or backticks after its own quote removal. Empty candidates and control characters are omitted. Input is limited to 64 KiB/4096 tokens, and emitted candidates to 128/64 KiB after quoting. Broader shell syntax, cursor-in-word suffixes, platforms and shell versions remain outside the current interactive test matrix.

For library embedding, `mcpx::command::complete_argv(descriptors, completed, current)` accepts logical argv words including the tool name, without parsing a command string. `CommandSpec::complete_argv` accepts only that tool's arguments. Both are pure metadata operations; the caller manages catalog freshness.

For a local example, run `node scripts/mock-server.mjs` and use the printed endpoint:

```bash
cargo run -p mcpx-cli -- --url http://127.0.0.1:PORT/mcp echo --message hello
```

## Rust library

Enable the `native` feature of the local `mcpx` crate for its HTTP adapter. The caller owns the Tokio runtime. The initial adapter serializes operations through `&mut Remote`.

```rust,no_run
use mcpx::{Dialect, native::Client};

async fn example() -> mcpx::Result<()> {
    let mut remote = Client::new()?
        .connect("https://example.com/mcp", Dialect::Modern)
        .await?;
    let search = remote.fresh_tool("search_repositories").await?;
    let args = search.args()
        .set("query", "rust")?
        .set("limit", 10)?
        .finish()?;

    let outcome = remote.call(&search, &args).await?;
    let command_outcome = remote.execute(
        "search_repositories", "--query rust --limit 10"
    ).await?;
    Ok(())
}
```

`finish()` checks the whole object, including conditional constraints. Missing values remain missing, `null` remains explicit, and Schema defaults are not injected. The builder does not coerce a Rust string into a number. Bindings and bound arguments cannot be silently reused against a different remote/tool.

For full command routing, register an already-connected remote with `mcpx::native::Engine::register("github", remote)` and call `engine.execute("github search_repositories --query rust").await`. `execute_argv()` accepts the equivalent argument array. `Config::load()` provides the same named server configuration to embedding applications. `Client::connect_stdio(StdioConfig, Dialect)` connects a child process. Call `remote.close().await` for graceful shutdown. Once closing starts, new operations stay closed; failed or dropped cleanup can be retried by awaiting `close()` again, and a successful close is idempotent.

Tool business errors stay in `CallOutcome::Completed`; `NeedsInput` preserves an unfinished result. Output validation preserves raw content rather than treating text as JSON. Callers inspect these states instead of assuming every `Ok` is business success.

### Caller-owned Rust types

`args_from()` accepts any `Serialize` argument object and validates it against the current binding's remote Schema. Caller-defined types are optional; remote tools still require no SDK generation:

```rust,ignore
#[derive(serde::Serialize)]
struct EchoArgs<'a> { message: &'a str }
#[derive(serde::Deserialize)]
struct EchoReply { message: String }

let args = echo.args_from(EchoArgs { message: "hello" })?;
let response = remote.call_checked(&echo, &args).await?;
let decoded: EchoReply = response.deserialize()?;
// response.raw remains available, including protocol extensions.
```

`remote.call_typed(&echo, arguments).await` is the ordinary `CallOutcome` path for direct typed inputs. It keeps business failures and `NeedsInput` explicit, just like `call_json`. Queued bindings provide `args_from`, `submit_typed` and `call_typed`. `submit_typed` serializes locally before returning the queued operation, so the source can be non-Send even when the resulting operation moves to another thread. Binding/lease identity, authorization, deadlines and remote Schema validation continue through the same executor.

Argument conversion invokes each caller serializer once and writes directly into a JSON Value. It preserves Serde renames, enum representation, skipped fields, explicit nulls and exact i64/u64 values; it rejects non-finite float events before they can become null. Conversion limits Serde nesting to 128; the resulting JSON keeps the existing 64-level/1 MiB argument limits. Serialized size is counted without allocating a second JSON document. These checks do not impose a hard allocator/CPU limit on arbitrary caller code.

`ToolResponse::deserialize<T: DeserializeOwned>()` reads only an object in `structuredContent`, borrows the response and never treats text content as JSON. It rejects `ToolFailure`, `OutputValidation` and `OutputValidationLimitExceeded` first; absent/non-object structured content yields `StructuredContentMissing`, and a Rust type mismatch yields `DeserializationFailed`. Returned serializer/deserializer errors use fixed diagnostic messages; raw results remain available. Field and numeric conversions follow the requested Rust type's Serde rules; use integer fields when exact integral values are required.

### Validation diagnostics and regex limits

Input validation returns the first JSON Pointer diagnostic. The validator eagerly collects errors in `iter_errors()`, so truncating its iterator after 32 did not bound flat error generation. The current path uses `validate()`. It preserves whole-object Schema semantics; combinators can still allocate internal diagnostics. The [native allocation report](docs/benchmarks/schema-memory-native.md) compares both paths without claiming a general allocator/CPU bound.

Schema policy version 2 audits every Schema node in the root and registered bundle, including unused definitions and explicitly referenced annotation data. Ordinary annotation/const/enum data is not treated as a Schema unless referenced.

| Regex admission limit | Value |
| --- | --- |
| Pattern occurrences per audited closure | 256 |
| UTF-8 source bytes per pattern / total | 4096 / 64 KiB |
| AST depth / finite repetition bounds | 64 / 4096 |
| Conservative finite static-work estimate | 65,536 |
| VM instructions per pattern | 16,384 |
| Approximate delegate NFA / DFA-cache size per pattern | 1 MiB / 1 MiB |
| Backtracking attempts per match | 20,000 |

Preflight uses the same pinned ECMA translation and fancy-regex engine as jsonschema, with explicit VM and delegate limits, before accepting its compilation. Patterns that exceed the source quotas return `LimitExceeded`; invalid, untranslatable or excessive engine/static forms return `SchemaUnsupported`. The policy can conservatively reject a pattern that a validator optimization could otherwise handle. The static-work estimate is not execution fuel: unbounded repetition remains input-dependent. General Schema compilation/cache heap and total validation CPU budgets remain unfinished.

Unfinished input regex evaluation returns `ValidationLimitExceeded` before a tool request. Its diagnostic never formats the subject or pattern; boolean-only applicator probes can identify only the root instance plus the schema site. An unfinished output check produces `output_validation: "indeterminate"` and retains the complete raw result. `call_checked()` / `deserialize()` reject it with `OutputValidationLimitExceeded` and `result_received` evidence; the CLI returns 1. The older `validate_output()` boolean projection remains conservative; use `validate_output_status()` to distinguish invalid and indeterminate results. WIT's output enum and registry stats include these additions, so component hosts must rebuild with the interface.

### Validator cache

Each remote owns an LRU with 128 compiled validators by default. Input, output and elicitation-form schemas share it; identical schemas reuse one entry. Keys include the Draft 2020-12 policy version and registered resource contents. Authorization changes and successful resource replacement clear the cache. No global cache crosses remote identities.

Rust `Client::with_validator_cache_capacity(n)` configures a native connection. `Catalog`, `Machine`, `HostRuntime` and async remotes expose `validator_cache_stats`, `set_validator_cache_capacity` and `clear_validator_cache`. `QueuedRemote` returns cancellable operations for the same controls. JS `Remote`/`WorkerRemote` accept `validatorCacheCapacity` and expose asynchronous `validatorCacheStats()`, `setValidatorCacheCapacity(n)` and `clearValidatorCache()`. WIT exposes the corresponding typed resource methods. Capacity is 0..4096; zero disables retention. Statistics count validator hits, misses, successful compilations and LRU evictions; explicit clear preserves these lifetime counters. Counters saturate at the exact JS integer ceiling and report `counters_saturated`.

Evicting or clearing an entry releases the cache's reference. Live bindings, bound arguments and continuations keep their immutable validators and remain valid in the same owner context. A live catalog binding can be reused without looking up the LRU again. Native queued bindings share these compiled validators with the owner instead of recompiling on the caller thread; argument ownership remains separate.

Capacity bounds retained entries, not allocator bytes. Applications can retain validators through their handles, and compiled schemas can be substantially larger than source JSON. See the [native allocation measurements and reproducible command](docs/benchmarks/schema-memory-native.md); a hard compiled-memory budget and broader CPU/performance validation remain open.

### Preloaded Schema resources

`SchemaRegistry` is an immutable, in-memory bundle supplied by the embedding host. Root/embedded `$id`, relative references, anchors, recursive and dynamic references retain their base URI semantics. When a tool root's `$id` is already registered with identical contents, the compiler reuses that resource; conflicting definitions are rejected. The engine never retrieves a missing resource: an unregistered HTTP or file reference returns `SchemaReferenceUnavailable`. Resource URIs are identifiers; even a registered `file:` URI reads only the supplied JSON.

```rust,no_run
use mcpx::{SchemaRegistry, SchemaResource, Dialect, native::Client};

async fn connect() -> mcpx::Result<()> {
    let registry = SchemaRegistry::from_resources([SchemaResource {
        uri: "https://schemas.example/args.json".into(),
        schema_json: r#"{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}"#.into(),
    }])?;
    let mut remote = Client::new()?.with_schema_registry(registry)
        .connect("https://example.com/mcp", Dialect::Auto).await?;
    // The server's inputSchema can reference https://schemas.example/args.json.
    let tool = remote.fresh_tool("search").await?;
    let args = tool.args().set("query", "rust")?.finish()?;
    remote.call(&tool, &args).await?;
    remote.close().await
}
```

Rust can also construct `SchemaRegistry::new([(uri, serde_json_value), ...])`. `ToolBinding::with_schema_registry()` supports standalone bindings. Core/native `remote.set_schema_registry(registry).await` replaces resources; the queued method returns a cancellable catalog barrier. A queued binding carries its resource snapshot for local typed/JSON validation, while the owner still checks its current binding before dispatch. `schema_registry_stats()` reports resource count, source charges, audited Schema nodes and pattern count/source bytes. Pattern statistics describe the registered bundle; each compiled root is checked together with that bundle.

For the CLI, `--schema-registry schemas.json` is a global option. The file contains fixed records, with Schema JSON stored as text:

```json
[{"uri":"https://schemas.example/args.json","schema_json":"{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\"}},\"required\":[\"query\"]}"}]
```

```bash
mcpx --url https://example.com/mcp --schema-registry schemas.json call search --args '{"query":"rust"}'
```

JS `Remote` and `WorkerRemote` accept `schemaRegistry: [{ uri, schemaJson }]` at construction, and expose `await remote.setSchemaRegistry(resources)` / `schemaRegistryStats()`. Schema text stays opaque in JS so integer literals remain intact. A host may load approved resources asynchronously before providing them; discovery and compilation do not select or fetch those URLs. In a Worker, Rust parses and prepares the bundle inside the worker. Wasm's low-level `set_schema_registry` takes the CLI record array as JSON text; WIT uses typed `schema-resource` records with `uri`/`schema-json`, exposed as `setSchemaRegistry()` by jco.

Input, output and elicitation form Schemas use the same registry. Installation validates resource syntax against the Draft 2020-12 meta-schema and checks the whole bundle's reference closure before replacing it; failed installation preserves the current resources and bindings. A successful replacement requires idle I/O, invalidates old bindings and continuations, and preserves the catalog's existing TTL. Authorization identity changes clear the registry; install resources explicitly for the new identity. Retained standalone bindings/builders remain immutable snapshots, and cannot be sent through an owner that has invalidated them.

Limits are 128 registered documents, 1 MiB per document, 8 MiB of source JSON/URI bytes plus 128 bytes per document, 16 MiB for the encoded bundle, and 2048 bytes per URI/reference. The audited graph has at most 16384 Schema nodes across the bundle and the tool Schema. Record deserialization enforces count/source limits while consuming the array. Aliases, embedded identifiers and anchors cannot shadow another resource. These are source/graph budgets; the validator LRU separately bounds retained entries. A hard compiled-memory budget and full CPU isolation remain unfinished.

The exact accepted Draft 2020-12 vocabulary URI suffixes under `https://json-schema.org/draft/2020-12/vocab/` are `core`, `applicator`, `validation`, `unevaluated`, `meta-data`, `format-annotation` and `content` (`SCHEMA_VOCABULARIES` in Rust). Unknown required vocabularies, including invented names under that prefix, return `SchemaUnsupported`; unknown optional ones remain extensions. Required `format-assertion` is rejected because this policy treats `format` as an annotation. Encoded content is also advisory; validation does not decode or retrieve it. This follows the [Draft 2020-12 format vocabulary distinction](https://json-schema.org/draft/2020-12/json-schema-validation#section-7.2). Explicitly referenced targets are checked even when reached through an extension field; unreferenced data in defaults/examples/const stays data. Custom dialects remain unsupported.

### Explicit input interactions

Enable the interaction modes your application can present with `remote.set_elicitation(true, false)?` (form, URL). They are advertised on modern requests only. A `CallOutcome::NeedsInput(continuation)` exposes the server's input requests through `continuation.raw()`. After your application collects an explicit response, continue with:

```rust,ignore
let outcome = remote.resume(&continuation, serde_json::json!({
    "confirm": { "action": "accept", "content": { "approved": true } }
})).await?;
```

Input identifiers must exactly match the requested identifiers; accepted form content is Schema-checked. `decline` and `cancel` are also supported. URL interactions return an action without form content; the library does not navigate automatically. Sampling and roots input requests are rejected as unsupported. A state-only response can be resumed explicitly with `{}`.

The remote retains the original tool arguments and echoes `requestState` unchanged under a new request ID, following [MCP multi round-trip requests](https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/mrtr). Handles are local to that remote, single-use after dispatch, invalidated by authorization changes, and checked against the current tool binding. Invalid input can be corrected before dispatch. There is no automatic retry after uncertain execution. Use `release_continuation()` to abandon an interaction.

Limits are 32 stored continuations, 8 MiB of retained argument/result JSON, 16 requested inputs per round, 16 input-required rounds, and a five-minute interaction deadline that later rounds do not extend. Input responses are limited to 1 MiB. Low-level `HostRuntime`/WIT hosts must advance the monotonic clock before operations and incoming responses; Rust async clients and the JS wrapper supply it. The byte budget measures retained JSON, not a full allocator/validator memory limit. Continuation serialization is for local host integration; reconstructing a JSON object does not create a Rust continuation handle.

`call_checked()` returns a successful `ToolResponse` or `CheckedCallError`. Its `outcome` retains the raw tool failure, output validation failure or unfinished input result. A transport/input failure has no tool outcome. See the runnable [Rust library example](crates/mcpx-native/examples/library.rs):

```bash
cargo run -p mcpx-native --example library -- http://127.0.0.1:PORT/mcp hello
```

### Error evidence and explicit retry decisions

Rust `Error` and JS errors carry `execution_certainty` and `retry_advice`, plus optional `operation_id` and `queue_operation_id`. These are owner-local core and host queue IDs, respectively; neither is the JSON-RPC wire ID. A failure before admission can have neither ID. Worker errors use the outer caller's queue ID while preserving the core ID. JSON CLI errors on stderr carry the same fields. Direct WIT records use typed enums and optional `u64` IDs (`executionCertainty`/`retryAdvice` in jco).

| Execution certainty | Evidence for this attempt |
| --- | --- |
| `not_sent` | This tool attempt has not been handed to a transport host. Metadata discovery/refresh can still have made requests. |
| `unknown` | The host may have sent the tool request, or evidence is unavailable. A timeout, cancellation, 401, RPC error or broken response does not prove absence of execution. |
| `result_received` | A valid tool result was accepted, including a business failure, invalid output or later local result decoding failure. |
| `needs_input` | An input-required outcome was accepted; the caller must explicitly use its continuation. |

This describes the current attempt only. Cancelling an unsent resume does not undo previous interaction rounds. Handoff to a low-level host is conservatively `unknown` even if that host later fails before network I/O. Low-level hosts can snapshot `action_error_context(id)` before consuming an action's response state. `Error::new` defaults to unknown evidence; `Error::local` is for failures known to precede tool dispatch. Legacy serialized errors without these fields deserialize as `unknown`/`none`.

Retry advice is one of `none`, `correct_input`, `refresh_binding`, `refresh_authentication`, `explicit_policy`, `inspect_execution`, `inspect_result`, or `resume_input`. It is guidance for an explicit caller decision and never authorizes automatic tool replay. Inspect received results or uncertain execution before deciding what to do. These fields are stable structured data; do not parse the human-readable message to make retry decisions.

Returned `ToolResponse` values and continuation handles retain their originating operation IDs. `ToolResponse::deserialize` uses that evidence in errors without consuming `raw`. `CheckedCallError.error` is boxed to keep checked failures small; `outcome` still owns the complete result or continuation. The development WIT ABI has added fields, so rebuild fixed host bindings/components together; no remote-tool SDK generation is involved.

### Reconnect without replay

Native mutable `Remote` supports `reconnect().await`: it closes the old stdio child or HTTP session, resolves current credentials, then discovers a fresh catalog. HTTP reconnect resets the logical MCP session; it does not promise a new TCP socket. `QueuedRemote::reconnect()` submits the same operation as a FIFO barrier in a live owner; an ended owner requires a new `QueuedRemote`. Finish or cancel active work before mutable reconnect; the queued form waits behind active calls.

Recovery invalidates bindings and continuation handles even when schemas are unchanged. Obtain new bindings after success. Valid old arguments are rejected as `BindingStale` at admission; local argument errors can precede that check. Host action/RPC/operation IDs remain monotonic across epochs, and retained concurrent completions remain readable. Low-level hosts must consume a completed serial outcome before `reset_transport()`. Registry resources survive a same-identity reconnect; an authorization-context change still clears them. Auto mode probes its preferred protocol again, rather than inheriting the last negotiated version.

Failed or dropped reconnect keeps `is_suspended()` true and blocks ordinary/prepared admission. Explicitly retry `reconnect()` or call `close()`. A final close cannot be reopened. The native lifetime event stream stays open across recoverable transport errors and ends on final close; legacy session cleanup keeps its original credential snapshot and retries failed cleanup. Cleanup futures can be dropped and retried without polling completed child-task handles again.

Automatic recovery is **off by default**. A queued stdio owner can opt in through its native client factory:

```rust
use mcpx::native::{Client, QueueOptions, QueuedRemote, StdioReconnectPolicy};
use std::time::Duration;

let remote = QueuedRemote::spawn_with(server, QueueOptions::default(), || {
    Client::new()?.with_stdio_reconnect(StdioReconnectPolicy {
        max_attempts: 2,
        backoff: Duration::from_millis(100),
        timeout: Duration::from_secs(5),
    })
}).await?;
```

This setting applies to the `QueuedRemote` stdio scheduler; mutable native clients use explicit reconnect. Native queued legacy HTTP session renewal has a separate opt-in policy below; notification GET resumption also has its own policy. Only transport/start/timeout failures are eligible for stdio recovery. Malformed protocol, authorization, schema and capability errors do not trigger it. Bounds are 1..8 full reconnect/discovery rounds, backoff at most 60 seconds, and a positive recovery window at most 600 seconds. The first round has no backoff; subsequent rounds share one monotonic deadline. Consecutive metadata-success/EOF flaps spend the same budget until a real tool outcome is received or the connection stays healthy for the window duration. Auto protocol probing can include its separately controlled child restart within a round.

Already failed calls return their original error with execution uncertainty and are never replayed. Jobs still waiting in the queue retain their deadlines; named commands can bind the new catalog for their first dispatch, while old typed bindings/resumes are rejected. Cancellation, queue expiry and shutdown continue during discovery/backoff. If stdio automatic recovery fails, the owner closes the queue and waiting work receives `TransportClosed`; detailed connection-level recovery status is not a separate subscription yet. Async waits are deadline-bound and cleanup has its own grace; synchronous schema work is still not preemptible. WASI and JS owners provide the explicit HTTP operations below.

### Native HTTP session renewal

Native clients can opt into session renewal during initial HTTP discovery, before
a mutable Remote's next operation, and in an established queue:

```rust
use mcpx::native::{Client, HttpSessionRecoveryPolicy, QueueOptions, QueuedRemote};
use std::time::Duration;

let remote = QueuedRemote::spawn_with(server, QueueOptions::default(), || {
    Client::new()?.with_http_session_recovery(HttpSessionRecoveryPolicy {
        max_attempts: 2,
        backoff: Duration::from_millis(100),
        timeout: Duration::from_secs(5),
    })
}).await?;
```

This policy defaults to off. `Client::connect`, `ServerConfig::connect` and queued
startup retry initial metadata discovery after a session expires, without issuing
a tool call. A failed later catalog page discards the partial catalog before
starting again. A 404 for a request containing the current legacy MCP session triggers renewal,
as described by the [legacy session specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports#session-management).
Core records that evidence at response headers for POST, notification GET and
resumed tool GET; an error body's format or a stalled body cannot hide it. Modern
and stateless 404s, authentication failures and provider error strings do not
trigger this policy. A late 404 for an older session does not expire a newer session.

Initial renewal and the connected owner share the original observation deadline and
remaining attempts. Startup metadata success alone does not replenish that budget.
If startup fails, connection returns its metadata error, timeout or cancellation
after bounded cleanup. A cancellation handle obtained from the client before
connecting covers every startup round, including backoff. Dropping a queued startup
future cancels its owner; with a mutable connection future, cancel through that
handle and await it to let cleanup finish.

For a mutable `Remote`, the failed tool returns its original error. The next async
operation renews the session before admission; metadata expiry can renew within
that operation while its argument builder has not yet run. Renewal and subsequent
tool I/O share the caller's original cancellation token and deadline. Old typed
bindings and continuations return stale-handle errors against the new catalog;
named commands build their arguments once using the new Schema. Low-level hosts
must finish active operations first (`Busy` preserves the old epoch and results).
Failure or dropping the renewal future suspends admission until explicit
`reconnect()` succeeds or the remote is closed. Cancel before renewal starts to
preserve the old epoch; cancel and await a running operation to drive cleanup.
JS/Worker can opt into the policy described below. WASI automatic session renewal remains separate work.

In a queued owner, failed tools retain their original `SessionExpired` error, operation ID and
execution uncertainty. New dispatch pauses while other old calls finish; their
positive results remain valid. One monotonic window starts when Core observes the
first session failure and includes peer drain, cleanup, provider resolution,
discovery and backoff. It allows 1..8 renewal rounds, backoff at most 60 seconds and
a total window in (0,600s]. Checks are cooperative; synchronous Schema work is not
preempted. Old-session DELETE uses its creating credential before resolving the
current provider. Transport failures or another expired session during renewal can
spend another round; authentication, protocol, Schema and HTTP server rejection
errors end recovery.

Waiting commands retain their deadlines and bind the new catalog for their first
tool dispatch. A command still resolving credentials or refreshing metadata can
wait for renewal without becoming a tool replay. Old typed bindings and
continuations remain invalid. Cancellation and expiry continue during recovery,
and a queued notification stop keeps the new listener disabled. Consecutive idle
or metadata expiry loops share one budget; a real tool result or a healthy interval
of `timeout` resets it. Receiving an old retained result after renewal does not
replenish the budget. Queued exhaustion closes the owner and waiting work receives
`TransportClosed`; no independent connection-status subscription is provided yet.

Low-level Rust hosts can inspect `Machine::http_session_expired()` and
`http_session_expired_at()` using the supplied monotonic millisecond clock. The
latch survives unrelated positive replies and clears on transport reset or an
authorization-context change. It never authorizes retrying an earlier tool call.

### WASI HTTP reconnect

`run_blocking(client.reconnect())` reconnects a synchronous `Client<WasiDriver>`.
`WasiQueue::reconnect()` and `WasiRemote::reconnect()` return a `WasiOperation<()>`
that can be awaited with the queue or Reactor runner. The queued operation waits
for earlier calls and their cleanup, then resets the MCP session and discovers a
new catalog using the existing nonblocking pollable state machine.

The operation's original deadline covers queue waiting, old-session DELETE,
credential resolution and discovery; cleanup is additionally capped at 500 ms.
Checks occur at cooperative I/O/owner boundaries; synchronous Schema work is
not preempted.
The old session is deleted with its creating credential before resolving the
current queue provider exactly once. Cleanup failure retains that snapshot for
an explicit retry. Cancelling or expiring before reset preserves the current
epoch; failure after reset keeps `is_suspended()` true. Local configuration,
notification stop/status, reconnect and close remain available while suspended.
Starting or resuming a notification stream requires successful reconnect.

Same-identity Schema resources survive, while identity changes clear them. Old
bindings, continuations and notification cursors cannot cross reset. A configured
listener opens fresh after success; an explicitly stopped listener stays stopped.
Results already completed remain readable from retained operation futures.
There is no automatic session renewal or tool-call replay. The synchronous driver
still blocks within one Future poll; use the Queue/Reactor APIs for cooperative I/O.

### JavaScript/Worker HTTP reconnect

`Remote` and `WorkerRemote` expose an explicit, metadata-only queue barrier:

```js
await remote.reconnect({ timeout: 5000 });
const fresh = await remote.freshTool("echo");
const result = await fresh.call({ message: "a new explicit call" });
```

The barrier waits for active calls and their I/O cleanup. It resets the logical MCP connection, invalidates old bindings/continuations and listener checkpoints, stops old listener I/O, and deletes an old legacy session with the credentials that created it. Only then does it resolve current credentials once and discover a new complete catalog. Modern connections also reprobe/refetch without a legacy DELETE; Auto starts from its preferred protocol again. This does not promise a new TCP connection or replay a failed tool call. Same-identity registry resources survive; a changed authorization identity clears them as usual.

Queue waiting, cleanup, provider resolution and discovery use the reconnect operation's original timeout. Cancelling/expiring it while still queued leaves the old connection usable. After reset starts, failure/cancellation keeps ordinary operations suspended (`TransportClosed`) until an explicit reconnect succeeds; close, notification stop/status and resource release remain available. Retryable session cleanup retains its original ID and credentials. Commands queued behind a successful reconnect bind the new schema for their first dispatch; old typed arguments and continuations remain invalid. Already delivered results remain readable, and action/RPC/operation/worker binding IDs are never reset. Every reconnect keeps one queue lifecycle terminal and reports its current business attempt as `not_sent`, not as evidence about earlier tool effects.

An enabled listener opens fresh after success with no cursor; an explicitly stopped listener stays disabled. Older pending notification resumes are cancelled rather than redirected to a new session. Worker reset confirmation is tied to the exact reconnect request, releases obsolete binding records in both realms, and carries the originating connection epoch with business results so a late old interaction cannot become a new valid continuation. Retaining settled reconnect promises does not retain their owner, provider or credentials.

The low-level Wasm and typed WIT expose `reset_transport()` for exclusive hosts. Close/join old I/O first and retain session credentials for cleanup; the method itself performs no I/O or credential lookup. It refuses active business work or an unconsumed serial result, preserves delivered concurrent completions and counters, and invalidates connection-scoped handles. Do not reset a runtime concurrently with its high-level owner. Rebuild the Component and fixed adapter together for this added ABI method.

`npm run test:http-reconnect` exercises Node Wasm/jco, cross-origin Chromium and WorkerRemote across both explicit versions and Auto: 200 scenarios, 16 retained-promise GC cases, a real HTTP/2 16-call barrier, failure/cancellation retry, registry/identity boundaries, protocol switching and notification cleanup. A Worker additionally crosses 132 reconnect epochs while reusing its binding capacity. WASI also provides explicit high-level reconnect as described above; automatic HTTP session renewal is opt-in in JS/Worker, while WASI automatic renewal remains separate work.

### Web and Worker HTTP session renewal

`Remote` and `WorkerRemote` accept the same opt-in policy:

```js
const remote = await WorkerRemote.connect(endpoint, {
  httpSessionRecovery: { maxAttempts: 2, backoff: 100, timeout: 5000 },
});
```

Omit `httpSessionRecovery` to disable it. Bounds are 1..8 full renewal rounds,
0..60,000 ms backoff and a total window of 1..600,000 ms. A policy does not enable
notifications. Core must have observed a 404 for the current legacy session;
modern/stateless 404s, authentication failures and provider errors do not trigger
renewal. Initial discovery, a later catalog page, notification GET and resumed
business GET can supply this evidence. An error body cannot delay the decision.

The owner pauses new dispatch, retires old notification I/O, drains existing
calls, then resets the logical connection, deletes the old session with its
creating credential, resolves current credentials once and discovers a complete
catalog. Failed tools keep their original error, operation ID and execution
uncertainty. They are never resubmitted. Waiting commands and unsent metadata
preflights retain their original queue IDs, byte reservations, cancellation and
deadlines; they bind the new Schema before their first tool dispatch. Old bindings
and continuations remain invalid. Queue lifecycle emits one start and one terminal
per submitted operation, including a preflight resumed after renewal.

One deadline starts at Core's first expiry observation and covers drain, cleanup,
provider, discovery and backoff. Startup also keeps its original connect timeout
and AbortSignal. Metadata success alone does not replenish rounds or time; an
actual new tool result or a healthy interval of `timeout` resets the burst. During
a proved renewal burst, transport failures can spend another round. Protocol,
authorization, Schema and HTTP server rejections stop it. On failure the owner
suspends admission and rejects waiting work with `TransportClosed`; explicit
`reconnect()` can retry. Notification stop/status, cancellation and close remain
available. A stop during renewal keeps the next listener disabled.

The Wasm `http_session_status()` and typed WIT `http-session-status()` expose
non-consuming receipt/expiry evidence for exclusive hosts. The JS adapter keeps
operation IDs as decimal strings. Rebuild the Component and fixed adapter together
for this added ABI method. Worker resets carry a separate monotonically increasing
epoch, including automatic resets without a fabricated queue operation. Late old
results remain readable; late bindings and continuations cannot become current.

`npm run test:http-session` runs real HTTP/SDK and retained-promise GC checks in
Node Wasm/jco, Chromium and WorkerRemote. Browser HTTP/1.1 cases use four active
calls; Node uses sixteen. It is also part of `npm run test:wasm`.

### Queue lifecycle subscriptions

Queue lifecycle is separate from MCP protocol events. Rust `QueuedRemote::connect_with_events(server, queue_options, event_limits)` (or `spawn_with_events` with a client factory) returns `(remote, OperationEventStream)`. Drain `next_batch().await` concurrently; the [queued example](crates/mcpx-native/examples/queued.rs) prints these batches on stderr while returning tool results on stdout.

Each lifecycle record has `queue_operation_id`, its own monotonic `sequence`, a `kind`, bounded status/diagnostic `payload`, and an optional `core_operation_id`. Kinds are `queued`, `started`, `completed`, `needs_input`, `failed`, and `cancelled`. `started` means the owner began processing, including preflight; it does not mean a tool request was sent. Every admitted job reserves exactly one terminal, including local help, catalog/configuration controls and cancellation before dispatch. Completed records can contain `is_error`/`output_validation`: completion of a host job does not imply business success. Arguments, raw results and credentials are excluded. Lifecycle errors keep at most 512 UTF-8 message bytes and four pointer pairs of at most 64 bytes per path, with `truncated` when diagnostics are shortened.

The native subscription has independent count/byte limits and does not consume the separate protocol event backlog; configure or drain protocol events as usual. `queued`/`started` records may be dropped with an explicit batch `dropped` count. Terminals are protected; a stopped consumer causes `ConsumerLagged` before further admission. Draining releases credits. Dropping the stream explicitly unsubscribes without blocking later submissions; closing the queue drains retained records before `next_batch()` returns `None`. Initial native connection occurs before the queue exists, so it remains a protocol operation rather than a queue job.

JS `Remote` and `WorkerRemote` accept `onOperationEvent(event)` alongside `onEvent`. It observes the same lifecycle model, synchronously, without another library buffer. Callback failures go to `onEventError` and do not change execution. An observer may cancel a queued job before dispatch. The Worker proxy uses outer queue IDs; owner-start acknowledgements and safe scalar result context preserve correlation without parsing business JSON. Lifecycle sequence exhaustion rejects admission or uses an already reserved final terminal. Retained events and settled promises do not retain an owner/provider in the GC integration tests.

Protocol `onEvent` keeps its core sequence domain. Metadata refreshes now carry their originating queue ID, so a single job may have several core operations/terminals (for example refresh then call). Use lifecycle terminals for exactly one host-job settlement. Rust hosts can use the I/O-free `OperationEvents` journal and keep a `machine.queue_scope(id)?` guard alive during admission. That scope links newly created core operations; dropping it does not remove links from already admitted operations. The low-level WIT component remains a protocol engine; queue lifecycle belongs to its host.

### Catalog refresh and change watches

`QueuedRemote::refresh()` captures an owner-bound catalog revision when submitted. Requests submitted against the same revision reuse a newer complete publication only while it remains fresh and the authorization, schema resources and transport context remain unchanged. Each request still resolves its own credentials and keeps its queue ID, cancellation, deadline and lifecycle terminal. Failed/cancelled metadata work is not cached as success. `refresh_with_timeout(duration)` gives one queued refresh its own deadline; a refresh submitted after publication explicitly refreshes again. Direct Rust clients can opt into the same policy with `catalog_revision()` and `refresh_if_unchanged(&revision)`; ordinary mutable `refresh()` always performs metadata I/O.

Rust clients and native queues expose `subscribe_catalog()`:

```rust,ignore
let mut updates = remote.subscribe_catalog()?;
remote.refresh()?.await?;
if let Some(change) = updates.changed().await {
    println!("published={}, invalidated={}", change.published, change.invalidated);
}
```

Subscriptions start at the current revision, with no initial event. They coalesce intermediate changes into the latest revision: `published` means a complete catalog was replaced, even if descriptors are unchanged; `invalidated` means a context change or observed `tools/list_changed`. They carry no schemas, tool results or credentials. A publication is not a freshness guarantee: use the usual fresh-read/call APIs, particularly if a notification arrived during pagination. TTL expiry, refresh start and failure do not generate watch events.

Each owner allows at most 64 retained watchers. Dropping a pending `changed()` future releases its executor waker without ending the subscription; dropping the watcher frees its slot. `try_changed()` polls synchronously without retaining a waker. Final client close or catalog drop wakes readers, delivers any unseen latest change once, then returns `None`. Reconnect invalidates the revision and keeps subscriptions open. Retained tokens/watchers do not directly retain the client, catalog, provider or schemas; pending waits register executor wakers, whose tasks may hold their own application references.

JS `Remote` (Wasm or jco) and `WorkerRemote` offer the same local-state observation:

```javascript
const changes = remote.subscribeCatalog();
try {
  await remote.refresh();
  const { value } = await changes.next();
  console.log(value.published, value.invalidated);
} finally {
  changes.close();
}
```

Subscriptions are async iterators, with no initial event. `next({signal})` can cancel a pending read without unsubscribing or consuming a change. Only one read per subscription may be pending (`Busy` otherwise). `tryNext()` returns an iterator result, or `undefined` when waiting. Close the subscription with `close()`/`return()`; breaking a `for await` loop also releases its slot. A remote supports 64 retained subscriptions, each keeping only latest-state identity markers and at most one waiting reader. Remote close drains any unseen change and then returns `done:true`; forced Worker termination ends readers using the last state delivered by that worker.

The Wasm runtime's destructive `catalog_change()` JSON read and WIT's typed `catalog-change` poll (`pending`, `changed`, `closed`) read the core revision source independently of the event journal. One host owns this low-level poll; do not also consume it while a high-level Remote owns that runtime. Worker forwarding retains one unacknowledged record and one coalesced pending cell on a separate ACK channel, unaffected by ordinary event ACKs. On graceful shutdown the final cell is posted before the close reply, including a cleanup error; at most those two records can remain posted. Slow subscribers coalesce changes rather than blocking tool calls. Retaining closed JS watchers and their settled reads is covered by the owner/provider GC tests.

These are local catalog observations, not MCP wire subscriptions, background polling or automatic refresh. Publication does not guarantee freshness, and pending application tasks can retain their own references.

### HTTP catalog notifications

Native HTTP queues can opt into a background notification resource:

```rust,ignore
let remote = mcpx::native::QueuedRemote::connect(config, mcpx::native::QueueOptions {
    http_notifications: true,
    ..Default::default()
}).await?;
let mut changes = remote.subscribe_catalog()?;
let status = remote.notification_status()?.await?;
// The existing catalog watcher receives changes from the notification stream.
if let Some(change) = changes.changed().await {
    println!("invalidated={}", change.invalidated);
}
remote.close().await?;
```

The option defaults to false and applies to HTTP queues. Connection success establishes the tool connection; query `notification_status()` to distinguish `opening`, `listening`, `closed`, `failed` and `cancelled`. Modern HTTP opens `subscriptions/listen` with `toolsListChanged:true`, validates the first acknowledgment, and correlates every notification and final result with its exact RPC ID. Legacy HTTP opens a GET with its negotiated session/version headers and answers unsupported server requests with capability errors. These are separate protocol paths. See the [modern subscription specification](https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/subscriptions) and [legacy transport specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports).

Native HTTP and stdio queues also provide explicit controls, including when automatic listening was initially disabled:

```rust,ignore
let opening = remote.start_notifications()?.await?;
let stopped = remote.stop_notifications()?.await?;
let status = remote.notification_status()?.await?;
```

Start is an ordinary FIFO barrier: it resolves current credentials once, refreshes the catalog if needed, and returns without waiting for a remote ACK. An active listener is reused after that preflight; an ended or failed listener is explicitly replaced. Stop disables automatic listening, joins the listener's I/O, and cancels older queued or provider-waiting starts. A later start remains eligible. Stopping before any listener exists returns `None`. The disabled setting survives reconnect until another explicit start.

Stop, status and explicit legacy resumption share two reserved owner slots and a separate 512-byte accounting budget. At most one resume can wait, preserving a slot for stop/status. These controls do not resolve credentials or wait behind business-call admission, including all 16 occupied call slots or a call/start credential provider that is pending. They still require lifecycle-observer capacity when that subscription is enabled. During serialized metadata I/O or reconnect they wait for the owner boundary; their queue cancellation and deadlines continue to advance. Stopping a pending start does not interrupt a metadata frame in the shared stdio channel.

All notification controls have a `_with_timeout(Duration)` variant, including queue time; the default is `QueueOptions.timeout`. Cancelling or expiring a stop before execution leaves listening unchanged. Once stop cleanup starts it runs to completion, even if its caller cancels or its deadline expires; the result then reports cancellation/timeout without restoring the listener. Control operations keep their queue IDs and exactly one lifecycle terminal. Retaining a settled operation retains its status/result, without retaining the remote or its credential provider.

A clean legacy EOF retains capability replies already accepted from that stream. The native writer drains them asynchronously, so a slow reply does not block new business calls; explicit stop or an identity change discards them. Low-level hosts drain `notification_replies` before opening another resource, or explicitly release the old one.

The JS `Remote` and `WorkerRemote` accept the same opt-in as `httpNotifications: true`:

```js
const remote = await WorkerRemote.connect(endpoint, { httpNotifications: true });
const changes = remote.subscribeCatalog();
const status = await remote.notificationStatus();
await remote.stopNotifications();
await remote.startNotifications();
await changes.return();
await remote.close();
```

For direct Wasm or jco embedding, pass the option to `new Remote(runtime, options)` and call `connect()` as usual. `startNotifications()` enables listening and returns the current status; it can return `opening` before the network acknowledgment arrives. The constructor's `timeout` bounds opening and acknowledgment; a control method's `timeout` includes its own queue time. An already active start is idempotent. `stopNotifications()` disables automatic listening and joins the reader and reply writer. Stop supersedes older pending starts and resumes, while a later start remains eligible. Settled control promises retain their own status, without keeping the owner or credentials alive.

Stop, status and legacy resumption use two reserved owner slots with a 512-byte accounting budget, outside ordinary call admission, credential resolution and event waits. Only one resume can wait, preserving a slot for stop/status. Worker proxy cleanup remains bounded separately. These methods work while business slots are occupied or event acknowledgments are paused; exhausting cleanup or lifecycle-observer capacity can still reject admission. JS reads one chunk at a time, bounds chunks to 64 KiB, and queues at most 16 legacy capability replies plus one writer with a 500 ms reply deadline. Its credential snapshot remains fixed through EOF cleanup. Notification streams through the Worker fetch proxy have no cumulative size cap; ordinary response and shared 8 MiB retained-buffer caps remain enforced.

Both JS owners support manual legacy GET resumption and an optional recovery policy (all durations are milliseconds):

```js
const remote = await WorkerRemote.connect(endpoint, {
  httpNotifications: true,
  notificationRecovery: { maxAttempts: 3, backoff: 100, timeout: 10_000 },
});
// For a failed legacy resource with a usable cursor:
const failed = await remote.notificationStatus();
const opening = await remote.resumeNotifications(failed.resource_id, { timeout: 5_000 });
```

Omit `notificationRecovery` to disable automatic recovery. A policy alone does not enable listening. It accepts 1..8 attempts, backoff 0..60,000 ms and a total window 1..600,000 ms. Manual resumption waits for the server's retry and accepted capability replies, then returns `opening` for a fresh resource ID. Resumed GETs and capability replies keep the original credential/session snapshot even across same-identity token rotation; `Last-Event-ID` is sent only on GET, preserving its UTF-8 bytes. No provider lookup or tool POST is part of recovery.

Automatic attempts share one window from the reader-observed disconnect, including reply draining, retry/backoff and opening; opening is capped by the remaining window. Short EOF flaps spend one attempt budget; a stream healthy for a full window resets it. A successful manual resume resets the burst budget. Stop clears the pending timer and checkpoint. Identity changes and fresh starts invalidate old resource IDs. Missing/reset cursors, malformed input, authentication/session/endpoint rejection and failed reply writes stop recovery. Modern HTTP completion does not use this legacy path. Timers remain cooperative with JavaScript and event/read backpressure; they do not preempt synchronous work. Business-response recovery is configured separately below; session renewal has its own policy above.

The resource does not occupy one of the 16 business-call slots. Drivers freeze credentials for the stream and legacy replies and retain at most 16 queued replies in each core/host queue. The native driver bounds input through its 16-message/64-KiB-chunk channel. Closing or changing identity cancels and joins its I/O. A newly accepted identity or explicit reconnect can open a new stream; by default an ended or failed stream is not automatically reopened. Unsupported endpoints or acknowledgment timeouts fail only that resource. A gap invalidates the catalog, so subsequent work uses normal fresh-read policy; business calls already in flight retain their own results. Credential providers are not polled in the background. Opening/acknowledgment deadlines are cooperative with the queue owner, not synchronous CPU preemption.

The shared low-level API is available in Rust `HostRuntime`, browser Wasm and typed WIT: `start_notifications`, `notification_status`, `begin_notifications`, `push_notifications`, `finish_notifications`, `stop_notifications`, `abort_notifications`, `notification_replies`, `notification_resume` and `resume_notifications` (camelCase in jco). A request has an owner-local `resource_id`, a separate optional wire `rpc_id`, method, endpoint, headers and optional body. Resource IDs are distinct from tool actions, operations and bindings. Hosts must freeze authorization, forward bounded chunks, send legacy capability replies with the same snapshot, and close their readers before freeing the runtime. The WIT interface has changed; rebuild components and fixed adapters together.

Each runtime permits one explicit notification resource for its transport. HTTP chunks are limited to 64 KiB and SSE frames to 8 MiB, without a cumulative response-size ceiling on a long-lived stream. Modern completion requires its correlated `resultType:"complete"` result; a bare EOF is a disconnect. Ordinary event backpressure can lose journal entries while the catalog revision still records invalidation. `subscribeCatalog()` alone remains a local observer: enable a network listener separately to receive remote changes. Standalone WASI offers cooperative notification driving as described below. Low-level legacy listener resumption is described next; High-level business-response recovery is described below; automatic session renewal has its own policy above; broader notification filters remain to be implemented. The fixture host in [notification-host.mjs](scripts/notification-host.mjs) demonstrates explicit low-level driving; it is not a second high-level scheduler.

### Legacy HTTP listener resumption

Rust `HostRuntime`, browser Wasm and typed WIT expose `notification_resume(resource_id)` and `resume_notifications(resource_id)` for interrupted legacy HTTP notification GETs. The first returns a resource ID and `retry_after_ms`; the second plans a new GET with `Last-Event-ID` once that delay has elapsed on the runtime's monotonic clock. It returns `RetryLater` early, without changing the old resource. These APIs never retry a business POST or resolve credentials. Modern HTTP and stdio return `ResumeUnavailable`. This follows the [legacy resumption specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports#resumability-and-redelivery).

```rust,ignore
let wait_ms = runtime.notification_resume(interrupted_id)?.retry_after_ms;
// Host waits, then calls runtime.advance_time(host_now_ms).
let get = runtime.resume_notifications(interrupted_id)?;
// Join the old reader and send this GET with its original credential snapshot.
```

The host must preserve the original credentials, drain both runtime capability replies and already accepted host reply writes, and close the old reader before opening the new one. The new plan has a fresh owner-local resource ID; late input for the old ID is rejected. The existing session and protocol headers are retained. Resumption keeps the catalog invalidation caused by the gap; it does not guarantee freshness or automatically refresh metadata. Stop, identity/transport changes, authorization rejection, session expiry and malformed messages make the checkpoint unavailable. A transport failure while opening a resumed GET can retain the prior checkpoint. A new ordinary `start_notifications()` starts fresh without `Last-Event-ID`.

SSE IDs commit at complete event boundaries, including empty priming events. Partial frames after the last checkpoint can be discarded on disconnection; malformed complete frames cannot resume. Empty `id` resets the cursor, an ID containing NUL is ignored, and IDs over 4096 UTF-8 bytes disable resumption until a later usable ID. IDs are opaque and omitted from default Debug output. A cursor that cannot be represented exactly in an HTTP header is rejected rather than transformed. `retry` accepts ASCII digits, including zero; without one the default delay is 1000 ms. A delay outside the supported monotonic clock range returns `LimitExceeded` and is never shortened.

WIT returns typed `notification-resume`/`notification-request` records; the fixed jco adapter provides the same snake_case JSON API as browser Wasm. In browser/Node `Headers`, encode a Unicode `last-event-id` to its UTF-8 byte string (`String.fromCharCode(...new TextEncoder().encode(id))`). Rust HTTP/WASI hosts send `id.as_bytes()` directly. Include `last-event-id` in the server's CORS allowed headers, and omit it from capability-response POSTs. See [the shared explicit host](scripts/legacy-resume-host.mjs) and [the standalone WASI example](crates/mcpx-wasi/examples/legacy_resume.rs). Rebuild the Component and its fixed adapters together for the added WIT methods.

Native queues, JS `Remote`/`WorkerRemote`, and the WASI client/queue integrate this checkpoint with explicit controls and opt-in policies described in their sections. Ordinary start methods create a fresh listener. The low-level APIs are for hosts that exclusively own the runtime, its readers, credentials and retry schedule.

### Legacy tool response stream recovery

The shared `HostRuntime`, browser Wasm and typed WIT expose explicit recovery for a dispatched legacy HTTP `tools/call` response. This applies to the original POST's SSE stream. Modern HTTP and stdio reject this path; the protocol distinction is defined by the [legacy transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) and [modern transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http) specifications.

An exclusive low-level host calls `enable_response_resumption(action_id)` before `begin_response`, then uses the ordinary bounded input methods. If the reader disconnects before a final response, call `interrupt_response_for(action_id)` **instead of** `finish_response_for`. With a usable cursor, the core discards an incomplete trailing frame while keeping the original call active. `response_resume(action_id)` reports the retry floor; advance the core's monotonic clock and wait within the original call deadline before `resume_response(action_id)` returns a GET plan. It has no body or new JSON-RPC request. Its fresh `action_id` rejects old input, while `rpc_id`, `operation_id`, progress token, argument/schema snapshot and terminal reservation remain those of the original call. The session/version headers come from the original POST, including if another response has since changed the current session header. Hosts retain their original credential/client/cancellation snapshots.

Drain `response_replies(action_id)` and finish accepted capability-response writes with the original credentials before planning a GET. Never copy `Last-Event-ID` onto those POSTs. The core retains at most 16 such replies with bounded IDs; pending replies make resume return `Busy`. After the final response, drain once more (even if empty) or call `release_response_resumption(action_id)` to release its terminal tracker. At most 16 enabled/retained trackers exist, and tracked input chunks are at most 64 KiB. Cancelling/failing the original operation clears its tracker. An active tracker cannot simply be released while leaving the call stranded.

Enabling tracking does not change EOF into automatic retry: calling ordinary finish/fail remains terminal. JSON responses, missing/reset/unrepresentable cursors, malformed complete messages and authorization/session/endpoint rejection cannot be resumed. A resumed GET must return HTTP 200 SSE. Before a final result, the original call's execution evidence remains `unknown`; GET recovery does not establish that an unobserved tool side effect did not run. A final result keeps its original output validation and queue/operation context. No automatic session renewal or tool POST replay is part of this API.

Native Rust clients, JS `Remote`/`WorkerRemote`, and WASI clients/queues integrate this foundation through the opt-in policies below. See [the explicit JS host](scripts/response-resume-host.mjs), [the standalone WASI host](crates/mcpx-wasi/examples/response_resume.rs), `npm run test:response-resume` and `npm run test:wasi-response-resume`. Low-level hosts exclusively own the runtime and join their old reader/writers before opening the next GET. They also own the total deadline and attempt policy; the core does not extend either. A suspended, reply-drained call can report host attempt exhaustion through `exhaust_response_resumption(action_id)`, preserving its original error context and one terminal event.

### Native tool response recovery

Mutable `Remote` and `QueuedRemote` share `ResponseRecoveryPolicy`, disabled by default:

```rust,ignore
let policy = mcpx_native::ResponseRecoveryPolicy {
    max_attempts: 2,
    backoff: std::time::Duration::from_millis(100),
    timeout: std::time::Duration::from_secs(5),
};
let client = mcpx_native::Client::new()?.with_response_recovery(policy)?;
// Connect normally; typed, JSON, string and argv calls use the same policy.
let options = mcpx_native::QueueOptions {
    response_recovery: Some(policy),
    ..Default::default()
};
```

The policy allows 1..=8 GETs per original legacy HTTP tool call, backoff up to 60 seconds, and a positive recovery window up to 600 seconds. A queue option of `Some` overrides its factory client's policy; `None` inherits it, normally disabled. Invalid queue policies fail before invoking the factory or making a connection. Modern HTTP, stdio and metadata retain their ordinary behavior.

The first reader-observed disconnect starts one fixed window. Owner waiting, accepted capability replies, server retry/backoff, GET opening and body reading all consume it, and the original call deadline also applies. Further disconnects do not reset either budget. Readers and capability writers advance independently; a pending queued credential provider does not block their timers or completion. Each GET and reply keeps the original client, credential, session and RPC context. No provider lookup, initialization or tool POST is repeated. Rejection, malformed messages, unusable cursors and failed capability replies terminate recovery; exhausted GET attempts return `ResponseRecoveryExhausted` with unknown execution evidence.

The business slot and input credits remain held across recovery and reply cleanup. Pending replies are bounded to 16 per call plus one active writer. Final results retain their original validation and are preserved across later cancellation/deadline/owner close; terminal reply cleanup shares a 500ms cap, while owner close aborts and joins remaining I/O. Before a result, cancellation closes the current reader and sends a bounded request-specific legacy cancellation using the original RPC and credentials. Dropping a mutable call future aborts its reader/writer immediately; use `cancel_active()` before reusing that mutable client to clear the core operation. None of these operations guarantee rollback of remote side effects or renew an expired session.

Real HTTP tests cover 16 concurrent recoveries, provider waiting/token rotation, retry windows, cancellation, dropped futures and close. An official rmcp 3.10 test cuts the frontend after intact SDK priming while draining the backend original stream: mutable and queued clients receive the original final response through GET with one tool POST and one initialization. It verifies an in-flight SDK route; completed-result replay depends on the server's event retention.

### JavaScript/Worker tool response recovery

`Remote` and `WorkerRemote` accept the same optional `responseRecovery` object:

```js
const responseRecovery = { maxAttempts: 2, backoff: 100, timeout: 5000 };
const remote = new Remote(runtime, { responseRecovery });
// Or: await WorkerRemote.connect(endpoint, { responseRecovery });
```

Values are integer milliseconds: 1..8 GET attempts, backoff 0..60000, and timeout 1..600000. Recovery is disabled by default and independent of `notificationRecovery`/listener activation. The policy applies to legacy HTTP tool responses for object/raw, argv and command-line calls. Modern HTTP and metadata keep their ordinary behavior. Invalid policies fail before a Worker is created or I/O starts.

One fixed window begins at the first reader-observed disconnect and is bounded by the original queue/call deadline. Later EOFs do not reset it. Accepted capability reply writes, server retry/backoff, GET opening and reading all consume that window. The host freezes its fetch implementation, credentials and original cancellation request; each GET uses the original RPC/session and a new core action ID. Unicode cursors are encoded as UTF-8 header bytes and appear only on GET. No tool POST, provider lookup or initialization is replayed. Status rejection, bad complete messages, unusable cursors and reply writer failure stop recovery. Exhausted attempts produce `ResponseRecoveryExhausted` with unknown execution evidence.

Readers and bounded capability writers advance independently, including while a peer provider is pending. All 16 business slots and their input credits remain held until response and reply cleanup finish. A known final result wins later cancellation/deadline; final reply cleanup shares a 500ms cap. Closing also preserves that result while aborting/joining its remaining I/O. Before a result, cancellation waits for reader/writer cleanup and sends a bounded original-RPC legacy cancellation. Worker transport cleanup awaits host ACKs; event ACK backpressure does not prevent cancellation of a recovery reader. Original output validation, raw uint64 JSON and operation/queue event correlation are preserved.

`npm run test:response-recovery` exercises Node Wasm, jco, cross-origin Chromium and WorkerRemote, including real HTTP/2 16-way mixed calls, retry windows, frozen credentials, failures, close and GC with retained results. The official SDK fixture in `test:response-resume` additionally verifies high-level Node Wasm/jco GET continuation of an in-flight original request. These checks do not imply arbitrary completed-result retention by remote servers. Rebuild the Component and adapter together for the added exhaustion-report method.

### WASI tool response recovery

WASIp2 Rust clients, `WasiQueue` and `WasiReactor` inherit an optional driver policy:

```rust,ignore
let driver = mcpx_wasi::WasiDriver::new(credential, std::time::Duration::from_secs(60))?
    .with_response_recovery(Some(mcpx_wasi::ResponseRecoveryPolicy {
        max_attempts: 2,
        backoff: std::time::Duration::from_millis(100),
        timeout: std::time::Duration::from_secs(5),
    }))?;
// Use this driver with Client::new(driver, runtime) or WasiQueue::new(...).
```

It defaults to `None`, accepts 1..8 GETs, backoff up to 60 seconds and a positive window up to 600 seconds, and is separate from notification recovery. Only legacy `tools/call` response SSE is eligible; modern HTTP, metadata and ordinary JSON retain their normal behavior. The original POST's credentials, session, RPC, progress, input/schema snapshot and cancellation packet stay fixed. GETs have new action IDs and UTF-8 cursor headers; capability-response and cancellation POSTs never carry a cursor. No provider lookup, initialization or business POST is repeated.

Sync and queued callers share the same bounded I/O state. The first owner-observed disconnect starts one fixed window, bounded by the original call deadline; reply draining, retry/backoff, opening and reading all consume it. EOF never resets it. GET request options use the remaining deadline, and retry/window/writer timers join the same WASI poll set. A pending peer provider does not block recovery I/O. At most 16 replies wait alongside one active writer per call, and business/input credits remain held through recovery and final cleanup. Missing/reset cursors, malformed complete messages, HTTP rejection and failed capability writes terminate recovery. Attempt exhaustion returns `ResponseRecoveryExhausted` with unknown execution evidence.

A final result retains its output validation, original IDs and lifecycle outcome. Later cancellation/deadline does not replace it; accepted reply cleanup shares a 500ms cap. Queue/Reactor close or drop also preserves a known result while releasing remaining I/O. Before a result, cancellation drops reader/writer resources before sending the bounded original-RPC cancel packet; dropping an owner performs no new I/O or session DELETE. Explicit close drives queued cancellation cleanup and session cleanup. Dropping an unfinished operation requests cancellation on the next owner service pass.

This remains cooperative: drive queue/Reactor operation futures with `poll`, `wait` or `run`, including during backoff. A synchronous `Client<WasiDriver>` call still completes in one blocking poll and has no independently running cancellation executor. `npm run test:wasi-response-recovery` covers real Wasmtime clients, 16 mixed typed/command calls, pending providers/token rotation, dependent Reactor calls, budgets, rejects, close/drop, raw uint64 and official SDK in-flight GET recovery. It does not implement session renewal, arbitrary completed-result replay or a general tool retry switch.

### Native listener recovery

`QueuedRemote` can explicitly resume one exact interrupted legacy HTTP listener:

```rust,ignore
let failed = remote.notification_status()?.await?.unwrap();
let opening = remote.resume_notifications(failed.resource_id)?.await?;
// resume_notifications_with_timeout(id, duration) also bounds queue/retry waiting.
```

The owner waits for the server's retry interval and accepted capability replies without holding a business-call slot. It retains the original HTTP client, credential snapshot and session for resumed GETs and capability-response POSTs, including after a same-identity token rotation. The cursor is sent only on GET. Provider waits continue processing reply writes, notification input, resume timers, cancellation and deadlines. Stop supersedes earlier waiting resumes; an identity change, explicit reconnect or fresh start cannot retarget an old resume to a new resource. A completed control returns `opening` without waiting for response headers; that stream uses the native driver's opening timeout. Cancelling/expiring a queued resume sends no GET. A cancellation noticed just after scheduling joins the new I/O before returning; cancelling an already settled operation does not undo it.

Automatic recovery is separately opt-in and defaults to `None`:

```rust,ignore
let options = mcpx::native::QueueOptions {
    http_notifications: true,
    notification_recovery: Some(mcpx::native::NotificationRecoveryPolicy {
        max_attempts: 3,
        backoff: std::time::Duration::from_millis(100),
        timeout: std::time::Duration::from_secs(10),
    }),
    ..Default::default()
};
```

Listening must be enabled by `http_notifications` or an explicit start; a recovery policy alone opens no listener. Bounds are 1..8 resumed GETs, backoff at most 60 seconds, and a positive total recovery window at most 600 seconds. Each attempt respects the greater of server `retry` and policy backoff. The window starts when the reader reports disconnection, so owner waiting, accepted reply draining, retry delay and opening spend the same window; opening is tightened to its remaining time. EOF flaps share one budget. A reader that remains healthy for the configured window resets that budget; the reader records opening/end times so a delayed EOF processed after serialized metadata I/O cannot appear healthy just because the owner waited.

Missing/reset/unsafe cursors, malformed messages, authorization rejection, session expiry, unsupported endpoints and failed capability-response writes prevent automatic resumption. Modern HTTP and stdio use their existing paths. Exhaustion leaves the resource's failure status readable and business calls usable; it neither renews a session nor replays a tool POST. Explicit resumption can override exhaustion and starts a new burst after success. Stop disables background listening/recovery until a later explicit start. Cancelling a manual resume alone does not disable a separately configured recovery policy. Automatic attempts have resource IDs, not fabricated user queue operations; submitted manual controls keep their normal lifecycle terminals.

These timers are cooperative with the owner. Serialized metadata/reconnect and synchronous work can postpone handling; the policy is not CPU preemption. Broader business-response resumption and session renewal remain in progress.

### stdio catalog notifications

Native queues can enable a catalog listener on their existing child process:

```rust,ignore
let options = mcpx::native::QueueOptions {
    stdio_notifications: true,
    ..Default::default()
};
let remote = mcpx::native::QueuedRemote::connect(config, options).await?;
let mut changes = remote.subscribe_catalog()?;
let status = remote.notification_status()?.await?;
remote.close().await?;
```

The option defaults to false and is independent of `http_notifications`. Modern stdio sends one `subscriptions/listen` request through the shared writer and validates its ACK, scoped changes and final result. Legacy stdio observes ordinary notifications without sending a subscription request. Both retain all 16 business-call slots. The existing reader processes idle changes and continues reading while a credential provider is pending; observing a change invalidates the catalog without automatically refreshing it.

The shared stdio reader remains active after `stop_notifications()` so business replies can still arrive. Legacy peers can continue sending unsolicited catalog notifications through that reader; those observed changes still invalidate the catalog.

An opening/ACK timeout or invalid subscription message fails that listener and invalidates the catalog while other calls retain their results. Invalid shared-stream envelopes and transport failure still end the shared channel. Late messages for replaced subscriptions cannot invalidate the replacement. Closing or changing identity cancels an unfinished subscription and joins its writer task; a clean final result or explicit rejection needs no cancellation. Failed or completed listeners are not automatically reopened. An accepted identity change or reconnect creates a new listener; the existing opt-in stdio recovery policy still never replays old business calls.

HTTP bearer credentials remain unsupported on stdio. A provider can return `Credential::scoped(identity)` to label an identity authenticated outside HTTP; this creates no bearer token and does not itself authenticate the child process. Identity labels are non-secret, nonempty and limited to 256 UTF-8 bytes. Changing the label invalidates bindings and replaces the listener.

Low-level Rust hosts call `HostRuntime::start_stdio_notifications()`, write its optional `body`, and feed decoded messages through `stream_input`. The request uses `method:"STDIO"` and empty headers. Modern requests have a separate wire `rpc_id`; legacy listeners have no wire request. Hosts can tighten the opening/ACK deadline with `set_notification_deadline(resource_id, absolute_monotonic_ms)` and advance the runtime clock as usual. Hosts own actual writes and cancellation. The existing HTTP/SSE methods and WIT layout are unchanged by the stdio API.

### Drivers and cached bindings

The default `mcpx::Client<D>` is platform-independent. A `Driver` supplies asynchronous action execution, monotonic milliseconds and an authorization context. It drives `HostRuntime` with response headers/chunks, rather than implementing tool discovery or Schema validation. Futures are not forced to be `Send`. Enable the `command` feature and import `ClientCommandExt` to add string/argv calls to any such client. `native::Client` is a convenience builder for the included driver, not a separate protocol implementation.

Schedulers that already resolved an operation through `Driver::prepare()` can use `with_prepared_catalog(context, read)` after draining existing operations. It accepts that unchanged context, applies authorization and TTL/refresh policy, invokes a local read, and finishes the prepared snapshot on every return path. It never resolves the provider again. Like other prepared admission methods, the host must cancel/abort an unfinished operation if it drops the future.

`tool()` binds the current cached descriptor; `fresh_tool().await` checks credentials and refreshes an expired catalog first. Before every call, the Rust client checks its default 60-second TTL, clamps it to server `ttlMs`, and refreshes with `tools/list` without repeating an established handshake. Unchanged bindings remain valid. A changed Schema or authorization context invalidates old bindings before sending a business request. The same catalog policy lives in HostRuntime and is exposed through Wasm and WIT; the JS hosts perform its asynchronous credential and I/O steps.

Use `with_credential_provider()` for asynchronous credential resolution. The request includes the endpoint and optional configuration reference. The provider returns a bearer credential and a non-secret `auth_context_id`; that ID must change when identity or permissions change. Credentials are resolved once per Rust client operation, frozen across its pages/requests, and redacted from `Credential` debug formatting. A new authorization context clears the old catalog; refresh and bind again after `BindingStale`. `EnvCredentialProvider` detects token changes conservatively using a fingerprint.

### Cancellation and asynchronous events

Obtain `client.cancellation_handle()` before connecting or `remote.cancellation_handle()` before a call. The cloneable handle can be used by another task or thread. Handles apply to their current/next operation; retaining and cancelling an old handle does not cancel a later call. Dropping a pending call future leaves an active operation: explicitly use `cancel_active().await` or `close().await` before reuse.

Modern HTTP cancellation closes the response stream. Legacy HTTP and stdio send a best-effort `notifications/cancelled`. The direct mutable native stdio adapter closes the connection on interruption. The queued stdio adapter keeps its persistent frame reader running and can cancel one bound call while other calls continue. No cancellation notification is sent for `initialize`. Cancellation does not promise rollback of side effects. These transport rules follow [legacy cancellation](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation) and [modern HTTP cancellation](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#cancellation).

`Client::with_event_stream(capacity)` returns a builder and `EventStream`. Consume `EventStreamItem::Event` with `next().await`; `Lagged { dropped }` reports core loss plus records evicted by this consumer buffer, without inferring loss from sequence gaps. A synchronous `with_event_handler()` receives the same enum, including core loss, and can be used alongside the stream. The callback sees records before the consumer buffer, so it does not report that buffer's later evictions.

Terminal records are retained: if a slow consumer fills those slots, the next operation fails with `ConsumerLagged` before I/O. Drain events and retry that unissued operation. Closing the remote ends the stream after queued records; dropping the receiver explicitly disables its backpressure. `with_event_stream_options(EventLimits { capacity, max_bytes })` configures independent record and byte limits: 4..4096 records and 48 KiB..64 MiB, defaulting to 256 records/4 MiB. The capacity-only method retains the default byte budget.

The core has its own buffer with those defaults and reserves a terminal record before each operation. `HostRuntime::set_event_limits()` and `Machine::set_event_limits()` change its limits only when drained with no active reservations; `event_stats()` reports retained records/charged bytes, reserved terminals and pending loss. Each record has a 16 KiB portable encoding allowance: twice its JSON length plus 128 bytes. This bounds host encoding costs, not allocator memory. Oversized notification payloads become `{ "truncated": true }`; oversized failure events keep bounded diagnostics. The returned call outcome/error remains intact. Sequence exhaustion returns `LimitExceeded` while preserving already reserved terminal records.

`Machine::drain_events()`, `HostRuntime::take_events()` and Rust client/queue `drain_events()` return `EventBatch { events, dropped }`. Consume `dropped` even when `events` is empty: a final dropped notification needs no later retained record to report its loss. Draining consumes the pending count once.

### Concurrent callers and the native queue

`native::QueuedRemote` owns a connection on a dedicated thread with its own current-thread Tokio runtime. The remote, `QueuedBinding`, finished `QueuedArgs`, operations and cancellation handles can cross threads. Schema builders remain local until `finish()`. The owner keeps the core and its `Rc` bindings on one thread; core drivers and credential futures retain their existing bounds.

```rust,ignore
let remote = QueuedRemote::connect(server_config, QueueOptions::default()).await?;
let echo = remote.tool("echo").await?;
let args = echo.args()?.set("message", "typed")?.finish()?;
let operation = echo.submit(&args)?;
let cancellation = operation.cancellation_handle();
let outcome = operation.await?;
remote.close().await?;
```

The runnable [queued example](crates/mcpx-native/examples/queued.rs) starts concurrent caller tasks:

```bash
cargo run -p mcpx-native --example queued -- http://127.0.0.1:PORT/mcp
```

HTTP and stdio typed/JSON calls, string/argv commands and continuation resumes share up to 16 requests in flight; `QueueOptions::max_in_flight` accepts 1..16. Calls enter in FIFO order and may complete out of order. Binding, refresh, explicit reconnect, settings, event draining and continuation release act as barriers. Defaults are 64 admitted operations (including active work), 8 MiB of serialized input plus record allowance, and a 60-second deadline that includes queue time. The native driver's timeout also applies. `submit_with_timeout()` can override a call's queue deadline. Admission fails immediately with `QueueFull` when either budget is exhausted. The byte budget measures queued input; allocator overhead and caller-retained results require separate budgets.

`QueuedOperation::id()` is a queue request ID, separate from MCP request IDs and core event operation IDs. Calling its cancellation handle or dropping an unfinished operation cancels it. A queued cancellation or expiry sends no business request; an active interruption runs transport cleanup before the next request. Old cancellation handles cannot target new operations. `close().await` rejects queued work, cancels active I/O and waits for resource cleanup. Dropping every remote/binding handle also stops the owner. Calls preserve binding ownership, Schema/identity invalidation and continuation single-use checks.

Named commands bind and parse against the fresh remote Schema after their one credential preflight. An identity change lets a new named command rebind after old calls drain; existing typed bindings and continuations fail their scope checks. A catalog change or TTL expiry that appears while credentials are pending waits for in-flight I/O to finish, then refreshes using that same credential snapshot. Crossing the TTL boundary immediately before admission also retains the resolved context. A cancelled or expired operation is checked again before I/O starts, and the driver deadline includes the wait for exclusivity.

Help and argument errors complete without a business request and can run alongside an existing call when a slot is available. Invalid continuation responses leave the original handle available for correction; cancelling before admission does too. An admitted resume consumes its handle, so a duplicate or cancelled-dispatch retry sends no second business request.

`execute()` and `execute_argv()` provide string and argv entry points. `submit_json()` passes JSON directly. `drain_events()?.await` drains an `EventBatch` when no event sink is configured; reserve queue capacity for control requests if submitting a full batch. Parallel native events retain their core `operation_id` and include `queue_operation_id` to associate progress/terminal records with the submitted future. Pending-only cancellation events remain to be integrated; futures already return those outcomes. `spawn_with(config, options, factory)` runs the client factory inside the owner thread, allowing it to construct non-`Send` credential providers or callbacks there.

HTTP readers run independently through a bounded 16-message channel with chunks capped at 64 KiB. A slow credential provider leaves other response processing, completion and event delivery running, and is resolved once for the admitting call. Each admitted operation captures its own credential. Cancelling one HTTP call closes its stream; legacy calls also send a best-effort request-specific cancellation notification. Shutdown aborts and joins all readers before closing the session. Legacy session cleanup retains the original session credential across identity invalidation and uses it before reinitialization or final close.

The queued stdio transport has one persistent stdout framer and one ordered stdin writer for the child. Cancelling a call does not discard a partial line or UTF-8 character; late replies for allocated past IDs are ignored. A broken shared frame ends all in-flight calls and closes the child. A write already in progress finishes its frame before its cancellation notification, and cancelled buffered writes retain their input-byte lease until discarded or flushed. Explicit close stops stdin, drains output during the grace period, and joins reader, writer and stderr tasks after reaping the child. These transport semantics follow [MCP stdio](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio) and [cancellation](https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/cancellation).

Unscoped shared-stream diagnostics use core `operation_id: 0` without a queue ID; correlated progress retains its originating operation. This avoids assigning a global log to whichever call most recently started.

## WebAssembly and WASI

```bash
npm ci --ignore-scripts
cargo install wasm-bindgen-cli --version 0.2.130 --locked --root target/tools
bash scripts/build-wasm.sh
npx playwright install chromium
npm run test:wasm
```

The build creates `dist/web` for browser loading and `dist/component` for testing the WASI component through jco. The original component is `target/wasm32-wasip2/debug/mcpx_component.wasm`.

```javascript
import init, { Runtime } from "./dist/web/mcpx_web.js";
import { Remote } from "./web/client.js";

await init();
const remote = await new Remote(Runtime.auto("https://example.com/mcp")).connect();
const echo = remote.tool("echo");
const outcome = await echo.call({ message: "hello" });
echo.dispose();
await remote.close();
remote.runtime.free();
```

Serve the modules over HTTP. The MCP endpoint must permit the browser origin and expose the required response headers. The JS wrapper also accepts an injected fetch function, token and event callback. Object properties with `undefined` are omitted; `null` is preserved. Non-JSON values, cyclic objects, unsafe JS integers and non-finite numbers are rejected; `callRaw()` accepts/returns JSON text when values must bypass JS Number conversion.

Pass `{ elicitation: { form: true, url: false } }` to `Remote` to enable explicit interactions. For `outcome.kind === "needs_input"`, pass the original `outcome.value` object to `remote.resume(outcome.value, responses)`. The wrapper checks ownership and rejects reconstructed handles; `resumeRaw(id, json)` is the lower-level remote-local interface. The component resource exposes the corresponding `set-elicitation`, `advance-time`, `resume` and `release-continuation` methods.

The component exports `mcpx:runtime/engine@0.1.0`. Its `create` factory returns a resource. A host calls `discover`, drains `actions`, performs HTTP I/O, then supplies response headers and byte chunks through `begin-response`, `push-response` and `finish-response`. See [the WIT contract](wit/mcpx.wit) and [the executable smoke test](scripts/smoke-wasm.mjs) for host integration. The WIT ABI carries typed action/event/error/outcome records and variants; request bodies and input chunks are bytes. Schema, business arguments/results and unknown extensions remain JSON text. The ABI is experimental: rebuild the component and host together after interface changes.

### Standalone WASI CLI

The same CLI package builds a `wasi:cli` command with a `wasi:http` backend:

```sh
bash scripts/install-wasmtime.sh
cargo build -p mcpx-cli --target wasm32-wasip2 --locked
target/tools/wasmtime/wasmtime run -S http -S cli-exit-with-code \
  target/wasm32-wasip2/debug/mcpx.wasm \
  --url http://localhost:8000/mcp --output json echo --message hello
```

The installer pins [Wasmtime 50.0.1](https://github.com/bytecodealliance/wasmtime/releases/tag/v50.0.1) and verifies its release archive SHA-256 on Linux x86_64. Other hosts can install that release and set `WASMTIME` to the executable for the tests. The runtime must provide HTTP and `wasi:cli/exit.exit-with-code`; Wasmtime requires the two `-S` options above. The older CLI exit interface reports only success/failure, so the explicit extension preserves mcpx's detailed exit statuses. This is a declared host requirement rather than a portable assumption about every WASIp2 runtime.

The WASI entry point supports named HTTP configurations and direct `--url`, both protocol versions/Auto, `tools`, `call TOOL`, dynamic help/completion, `--args`/`--args-file`, all three output modes, timeouts, `--token-env` and `--schema-registry`. Native and WASI share route parsing, configuration validation, argument-file bounds, Schema-bundle loading, output escaping and exit classification. A file such as `/data/request.json` needs an explicit host preopen (`--dir ./requests::/data` before the Wasm path); environment variables similarly need host grants. Global `--help` and `complete --help` perform no MCP I/O. Configured stdio servers can be listed, but calling one returns `CapabilityUnavailable` before credentials, argument files or network I/O are processed. Subprocess transport and guest signal-driven Ctrl-C remain unavailable in this WASI entry point.

The `mcpx` facade's `wasi` feature exposes `mcpx::wasi::WasiDriver` on `wasm32-wasip2`. It implements the same core `Driver` using bounded WASI input/output streams and pollables, with one absolute operation deadline across writes, headers, response chunks, discovery and pagination. It does not create Tokio, read environment/files or print. This driver blocks the host task during its async trait methods; `run_blocking()` drives these immediately-ready futures and rejects a genuinely pending future. Use `WasiQueue` below for standalone concurrency. The existing low-level component/host API also supports asynchronous concurrent embedding.

Opt into HTTP catalog notifications with `WasiDriver::with_http_notifications(true)`. `WasiClientExt` adds `start_notifications()`, `stop_notifications()`, `notification_status()` and `poll_notifications(wait)` to `Client<WasiDriver>`:

```rust,ignore
use mcpx::wasi::{WasiClientExt, WasiDriver, run_blocking};
let driver = WasiDriver::new(credential, std::time::Duration::from_secs(5))?
    .with_http_notifications(true);
let mut remote = mcpx::Client::new(driver, runtime);
let binding = run_blocking(remote.fresh_tool("echo"))?;
let mut changes = remote.subscribe_catalog()?;
remote.poll_notifications(std::time::Duration::from_millis(100))?;
let status = remote.notification_status();
remote.stop_notifications()?;
run_blocking(remote.close())?;
```

Ordinary HTTP output/header/body waits poll the notification reader and capability writer alongside their own resources. Idle applications must call `poll_notifications`: notifications make progress only while the host drives this owner. A poll wait is at most 24 hours; zero runs one bounded service pass, and a normal poll timeout is not a subscription failure. Opening/ACK uses the driver timeout; a listening stream has no aggregate operation deadline, though the WASI host may impose an idle timeout. The core continues to enforce 64 KiB chunks and 8 MiB SSE frames. Each service pass performs at most 16 steps, with at most 16 queued legacy replies and one 500 ms writer. Already accepted replies can drain after EOF or a reader failure while ordinary requests proceed. Stop drops reader/writer resources before closing the session; child streams and pollables drop before their parents. By default failed or complete streams require explicit restart; ordinary fresh reads do not reopen them.

For a failed legacy HTTP resource with a cursor, `WasiClientExt::resume_notifications(id)` waits for the server's retry and accepted replies using the original credential/session, then returns a fresh `opening` status. `resume_notifications_with_timeout(id, duration)` bounds that cooperative wait; the default is the driver timeout. It does not renew a session or replay a tool POST. Modern streams cannot use this legacy control.

Enable automatic recovery separately on the driver:

```rust,ignore
use mcpx::wasi::NotificationRecoveryPolicy;
use std::time::Duration;
let driver = WasiDriver::new(credential, Duration::from_secs(5))?
    .with_http_notifications(true)
    .with_notification_recovery(Some(NotificationRecoveryPolicy {
        max_attempts: 3,
        backoff: Duration::from_millis(100),
        timeout: Duration::from_secs(10),
    }))?;
```

The policy defaults to `None` and does not enable listening itself. It permits 1..8 GETs per burst, backoff at most 60 seconds and a total window in (0,600 seconds]. The server's retry is a minimum. Reader-observed disconnection, accepted reply draining, delay and opening spend one window; opening is tightened to the time left. Short EOF flaps share one budget; a stream healthy for a full window resets it. Manual success resets the burst. Missing/reset cursors, malformed input, authorization/session/endpoint rejection and failed capability writes stop recovery. All timers are WASI pollables: idle hosts must continue polling, with no background task or CPU preemption.

`WasiQueue::resume_notifications(id)` and `WasiRemote::resume_notifications(id)` return a `WasiOperation<NotificationStatus>`. One independent reserved record (128 accounting bytes) can wait outside the normal queue/input and 16 business slots, including while a provider is pending. A second pending resume returns `QueueFull`. Cancellation/drop and `set_timeout` work as for other operation futures; waiting sends no GET, and a settled success remains successful. Stop/status retain their synchronous APIs and need no queue permit. Stop or a fresh start cancels a pending resume; identity changes invalidate it and preserve the original session credentials for cleanup. Closing or dropping the owner settles it and releases credentials; drop starts no session DELETE. Automatic attempts create no user operation IDs. Queue stats include the waiting resume in `queued`, while normal input bytes remain separate.

See [the recovery example](crates/mcpx-wasi/examples/notification_recovery.rs) and `npm run test:wasi-recovery` for client, queue and Reactor cases. WASI HTTP finishes an empty outgoing body before dispatch, avoiding a needless output-stream flush racing an immediate GET/DELETE response.

See [the fixture-driven standalone notification example](crates/mcpx-wasi/examples/notifications.rs). Build it with `cargo build -p mcpx-wasi --examples --target wasm32-wasip2 --locked`; `npm run test:wasi-notifications` runs the local Wasmtime HTTP fixture. The existing CLI leaves notification listening disabled; a host-driven library owner is required for a persistent listener. The concurrent owner below adds 16-way scheduling; additional runtimes remain open.

`WasiQueue` multiplexes up to 16 mixed calls and explicit continuations through WASI pollables:

```rust,ignore
use mcpx::wasi::{WasiQueue, WasiQueueOptions};
let mut queue = WasiQueue::connect(driver, runtime, WasiQueueOptions::default())?;
let echo = queue.fresh_tool("echo")?;
let mut first = queue.submit_typed(&echo, &request)?;
let mut second = queue.execute("echo", "--message 'another request'")?;
let first_result = queue.wait(&mut first)?;
let second_result = queue.wait(&mut second)?;
queue.close()?;
```

`new(driver, runtime, options)` performs no I/O; use a fresh driver and an idle configured runtime. `bind`, `refresh`, `submit_args`, `submit_json`, `submit_raw`, `submit_typed`, string/argv execution, completion, command specs and `resume` enqueue one-shot `WasiOperation<T>` handles. The constructor rejects stdio and an already busy runtime. Discovery, pagination and refresh use the same nonblocking request states as business calls; a required refresh waits for existing calls to drain, while other streams and notifications continue progressing. Use `bind` and drive its result for asynchronous initialization; `connect` and `fresh_tool` are blocking conveniences.

`poll(wait)` performs bounded service work and may wait for readiness. Zero never waits for HTTP readiness, including metadata I/O; CPU parsing/validation remains synchronous. An operation implements `Future`, but polling that future only reads its result and registers a waker. An embedding executor must drive the owner too. `wait(&mut operation)` drives all queued work until that operation settles. `run(future)` drives a local, potentially non-Send future alongside the owner; submit its operations before entering the runner. For example, `queue.run(async move { Ok((first.await?, second.await?)) })` waits for two already submitted handles while their I/O runs concurrently. It returns when that future finishes and leaves other work under the owner's control. `run_blocking` continues to drive the synchronous `Client<WasiDriver>` futures. The local runner creates no task pool or external runtime services.

Defaults are 64 admitted jobs, 8 MiB of serialized input plus 128 bytes per job, 16 in flight and a 60-second deadline capped by the driver timeout. `set_timeout(&operation, duration)` tightens the deadline measured from submission, including queue and metadata time. Input accounting uses a bounded serializer rather than allocating a second full JSON buffer; it is not an allocator limit. Oversized raw text is rejected before parsing, and a full queue rejects typed arguments before invoking their serializer. Response chunks and headers recheck the individual deadline before feeding the core. One timeout, cancellation or malformed stream leaves peers running. A queued cancellation is `not_sent`; dispatched interruptions retain `unknown` evidence and never replay business calls. Legacy cancellations use a separate bounded pool of 16 requests with 200 ms deadlines.

For dependent async workflows, transfer the queue into a `WasiReactor` and obtain a clonable `WasiRemote` submission handle. Its methods use the same budgets, validation, FIFO controls, credential checks and operation futures as the queue. Submit new work after an earlier await, or submit several operations before awaiting them to run concurrently:

```rust,ignore
#[derive(serde::Serialize)]
struct Echo<'a> { message: &'a str }

let mut reactor = queue.into_reactor();
let remote = reactor.remote();
let result = reactor.run(async {
    let echo = remote.bind("echo")?.await?;
    let first = remote.submit_typed(&echo, Echo { message: "typed" })?;
    let second = remote.execute("echo", "--message 'from a string'")?;
    Ok((first.await?, second.await?))
})?;
reactor.close()?;
```

The local root future can borrow stack data and hold non-Send values. `run` polls it without holding a queue borrow; short submission borrows end before awaiting an operation. `WasiRemote` holds only a weak reference, so retained handles, results and root wakers do not keep the reactor or provider alive. Existing operations remain valid when transferring a queue. Returning or failing the root future leaves unrelated work under the owner; dropping unfinished operation handles requests their individual cancellation. Owner Drop settles unfinished work without starting new I/O; explicit `close()` performs bounded session cleanup and can retry a failed cleanup. Keep the reactor to drive work; a remote handle alone cannot perform I/O.

Provider, event-handler or serializer callbacks that reenter the same reactor receive `Busy` instead of a borrow panic. Once the owner is dropped, handle access returns `TransportClosed`; submissions also reject an explicitly closed owner. `reactor.poll(Duration::ZERO)` provides bounded cooperative service for an embedding executor, and operation futures wake that executor on settlement. The local runner supplies the same cooperative readiness fallback as `WasiQueue::run`; runtime-specific timers, task pools or reactor services remain the embedding application's responsibility. See [the dependent workflow example](crates/mcpx-wasi/examples/reactor.rs) and `npm run test:wasi-reactor`.

Dropping an unfinished operation requests cancellation on the next owner service pass. An accepted result wins a later cancellation. Results are consumed once, and retained handles keep their own result/owner identity marker rather than the reactor or credentials. Foreign-owner waits and deadline changes return `InvalidHandle`. `close()` settles pending work, drops readers, drains bounded cancellation I/O, and closes the original legacy session; failed session cleanup can be retried. Dropping the owner closes its I/O and fails unfinished handles without starting session-delete I/O, so use explicit close for that cleanup. `drain_operation_events()` reads the bounded lifecycle journal with terminal reservations; drain it regularly to avoid admission backpressure. `drain_events()` reads protocol events when no driver callback consumes them.

The notification controls and catalog watcher also work on this owner, outside business-call slots. See [the concurrent fixture example](crates/mcpx-wasi/examples/queue.rs) and `npm run test:wasi-queue`.

Schema registry replacement, registry/cache statistics, validator cache capacity/clear, catalog TTL, elicitation modes and continuation release use the same queue as FIFO barriers. Earlier calls finish before a control runs; later jobs wait behind it. These local controls do not resolve credentials or fetch metadata, even on an unconnected owner. Registry source accounting counts toward the input budget. Cancellation/deadline before execution leaves the configuration unchanged, and every control keeps its own lifecycle events and one-shot result.

`set_catalog_ttl`, `set_elicitation` and `release_continuation` now return queued operation handles too. Keep and drive the returned handle: dropping an unfinished handle requests cancellation. For example:

```rust,ignore
let configure = queue.set_elicitation(true, false)?;
queue.run(configure)?;
let configure = queue.set_validator_cache_capacity(32)?;
queue.run(configure)?;
let stats = queue.validator_cache_stats()?;
let stats = queue.run(stats)?;
```

Explicit `refresh()` captures the catalog revision at submission. Pending refreshes may reuse a complete, fresh publication made since that revision in the same authorization/registry context. Every operation still resolves its credential independently and keeps its own deadline, cancellation, budget and terminal event. A canceled/failed leader does not fail followers; the next eligible request can perform metadata discovery. A new explicit refresh submitted after publication forces another refresh. Context invalidation and expired/zero TTL prevent reuse; business calls are never replayed. See [the configuration/refresh fixture](crates/mcpx-wasi/examples/controls.rs) and `npm run test:wasi-controls`.

Install a core `CredentialProvider` with `queue.set_credential_provider(provider, Some("account:work".into()))?` while the owner is idle. Installation performs no resolution or network I/O. Each job resolves once and freezes its credential across metadata and business requests; one provider future runs at a time while already admitted responses and notifications keep progressing. The owned reference is bounded to 1..1024 UTF-8 bytes and comes from configuration. Resolution and session-transition jobs count toward queue capacity and input accounting; `stats().resolving` and `authenticating` expose those phases. Provider errors become a fixed `CredentialUnavailable`, and bearer values over 64 KiB are rejected without sending a request.

Provider futures may borrow their provider and request and need not be Send. They must perform nonblocking, cancellation-safe polling. Cancellation/deadline drops the current future, returns its exclusive provider lease for reuse, and releases the caller; late wakes cannot restart it. Close also releases the configured provider while the closed owner and settled handles remain alive. The reactor honors wake requests and includes a cooperative 10 ms readiness check while a provider is pending. The local runner has the same fallback for idle local futures, so a future that depends on an external runtime still needs that runtime's services.

When a resolved identity changes, earlier calls drain before the old listener stops and the old legacy session is deleted with its original credential. Only then is the new identity accepted and its catalog discovered. Old bindings/continuations fail before business I/O. Updating a token with the same identity keeps bindings valid; existing requests and notification streams retain their own snapshots. Legacy session cleanup retains the credential that established that session, including after same-identity token updates. A failed or interrupted DELETE preserves its snapshot for explicit retry and clears authority over the uncertain old session. These paths never replay a business call. Status/stop and background notification processing do not resolve providers; notification start uses the already accepted scope and rejects an in-progress identity transition.

See [the credential/runner fixture](crates/mcpx-wasi/examples/credentials.rs) and `npm run test:wasi-credentials`. More executor/reactor integrations and broader WASI runtime/SDK/lifecycle validation remain outstanding.

The [WASI Rust library example](crates/mcpx-wasi/examples/library.rs) shows runtime builder, caller-defined `Serialize` arguments, string/argv calls and explicit form recovery:

```sh
cargo build -p mcpx-wasi --example library --target wasm32-wasip2 --locked
target/tools/wasmtime/wasmtime run -S http \
  target/wasm32-wasip2/debug/examples/library.wasm http://localhost:8000/mcp
npm run test:wasi-cli
```

The smoke suite actually launches the CLI and library in Wasmtime against local fixtures. It checks bounded multi-chunk request/response I/O, input rejection without a business request, metadata, raw uint64, preopens/stdin, env grants, registered Schema refs, event output, legacy session DELETE and explicit cleanup retry. Separate stalled-header and stalled-body cases prove poll deadlines abort the stream, send only the legacy core cancellation packet and do not replay business calls. Authentication failures do not downgrade or retry; redirects are not followed. The configured-trust HTTPS embedder below adds successful TLS connections; additional actual runtimes, broader SDK/HTTPS interoperation and the remaining lifecycle matrix still require validation.

The separate official SDK matrix uses the locked `rmcp = 3.10.0` server and the standalone WASI reactor:

```sh
cargo build -p mcpx-native --example sdk_http_server --locked
cargo build -p mcpx-wasi --example sdk_interop --target wasm32-wasip2 --locked
npm run test:wasi-sdk
```

It launches 45 Wasmtime guests and 42 SDK server processes: Modern, explicit Legacy and Auto-to-Modern with JSON/SSE preferences; dependent typed/JSON/raw/string/argv workflows; 16 actual concurrent calls and 16 explicit resumes; progress, structured result deserialization, subscriptions, timeout isolation and original legacy session cleanup. Each barrier waits for all 16 SDK handlers before any response, so serial execution cannot pass. A local observing proxy forwards SDK payloads unchanged and, for the timeout scenario, withholds one already completed SDK response to verify `unknown` evidence, an independent peer and zero business replay.

The matrix asserts actual response MIME types: modern terminal results exercise JSON when preferred, progress falls back to SSE, and this SDK keeps legacy session calls on SSE even with JSON preference. The service explicitly advertises 60-second TTLs for concurrency tests; a separate zero-TTL case requires nine actual list RPCs during its six calls and local metadata operations. Modern version rejection is terminal for both Modern and Auto; explicit Legacy verifies the SDK's older protocol path. This does not expand Auto's downgrade rules.

Three additional guests verify that an untrusted local HTTPS certificate produces a certificate-trust alert, reaches no HTTP handler and causes no retry or downgrade. Certificate policy belongs to the host: [Wasmtime 50.0.1's default sender](https://raw.githubusercontent.com/bytecodealliance/wasmtime/v50.0.1/crates/wasi-http/src/default_send_request.rs) uses built-in public roots. The configured-trust embedder below supplies a separate verified HTTPS path. The matrix is local interoperability evidence for this SDK/runtime pair; CI is configured but has not been run remotely.

### Configured-trust HTTPS embedding

`tools/wasi-https-host` is a native test embedder using Wasmtime 50.0.1 and the standard WASIp2 HTTP/CLI interfaces. Its independent workspace/lockfile requires Rust 1.100 for host tooling; mcpx's ten product crates and the guest retain Rust 1.88. Build it and run the same SDK assertions over HTTPS:

```sh
cargo build --manifest-path tools/wasi-https-host/Cargo.toml \
  --target-dir target/tools/wasi-https-host --locked
cargo build -p mcpx-native --example sdk_http_server --locked
cargo build -p mcpx-wasi --example sdk_interop --target wasm32-wasip2 --locked
npm run test:wasi-https
```

The host accepts `--ca-pem PATH --allow-origin HTTPS_ORIGIN MODULE [ARGS...]`. It reads at most 1 MiB of PEM with 1..64 certificates, trusts only that bundle and retains certificate validity/name checks. It grants one HTTPS origin, disables proxy inheritance and redirects, and gives the guest stdout/stderr/arguments with no inherited environment or filesystem preopens. Its [`WasiHttpHooks` bridge](https://raw.githubusercontent.com/bytecodealliance/wasmtime/v50.0.1/crates/wasi-http/src/ctx.rs) streams request and response bodies through reqwest/rustls and preserves the guest's connection, header and between-byte timeouts. It uses HTTP/1.1.

The HTTPS matrix reuses the existing 42 SDK cases with an HTTPS observing gateway in front of the loopback SDK server. It asserts encrypted sockets and unchanged SDK payloads, including JSON/SSE, 16-way calls/resumes, long-lived notifications, zero-wait polling, deadlines and legacy DELETE. Another 18 guests cover Modern/Legacy/Auto rejection of wrong CA, wrong hostname, expired certificate, foreign origin, cleartext and redirect-to-cleartext. The temporary CA and leaf keys are removed after each run; certificate verification is never disabled. This is evidence for the configured embedder/gateway path, while the stock CLI's public-root HTTPS and more runtime/platform combinations remain separate acceptance work.

### Shared named configuration

`mcpx-config` owns the common TOML definition. The facade's `config` feature exposes `mcpx::config::Config::parse(text)` without I/O and `Config::load(path)` for explicit bounded file loading. Native `mcpx::native::Config`, `ServerConfig` constructors and `.connect()` remain available through an adapter over this definition. Parsing does not fetch endpoints, resolve credentials or launch processes. `load` resolves relative stdio working directories against the configuration file's canonical parent, including when the file is opened through a symlink. `load_unresolved(path)` applies the same bounded UTF-8/TOML loading while retaining relative directories, as `parse` does. The WASI CLI uses this entry point because it lists but cannot launch stdio servers; unused process directories require no canonicalization capability.

WASI accepts the same named routes as native: `SERVER TOOL`, `tools SERVER`, `call SERVER TOOL` and `complete SERVER --line LINE`. For example, list the repository's example configuration without connecting:

```sh
target/tools/wasmtime/wasmtime run -S http -S cli-exit-with-code \
  --dir .::/data -S cwd=/data target/wasm32-wasip2/debug/mcpx.wasm \
  --config /data/mcpx.example.toml servers list
```

Without `--config`, it reads `mcpx.toml`. WASI relative config, argument-file and Schema-bundle paths use the host's `wasi:cli/environment.initial-cwd`; the CLI does not assume Rust's process working directory reflects it. `-S cwd=/data` declares that directory in Wasmtime, while `--dir` separately grants filesystem access. Resolving a path does not grant access outside preopened directories.

Configured `credential = "env:MCP_TOKEN"` needs an explicit guest environment grant, such as Wasmtime's `--env MCP_TOKEN` before the Wasm path. The variable is resolved only for the selected HTTP server. Global `--token-env` can override the configured credential for this invocation. The WASI CLI returns `CredentialUnavailable` for other reference namespaces; an embedding application can resolve its own reference and pass a `Credential` to `WasiDriver`. Listing mixed HTTP/stdio configurations does not resolve any credentials or attempt child-process execution.

`npm run test:config-cli` runs the same named-configuration scenarios against native and Wasmtime executables. Pass `native` or `wasi` directly to `scripts/smoke-config-cli.mjs` to run one target. Tests include offline/denied-file behavior, strict parsing, protocol and credential overrides, default/relative paths, reserved management names, Unicode/oversized completion cursors, and rejection before business dispatch. Both executables must be built first.

### Typed component embedding

The [typed host example](examples/wasi-component/README.md) drives WIT directly from Node/jco:

```javascript
import { engine } from "./dist/component/mcpx_component.js";
import { driveComponent, closeComponent } from "./examples/wasi-component/host.mjs";

const remote = engine.createAuto(endpoint);
try {
  remote.discover();
  await driveComponent(remote);
  const binding = remote.bind("echo");
  const operation = remote.callConcurrent(binding, '{"message":"hello"}');
  await driveComponent(remote);
  const outcome = remote.completion(operation); // completed(tool-response) | needs-input(continuation)
  if (outcome.tag === "completed") console.log(outcome.val.rawJson);
} finally { await closeComponent(remote); }
```

`actions()` returns a list of `http`, `stream` or `cancel` variants. HTTP includes a host `actionId`, separate optional `rpcId`, `rpcMethod`, `operationId`, a typed HTTP method, URL, header list and byte body; `begin-response` accepts a header list and chunks use `list<u8>`. `events()` returns an `event-batch` record containing `events` and a u64 `dropped` count. Each event has u64 IDs/sequence, a kind variant, optional typed failure and raw payload JSON. `setEventLimits(capacity, maxBytes)` and `eventStats()` configure/inspect the core buffer. The example driver exposes whole batches through `onEvents(batch)`, including empty loss-only batches. `completion()` returns an optional typed outcome once; output validation is an enum. Errors have `code`, `message` and validation pointer records. `tools()` exposes descriptor records with Schema/extension JSON text. Native WIT hosts use these types directly; the jco adapter preserves business JSON text and u64 metadata rather than rounding it through ordinary JSON parsing.

Response metadata is checked before opening a decoder: integer status 100..599; up to 256 header fields and 64 KiB of names/values; valid field names/values; no conflicting content-type/session/version singleton values. Ordinary repeated extension fields are combined. The JSON host path additionally limits its header text to 64 KiB. Invalid metadata leaves the request available for correction. Wasm/jco Number status inputs reject overflow aliases such as `65536 + 200`.

### Concurrent JavaScript calls

`Remote` schedules typed/JSON calls, argv/string commands and continuation resumes with 16 requests in flight by default. `maxInFlight` accepts 1..16, `queueCapacity` 1..1024 (default 64, including active work), and `maxInputBytes` 1 KiB..64 MiB (default 8 MiB). UTF-8 input bytes plus a record allowance are charged until I/O cleanup finishes. Discovery remains a FIFO barrier; commands share the same slots as bound calls. Results can finish out of order.

```javascript
const controller = new AbortController();
const operation = echo.call({ message: "hello" }, {
  signal: controller.signal,
  timeout: 5_000,
});
console.log(operation.operationId); // Host queue ID, separate from the core operation ID.
// operation.cancel() or controller.abort() cancels this operation.
const outcome = await operation;
await remote.close();
remote.runtime.free();
```

The constructor's `timeout` defaults to 60 seconds; per-call deadlines include queue time. The host rechecks the deadline after event waits, before sending I/O, and before accepting incoming bytes. A synchronous parser that overruns the deadline cannot subsequently send its request; this check does not preempt the parser itself. Capacity rejection returns `QueueFull`; expiry returns `Timeout`. Unsent cancellation/expiry does not dispatch a request. An active interruption closes that call's stream; legacy HTTP also sends a bounded best-effort cancellation notification with the call's credential snapshot. `remote.cancel()` cancels all currently admitted work. An accepted terminal result wins a later cancellation.

Disposing a binding stops new submissions; already-admitted calls retain its runtime handle through cleanup. `close()` rejects queued work, interrupts active work and waits for cleanup before releasing bindings and tracked continuations. It closes the wrapper; the caller still owns the supplied runtime and must subsequently call `free()` or dispose the component resource. Do not drive or free that runtime while its `Remote` is active.

Concurrent progress and terminal callbacks retain `operation_id` and add the submitted promise's `queue_operation_id`. `onEvent` is a synchronous observer; exceptions are delivered to `onEventError` and do not terminate other requests. `coreEventCapacity` and `coreEventBytes` configure the independent core buffer with the bounds above. Core loss arrives as `{ kind: "lagged", operation_id: 0, payload: { source: "core", dropped } }`; this host report has no invented core sequence. Pending-only and control-operation events still need a unified host event contract.

The low-level Wasm `events()` string is a batch envelope with `events` and `dropped`; event payloads use a `payload_json` string. The high-level host safely decodes each payload separately into `event.payload`. If it contains an unsafe JS number, that event instead exposes `raw_payload` and `payload_error: "NumberOutOfRange"`; its metadata and the rest of the batch, including terminals, still arrive. `parseSafe` rejects unsafe call output numbers and retains the original text in `error.raw`; `callRaw` preserves raw JSON results.

For a jco component, `componentRuntime(component)` from `web/component.js` adapts the fixed ABI to `Remote`. The adapter does not generate any remote tool methods. [The high-level host scenarios](scripts/web-client-host.mjs) run on Node Wasm, jco and cross-origin Chromium HTTP/2, including a 16-request mixed typed/argv/string server barrier, the seventeenth queued call, budgets, parallel form resumes, local failure isolation, timeout/cancel races and close cleanup. Help and parse errors finish locally while another command runs, and errors do not end its stream. The command input budget includes its tool name.

### JavaScript credentials and catalog freshness

Direct `Remote`, its jco adapter and `WorkerRemote` support `catalogTtl` (milliseconds, default 60,000), `refresh(options)` and `freshTool(name, options)`. `tool()` uses the cached descriptor; `freshTool()` resolves credentials and refreshes an expired catalog before binding. Modern server `ttlMs` can shorten the configured TTL. A successful refresh uses `tools/list` on an established connection and preserves unchanged bindings. Queued explicit refreshes observing the same catalog generation are coalesced while fresh, with separate credential preflight and context fences for authorization, schema resources and observed invalidation. Schema changes reject old bindings with `BindingStale`; named commands parse against the refreshed descriptor.

```javascript
const remote = await WorkerRemote.connect(endpoint, {
  catalogTtl: 60_000,
  credentialReference: "account:work",
  credentialProvider: async ({ endpoint, reference, signal }) => {
    const account = await accounts.resolve({ endpoint, reference, signal });
    return { token: account.accessToken, authContextId: account.permissionEpoch };
  },
});
const search = await remote.freshTool("search");
await search.call({ query: "rust" });
await remote.close();
```

The application supplies `accounts`; no account SDK or tool-specific code is generated. For direct JS, pass the same options to `new Remote(runtime, options)` and await `connect()`. `authContextId` is a non-secret identifier of 1..256 UTF-8 bytes and must change when identity or permissions change. The optional `credentialReference` is bounded to 1..1024 bytes and requires a provider. Choose either a provider or a static token. Assigning a different static `remote.token` advances its opaque authorization context conservatively.

Each operation resolves credentials once and freezes them across discovery, pagination, refresh and its business request. Provider waits leave existing response processing running. An identity switch waits for earlier calls to drain, invalidates their bindings/continuations and lets a named command or `freshTool()` bind the new catalog. Reusing an old binding returns `BindingStale` before business I/O. An old legacy session is deleted with its original credential before a new principal initializes; final close also performs bounded session cleanup. Cleanup failure is reported as `SessionCleanupFailed`. Direct `Remote` retains its cleanup record so `close()` can retry; `WorkerRemote.close()` reports the error and disposes the worker.

Provider errors are sanitized. Only one provider callback may remain unresolved per remote, including a callback that ignores abort; cancelling its operation releases the caller promptly but new lookups return `CredentialUnavailable` until that callback settles. The worker broker keeps callbacks in the page, limits credential messages to 64 KiB, and binds the requested endpoint/reference to connection configuration. Secrets are passed to HTTP only, never catalog keys or standard events. Cancellation and deadlines include provider/admission waits. Providers should honor `signal` to release their own resources.

Local binding/catalog epochs use opaque identity markers. Static-token credential rotations use a checked identity counter: after its final exact value, changing the token fails before MCP I/O and leaves the previous identity intact; an unchanged final token remains usable. Invalid static credentials do not consume an identity. Worker credential failures remain redacted through its credential channel.

After settlement, an operation's promise keeps its result or rejection and `operationId`; its `cancel()` becomes a no-op. The host disconnects cancellation and releases the job's input closure, raw completion, credential, action, binding lease and abort listeners. [Default V8 error stacks](https://v8.dev/docs/stack-trace-api) are materialized before rejection so their lazy CallSites do not keep an earlier receiver/async job alive; error identity, type, code, pointer issues and `raw` JSON are preserved. Custom stack formatters or application references can retain their own objects.

`close()` releases the configured credential provider/reference and static-token copies. It cannot stop a provider's own work if that callback ignores cancellation. A failed direct-Remote session DELETE still retains its original bounded cleanup credential for an explicit `close()` retry; successful cleanup releases it. Low-level runtimes remain owned by their embedding caller and require their own disposal.

The low-level host must drive this policy explicitly: advance its monotonic clock, inspect `catalog-status(scope)`, wait for exclusive access if required, accept the non-secret scope, and discover or refresh when needed. `catalog-status` is a typed WIT record. Credentials and asynchronous provider callbacks remain host-owned; these methods do not perform HTTP themselves.

### Web Worker and browser CLI

`WorkerRemote` keeps the Rust runtime, catalog, bindings, command parser and Schema validation in one dedicated module worker. The page owns a small fetch driver that only executes I/O actions and transfers byte chunks; it does not parse MCP or validate tool schemas. Keeping cancellation controllers outside the core worker lets the page abort its HTTP streams even if that worker stops yielding.

```javascript
import { WorkerRemote } from "./web/worker.js";

const remote = await WorkerRemote.connect("https://example.com/mcp", {
  onEvent: event => console.log(event),
  elicitation: { form: true },
});
const echo = await remote.tool("echo");
await echo.validate({ message: "hello" });
const operation = echo.call({ message: "hello" }, { timeout: 5_000 });
// operation.cancel() or an options.signal aborts this call.
const outcome = await operation;
await remote.executeLine('echo --message "quoted text"');
await remote.execute("echo", ["--message", "argv"]);
await echo.dispose();
await remote.close();
```

`executeLine()` is also available on the direct JS `Remote` and through the Wasm/WIT `command-line` method. A line starts with the tool name for that connected remote. All three hosts reuse the Rust tokenizer; quotes and escapes do not perform shell expansion. String/argv commands share the same 16-slot schedule as typed/JSON calls and resumes. Typed calls use JSON-compatible objects, while `callRaw`, `validateRaw` and `resumeRaw` keep JSON text intact. Unsafe typed output retains the original JSON in `error.raw`. Schema errors retain their JSON pointer `issues`.

Worker binding creation, validation and disposal are asynchronous. Calls and resumes return promises carrying a host `operationId` and `cancel()`. The ordinary request queue defaults to 64 records and 8 MiB of UTF-8 messages; the input budget is held until the worker acknowledges completion/cleanup. Cleanup requests have a separate bounded reserve of 160 records/64 KiB, and close always has an emergency slot. Already-submitted calls retain disposed bindings until cleanup. Continuations retain the original-object ownership check and core single-use semantics.

The event mailbox counts both buffered and posted-but-unacknowledged records. Defaults are 256 records/4 MiB. `eventCapacity` must be between `2 * maxInFlight + 4` and 4096; `eventBytes` between `(maxInFlight + 3) * 16 KiB` and 64 MiB. Terminal space is reserved. When notification credit is exhausted, additional progress/log records are dropped with a host `lagged` report carrying `payload.source: "worker-mailbox"`, while admission and subsequent stream reads wait for acknowledgements. Independent core losses use source `"core"`; `diagnostics()` exposes `droppedEvents` and `coreDroppedEvents` separately. The core options `coreEventCapacity`/`coreEventBytes` also work through WorkerRemote.

A loss-only mailbox report consumes a counted credit, with at most one such report outstanding; further loss accumulates until ACK. Graceful close returns remaining unposted loss before terminating the worker. Duplicate ACKs do not release more credit. Call cancellation and close interrupt event waits. Forced termination cannot recover unreported worker state; pending/control event semantics and the full resource budget matrix remain unfinished.

The I/O bridge has 36 slots, at most one outstanding 64 KiB chunk per reader, and an 8 MiB aggregate budget for retained input chunks. MCP responses are limited to 8 MiB, runtime binary loading to 64 MiB. Completed, cancelled and closed operations wait for the I/O owner's cleanup acknowledgement. `diagnostics()` reports non-secret queue/transport counters. Results retained by application code and browser-internal allocations require separate budgets.

Deadlines include message and queue time, translated using [`performance.timeOrigin` with the monotonic clock](https://www.w3.org/TR/hr-time-3/#examples). `shutdownTimeout` defaults to two seconds. If a worker does not acknowledge interruption within that grace period, the page terminates it, aborts its I/O and rejects outstanding work with `WorkerUnresponsive` or the initiating timeout/cancellation. Execution can be unknown; no business request is retried. This forced path is separate from graceful close because [worker termination aborts the worker's execution](https://html.spec.whatwg.org/multipage/workers.html#terminate-a-worker).

`close()` waits for cleanup and owns runtime disposal; no separate `free()` is needed. To interrupt startup explicitly, construct `new WorkerRemote(options)` and retain the promise from `remote.connect(endpoint, { signal })`. `workerURL` and `wasmURL` can adapt deployment paths. The module files and generated Wasm must be served over HTTP; the MCP endpoint needs ordinary browser CORS support. Static `token` and asynchronous `credentialProvider` options use the same policy as direct JS and jco hosts; the provider runs in the embedding page and is never cloned into the worker.

[The browser CLI example](examples/web-worker/README.md) provides Connect, a command input, Run, Cancel and Close. It renders server output as bounded text. [The worker smoke test](scripts/smoke-worker.mjs) runs real workers over cross-origin HTTP/2 in four protocol modes, forbids main-thread Wasm instantiation, and checks network closure and actual worker exit, including a deliberately non-yielding worker. `npm run test:wasm` includes this suite and the runnable page.

### Exact host handles

Core request, operation, binding and continuation IDs, and native/JS queue IDs, are opaque positive integers in `1..=9_007_199_254_740_991` (`MAX_HOST_ID` in Rust). Zero is reserved for unassociated events. IDs are never reused within their owner lifetime; reaching the limit returns `LimitExceeded` before allocating another operation. Create a new remote after exhaustion. This common ceiling keeps IDs exact in JSON and every supported host.

Wasm exports use checked JavaScript Numbers instead of truncating through u32. Fractional, negative, zero, non-finite, unsafe or coerced string/boolean handles return `InvalidHandle`. A safe but unknown ID remains a missing handle; adding `2 ** 32` cannot alias the original. `componentRuntime()` checks the same bounds before lowering Number or BigInt IDs into WIT u64. Its binding/operation results retain jco's BigInt representation. Raw WIT hosts must obey the declared integer types; use the adapter when embedding from JavaScript. Generated TypeScript declarations expose Number inputs for the Wasm entry point; runtime validation also protects untyped callers.

Core action, wire RPC, operation, binding and continuation IDs use disjoint owner-local domains with independent counters. Tags/step sizes are internal details: never derive one ID from another or assume consecutive numbers. A domain can reach its own limit below the common ceiling. Host queue, worker RPC, credential and I/O IDs remain scoped to their respective owners/maps; the numbers alone are not globally unique.

ID exhaustion during resume admission preserves the original continuation, whichever allocation domain is exhausted. Exhaustion during discovery/pagination returns one failure and publishes no partial catalog; a failed continuation allocation leaves existing handles valid. Core event sequence/loss counters and catalog generations have explicit limits. The final catalog generation is exactly `MAX_HOST_ID`; further discovery/refresh returns `LimitExceeded` before metadata admission, preserving the last snapshot. Authority and transport invalidation still clear old state at the ceiling and saturate the diagnostic generation; revision tokens continue to observe each invalidation. Create a new owner after exhaustion. Remaining resource budgets and broader ownership races are still planned work.

### Multiplexed host operations

The shared machine and low-level Wasm/WIT resource now admit 16 concurrent calls. `call_concurrent(binding, args)` returns an operation ID; each host action has its own action ID. Feed streams with `begin_response(action_id, ...)`, `push_response_for(action_id, bytes)` and `finish_response_for(action_id)`, then consume `completion(operation_id)` once. More than 16 active or unconsumed completions returns `ConcurrencyLimit`. The serial response methods require exactly one open stream. `cancel_operation()` and `fail_request()` affect one operation/stream, while `cancel()` and `fail()` terminate all active work.

`command_concurrent(tool, argv)` and `command_line_concurrent(line)` prepare commands with the same Rust parser. `CommandAdmission::Help` is a local value and allocates no request; `Operation` uses the ordinary completion/cancellation paths. Wasm-bindgen returns a small JSON admission envelope. The WIT methods return the typed `command-admission` variant (`help(string)` or `operation(u64)`), and the jco adapter preserves its BigInt operation ID. Existing serial command methods remain available for hosts that drive one operation at a time. The WIT action, event, error and outcome envelopes are also typed; `componentRuntime()` adapts them for the shared JS host without parsing/re-encoding business JSON.

Host actions carry separate `rpc_id`/`rpc_method`/`operation_id` metadata. In WIT these are `rpcId`, `rpcMethod`, `operationId`, alongside `actionId`. Notifications have no RPC ID and do not consume the wire counter. `begin_response`, chunk/EOF methods and `fail_request` take an action ID; completion/cancel take an operation ID. A JSON-RPC response must echo the wire ID inside the original byte body. `Machine::action_for_rpc_id()` maps active shared-stream replies; `rpc_id_for_action()` exposes the inverse. `active_action_id()`, `has_action()` and `action_operation()` name the host domain explicitly. Late stdio replies are ignored only for previously allocated wire IDs.

A core action also carries an optional `cancellation` packet. It is JSON text in the Wasm host envelope and bytes in WIT. The packet exists for legacy HTTP and supported stdio requests, contains the original wire RPC ID, and omits tool arguments. It is absent for initialized notifications, initialize, and modern HTTP. Hosts abort the action's I/O and send that packet best effort with the original credential/headers, without parsing its business body or rebuilding MCP cancellation JSON. The typed component example follows this path too.

Every call requests a unique `progressToken`; modern metadata merges preserve that token alongside the mandatory version/capability fields. Progress and HTTP-stream logs carry the correct core operation ID. Discovery, authorization invalidation and catalog replacement are barriers while calls remain active. Continuation retries keep independent arguments/state and obtain new request/operation IDs.

`Client<Driver>::begin_concurrent_call()` applies credential/TTL preflight and admits an external-host operation. Hosts feed data through `host_runtime_mut()` (or split runtime/driver borrowing with `host_parts_mut()`), call `flush_host_events()` after input, and consume `take_concurrent_completion()`. They own asynchronous I/O and cancellation cleanup. `admit_prepared_call()` and `admit_prepared_resume()` accept a context from an already-resolved driver preflight. `admit_prepared_tool()` binds a named tool and builds arguments against the fresh Schema; a `ControlFlow::Break` value returns locally. `ClientCommandExt::admit_prepared_command()` supplies the command parser through that API. The native scheduler uses these interfaces to keep processing other I/O during credential and exclusivity waits. `call_tool_with()` provides the serial named-builder path, so Rust string/argv execution resolves credentials once instead of composing separate bind and call preflights. The native queue drives HTTP and stdio through this interface; the high-level JS `Remote` drives the same per-request runtime methods with parallel fetch/read tasks.

[The concurrent host test](scripts/concurrent-host.mjs) demonstrates actual parallel fetches. Node Wasm, the jco component and Chromium must each deliver all 16 requests before the fixture releases any response. Chromium uses a local HTTP/2 TLS server to avoid HTTP/1 connection-pool limits. This test requires `openssl`, generates a temporary self-signed certificate, and accepts that certificate only in its test browser context.

Language binding generators only build fixed Rust/JS/WIT interoperability glue. Remote tools remain entirely runtime-bound; they do not generate SDK methods.

## Verification

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build -p mcpx-cli --locked
node scripts/smoke-native.mjs
cargo build -p mcpx-native --examples --locked
node scripts/smoke-library.mjs
npm run build:wasm
npm run test:wasm
```

The tested package minimum is Rust 1.88.0; the development toolchain remains pinned to 1.100.0. Install 1.88.0 and its `wasm32-unknown-unknown` / `wasm32-wasip2` targets, then run `bash scripts/check-msrv.sh`. It checks every workspace package against the declared floor, runs native Rust tests, builds native CLI/examples and both Wasm deliverables, and checks the facade without default features on all three targets. It explicitly selects the minimum compiler without changing the default toolchain. CI has a separate minimum-compiler job with actual native/WASI smoke and configuration execution; this configuration is not evidence of a completed remote CI run. Local runtime validation also covers Wasm/jco/Chromium/Worker output built with both compilers.

After building both CLI targets, `npm run test:shells` runs real interactive Tab/argv tests against native and Wasmtime backends. It requires Bash, Zsh, Fish, Python 3 and Wasmtime (`target/tools/wasmtime/wasmtime`, or `WASMTIME`). Locally tested versions are Bash 5.3, Zsh 5.9 and Fish 4.9.3 on Linux. The harness records accepted argv without executing business commands, verifies literal insertion of command-substitution text, and checks that stdio discovery closes and reaps its child.

`npm run test:lifecycle` separately runs the promise-retention suite with explicit GC in Node and Chromium. It retains successful and rejected promises without reading their error stacks, first proves the provider is collectible while a closed remote remains held, then proves the remote is collectible while the promises remain held. A positive control verifies the promise's own result remains alive. Node Wasm/jco, Chromium Wasm and four explicit/Auto workers cover both protocols; this is targeted lifecycle evidence, not a heap-size or RSS bound. The suite is also included in `test:wasm`.

The Wasm smoke test runs real HTTP discovery, SSE calls and explicit interaction recovery in Node WebAssembly, a WASI Component loaded through jco, and headless Chromium. Chromium uses a cross-origin endpoint to exercise CORS. A second suite runs the high-level JS queue and its lifecycle scenarios on the same three hosts. Another exercises the typed WIT host directly, its executable example, and a 16-way server barrier in explicit/Auto modes. The worker suite exercises dedicated browser workers and the interactive CLI example. These cross-platform tests use the project's local fixture server.

`cargo test -p mcpx-native --test sdk --locked` additionally runs against the released official Rust SDK `rmcp 3.10.0`, pinned as a test-only dependency. It covers native modern/legacy HTTP, JSON/SSE responses, automatic modern discovery, input rejection before dispatch, mixed typed/string/argv calls and 16 modern form resumptions per applicable mode. The SDK's empty-data SSE priming events are covered by a shared decoder regression test. This is a bounded interoperability matrix, not complete MCP conformance; cross-platform SDK hosts and the remaining protocol matrix still need coverage.

`cargo test -p mcpx-native --test sdk_stdio --locked` launches the released SDK in an actual child process using the integration-test executable itself. Modern, legacy and auto modes each run 16 mixed typed/string/argv calls, modern modes recover 16 form interactions concurrently, and an EOF marker verifies that SDK shutdown completes before forced termination. The custom harness keeps test output out of the child's protocol stream. The project fixture separately requires all 16 mixed calls or form resumes to reach one child before a result, and tests one cancelled resume alongside others. It also covers cancellation during a split UTF-8 frame, fatal shared-frame fanout, and retained byte accounting for a blocked write.

## Remaining implementation

The complete design still includes subscriptions, persistent caches, broader refresh coalescing and worker/browser lifecycle coverage. System credential storage, OAuth login, persisted catalogs and the full resource/validation budgets remain outstanding. The controlled in-memory Schema registry is implemented. The embedding component requires a host HTTP driver; the separate standalone WASI CLI imports wasi:http directly. Broader WASI executor/reactor integration and runtime/platform/lifecycle validation remain open. Complex schemas without an unambiguous flag projection use `--args`. Broader official SDK interoperability and stricter capability/version error coverage remain required before claiming full protocol compatibility.

The current public APIs and component ABI are experimental. See [implementation status](docs/implementation-status.md) for stage boundaries and known limits.
