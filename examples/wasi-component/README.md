# Typed WASI Component host

Build with `npm run build:wasm`, then run:

```sh
node examples/wasi-component/main.mjs http://localhost:8000/mcp echo '{"message":"hello"}'
```

This is a Node/jco host for the Rust `wasm32-wasip2` component. It is a runnable embedding example. The separate [standalone WASI CLI](../../README.md#standalone-wasi-cli) imports `wasi:http` and runs directly in Wasmtime.

`host.mjs` executes typed `host-action` variants directly. Host actionId, wire rpcId and operationId are distinct metadata fields; HTTP methods/headers are fields, bodies/chunks/cancellation packets are bytes, errors are records, and completion returns `completed` or `needs-input`. MCP discovery, JSON/SSE decoding, Schema checking and command parsing stay inside the component. The host does not parse a JSON action envelope or call an MCP SDK.

`driveComponent(remote, options)` advances the monotonic clock, runs batches of up to 16 core-admitted requests, drains typed event batches between chunks, limits each response to 16 MiB and feeds at most 64 KiB per chunk. `onEvents(batch)` receives `{ events, dropped }`; consume the u64 loss count even when the event list is empty. Each event retains raw `payloadJson` text and typed metadata. The old singular `onEvent` option is rejected. Observer exceptions go to `onEventError`. The driver accepts an AbortSignal and an I/O deadline. A terminal, cancellation or failure waits for stream cleanup. One driver may own a component at a time; wait for it before mutating or closing the resource. A progress observer may call `cancelOperation()` for a running operation.

Before discovery, `remote.setEventLimits(256, 4 * 1024 * 1024)` configures independent core record/byte budgets; `remote.eventStats()` exposes retained records/charged bytes, terminal reservations and pending loss. Limits may change after the buffer is drained and active reservations end. Core admission reserves terminal capacity before I/O. Per-record encoding charges are bounded to 16 KiB; large event previews are compacted while call outcomes remain intact.

`closeComponent()` deletes a retained legacy session with a 500 ms limit, then disposes the component. Cleanup failure retains the resource/session for a retry. The executable reports failure and disposes the resource if that cleanup cannot complete. This example uses anonymous HTTP. The driver aborts cancelled streams and forwards the optional core-provided cancellation bytes with a 200 ms bound. Applications needing credential providers, identity changes, catalog TTL preflight or queues can use `Remote(componentRuntime(component), options)`.

Tool parameters, Schema and raw business results remain JSON text. The executable prints `rawJson` directly, keeping uint64 values intact, and sets a nonzero exit status for tool/output failure or an unfinished interaction. The component's continuation APIs support explicit resume; this command does not answer interactions automatically.

After discovery, `remote.commandSpec("echo")` returns typed parameter metadata and `remote.completeLine("echo --m", 8)` returns typed completion candidates with UTF-8 byte replacement ranges. These are local catalog reads; the driver must refresh an expired catalog first (`CatalogStale` otherwise). Enum values remain strings, including integer candidates. This completion API is separate from `remote.completion(operation)`, which consumes a tool call's result.

Before discovery or while idle, `remote.setSchemaRegistry([{ uri, schemaJson }])` installs a host-preloaded resource bundle. Input, output and form Schemas can reference its documents. No missing URI is retrieved; `schemaRegistryStats()` reports source charges/counts. Replacement invalidates bindings and continuations; failure preserves the prior snapshot. Authorization invalidation clears resources. `host.mjs` continues to perform MCP HTTP actions only, without fetching Schema URLs. Fixed WIT records keep Schema JSON text intact across the host boundary.

`npm run test:component` runs the host and executable against local fixtures in both protocol versions and Auto modes, including a real 16-request server barrier, typed errors/pointers/events, bounded event floods and exact loss accounting, raw uint64 results, form resume, cancel/deadline, reuse and legacy session deletion. The build also emits fixed TypeScript declarations in dist/component/interfaces. These describe the host ABI, not discovered tool-specific methods. Rebuild the experimental component and its host together after ABI changes. The tests do not exercise another Component runtime or a real business service.
