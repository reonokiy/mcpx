# Browser CLI using a Web Worker

From the repository root:

```bash
npm ci --ignore-scripts
npm run build:wasm
python3 -m http.server 8080 --bind 127.0.0.1
```

Open [the example](http://127.0.0.1:8080/examples/web-worker/), enter a reachable MCP HTTP endpoint and click Connect. The endpoint must permit this page's origin and protocol headers through CORS. Discovery uses automatic protocol selection.

Enter a tool name followed by its arguments, for example `echo --message "hello world"` or `echo --help`. Names and flags come from that endpoint's tool schemas. Quoting and escapes use the shared Rust parser; shell expansion and process execution are disabled. The example caps displayed output and inserts it as text.

Press Tab or click Complete for tool/flag names, boolean values and Schema enum values. A single candidate replaces the current token; multiple candidates appear as buttons. Quotes, spaces and text after the cursor are handled by the Rust completion API. Completion can refresh the authorized catalog but does not call a business tool. Editing the command cancels a pending completion and clears its suggestions.

Cancel interrupts the current operation. Close waits for transport cleanup and releases the worker's runtime. WorkerRemote also supports typed JSON objects, raw JSON, argv, explicit form/URL continuations and event callbacks; this small page enables ordinary tool calls with no interaction capabilities. See the root README for those library APIs.
