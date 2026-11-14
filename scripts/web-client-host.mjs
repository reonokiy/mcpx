// Shared integration scenarios, executed against real Wasm in Node, jco and Chromium.
import { schemaResources, exerciseSchemaRegistry, exerciseValidatorCache } from "./schema-fixture.mjs";
function check(condition, message) { if (!condition) throw new Error(message); }
async function rejected(promise, code) {
  try { await promise; } catch (error) {
    check(error.code === code, `Expected ${code}, got ${error.code}: ${error.message}`);
    return error;
  }
  throw new Error(`Expected rejection: ${code}`);
}
function message(outcome) { return outcome.value.raw.structuredContent.message; }

export async function runWebClient(Remote, createRuntime, fetchImpl = globalThis.fetch, { legacy = false } = {}) {
  const owned = [];
  const operationRecords = new Map();
  const open = async options => {
    const { runtime, dispose } = createRuntime();
    const records=[];
    const remote = new Remote(runtime, { fetch: fetchImpl, elicitation: { form: true }, onOperationEvent:event=>records.push(event), ...options });
    operationRecords.set(remote,records);
    owned.push({ remote, dispose });
    await remote.connect();
    return remote;
  };
  try {
    const events = [], observerErrors = [];
    let starts = 0, completed = 0, active = 0, peak = 0;
    const remote = await open({
      fetch: (url, init) => {
        if (JSON.parse(init.body).method === "tools/call") {
          starts++; active++; peak = Math.max(peak, active);
          check(starts !== 17 || completed > 0, "Seventeenth call escaped the concurrency limit");
        }
        return fetchImpl(url, init);
      },
      onEvent(event) {
        events.push(event);
        if (event.kind === "completed") { completed++; active--; }
        if (event.kind === "progress") throw new Error("Observer failure must not cancel I/O");
      },
      onEventError: error => observerErrors.push(error),
    });
    check((await remote.completeLine("ec")).candidates[0]?.value === "echo", "Tool completion failed");
    check((await remote.completeLine("echo --en")).candidates[0]?.value === "--enabled", "Flag completion failed");
    check((await remote.completeLine("echo --enabled t")).candidates[0]?.value === "true", "Boolean completion failed");
    const edited = await remote.completeLine('echo --mode "twZZ"', 15);
    check(edited.candidates[0]?.value === "two words" && edited.start === 12 && edited.end === 18, "Quoted cursor completion failed");
    check((await remote.completeLine("echo --mode 雪")).candidates[0]?.value === "雪 花", "UTF-8 completion failed");
    const spec = await remote.commandSpec("echo");
    check(spec.parameters.some(p => p.flag === "--mode" && p.values.includes("two words")), "CommandSpec was not discovered");
    await rejected(remote.completeLine("雪", 1), "InvalidCursor");
    await rejected(remote.completeLine("", 2 ** 32), "InvalidCursor");
    check(starts === 0, "Completion dispatched a tool call");
    const binding = remote.tool("echo");
    const calls = Array.from({ length: 16 }, (_, index) => index % 3 === 0
      ? binding.call({ message: `parallel-${index}` })
      : index % 3 === 1 ? remote.execute("echo", ["--message", `parallel-${index}`])
        : remote.executeLine(`echo --message 'parallel-${index}'`));
    calls.push(remote.executeLine("echo --message after-barrier"));
    check(new Set(calls.map(call => call.operationId)).size === 17, "Queue operation IDs are missing or reused");
    binding.dispose(); // Queued calls retain the underlying runtime binding until completion.
    await rejected(binding.call({ message: "disposed" }), "InvalidHandle");
    const results = await Promise.all(calls);
    check(results.every((result, index) => message(result) === (index === 16 ? "after-barrier" : `parallel-${index}`)), "Concurrent results crossed");
    check(peak === 16 && completed === 17 && observerErrors.length === 17, "Concurrency or observer isolation failed");
    check(new Set(events.filter(event => event.kind === "completed").map(event => event.queue_operation_id)).size === 17, "Events lost queue correlation");
    check(remote.inputBytes === 0 && remote.active.size === 0, "Completed calls retained queue budget");

    const interruptions = [];
    let cancelOnProgress, localChecks;
    const scopedCalls = [];
    const activeSignal = new AbortController();
    const scoped = await open({ fetch: (url, init) => {
      const rpc = JSON.parse(init.body);
      if (rpc.method === "tools/call") scopedCalls.push(rpc);
      return fetchImpl(url, init);
    }, onEvent: event => {
      interruptions.push(event);
      if (!localChecks && cancelOnProgress && event.kind === "progress" && event.queue_operation_id === cancelOnProgress.operationId) {
        localChecks = Promise.all([
          scoped.executeLine("echo --help", { timeout: 200 }).then(result => check(result.kind === "help", "Help blocked behind a running call")),
          scoped.completeLine("echo --en", undefined, { timeout: 200 }).then(result => check(result.candidates[0]?.value === "--enabled", "Completion blocked behind an active call")),
          rejected(scoped.execute("echo", ["--unknown", "x"]), "CommandParseError"),
          rejected(scoped.executeLine("echo --message '"), "TokenizeError"),
        ]).finally(() => activeSignal.abort());
        localChecks.catch(() => {}); // The main scenario awaits this after cancellation cleanup.
      }
    } });
    const echo = scoped.tool("echo");
    const line = await scoped.executeLine('echo --message "雪 $HOME $(literal)"');
    check(message(line) === "雪 $HOME $(literal)", "String command expanded shell text or changed Unicode");
    await rejected(scoped.executeLine("echo --message '"), "TokenizeError");
    await rejected(scoped.executeLine("  "), "CommandError");
    check((await scoped.executeLine("echo --help")).kind === "help", "String command help was not local");
    check(message(await echo.call({ message: "undefined-is-absent", unused: undefined })) === "undefined-is-absent", "Undefined object property was not omitted");
    cancelOnProgress = scoped.executeLine("echo --message slow-cancel", { signal: activeSignal.signal });
    const cancelled = rejected(cancelOnProgress, "Cancelled");
    const healthy = echo.call({ message: "healthy-during-cancel" });
    const cancelledError = await cancelled;
    check(cancelledError.execution_certainty === "unknown" && cancelledError.retry_advice === "inspect_execution", "Active cancellation claimed no execution");
    check(cancelledError.queue_operation_id === cancelOnProgress.operationId && Number.isSafeInteger(cancelledError.operation_id), "Cancellation lost operation correlation");
    await localChecks;
    check(message(await healthy) === "healthy-during-cancel", "Scoped cancel affected another call");
    await rejected(scoped.execute("echo", ["--message", "slow-timeout"], { timeout: 150 }), "Timeout");
    check(scopedCalls.length === 5, "Help or invalid commands dispatched a business request");
    check(interruptions.some(event => event.kind === "failed" && event.payload.code === "Timeout"), "Timeout terminal lost its code");
    check(interruptions.filter(event => event.kind === "cancelled").length === 1, "Cancel terminal was duplicated");
    check(message(await echo.call({ message: "reused" })) === "reused", "Cancelled remote could not be reused");
    const unsafeResult = echo.call({ message: "raw-number" });
    const conversion = await rejected(unsafeResult, "NumberOutOfRange");
    check(conversion instanceof RangeError && conversion.raw?.includes('"n":18446744073709551615'), "Result conversion lost raw uint64");
    check(conversion.execution_certainty === "result_received" && conversion.retry_advice === "inspect_result" && conversion.queue_operation_id === unsafeResult.operationId && Number.isSafeInteger(conversion.operation_id), "Result conversion lost terminal evidence");

    // The core accepts the terminal before observers run. Cancellation from its
    // completed callback must not replace an already accepted successful result.
    let terminalCall;
    scoped.onEvent = event => {
      if (event.kind === "completed" && event.queue_operation_id === terminalCall?.operationId) terminalCall.cancel();
    };
    terminalCall = echo.call({ message: "terminal-wins" });
    check(message(await terminalCall) === "terminal-wins", "Cancellation replaced a completed result");

    const firstInteraction = await echo.call({ message: "interactive" });
    if (firstInteraction.kind === "needs_input") {
      const interactions = [firstInteraction, ...await Promise.all(Array.from({ length: 15 }, () => echo.call({ message: "interactive" })))];
      const handles = interactions.map(outcome => outcome.value);
      await rejected(scoped.resume({ ...handles[0] }, {}), "InvalidContinuation");
      await rejected(scoped.resume(handles[0], { confirm: { action: "accept", content: { yes: "invalid" } } }), "ValidationFailed");
      const resumed = await Promise.all(handles.map(handle => scoped.resume(handle, { confirm: { action: "accept", content: { yes: true } } })));
      check(resumed.every(outcome => message(outcome) === "interactive"), "Parallel resumes crossed their state");
      await rejected(scoped.resume(handles[0], { confirm: { action: "cancel" } }), "InvalidContinuation");
      check(scoped.continuationIds.size === 0, "Consumed interaction bookkeeping was retained");

      // Cancelling an asynchronous credential preflight must not consume a
      // continuation: the original request/state can still be resumed once.
      let resumeGate, resumeEntered;
      const gated = await open({ credentialProvider: async () => {
        resumeEntered?.();
        if (resumeGate) await resumeGate;
        return { authContextId: "continuation-owner" };
      } });
      const pending = await gated.tool("echo").call({ message: "interactive" });
      let releaseResume;
      resumeGate = new Promise(resolve => { releaseResume = resolve; });
      const entered = new Promise(resolve => { resumeEntered = resolve; });
      const interruptedResume = gated.resume(pending.value, { confirm: { action: "accept", content: { yes: true } } });
      await entered;
      interruptedResume.cancel(); await rejected(interruptedResume, "Cancelled");
      releaseResume(); resumeGate = undefined; resumeEntered = undefined;
      while (gated.credentials.running) await new Promise(resolve => setTimeout(resolve, 0));
      check(message(await gated.resume(pending.value, { confirm: { action: "accept", content: { yes: true } } })) === "interactive", "Cancelled credential preflight consumed a continuation");

      // Resume changes insertion order but must not extend the original TTL.
      // An earlier-expiring handle can therefore follow a still-live handle.
      const retention = await open();
      const retentionEcho = retention.tool("echo");
      const early = await retentionEcho.call({ message: "interactive-repeat" });
      retention.clockOrigin -= 60_000; retention.tick();
      const live = await retentionEcho.call({ message: "interactive" });
      const earlyAgain = await retention.resume(early.value, { confirm: { action: "accept", content: { yes: true } } });
      check(earlyAgain.kind === "needs_input", "Repeated interaction fixture did not retain a handle");
      const later = await Promise.all(Array.from({ length: 30 }, () => retentionEcho.call({ message: "interactive" })));
      check(later.every(outcome => outcome.kind === "needs_input") && retention.continuationIds.size === 32, "Continuation budget was not filled");
      retention.clockOrigin -= 240_000; retention.tick();
      const fresh = await retentionEcho.call({ message: "interactive" });
      check(fresh.kind === "needs_input" && retention.continuationIds.size === 32, "Expired continuation bookkeeping did not release capacity");
      const liveResult = await retention.resume(live.value, { confirm: { action: "accept", content: { yes: true } } });
      check(message(liveResult) === "interactive", "Bookkeeping evicted a live handle instead of an expired later insertion");
    }

    const serial = await open({ maxInFlight: 1, queueCapacity: 3 });
    const serialEcho = serial.tool("echo");
    const blocked = serialEcho.call({ message: "slow-blocked" });
    const blockedResult = rejected(blocked, "Cancelled");
    const queued = serial.executeLine("echo --message never-dispatched", { timeout: 20 });
    const queuedResult = rejected(queued, "Timeout");
    const explicitlyCancelled = serial.execute("echo", ["--message", "cancelled-in-queue"]);
    const explicitResult = rejected(explicitlyCancelled, "Cancelled");
    await rejected(serialEcho.call({ message: "capacity-rejected" }), "QueueFull");
    explicitlyCancelled.cancel();
    for (const [promise, id] of [[explicitResult, explicitlyCancelled.operationId], [queuedResult, queued.operationId]]) {
      const error = await promise;
      check(error.execution_certainty === "not_sent" && error.queue_operation_id === id && error.operation_id === undefined, "Unstarted queue error lost evidence");
    }
    blocked.cancel();
    await blockedResult;
    check(message(await serialEcho.call({ message: "after-queue-cancel" })) === "after-queue-cancel", "Queue slots were not released");
    const before = serial.nextOperation;
    const aborted = new AbortController(); aborted.abort();
    await rejected(serialEcho.call({ message: "pre-aborted" }, { signal: aborted.signal }), "Cancelled");
    check(serial.nextOperation === before, "Pre-aborted operation was admitted");

    const small = await open({ maxInFlight: 1, maxInputBytes: 1024 });
    const smallEcho = small.tool("echo");
    await rejected(small.execute("x".repeat(2000), []), "QueueFull");
    const large = smallEcho.call({ message: "界".repeat(180) });
    const largeResult = large.then(result => check(message(result) === "界".repeat(180), "UTF-8 result changed"));
    await rejected(smallEcho.call({ message: "界".repeat(180) }), "QueueFull");
    await largeResult;
    await smallEcho.call({ message: "byte-budget-reused" });

    // Simulate a synchronous host/parser overrun after Rust has allocated the
    // operation. No fetch may start once that total deadline has elapsed.
    const late = createRuntime();
    let admitted = false, sent = 0;
    const delayed = new Proxy(late.runtime, { get(target, name) {
      const value = Reflect.get(target, name, target);
      if (name === "command_line_concurrent") return line => {
        const result = value.call(target, line); admitted = true;
        const until = performance.now() + 150;
        while (performance.now() < until) { /* Model synchronous parsing work. */ }
        return result;
      };
      return typeof value === "function" ? value.bind(target) : value;
    } });
    const expired = new Remote(delayed, { fetch: (url, init) => {
      if (JSON.parse(init.body).method === "tools/call") sent++;
      return fetchImpl(url, init);
    } });
    owned.push({ remote: expired, dispose: late.dispose });
    await expired.connect();
    await rejected(expired.executeLine("echo --message expired-parser", { timeout: 100 }), "Timeout");
    check(admitted && sent === 0 && !late.runtime.busy(), "Parser overrun sent I/O or left an orphaned operation");

    // A corrupt stream is local to its operation, including a core decoder error.
    const isolated = await open({ fetch: async (url, init) => {
      const rpc = JSON.parse(init.body);
      if (rpc.method === "tools/call" && rpc.params.arguments.message === "broken") return new Response("data: {broken}\n\n", { headers: { "content-type": "text/event-stream" } });
      return fetchImpl(url, init);
    } });
    const isolatedEcho = isolated.tool("echo");
    const bad = rejected(isolatedEcho.call({ message: "broken" }), "ProtocolError");
    check(message(await isolatedEcho.call({ message: "healthy-during-bad-stream" })) === "healthy-during-bad-stream", "Bad stream affected another operation");
    await bad;

    const floodEvents = [];
    const flood = await open({ coreEventCapacity: 4, coreEventBytes: 49152, maxInFlight: 2, onEvent: event => floodEvents.push(event) });
    const floodEcho = flood.tool("echo");
    await Promise.all([floodEcho.call({ message: "burst-worker" }), floodEcho.call({ message: "burst-worker" })]);
    const coreLost = floodEvents.filter(event => event.kind === "lagged" && event.payload.source === "core").reduce((sum, event) => sum + event.payload.dropped, 0);
    const retainedProgress = floodEvents.filter(event => event.kind === "progress").length;
    check(coreLost > 0 && coreLost + retainedProgress === 200, "Core loss was missing or double-counted");
    check(floodEvents.filter(event => event.kind === "completed").length === 2, "Core event pressure dropped a terminal");
    const unsafeEvents = [];
    flood.onEvent = event => unsafeEvents.push(event);
    check(message(await floodEcho.call({ message: "unsafe-event-number" })) === "unsafe-event-number", "Unsafe event payload changed a successful call");
    const unsafe = unsafeEvents.find(event => event.kind === "log");
    check(unsafe?.payload === undefined && unsafe?.payload_error === "NumberOutOfRange" && unsafe?.raw_payload?.includes("18446744073709551615"), "Event bridge rounded a uint64 or lost its raw payload");
    check(unsafeEvents.some(event => event.kind === "completed"), "Unsafe event payload swallowed a terminal");
    const floodStats = JSON.parse(flood.runtime.event_stats());
    check(floodStats.bytes === 0 && floodStats.reserved_terminals === 0, "Core event budget was retained after drain/completion");

    const closing = await open({ maxInFlight: 1 });
    const closingEcho = closing.tool("echo");
    let started;
    const start = new Promise(resolve => { started = resolve; });
    closing.onEvent = event => { if (event.kind === "progress") started(); };
    const activeClose = rejected(closingEcho.call({ message: "slow-close" }), "TransportClosed");
    const queuedClose = rejected(closingEcho.call({ message: "close-queued" }), "TransportClosed");
    await start;
    await closing.close();
    await Promise.all([activeClose, queuedClose]);
    check(closing.active.size === 0 && closing.inputBytes === 0, "Close returned before resource cleanup");
    await rejected(closingEcho.call({ message: "closed" }), "TransportClosed");
    await closing.close();

    // Discovery errors must reject connect, rather than produce a null success.
    const { runtime, dispose } = createRuntime();
    const invalid = new Remote(runtime, { fetch: async () => new Response("", { status: 401 }) });
    owned.push({ remote: invalid, dispose });
    await rejected(invalid.connect(), "AuthFailed");
    await runCatalogPolicy(Remote, createRuntime);
    await runCredentialFailures(Remote, createRuntime);
    await runCredentialIdentityBoundary(Remote, createRuntime);
    let registryIdentity = "registry-first";
    const registered = await open({ schemaRegistry:schemaResources(), credentialProvider:async () => ({authContextId:registryIdentity}) });
    await exerciseValidatorCache(registered);
    await exerciseSchemaRegistry(registered, { legacy, rotateIdentity:() => { registryIdentity = "registry-second"; } });
    let stopAtAdmission=false, stopQueue, businessStarts=0;
    const stopEvents=[], stopErrors=[];
    stopQueue=await open({fetch:(url,init)=>{if(JSON.parse(init.body).method==="tools/call")businessStarts++;return fetchImpl(url,init);},
      onOperationEvent:event=>{stopEvents.push(event);if(stopAtAdmission&&event.kind==="queued")stopQueue.cancelOperation(event.queue_operation_id);},onEventError:error=>stopErrors.push(error)});
    stopAtAdmission=true;
    const stopped=stopQueue.executeLine("echo --message operation-observer-never-sent");
    await rejected(stopped,"Cancelled");
    const stopTerminal=stopEvents.find(event=>event.queue_operation_id===stopped.operationId&&event.kind==="cancelled");
    check(stopTerminal?.payload.execution_certainty==="not_sent"&&businessStarts===0,"Queue callback cancellation dispatched a tool");
    stopAtAdmission=false;
    stopQueue.operationEvents.callback=()=>{throw new Error("Queue observer failed");};
    check(message(await stopQueue.tool("echo").call({message:"operation-observer-reused"}))==="operation-observer-reused","Observer exception cancelled execution");
    check(stopErrors.length===3,"Queue observer exceptions were not isolated");
    stopQueue.operationEvents.callback=event=>stopEvents.push(event);
    stopQueue.operationEvents.sequence=Number.MAX_SAFE_INTEGER-2;
    await rejected(stopQueue.executeLine("echo --message operation-sequence-never-sent"),"LimitExceeded");
    check(stopEvents.at(-1).sequence===Number.MAX_SAFE_INTEGER&&stopEvents.at(-1).kind==="failed","Sequence exhaustion lost the reserved terminal");
    const beforeSequence=stopQueue.nextOperation;
    await rejected(stopQueue.executeLine("echo --message operation-sequence-rejected"),"LimitExceeded");
    check(stopQueue.nextOperation===beforeSequence&&businessStarts===1,"Exhausted lifecycle sequence admitted another call");
    return { calls: 17, peak, cancelled: 1, timeout: 1, observerErrors: observerErrors.length };
  } finally {
    for (const { remote, dispose } of owned) {
      await remote.close(); dispose();
      const records=operationRecords.get(remote);if(!records)continue;
      check(records.every((event,index)=>index===0||records[index-1].sequence<event.sequence),"Queue lifecycle sequence was not monotonic");
      for(const queued of records.filter(event=>event.kind==="queued")) {
        const terminal=records.filter(event=>event.queue_operation_id===queued.queue_operation_id&&["completed","needs_input","failed","cancelled"].includes(event.kind));
        check(terminal.length===1,"Accepted queue operation did not have exactly one terminal");
        check(typeof terminal[0].payload.execution_certainty==="string","Queue terminal lost execution evidence");
      }
      check(records.every(event=>!("raw" in event.payload)),"Queue events leaked raw business output");
      check(remote.operationEvents.active.size===0,"Closed Remote retained lifecycle reservations");
    }
  }
}


// No generated tool SDK: schema changes and identity are learned at runtime.
export async function runCatalogPolicy(Remote, createRuntime) {
  const { runtime, dispose } = createRuntime();
  const requests = [], scopes = [], events = [];
  let scope = "alice", token = "first-token", schema = "string", resolutions = 0;
  let gate, enterProvider, finishCall, startedCall;
  let session = 0;
  const remote = new Remote(runtime, {
    catalogTtl: 5000, credentialReference: "account:test", onEvent: event => events.push(event),
    credentialProvider: async request => {
      check(request.endpoint === runtime.endpoint() && request.reference === "account:test", "Provider received a tool-controlled endpoint/reference");
      resolutions++;
      const snapshot = { authContextId: scope, token };
      enterProvider?.();
      if (gate) await gate;
      return snapshot;
    },
    fetch: async (_url, init) => {
      const auth = new Headers(init.headers).get("authorization");
      if (init.method === "DELETE") {
        scopes.push({ method: "DELETE", auth });
        return new Response(null, { status: 204 });
      }
      const rpc = JSON.parse(init.body); requests.push(rpc); scopes.push({ method: rpc.method, auth });
      let result;
      switch (rpc.method) {
        case "server/discover": result = { supportedVersions: ["2026-07-28"] }; break;
        case "initialize": result = { protocolVersion: "2025-11-25", capabilities: { tools: {} } }; break;
        case "notifications/initialized": case "notifications/cancelled": return new Response(null, { status: 202 });
        case "tools/list": result = { ttlMs: 1000, tools: [{ name: "echo", inputSchema: { type: "object", properties: { message: { type: schema } }, required: ["message"] } }] }; break;
        case "tools/call":
          if (rpc.params.arguments.message === "held") {
            startedCall?.();
            await new Promise(resolve => { finishCall = resolve; });
          }
          result = { content: [], structuredContent: rpc.params.arguments }; break;
        default: throw new Error("Unexpected policy request");
      }
      const headers = { "content-type": "application/json" };
      if (rpc.method === "initialize") headers["mcp-session-id"] = `session-${++session}`;
      return new Response(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result }), { headers });
    },
  });
  const lists = () => requests.filter(request => request.method === "tools/list").length;
  const handshakes = () => requests.filter(request => ["initialize", "server/discover"].includes(request.method)).length;
  let watcher;
  try {
    await remote.connect();
    watcher = remote.subscribeCatalog();
    check(watcher.tryNext() === undefined, "Catalog subscription emitted an initial event");
    check(resolutions === 1 && lists() === 1, "Connection resolved credentials more than once");
    await remote.completeLine("echo --m");
    check(resolutions === 2 && lists() === 1 && requests.every(r => r.method !== "tools/call"), "Completion repeated preflight or sent a business call");
    const echo = remote.tool("echo");
    await echo.call({ message: "before-expiry" });
    check(lists() === 1, "Fresh catalog was refreshed");
    remote.clockOrigin -= session ? 5100 : 1100;
    remote.tick();
    check(watcher.tryNext() === undefined, "TTL expiry emitted a catalog change");
    await echo.call({ message: "after-expiry" });
    check(lists() === 2 && handshakes() === 1, `TTL refresh counts: lists=${lists()}, handshakes=${handshakes()}`);
    check(watcher.tryNext()?.value.published === true, "TTL publication was not observable");
    const before = lists();
    const publication = watcher.next();
    await Promise.all([remote.refresh(), remote.refresh(), remote.refresh()]);
    check((await publication).value.published === true, "Waiting catalog consumer missed refresh");
    check(lists() === before + 1, "Queued explicit refresh was not coalesced");
    const beforeContext = lists(), beforeResolutions = resolutions;
    await Promise.all([remote.refresh(), remote.setSchemaRegistry([]), remote.refresh()]);
    check(lists() === beforeContext + 2, "Explicit refresh crossed a Schema registry context barrier");
    check(resolutions === beforeResolutions + 3, "Coalescing skipped a caller's credential preflight");
    const contextChange = watcher.tryNext();
    check(contextChange?.value.published && contextChange.value.invalidated, "Registry/catalog changes did not coalesce");
    await rejected(remote.setSchemaRegistry([{uri:"https://example.com/watch-invalid", schemaJson:'{"$vocabulary":{"https://example.com/required":true}}'}]), "SchemaUnsupported");
    check(watcher.tryNext() === undefined, "Rejected registry replacement emitted a catalog change");
    const originalEvents = runtime.events.bind(runtime);
    runtime.events = () => { const batch = JSON.parse(originalEvents()); batch.events = batch.events.filter(event=>!event.kind.startsWith("catalog_")); return JSON.stringify(batch); };
    schema = "integer"; remote.clockOrigin -= session ? 5100 : 1100;
    const changedSpec = await remote.commandSpec("echo");
    check(changedSpec.parameters.find(p => p.name === "message")?.value_kind === "integer", "CommandSpec ignored an expired changed schema");
    await rejected(echo.call({ message: "old-schema" }), "BindingStale");
    check(message(await remote.execute("echo", ["--message", "7"])) === 7, "Named command did not bind the new schema");
    schema = "string"; await remote.refresh();
    const current = await remote.freshTool("echo");
    const began = new Promise(resolve => { startedCall = resolve; });
    const running = current.call({ message: "held" });
    await began;
    let release;
    gate = new Promise(resolve => { release = resolve; });
    const providerEntered = new Promise(resolve => { enterProvider = resolve; });
    scope = "bob"; token = "second-token";
    const oldBinding = rejected(current.call({ message: "wrong-principal" }), "BindingStale");
    await providerEntered;
    finishCall();
    check(message(await running) === "held", "Awaiting credentials blocked existing I/O");
    release(); gate = undefined; enterProvider = undefined;
    await oldBinding;
    const fresh = await remote.freshTool("echo");
    check(message(await fresh.call({ message: "bob-call" })) === "bob-call", "Fresh binding could not use the new identity");
    const identityChange = watcher.tryNext();
    check(identityChange?.value.published && identityChange.value.invalidated, "Catalog observation relied on the filtered event journal");
    runtime.events = originalEvents;
    check(scopes.findLast(entry => entry.method === "tools/call").auth === "Bearer second-token", "New call reused old credentials");
    if (session) check(scopes.find(entry => entry.method === "DELETE")?.auth === "Bearer first-token", "Session cleanup used the new principal's token");
    const beforeCancel = requests.length;
    gate = new Promise(() => {});
    const interrupted = fresh.call({ message: "cancel-provider" });
    // Wait until the provider actually owns its bounded slot before cancelling.
    while (!remote.credentials.running) await new Promise(resolve => setTimeout(resolve, 0));
    interrupted.cancel();
    await rejected(interrupted, "Cancelled");
    await rejected(fresh.call({ message: "provider-still-running" }), "CredentialUnavailable");
    check(requests.length === beforeCancel, "Cancelled credential lookup dispatched MCP I/O");
    check(!JSON.stringify(events).includes("first-token") && !JSON.stringify(events).includes("second-token"), "Events exposed a credential");
    echo.dispose(); current.dispose(); fresh.dispose();
  } finally {
    finishCall?.(); await remote.close();
    if (watcher) { while (watcher.tryNext()?.done === false) {} check((await watcher.next()).done, "Closed catalog did not end"); }
    dispose();
  }
}


export async function runCredentialIdentityBoundary(Remote, createRuntime) {
  const { runtime, dispose } = createRuntime();
  const requests = [];
  const remote = new Remote(runtime, {
    token: "invalid\ntoken",
    fetch: async (_url, init) => {
      if (init.method === "DELETE") return new Response(null, { status: 204 });
      const rpc = JSON.parse(init.body);
      requests.push({ method: rpc.method, auth: new Headers(init.headers).get("authorization") });
      let result;
      switch (rpc.method) {
        case "server/discover": result = { supportedVersions: ["2026-07-28"] }; break;
        case "initialize": result = { protocolVersion: "2025-11-25", capabilities: { tools: {} } }; break;
        case "notifications/initialized": return new Response(null, { status: 202 });
        case "tools/list": result = { tools: [{ name: "echo", inputSchema: { type: "object" } }] }; break;
        case "tools/call": result = { content: [], structuredContent: rpc.params.arguments }; break;
        default: throw new Error("Unexpected identity boundary request");
      }
      const headers = { "content-type": "application/json" };
      if (rpc.method === "initialize") headers["mcp-session-id"] = "identity-session";
      return new Response(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result }), { headers });
    },
  });
  try {
    await rejected(remote.connect(), "CredentialUnavailable");
    check(requests.length === 0 && remote.credentials.epoch === 0, "Invalid static credential committed an identity or sent I/O");
    remote.token = "initial-static";
    await remote.connect();
    const old = remote.tool("echo");
    remote.credentials.epoch = Number.MAX_SAFE_INTEGER - 1;
    remote.token = "final-static";
    const final = await remote.freshTool("echo");
    check(remote.credentials.epoch === Number.MAX_SAFE_INTEGER, "Final credential identity was not exact");
    await rejected(old.call({ message: "old-principal" }), "BindingStale");
    check(message(await final.call({ message: "last-valid-principal" })) === "last-valid-principal", "Final credential identity was unusable");
    check(requests.at(-1).auth === "Bearer final-static", "Final credential used the wrong bearer");
    const before = requests.length;
    remote.token = "overflow-static";
    const error = await rejected(final.call({ message: "identity-never-dispatched" }), "LimitExceeded");
    check(error.execution_certainty === "not_sent" && requests.length === before, "Exhausted identity dispatched MCP I/O");
    check(remote.credentials.epoch === Number.MAX_SAFE_INTEGER && remote.credentials.lastToken === "final-static", "Failed rotation committed identity state");
    remote.token = "final-static";
    check(message(await final.call({ message: "retained-principal" })) === "retained-principal", "Unchanged final identity could not be reused");
    old.dispose(); final.dispose();
  } finally { await remote.close(); dispose(); }
}

export async function runCredentialFailures(Remote, createRuntime) {
  for (const provider of [
    () => { throw new Error("secret-provider-token"); },
    () => ({ token: "bad\ntoken", authContextId: "owner" }),
    () => ({ authContextId: "界".repeat(86) }),
    () => ({ token: "x".repeat(65536), authContextId: "owner" }),
  ]) {
    const { runtime, dispose } = createRuntime();
    let sent = 0;
    const remote = new Remote(runtime, { credentialProvider: provider, fetch: () => { sent++; throw new Error("Unexpected I/O"); } });
    try {
      const error = await rejected(remote.connect(), "CredentialUnavailable");
      check(sent === 0 && !error.message.includes("secret-provider-token"), "Invalid provider sent I/O or exposed its exception");
    } finally { await remote.close(); dispose(); }
  }
}
