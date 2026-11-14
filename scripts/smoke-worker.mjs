import assert from "node:assert/strict";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { createSecureServer } from "node:http2";
import { createServer } from "node:http";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { chromium } from "playwright";
import { createMockServer } from "./mock-server.mjs";

const files = new Map([
  ...["client.js", "worker.js", "worker-entry.js", "worker-events.js", "catalog.js", "notifications.js", "responses.js", "session-recovery.js", "worker-catalog.js", "worker-protocol.js", "worker-fetch.js", "worker-fetch-host.js", "credentials.js", "worker-credentials.js", "schemas.js", "operations.js", "errors.js", "operation-events.js"].map(name => [`/web/${name}`, [`web/${name}`, "text/javascript"]]),
  ["/schema-fixture.mjs", ["scripts/schema-fixture.mjs", "text/javascript"]],
  ["/dist/web/mcpx_web.js", ["dist/web/mcpx_web.js", "text/javascript"]],
  ["/dist/web/mcpx_web_bg.wasm", ["dist/web/mcpx_web_bg.wasm", "application/wasm"]],
  ["/examples/web-worker/", ["examples/web-worker/index.html", "text/html"]],
  ["/examples/web-worker/main.js", ["examples/web-worker/main.js", "text/javascript"]],
]);
const server = createServer(async (req, res) => {
  if (req.url === "/") return res.writeHead(200, { "content-type": "text/html" }).end("<!doctype html><title>mcpx worker</title>");
  if (req.url === "/slow.wasm") {
    const timer = setTimeout(() => res.writeHead(200).end(), 10_000);
    res.once("close", () => clearTimeout(timer)); return;
  }
  if (req.url === "/unresponsive-worker.js") return res.writeHead(200, { "content-type": "text/javascript" }).end(`
    import "/web/worker-entry.js";
    self.addEventListener("message", event => {
      if (event.data?.testFreeze) { for (;;) {} }
    });
  `);
  const file = files.get(req.url);
  if (!file) return res.writeHead(404).end();
  res.writeHead(200, { "content-type": file[1] }).end(await readFile(file[0]));
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const origin = `http://127.0.0.1:${server.address().port}`;
let browser, modern, legacy, directory;
const policyMocks = [];
try {
  directory = await mkdtemp(join(tmpdir(), "mcpx-worker-"));
  const key = join(directory, "key.pem"), cert = join(directory, "cert.pem");
  await promisify(execFile)("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=localhost"]);
  const tls = { key: await readFile(key), cert: await readFile(cert), allowHTTP1: true };
  const modernMock = createMockServer(undefined, { concurrencyBarrier: 16, serverFactory: handler => createSecureServer(tls, handler) });
  const legacyMock = createMockServer(undefined, { legacy: true, concurrencyBarrier: 16, serverFactory: handler => createSecureServer(tls, handler) });
  modern = modernMock.server; legacy = legacyMock.server;
  await new Promise(resolve => modern.listen(0, "127.0.0.1", resolve));
  await new Promise(resolve => legacy.listen(0, "127.0.0.1", resolve));
  for (const legacy of [false, true]) {
    const fixture = createMockServer(undefined, { legacy, catalogTtl: 100, sessions: legacy, serverFactory: handler => createSecureServer(tls, handler) });
    await new Promise(resolve => fixture.server.listen(0, "127.0.0.1", resolve));
    policyMocks.push(fixture);
  }
  browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage({ ignoreHTTPSErrors: true });
  page.on("console", message => { if (message.text().startsWith("worker-test:")) console.log(message.text()); });
  const workers = new Set(), workerClosures = []; let created = 0, closed = 0;
  page.on("worker", worker => {
    created++; workers.add(worker);
    workerClosures.push(new Promise(resolve => worker.on("close", () => { closed++; workers.delete(worker); resolve(); })));
  });
  await page.goto(origin);
  const endpoints = { policy: policyMocks.map(fixture => `https://localhost:${fixture.server.address().port}/mcp`), modern: `https://localhost:${modern.address().port}/mcp`, legacy: `https://localhost:${legacy.address().port}/mcp` };
  const result = await page.evaluate(async endpoints => {
    const { WorkerRemote } = await import("/web/worker.js");
    const { schemaResources, exerciseSchemaRegistry, exerciseValidatorCache } = await import("/schema-fixture.mjs");
    const check = (condition, message) => { if (!condition) throw new Error(message); };
    const rejected = async (promise, code) => {
      try { await promise; } catch (error) { check(error.code === code, `Expected ${code}, got ${error.code}: ${error.message}`); return error; }
      throw new Error(`Expected ${code} rejection`);
    };
    // Main-thread Wasm compilation/instantiation is forbidden in this test.
    for (const name of ["compile", "compileStreaming", "instantiate", "instantiateStreaming"]) WebAssembly[name] = () => { throw new Error("Wasm ran on the main thread"); };
    const remotes = [];
    let phase = "startup";
    const step = value => { phase = value; console.log(`worker-test: ${value}`); };
    const open = async (endpoint, options) => { const remote = await WorkerRemote.connect(endpoint, { timeout: 10_000, ...options }); remotes.push(remote); return remote; };
    try {
      const modes = ["modern", "legacy", "auto-modern", "auto-legacy"];
      for (const mode of modes) {
        let registryIdentity = "registry-first";
        step(`mode ${mode}`);
        const events = [], observerErrors = [], operations = [];
        const activeSignal = new AbortController();
        let cancel, localChecks;
        const remote = await open(endpoints[mode.endsWith("legacy") ? "legacy" : "modern"], {
          protocol: mode.startsWith("auto") ? "auto" : mode,
          elicitation: { form: true },
          schemaRegistry: schemaResources(), credentialProvider: async () => ({authContextId:registryIdentity}),
          onOperationEvent:event=>operations.push(event),
          onEvent: event => {
            events.push(event);
            if (!localChecks && cancel && event.kind === "progress" && event.queue_operation_id === cancel.operationId) {
              localChecks = Promise.all([
                remote.executeLine("echo --help", { timeout: 300 }).then(result => check(result.kind === "help", "Worker help blocked behind an active command")),
                remote.completeLine("echo --en", undefined, { timeout: 300 }).then(result => check(result.candidates[0]?.value === "--enabled", "Worker completion blocked behind an active call")),
                rejected(remote.execute("echo", ["--unknown", "x"]), "CommandParseError"),
                rejected(remote.executeLine("echo --message '"), "TokenizeError"),
              ]).finally(() => { if (mode.endsWith("legacy")) cancel.cancel(); else activeSignal.abort(); });
              localChecks.catch(() => {});
            }
            if (event.kind === "progress") throw new Error("Observer error");
          }, onEventError: error => observerErrors.push(error),
        });
        check((await remote.tools()).some(tool => tool.name === "echo"), "Worker discovery failed");
        const catalogWatcher = remote.subscribeCatalog();
        check(catalogWatcher.tryNext() === undefined, "Worker catalog emitted an initial event");
        const originalPost = remote.post.bind(remote);
        remote.post = (message, transfer) => { if (message.kind !== "catalog_ack") originalPost(message, transfer); };
        const firstPublication = catalogWatcher.next();
        await remote.refresh();
        check((await firstPublication).value.published, "Worker catalog publication was lost");
        const unacknowledged = remote.lastCatalogId;
        await remote.setSchemaRegistry([]); await remote.refresh();
        check(remote.lastCatalogId === unacknowledged, "Unacknowledged catalog mailbox grew");
        const pendingCatalog = catalogWatcher.next();
        remote.post = originalPost;
        originalPost({v:1, kind:"catalog_ack", id:unacknowledged});
        const coalescedCatalog = (await pendingCatalog).value;
        check(coalescedCatalog.published && coalescedCatalog.invalidated, "Worker catalog pending state did not coalesce");
        check((await remote.completeLine("ec")).candidates[0]?.value === "echo", "Worker tool completion failed");
        check((await remote.completeLine("echo --enabled t")).candidates[0]?.value === "true", "Worker boolean completion failed");
        check((await remote.completeLine("echo --mode 雪")).candidates[0]?.value === "雪 花", "Worker enum completion failed");
        check((await remote.commandSpec("echo")).parameters.some(p => p.name === "mode" && p.values.includes("two words")), "Worker CommandSpec failed");
        await rejected(remote.completeLine("雪", 1), "InvalidCursor");
        const echo = await remote.tool("echo");
        const validation = await rejected(echo.validate({ message: 1 }), "ValidationFailed");
        check(validation.issues?.some(issue => issue.instance_path === "/message"), "Worker validation lost its JSON pointer");
        const invalidCall = await rejected(echo.call({ message: 1 }), "ValidationFailed");
        check(invalidCall.issues?.some(issue => issue.instance_path === "/message"), "Worker call lost validation issues");
        const args = await echo.validate({ message: "checked", absent: undefined });
        check(args.message === "checked" && !Object.hasOwn(args, "absent"), "Typed validation changed absence");
        const batch = Array.from({ length: 16 }, (_, i) => i % 3 === 0
          ? echo.call({ message: `parallel-${i}` })
          : i % 3 === 1 ? remote.execute("echo", ["--message", `parallel-${i}`])
            : remote.executeLine(`echo --message 'parallel-${i}'`));
        batch.push(remote.executeLine("echo --message after-worker-barrier"));
        const disposed = echo.dispose();
        await rejected(echo.call({ message: "disposed-worker" }), "InvalidHandle");
        const results = await Promise.all(batch);
        await disposed;
        check(results.every((result, i) => result.value.raw.structuredContent.message === (i === 16 ? "after-worker-barrier" : `parallel-${i}`)), "Worker results crossed");
        const ids = new Set(batch.map(operation => operation.operationId));
        check(ids.size === 17 && events.filter(event => event.kind === "completed" && ids.has(event.queue_operation_id)).length === 17, "Worker event correlation failed");
        check(observerErrors.length >= 17, "Observer errors stopped worker transport");
        const next = await remote.tool("echo");
        step(`raw results ${mode}`);
        const raw = await next.callRaw('{"message":"raw-number"}');
        check(raw.includes('"n":18446744073709551615'), "Worker raw result rounded a large integer");
        try { await next.call({ message: "raw-number" }); throw new Error("Unsafe output was accepted"); }
        catch (error) { check(error instanceof RangeError && error.raw?.includes('"n":18446744073709551615'), "Unsafe output did not retain original JSON"); check(error.execution_certainty === "result_received" && error.retry_advice === "inspect_result" && Number.isSafeInteger(error.queue_operation_id), "Worker result conversion lost evidence"); }
        const argv = await remote.execute("echo", ["--message", "worker-argv"]);
        const line = await remote.executeLine('echo --message "worker 雪 $HOME $(literal)"');
        check(line.value.raw.structuredContent.message === "worker 雪 $HOME $(literal)", "Worker command string did not use literal Rust tokenization");
        await rejected(remote.executeLine("echo --message '"), "TokenizeError");
        await rejected(remote.executeLine("   "), "CommandError");
        check((await remote.executeLine("echo --help")).kind === "help", "Worker command help failed");
        step(`interaction ${mode}`);
        check(argv.value.raw.structuredContent.message === "worker-argv", "Worker argv failed");
        const interaction = await next.call({ message: "interactive" });
        if (interaction.kind === "needs_input") {
          await rejected(remote.resume({ ...interaction.value }, {}), "InvalidContinuation");
          const beforeInvalidId = remote.nextId;
          for (const id of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, "1", true]) await rejected(remote.resumeRaw(id, "{}"), "InvalidHandle");
          check(remote.nextId === beforeInvalidId, "Invalid continuation ID crossed the worker port");
          await rejected(remote.resumeRaw(interaction.value.id + 2 ** 32, "{}"), "InvalidContinuation");
          await rejected(remote.resume(interaction.value, { confirm: { action: "accept", content: { yes: "invalid" } } }), "ValidationFailed");
          const resumed = await remote.resume(interaction.value, { confirm: { action: "accept", content: { yes: true } } });
          check(resumed.value.raw.structuredContent.message === "interactive", "Worker resume changed state");
          await rejected(remote.resume(interaction.value, { confirm: { action: "cancel" } }), "InvalidContinuation");
        }
        const beforeAborted = remote.nextId;
        const preAborted = new AbortController(); preAborted.abort();
        await rejected(next.call({ message: "worker-pre-aborted" }, { signal: preAborted.signal }), "Cancelled");
        check(remote.nextId === beforeAborted, "Pre-aborted operation crossed the worker port");
        cancel = remote.executeLine("echo --message slow-worker-cancel", { signal: activeSignal.signal });
        step(`cancel ${mode}`);
        const cancelled = rejected(cancel, "Cancelled");
        const healthy = next.call({ message: "worker-healthy" });
        const cancelledError = await cancelled;
        check(cancelledError.execution_certainty === "unknown" && cancelledError.retry_advice === "inspect_execution", "Worker cancellation lost execution uncertainty");
        check(cancelledError.queue_operation_id === cancel.operationId && Number.isSafeInteger(cancelledError.operation_id), "Worker error lost outer queue/core correlation");
        await localChecks;
        check((await healthy).value.raw.structuredContent.message === "worker-healthy", "Worker scoped cancellation affected another call");
        await rejected(remote.execute("echo", ["--message", "slow-worker-timeout"], { timeout: 120 }), "Timeout");
        check(events.some(event => event.kind === "failed" && event.payload.code === "Timeout"), "Worker timeout code was lost");
        const diagnostic = await remote.diagnostics();
        check(diagnostic.workerContext && diagnostic.bindings === 1, "Worker did not own its runtime or release binding");
        check(diagnostic.ioRequests === 0 && diagnostic.ioBufferedBytes === 0, "Completed calls retained host I/O");
        step(`schema registry ${mode}`);
        await next.dispose();
        await exerciseValidatorCache(remote);
        await exerciseSchemaRegistry(remote, { legacy: mode.endsWith("legacy"), rotateIdentity:() => { registryIdentity = "registry-second"; } });
        await remote.close();
        while (catalogWatcher.tryNext()?.done === false) {}
        check((await catalogWatcher.next()).done, "Worker close did not end catalog subscription");
        await rejected(next.call({ message: "closed-worker" }), "TransportClosed");
        check(remote.pending.size === 0 && remote.inputBytes === 0 && remote.cleanupBytes === 0 && remote.io.records.size === 0 && remote.io.buffered === 0, "Worker close leaked pending messages or I/O");
        check(operations.every((event,index)=>index===0||operations[index-1].sequence<event.sequence),"Worker lifecycle sequence was not monotonic");
        for(const queued of operations.filter(event=>event.kind==="queued")) {
          const terminal=operations.filter(event=>event.queue_operation_id===queued.queue_operation_id&&["completed","needs_input","failed","cancelled"].includes(event.kind));
          check(terminal.length===1,"Worker queue task lost or duplicated its terminal");
          check(typeof terminal[0].payload.execution_certainty==="string","Worker lifecycle lost execution evidence");
        }
        const interrupted=operations.find(event=>event.queue_operation_id===cancel.operationId&&event.kind==="cancelled");
        check(interrupted?.payload.execution_certainty==="unknown"&&Number.isSafeInteger(interrupted.core_operation_id),"Worker lifecycle lost core correlation");
        check(remote.operationEvents.active.size===0,"Worker proxy retained lifecycle reservations");
      }
      const remote = await open(endpoints.modern, { maxInFlight: 1, queueCapacity: 2 });
      step("credential and catalog policies");
      for (const [index, endpoint] of endpoints.policy.entries()) {
        step(`credential policy ${index}`);
        let scope = "alice", token = "worker-first-token", resolutions = 0, gate, entered;
        let started;
        const remote = await open(endpoint, {
          protocol: index ? "legacy" : "modern", catalogTtl: 5000,
          credentialReference: "account:worker",
          credentialProvider: async request => {
            check(typeof document !== "undefined", "Provider ran outside its embedding realm");
            check(request.endpoint === endpoint && request.reference === "account:worker", "Worker changed provider endpoint/reference");
            resolutions++;
            const snapshot = { authContextId: scope, token };
            entered?.();
            if (gate) await gate;
            return snapshot;
          },
          onEvent: event => { if (event.kind === "progress") started?.(); },
        });
        check(resolutions === 1, "Worker discovery did not freeze credentials");
        const policyWatcher = remote.subscribeCatalog();
        const binding = await remote.tool("echo");
        await binding.call({ message: "worker-policy-before-ttl" });
        if (!index) await new Promise(resolve => setTimeout(resolve, 150));
        await binding.call({ message: "worker-policy-after-ttl" });
        await remote.refresh();
        const began = new Promise(resolve => { started = resolve; });
        const held = binding.call({ message: "slow-worker-policy-held" });
        await began;
        let release;
        gate = new Promise(resolve => { release = resolve; });
        const providerEntered = new Promise(resolve => { entered = resolve; });
        while (policyWatcher.tryNext()?.done === false) {}
        scope = "bob"; token = "worker-second-token";
        const stale = rejected(binding.call({ message: "worker-policy-wrong-principal" }), "BindingStale");
        await providerEntered;
        check((await held).value.raw.structuredContent.message === "slow-worker-policy-held", "Worker provider wait blocked active I/O");
        release(); gate = undefined; entered = undefined;
        await stale;
        const fresh = await remote.freshTool("echo");
        await fresh.call({ message: "worker-policy-bob" });
        const identityChange = (await policyWatcher.next()).value;
        check(identityChange.invalidated, "Worker catalog missed authorization invalidation");
        let finalCatalog;
        if (index) {
          while (policyWatcher.tryNext()?.done === false) {}
          const post = remote.post.bind(remote);
          remote.post = (message, transfer) => { if (message.kind !== "catalog_ack") post(message, transfer); };
          const first = policyWatcher.next();
          await remote.refresh(); await first;
          await remote.refresh();
          finalCatalog = policyWatcher.next();
          const fetch = remote.io.fetch;
          remote.io.fetch = async (url, init) => {
            const response = await fetch(url,init);
            if (init.method !== "DELETE") return response;
            await response.body?.cancel();
            return new Response(null,{status:503});
          };
        }
        const beforeCancel = resolutions;
        gate = new Promise(() => {});
        const pending = fresh.call({ message: "worker-policy-cancel-provider" });
        while (resolutions === beforeCancel) await new Promise(resolve => setTimeout(resolve, 0));
        pending.cancel(); await rejected(pending, "Cancelled");
        await rejected(fresh.call({ message: "worker-policy-unsettled-provider" }), "CredentialUnavailable");
        check(resolutions === beforeCancel + 1, "Worker grew uncooperative provider callbacks");
        if (index) {
          await rejected(remote.close(), "SessionCleanupFailed");
          check((await finalCatalog).value.published, "Cleanup failure lost the pending catalog cell");
        } else await remote.close();
        while (policyWatcher.tryNext()?.done === false) {}
        check((await policyWatcher.next()).done, "Worker policy watcher did not close");
        check(remote.credentials.closed && remote.io.records.size === 0, "Worker close retained credential/I/O channels");
      }
      const rotating = await open(endpoints.policy[0], { token: "static-first" });
      const oldStatic = await rotating.tool("echo");
      rotating.credentials.resolver.epoch = Number.MAX_SAFE_INTEGER - 1;
      rotating.token = "static-second";
      await rejected(oldStatic.call({ message: "worker-policy-stale-static" }), "BindingStale");
      const finalStatic = await rotating.freshTool("echo");
      await finalStatic.call({ message: "worker-policy-rotated" });
      check(rotating.credentials.resolver.epoch === Number.MAX_SAFE_INTEGER, "Worker final static identity was not exact");
      rotating.token = "static-overflow";
      const exhaustedIdentity = await rejected(finalStatic.call({ message: "worker-identity-never-dispatched" }), "CredentialUnavailable");
      check(exhaustedIdentity.execution_certainty === "not_sent", "Worker static identity exhaustion lost preflight evidence");
      rotating.token = "static-second";
      await finalStatic.call({ message: "worker-policy-retained" });
      await rotating.close();

      step("lifecycle callback cancellation");
      let cancelBeforePost=false, stopProxy;
      const stopRecords=[];
      stopProxy=await open(endpoints.modern,{onOperationEvent:event=>{stopRecords.push(event);if(cancelBeforePost&&event.kind==="queued"){const job=stopProxy.pending.get(event.queue_operation_id);stopProxy.interrupt(job,"Cancelled");}}});
      const stopBinding=await stopProxy.tool("echo");cancelBeforePost=true;
      const stopPromise=stopBinding.call({message:"worker-operation-observer-never-sent"});
      await rejected(stopPromise,"Cancelled");
      const stopRecord=stopRecords.find(event=>event.queue_operation_id===stopPromise.operationId&&event.kind==="cancelled");
      check(stopRecord?.payload.execution_certainty==="not_sent"&&!stopRecords.some(event=>event.queue_operation_id===stopPromise.operationId&&event.kind==="started"),"Worker queued observer cancellation crossed dispatch");
      cancelBeforePost=false;await stopProxy.close();

      step("reserved cleanup");
      const echo = await remote.tool("echo");
      let progress;
      const started = new Promise(resolve => { progress = resolve; });
      remote.onEvent = event => { if (event.kind === "progress") progress(); };
      const active = echo.call({ message: "slow-worker-close" });
      const activeResult = rejected(active, "TransportClosed");
      await started;
      const queued = echo.call({ message: "worker-never-dispatched" });
      const queuedResult = rejected(queued, "TransportClosed");
      await rejected(echo.call({ message: "worker-capacity-rejected" }), "QueueFull");
      // Cleanup has reserved capacity even with the ordinary queue completely full.
      await echo.dispose();
      await remote.close();
      const [activeError, queuedError] = await Promise.all([activeResult, queuedResult]);
      check(activeError.execution_certainty === "unknown" && activeError.queue_operation_id === active.operationId, "Worker close lost active evidence");
      check(queuedError.execution_certainty === "not_sent" && queuedError.queue_operation_id === queued.operationId, "Worker close lost unsent evidence");

      step("worker UTF-8 input budget");
      const small = await open(endpoints.modern, { maxInputBytes: 1024 });
      const smallEcho = await small.tool("echo");
      const unicode = smallEcho.call({ message: "界".repeat(150) });
      await rejected(smallEcho.call({ message: "界".repeat(150) }), "QueueFull");
      check((await unicode).value.raw.structuredContent.message === "界".repeat(150), "Worker changed Unicode input");
      await smallEcho.call({ message: "worker-byte-budget-reused" });
      await small.close();

      const buffered = await open(endpoints.modern, { maxInFlight: 2, eventCapacity: 8, eventBytes: 128 * 1024 });
      step("event backpressure");
      const bufferedEcho = await buffered.tool("echo");
      const observed = [];
      buffered.onEvent = event => observed.push(event);
      const send = buffered.post.bind(buffered), heldAcks = [];
      let holdAcks = true;
      buffered.post = (message, transfer) => {
        if (message.kind === "ack" && holdAcks) heldAcks.push(message);
        else send(message, transfer);
      };
      buffered.worker.addEventListener("message", event => {
        if (event.data.kind === "events") {
          buffered.post({ v: 1, kind: "ack", batch: event.data.batch });
          buffered.post({ v: 1, kind: "ack", batch: 9_007_199_254_740_991 });
        }
      });
      const bursts = [bufferedEcho.call({ message: "burst-worker" }), bufferedEcho.call({ message: "burst-worker" })];
      const creditExpiry = rejected(bufferedEcho.call({ message: "worker-credit-expired" }, { timeout: 100 }), "Timeout");
      // Stop event acknowledgements while the independent I/O driver keeps running.
      await new Promise(resolve => setTimeout(resolve, 500));
      holdAcks = false;
      for (const ack of heldAcks) send(ack);
      await Promise.all([...bursts, creditExpiry]);
      const bufferedStats = await buffered.diagnostics();
      check(bufferedStats.peakEventRecords <= 8 && bufferedStats.peakEventBytes <= 128 * 1024, "Unacknowledged worker events exceeded their budgets");
      check(bufferedStats.droppedEvents > 0 && bufferedStats.peakEventWaiters > 0, "Worker did not apply bounded event backpressure");
      check(bufferedStats.eventRecords >= 0 && bufferedStats.eventBytes >= 0, "Duplicate ACK freed event credit twice");
      const lagged = observed.filter(event => event.kind === "lagged").reduce((sum, event) => sum + event.payload.dropped, 0);
      check(lagged === bufferedStats.droppedEvents, "Worker did not report dropped notifications");
      check(observed.filter(event => event.kind === "completed").length === 2, "Worker dropped or duplicated a terminal event");
      await buffered.close();

      step("core and mailbox event loss");
      const coreBuffered = await open(endpoints.modern, { maxInFlight: 2, eventCapacity: 8, eventBytes: 128 * 1024, coreEventCapacity: 4, coreEventBytes: 49152 });
      const coreEvents = [];
      coreBuffered.onEvent = event => coreEvents.push(event);
      const coreEcho = await coreBuffered.tool("echo");
      await Promise.all([coreEcho.call({ message: "burst-worker" }), coreEcho.call({ message: "burst-worker" })]);
      const coreStats = await coreBuffered.diagnostics();
      const coreLoss = coreEvents.filter(event => event.kind === "lagged" && event.payload.source === "core").reduce((sum, event) => sum + event.payload.dropped, 0);
      const mailboxLoss = coreEvents.filter(event => event.kind === "lagged" && event.payload.source === "worker-mailbox").reduce((sum, event) => sum + event.payload.dropped, 0);
      check(coreLoss > 0 && coreLoss === coreStats.coreDroppedEvents, "Worker lost or double-counted core losses");
      check(mailboxLoss === coreStats.droppedEvents, "Worker mailbox loss was conflated with core loss");
      check(coreLoss + mailboxLoss + coreEvents.filter(event => event.kind === "progress").length === 200, "Worker event conservation failed");
      check(coreEvents.filter(event => event.kind === "completed").length === 2, "Worker core pressure lost a terminal");
      const beforeUnsafe = coreEvents.length;
      await coreEcho.call({ message: "unsafe-event-number" });
      const unsafeEvent = coreEvents.slice(beforeUnsafe).find(event => event.kind === "log");
      check(unsafeEvent?.payload_error === "NumberOutOfRange" && unsafeEvent.raw_payload?.includes("18446744073709551615"), "Worker lost the raw uint64 event payload");
      check(coreEvents.slice(beforeUnsafe).some(event => event.kind === "completed"), "Worker unsafe payload lost its terminal");
      await coreBuffered.close();

      step("non-yielding worker");
      const frozen = await open(endpoints.modern, { workerURL: "/unresponsive-worker.js", shutdownTimeout: 100 });
      const frozenWatcher = frozen.subscribeCatalog();
      const frozenRead = frozenWatcher.next();
      const frozenEcho = await frozen.tool("echo");
      let freeze;
      const freezeStarted = new Promise(resolve => { freeze = resolve; });
      frozen.onEvent = event => {
        if (event.kind === "progress") { frozen.post({ v: 1, kind: "ack", batch: 9_007_199_254_740_991, testFreeze: true }); freeze(); }
      };
      const expires = rejected(frozenEcho.call({ message: "slow-worker-frozen" }, { timeout: 1000 }), "Timeout");
      const collateral = rejected(frozenEcho.call({ message: "slow-worker-collateral" }), "WorkerUnresponsive");
      let freezeTimer;
      try {
        await Promise.race([freezeStarted, new Promise((_, reject) => { freezeTimer = setTimeout(() => reject(new Error("No progress arrived before freezing the worker")), 2000); })]);
      } finally { clearTimeout(freezeTimer); }
      const errors = await Promise.all([expires, collateral]);
      check(errors.every(error => error.message.includes("execution may be unknown")), "Forced termination implied known execution");
      check(errors.every(error => error.execution_certainty === "unknown" && error.retry_advice === "inspect_execution" && Number.isSafeInteger(error.queue_operation_id)), "Forced termination lost conservative metadata");
      check(frozen.terminated && frozen.pending.size === 0 && frozen.inputBytes === 0 && frozen.io.records.size === 0 && frozen.io.buffered === 0, "Frozen worker was not terminated and drained");
      check((await frozenRead).done, "Forced worker termination retained a catalog waiter");
      await frozen.close();

      const startup = new WorkerRemote({ wasmURL: "/slow.wasm" }); remotes.push(startup);
      step("startup cancellation");
      const connecting = rejected(startup.connect(endpoints.modern), "TransportClosed");
      await new Promise(resolve => setTimeout(resolve, 50));
      await startup.close(); await connecting;
      await rejected(WorkerRemote.connect(endpoints.modern, { wasmURL: "/missing.wasm" }), "WasmLoadError");
      const invalidRegistry = new WorkerRemote({ schemaRegistry:[{uri:"https://schemas.mcpx.test/invalid",schemaJson:'{"$vocabulary":{"https://example.com/required":true}}'}] });
      remotes.push(invalidRegistry);
      await rejected(invalidRegistry.connect(endpoints.modern), "SchemaUnsupported");
      await invalidRegistry.close();
      check(invalidRegistry.terminated && invalidRegistry.pending.size === 0 && invalidRegistry.inputBytes === 0 && invalidRegistry.io.buffered === 0 && invalidRegistry.config.options.schemaRegistry === undefined, "Invalid startup registry retained runtime, I/O or source configuration");
      return { modes: modes.length };
    } catch (error) { throw new Error(`${phase}: ${error.code}: ${error.message}`); }
    finally { await Promise.allSettled(remotes.map(remote => remote.close())); }
  }, endpoints);
  assert.equal(result.modes, 4);
  for (const [index, fixture] of policyMocks.entries()) {
    const calls = fixture.requests.filter(request => request.method === "tools/call");
    const names = calls.map(request => request.params.arguments.message);
    for (const forbidden of ["worker-policy-wrong-principal", "worker-policy-cancel-provider", "worker-policy-unsettled-provider", "worker-policy-stale-static", "worker-identity-never-dispatched"]) assert.ok(!names.includes(forbidden), `${forbidden} was dispatched`);
    assert.equal(fixture.requests.filter(request => ["server/discover", "initialize"].includes(request.method)).length, index ? 2 : 4);
    assert.ok(fixture.requests.filter(request => request.method === "tools/list").length >= (index ? 3 : 6));
    if (index) assert.deepEqual(fixture.transportRequests.filter(request => request.method === "DELETE").map(request => request.auth), ["Bearer worker-first-token", "Bearer worker-second-token"]);
    assert.ok(fixture.transportRequests.some(request => request.auth === "Bearer worker-second-token"));
  }
  console.log("PASS worker credentials: provider realm/reference, TTL, identity drain, frozen credentials, session DELETE, cancellation and bounded unresolved provider");
  await page.goto(`${origin}/examples/web-worker/`);
  await page.getByLabel("MCP endpoint", { exact: true }).fill(endpoints.modern);
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await page.getByText("Connected", { exact: true }).waitFor();
  await page.getByLabel("Command", { exact: true }).fill("ec");
  await page.getByRole("button", { name: "Complete", exact: true }).click();
  await page.waitForFunction(() => document.getElementById("command").value === "echo");
  await page.getByLabel("Command", { exact: true }).fill("echo --en");
  await page.getByLabel("Command", { exact: true }).press("Tab");
  await page.waitForFunction(() => document.getElementById("command").value === "echo --enabled");
  await page.getByLabel("Command", { exact: true }).fill("echo --mode ");
  await page.getByRole("button", { name: "Complete", exact: true }).click();
  await page.getByRole("button", { name: "two words", exact: true }).click();
  assert.equal(await page.getByLabel("Command", { exact: true }).inputValue(), "echo --mode 'two words'");
  await page.getByLabel("Command", { exact: true }).fill('echo --message "<b>browser-cli</b>"');
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await page.waitForFunction(() => document.getElementById("output").textContent.includes('"message": "<b>browser-cli</b>"'));
  assert.doesNotMatch(await page.locator("#output").innerHTML(), /<b>/);
  await page.getByLabel("Command", { exact: true }).fill("echo --help");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await page.waitForFunction(() => document.getElementById("output").textContent.includes("Usage:"));
  await page.getByLabel("Command", { exact: true }).fill("echo --message slow-example");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await page.waitForFunction(() => document.getElementById("output").textContent.includes("> echo --message slow-example"));
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.waitForFunction(() => document.getElementById("output").textContent.includes('"code": "Cancelled"'));
  await page.getByRole("button", { name: "Close", exact: true }).click();
  await page.getByText("Disconnected", { exact: true }).waitFor();
  let closureTimer;
  try {
    await Promise.race([Promise.all(workerClosures), new Promise((_, reject) => { closureTimer = setTimeout(() => reject(new Error("Worker closure was not observed")), 2000); })]);
  } finally { clearTimeout(closureTimer); }
  assert.equal(workers.size, 0, `Workers still alive: ${workers.size}`);
  assert.equal(created, closed);
  for (const requests of [modernMock.requests, legacyMock.requests]) {
    const messages = requests.filter(request => request.method === "tools/call").map(request => request.params.arguments.message);
    for (const forbidden of ["disposed-worker", "closed-worker", "worker-never-dispatched", "worker-capacity-rejected", "worker-credit-expired", "worker-pre-aborted", "worker-operation-observer-never-sent"]) assert.ok(!messages.includes(forbidden), `${forbidden} was dispatched`);
  }
  assert.equal(modernMock.requests.filter(request => request.method === "notifications/cancelled").length, 0);
  assert.equal(legacyMock.requests.filter(request => request.method === "notifications/cancelled").length, 4);
  const forced = modernMock.requests.filter(request => request.params?.arguments?.message?.startsWith("slow-worker-frozen") || request.params?.arguments?.message === "slow-worker-collateral");
  assert.equal(forced.length, 2);
  const cleanupDeadline = performance.now() + 1500;
  while (performance.now() < cleanupDeadline && forced.some(request => !modernMock.closedRequests.includes(request))) await new Promise(resolve => setTimeout(resolve, 10));
  assert.ok(forced.every(request => modernMock.closedRequests.includes(request)), `Terminating a worker left its HTTP stream open: ${JSON.stringify(forced.map(request => ({message:request.params.arguments.message,closed:modernMock.closedRequests.includes(request)})))}`);
  console.log(`PASS dedicated module workers: ${result.modes} protocol modes, 16-way barrier, validation/raw JSON/continuation, cancel/deadline, event ACK budgets, reserved cleanup, frozen-worker termination, ${closed} workers released`);
  console.log("PASS browser CLI example: connect, quoted string command, local help, cancellation, escaped text output and close");
} finally {
  await browser?.close();
  await new Promise(resolve => server.close(resolve));
  if (modern) await new Promise(resolve => modern.close(resolve));
  if (legacy) await new Promise(resolve => legacy.close(resolve));
  for (const fixture of policyMocks) await new Promise(resolve => fixture.server.close(resolve));
  if (directory) await rm(directory, { recursive: true, force: true });
}
