import assert from "node:assert/strict";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { createSecureServer } from "node:http2";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { chromium } from "playwright";
import { createMockServer } from "./mock-server.mjs";
import { Remote, parseSafe } from "../web/client.js";
import { runConcurrent } from "./concurrent-host.mjs";
import { runHostIds } from "./host-id-tests.mjs";
import { componentRuntime } from "../web/component.js";
import init, { Runtime } from "../dist/web/mcpx_web.js";

const { server, requests } = createMockServer(async (req, res) => {
  if (req.url === "/") return res.writeHead(200, { "content-type": "text/html" }).end("<!doctype html><title>mcpx integration test</title>");
  const allowed = new Map([
    ["/client.js", ["web/client.js", "text/javascript"]],
    ["/credentials.js", ["web/credentials.js", "text/javascript"]],
    ["/schemas.js", ["web/schemas.js", "text/javascript"]],
    ["/operations.js", ["web/operations.js", "text/javascript"]],
    ["/errors.js", ["web/errors.js", "text/javascript"]],
  ["/operation-events.js", ["web/operation-events.js", "text/javascript"]],
  ["/catalog.js", ["web/catalog.js", "text/javascript"]],
  ["/notifications.js", ["web/notifications.js", "text/javascript"]],
  ["/responses.js", ["web/responses.js", "text/javascript"]],
  ["/session-recovery.js", ["web/session-recovery.js", "text/javascript"]],
    ["/worker-protocol.js", ["web/worker-protocol.js", "text/javascript"]],
    ["/mcpx_web.js", ["dist/web/mcpx_web.js", "text/javascript"]],
    ["/mcpx_web_bg.wasm", ["dist/web/mcpx_web_bg.wasm", "application/wasm"]],
    ["/host-id-tests.mjs", ["scripts/host-id-tests.mjs", "text/javascript"]],
    ["/concurrent-host.mjs", ["scripts/concurrent-host.mjs", "text/javascript"]],
  ]);
  const file = allowed.get(req.url);
  if (!file) return res.writeHead(404).end();
  res.writeHead(200, { "content-type": file[1] }).end(await readFile(file[0]));
}, { concurrencyBarrier: 16 });
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const origin = `http://127.0.0.1:${server.address().port}`;
const { server: legacyServer, requests: legacyRequests } = createMockServer(undefined, { legacy: true });
await new Promise(resolve => legacyServer.listen(0, "127.0.0.1", resolve));
const legacyOrigin = `http://127.0.0.1:${legacyServer.address().port}`;
let browser, http2Server, tlsDirectory;
try {
  tlsDirectory = await mkdtemp(join(tmpdir(), "mcpx-http2-"));
  const tlsKey = join(tlsDirectory, "key.pem"), tlsCert = join(tlsDirectory, "cert.pem");
  await promisify(execFile)("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", tlsKey, "-out", tlsCert, "-days", "1", "-subj", "/CN=localhost"]);
  const tls = { key: await readFile(tlsKey), cert: await readFile(tlsCert), allowHTTP1: true };
  const secure = createMockServer(undefined, { concurrencyBarrier: 16, serverFactory: handler => createSecureServer(tls, handler) });
  http2Server = secure.server;
  const http2Requests = secure.requests;
  await new Promise(resolve => http2Server.listen(0, "127.0.0.1", resolve));
  const http2Origin = `https://127.0.0.1:${http2Server.address().port}`;
  await init({ module_or_path: await readFile("dist/web/mcpx_web_bg.wasm") });
  for (const mode of ["modern", "legacy", "auto-modern", "auto-legacy"]) {
    const events = [];
    const runtime = mode.startsWith("auto") ? Runtime.auto(`${mode === "auto-legacy" ? legacyOrigin : origin}/mcp`) : new Runtime(`${origin}/mcp`, mode === "legacy");
    const remote = await new Remote(runtime, { onEvent: event => events.push(event), elicitation: { form: true } }).connect();
    const tool = remote.tool("echo");
    await assert.rejects(tool.call({ message: 10 }));
    const outcome = await tool.call({ message: "node-wasm" });
    assert.equal(outcome.value.raw.structuredContent.message, "node-wasm");
    assert.equal((await remote.execute("echo", ["--message", "argv"])).value.raw.structuredContent.message, "argv");
    assert.ok(events.some(event => event.kind === "progress"));
    if (mode === "modern" || mode === "auto-modern") {
      const pending = await tool.call({ message: "interactive" });
      assert.equal(pending.kind, "needs_input");
      await assert.rejects(remote.resume({ ...pending.value }, {}), /original handle/);
      await assert.rejects(remote.resume(pending.value, { confirm: { action: "accept", content: { yes: "wrong" } } }));
      const completed = await remote.resume(pending.value, { confirm: { action: "accept", content: { yes: true } } });
      assert.equal(completed.value.raw.structuredContent.message, "interactive");
      await assert.rejects(remote.resume(pending.value, { confirm: { action: "cancel" } }));
    }
    if (mode === "modern") assert.deepEqual(await runConcurrent(runtime), Array.from({ length: 16 }, (_, i) => `parallel-${i}`));
    runHostIds(runtime);
    tool.dispose();
    await remote.close();
    remote.runtime.free();
  }
  console.log("PASS Node WebAssembly: discovery, Schema, argv, SSE, recovery, 16 in-flight calls");

  const { engine } = await import("../dist/component/mcpx_component.js");
  for (const mode of ["modern", "legacy", "auto-modern", "auto-legacy"]) {
    const component = mode.startsWith("auto") ? engine.createAuto(`${mode === "auto-legacy" ? legacyOrigin : origin}/mcp`) : engine.create(`${origin}/mcp`, mode === "legacy");
    const adapter = componentRuntime(component);
    const remote = await new Remote(adapter, { elicitation: { form: true } }).connect();
    const tool = remote.tool("echo");
    assert.throws(() => tool.validate({ message: 10 }));
    const outcome = await tool.call({ message: "wasi-component" });
    assert.equal(outcome.value.raw.structuredContent.message, "wasi-component");
    assert.equal((await remote.execute("echo", ["--message", "component-argv"])).value.raw.structuredContent.message, "component-argv");
    if (mode === "modern" || mode === "auto-modern") {
      const pending = await tool.call({ message: "interactive" });
      assert.equal(pending.kind, "needs_input");
      const completed = await remote.resume(pending.value, { confirm: { action: "accept", content: { yes: true } } });
      assert.equal(completed.value.raw.structuredContent.message, "interactive");
      await assert.rejects(remote.resume(pending.value, { confirm: { action: "cancel" } }));
    }
    if (mode === "modern") assert.deepEqual(await runConcurrent(adapter), Array.from({ length: 16 }, (_, i) => `parallel-${i}`));
    runHostIds(adapter);
    tool.dispose();
    await remote.close();
    component[Symbol.dispose]?.();
  }
  console.log("PASS WASI Component via jco: discovery, Schema, argv, SSE, recovery, 16 in-flight calls");

  browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage({ ignoreHTTPSErrors: true });
  await page.goto(origin);
  const result = await page.evaluate(async ({ origin, legacyOrigin, http2Origin }) => {
    const { default: init, Runtime } = await import("/mcpx_web.js");
    const { Remote } = await import("/client.js");
    const { runConcurrent } = await import("/concurrent-host.mjs");
    const { runHostIds } = await import("/host-id-tests.mjs");
    await init();
    const results = [];
    for (const mode of ["modern", "legacy", "auto-modern", "auto-legacy"]) {
      // Different host spelling forces a real browser CORS request and preflight.
      const endpoint = `${(mode === "auto-legacy" ? legacyOrigin : origin).replace("127.0.0.1", "localhost")}/mcp`;
      const runtime = mode.startsWith("auto") ? Runtime.auto(endpoint) : new Runtime(endpoint, mode === "legacy");
      const remote = await new Remote(runtime, { elicitation: { form: true } }).connect();
      const tool = remote.tool("echo");
      let rejected = false;
      try { await tool.call({ message: 10 }); } catch { rejected = true; }
      const outcome = await tool.call({ message: "browser" });
      if (mode === "modern" || mode === "auto-modern") {
        const pending = await tool.call({ message: "interactive" });
        if (pending.kind !== "needs_input") throw new Error("Missing continuation");
        const resumed = await remote.resume(pending.value, { confirm: { action: "accept", content: { yes: true } } });
        if (resumed.value.raw.structuredContent.message !== "interactive") throw new Error("Resume failed");
      }
      if (mode === "modern") {
        const parallelRuntime = new Runtime(`${http2Origin}/mcp`, false);
        await new Remote(parallelRuntime).connect();
        const parallel = await runConcurrent(parallelRuntime);
        if (parallel.some((message, index) => message !== `parallel-${index}`)) throw new Error("Concurrent result mismatch");
        parallelRuntime.free();
      }
      results.push({ rejected, message: outcome.value.raw.structuredContent.message });
      runHostIds(runtime);
      tool.dispose();
      await remote.close();
      remote.runtime.free();
    }
    return results;
  }, { origin, legacyOrigin, http2Origin });
  assert.deepEqual(result, Array.from({ length: 4 }, () => ({ rejected: true, message: "browser" })));
  assert.throws(() => parseSafe('{"n":18446744073709551615}'), RangeError);
  assert.equal(requests.filter(request => request.method === "tools/call").length, 59);
  assert.equal(http2Requests.filter(request => request.method === "tools/call").length, 16);
  assert.equal(legacyRequests.filter(request => request.method === "tools/call").length, 5);
  console.log("PASS headless Chromium: cross-origin HTTP, Schema, SSE, recovery, 16 HTTP/2 calls in flight");
} finally {
  await browser?.close();
  await new Promise(resolve => server.close(resolve));
  await new Promise(resolve => legacyServer.close(resolve));
  if (http2Server) await new Promise(resolve => http2Server.close(resolve));
  if (tlsDirectory) await rm(tlsDirectory, { recursive: true, force: true });
}
