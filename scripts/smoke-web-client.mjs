import assert from "node:assert/strict";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { createSecureServer } from "node:http2";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { chromium } from "playwright";
import { createMockServer } from "./mock-server.mjs";
import { runWebClient } from "./web-client-host.mjs";
import { Remote, parseSafe } from "../web/client.js";
import { componentRuntime } from "../web/component.js";
import init, { Runtime } from "../dist/web/mcpx_web.js";

const files = new Map([
  ["/", [null, "text/html"]],
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
  ["/schema-fixture.mjs", ["scripts/schema-fixture.mjs", "text/javascript"]],
  ["/worker-protocol.js", ["web/worker-protocol.js", "text/javascript"]],
  ["/mcpx_web.js", ["dist/web/mcpx_web.js", "text/javascript"]],
  ["/mcpx_web_bg.wasm", ["dist/web/mcpx_web_bg.wasm", "application/wasm"]],
  ["/web-client-host.mjs", ["scripts/web-client-host.mjs", "text/javascript"]],
]);
const { server, requests } = createMockServer(async (req, res) => {
  const entry = files.get(req.url);
  if (!entry) return res.writeHead(404).end();
  res.writeHead(200, { "content-type": entry[1] }).end(entry[0] ? await readFile(entry[0]) : "<!doctype html><title>mcpx JS queue</title>");
}, { concurrencyBarrier: 16 });
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const origin = `http://127.0.0.1:${server.address().port}`;
const { server: legacy, requests: legacyRequests } = createMockServer(undefined, { legacy: true, concurrencyBarrier: 16 });
await new Promise(resolve => legacy.listen(0, "127.0.0.1", resolve));
const legacyOrigin = `http://127.0.0.1:${legacy.address().port}`;
let browser, secureServer, directory;
try {
  await init({ module_or_path: await readFile("dist/web/mcpx_web_bg.wasm") });
  for (const isLegacy of [false, true]) {
    const endpoint = `${isLegacy ? legacyOrigin : origin}/mcp`;
    const result = await runWebClient(Remote, () => {
      const runtime = new Runtime(endpoint, isLegacy);
      return { runtime, dispose: () => runtime.free() };
    }, globalThis.fetch, { legacy:isLegacy });
    assert.equal(result.peak, 16);
    console.log(`PASS high-level JS Node Wasm (${isLegacy ? "legacy" : "modern"}): 16 concurrent, queue/byte budgets, cancel, timeout, disposal, terminal race`);
  }
  const { engine } = await import("../dist/component/mcpx_component.js");
  for (const isLegacy of [false, true]) {
    const result = await runWebClient(Remote, () => {
      const component = engine.create(`${isLegacy ? legacyOrigin : origin}/mcp`, isLegacy);
      return { runtime: componentRuntime(component), dispose: () => component[Symbol.dispose]() };
    }, globalThis.fetch, { legacy:isLegacy });
    assert.equal(result.peak, 16);
    console.log(`PASS high-level JS jco (${isLegacy ? "legacy" : "modern"}): shared host queue and independent cleanup`);
  }
  directory = await mkdtemp(join(tmpdir(), "mcpx-web-client-"));
  const key = join(directory, "key.pem"), cert = join(directory, "cert.pem");
  await promisify(execFile)("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=localhost"]);
  const tls = { key: await readFile(key), cert: await readFile(cert), allowHTTP1: true };
  const secure = createMockServer(undefined, { concurrencyBarrier: 16, serverFactory: handler => createSecureServer(tls, handler) });
  secureServer = secure.server;
  await new Promise(resolve => secureServer.listen(0, "127.0.0.1", resolve));
  const endpoint = `https://localhost:${secureServer.address().port}/mcp`;
  browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage({ ignoreHTTPSErrors: true });
  await page.goto(origin);
  const result = await page.evaluate(async endpoint => {
    const { default: init, Runtime } = await import("/mcpx_web.js");
    const { Remote } = await import("/client.js");
    const { runWebClient } = await import("/web-client-host.mjs");
    await init();
    return runWebClient(Remote, () => {
      const runtime = new Runtime(endpoint, false);
      return { runtime, dispose: () => runtime.free() };
    }, globalThis.fetch, { legacy:false });
  }, endpoint);
  assert.equal(result.peak, 16);
  console.log("PASS high-level JS Chromium: real cross-origin HTTP/2, cancellation, timeout, queue and resource release");
  for (const list of [requests, legacyRequests, secure.requests]) {
    const messages = list.filter(request => request.method === "tools/call").map(request => request.params.arguments.message);
    for (const forbidden of ["never-dispatched", "cancelled-in-queue", "capacity-rejected", "pre-aborted", "close-queued", "closed", "disposed", "operation-observer-never-sent", "operation-sequence-never-sent", "operation-sequence-rejected"]) assert.ok(!messages.includes(forbidden), `${forbidden} reached the server`);
  }
  assert.equal(requests.filter(request => request.method === "notifications/cancelled").length, 0, "Modern HTTP sent legacy cancellation");
  assert.equal(secure.requests.filter(request => request.method === "notifications/cancelled").length, 0);
  // Each legacy suite cancels an active call, times one out, cancels the blocked
  // queue leader and closes an active call. Unsent queued work has no wire cancel.
  assert.equal(legacyRequests.filter(request => request.method === "notifications/cancelled").length, 8);
  for (const text of ['{"n":18446744073709551615}', '{"n":1e400}']) {
    assert.throws(() => parseSafe(text), error => error instanceof RangeError && error.raw === text);
  }
} finally {
  await browser?.close();
  await new Promise(resolve => server.close(resolve));
  await new Promise(resolve => legacy.close(resolve));
  if (secureServer) await new Promise(resolve => secureServer.close(resolve));
  if (directory) await rm(directory, { recursive: true, force: true });
}
