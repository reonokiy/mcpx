import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { chromium } from "playwright";
import init, { Runtime } from "../dist/web/mcpx_web.js";
import { engine } from "../dist/component/mcpx_component.js";
import { componentRuntime } from "../web/component.js";
import { Remote } from "../web/client.js";
import { createMockServer } from "./mock-server.mjs";
import { verifySettlement } from "./lifecycle-host.mjs";

assert.equal(typeof globalThis.gc, "function", "Run with node --expose-gc");
const root = fileURLToPath(new URL("../", import.meta.url));
const modern = createMockServer(async (request, response) => {
  const path = new URL(request.url, "http://localhost").pathname.slice(1);
  if (!path) return response.writeHead(200, {"content-type":"text/html"}).end("<!doctype html><title>mcpx lifecycle</title>");
  if (!/^(web|dist|scripts)\/[a-zA-Z0-9_./-]+\.(js|mjs|wasm)$/.test(path) || path.includes("..")) return response.writeHead(404).end();
  try { response.writeHead(200, {"content-type":path.endsWith(".wasm")?"application/wasm":"text/javascript"}).end(await readFile(join(root,path))); }
  catch { response.writeHead(404).end(); }
});
const legacy = createMockServer(undefined, {legacy:true, sessions:true});
let browser;
try {
  for (const fixture of [modern, legacy]) await new Promise(resolve => fixture.server.listen(0,"127.0.0.1",resolve));
  const origin = `http://127.0.0.1:${modern.server.address().port}`;
  const endpoints = [origin + "/mcp", `http://127.0.0.1:${legacy.server.address().port}/mcp`];
  await init({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
  for (const [index, endpoint] of endpoints.entries()) {
    for (const component of [false,true]) {
      const result = await verifySettlement(async options => {
        const raw = component ? engine.create(endpoint,Boolean(index)) : new Runtime(endpoint,Boolean(index));
        const remote = new Remote(component?componentRuntime(raw):raw, {...options, elicitation:{form:true}});
        try { await remote.connect(); } catch(error) { raw[Symbol.dispose]?.(); throw error; }
        return {remote,dispose:()=>component?raw[Symbol.dispose]():raw.free()};
      }, globalThis.gc, {legacy:Boolean(index)});
      assert.ok(result.retained >= 10);
      console.log(`PASS settled promise GC: Node ${component?"jco":"Wasm"} ${index?"legacy":"modern"}, ${result.retained} promises and catalog watcher retain results and release owner/provider`);
    }
  }
  browser = await chromium.launch({headless:true,args:["--no-sandbox","--js-flags=--expose-gc"]});
  const page = await browser.newPage();
  let workers=0, closed=0;
  page.on("worker",worker=>{workers++;worker.on("close",()=>closed++);});
  await page.goto(origin);
  const results = await page.evaluate(async endpoints => {
    const {verifySettlement} = await import("/scripts/lifecycle-host.mjs");
    const {Remote} = await import("/web/client.js");
    const {WorkerRemote} = await import("/web/worker.js");
    const {default:init,Runtime} = await import("/dist/web/mcpx_web.js");
    await init();
    const results=[];
    for (const [index,endpoint] of endpoints.entries()) {
      results.push(await verifySettlement(async options=>{
        const runtime=new Runtime(endpoint,Boolean(index));
        const remote=new Remote(runtime,{...options,elicitation:{form:true}});
        await remote.connect();
        return {remote,dispose:()=>runtime.free()};
      },globalThis.gc,{legacy:Boolean(index)}));
      for (const protocol of [index?"legacy":"modern","auto"]) {
        results.push(await verifySettlement(async options=>({
          remote:await WorkerRemote.connect(endpoint,{...options,protocol,elicitation:{form:true}}),dispose:()=>{},
        }),globalThis.gc,{legacy:Boolean(index)}));
      }
    }
    return results;
  },endpoints);
  assert.equal(results.length,6);
  for (const result of results) assert.ok(result.retained>=10);
  for (let attempt=0;closed<workers&&attempt<100;attempt++) await new Promise(resolve=>setTimeout(resolve,10));
  assert.equal(workers,4); assert.equal(closed,workers);
  for (const fixture of [modern,legacy]) assert.ok(!fixture.requests.some(r=>r.params?.arguments?.message==="lifecycle-never-sent"));
  console.log("PASS settled promise GC: Chromium Wasm and 4 explicit/Auto workers, owners/providers collected, results preserved, all workers exited");
} finally {
  await browser?.close();
  for (const fixture of [modern,legacy]) await new Promise(resolve=>fixture.server.close(resolve));
}
