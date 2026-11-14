import assert from "node:assert/strict";
import {createServer} from "node:http";
import {readFile} from "node:fs/promises";
import {componentRuntime} from "../web/component.js";
import {runLegacyResume} from "./legacy-resume-host.mjs";

const records = new Map(); let browser, failure, preflights = 0;
const wasi = process.argv.includes("--wasi");
const frame = value => `data: ${JSON.stringify(value)}\n\n`;
const server = createServer(async(req,res) => {
  try {
    const url = new URL(req.url, "http://localhost");
    res.setHeader("access-control-allow-origin", "*");
    res.setHeader("access-control-allow-headers", "authorization, content-type, mcp-protocol-version, mcp-session-id, mcp-method, mcp-name, last-event-id");
    res.setHeader("access-control-expose-headers", "mcp-session-id");
    res.setHeader("access-control-allow-methods", "GET, POST, OPTIONS");
    if (req.method === "OPTIONS") { preflights++; return res.writeHead(204).end(); }
    const files = {"/mcpx_web.js":["dist/web/mcpx_web.js","text/javascript"],"/mcpx_web_bg.wasm":["dist/web/mcpx_web_bg.wasm","application/wasm"],"/legacy-resume-host.mjs":["scripts/legacy-resume-host.mjs","text/javascript"]};
    if (url.pathname === "/") return res.writeHead(200,{"content-type":"text/html"}).end("<!doctype html><title>Legacy resume</title>");
    if (files[url.pathname]) { const [file,mime] = files[url.pathname]; return res.writeHead(200,{"content-type":mime}).end(await readFile(file)); }
    assert.equal(url.pathname, "/mcp"); assert.equal(req.headers.authorization, "Bearer resume-frozen");
    const key = url.searchParams.get("case"), legacy = url.searchParams.get("legacy") === "1";
    if (!records.has(key)) records.set(key, {legacy, gets:0, listens:0, replies:0, calls:0, lists:0, posts:[]});
    const record = records.get(key);
    if (req.method === "DELETE") { assert.equal(req.headers["mcp-session-id"],"resume-session"); record.deletes=(record.deletes??0)+1; return res.writeHead(204).end(); }
    let message;
    if (req.method === "POST") {
      assert.equal(req.headers["last-event-id"], undefined, "Resume cursor leaked into a POST");
      let raw=""; for await (const part of req) raw+=part; message=JSON.parse(raw); record.posts.push(message.method ?? "reply");
    }
    if (req.method === "GET" || message.method === "subscriptions/listen") {
      res.writeHead(200,{"content-type":"text/event-stream"});
      if (!legacy) {
        assert.equal(req.method,"POST"); record.listens++;
        res.write(frame({jsonrpc:"2.0",method:"notifications/subscriptions/acknowledged",params:{_meta:{"io.modelcontextprotocol/subscriptionId":message.id},notifications:{toolsListChanged:true}}}));
        return res.end("id: modern-unused\nretry: 0\ndata:\n\n");
      }
      assert.equal(req.headers["mcp-session-id"], "resume-session"); assert.equal(req.headers["mcp-protocol-version"], "2025-11-25");
      record.gets++;
      const cursor = req.headers["last-event-id"] == null ? undefined : Buffer.from(req.headers["last-event-id"],"latin1").toString("utf8");
      assert.equal(cursor, [undefined,"游标/é","cursor-two"][record.gets-1]);
      if (record.gets === 1) {
        record.disconnectedAt = performance.now();
        res.write("id: 游标/é\nretry: 25\ndata:\n\n");
        res.write(frame({jsonrpc:"2.0",id:-7,method:"roots/list"}));
        return res.end("id: never-committed\ndata: {");
      }
      if (record.gets === 2) {
        assert.ok(performance.now() - record.disconnectedAt >= 25, "Resumed before server retry interval elapsed");
        assert.equal(record.replies,1,"Resumed before accepted capability reply completed");
        res.write("id: cursor-two\nretry: 0\n"+frame({jsonrpc:"2.0",method:"notifications/tools/list_changed",params:{}}));
        return res.end(frame({jsonrpc:"2.0",id:-7,method:"roots/list"}));
      }
      assert.equal(record.gets,3); assert.equal(record.replies,2);
      return res.end("id:\ndata:\n\n");
    }
    if (message.method == null) { assert.equal(message.id,-7); assert.equal(message.error.code,-32601); record.replies++; return res.writeHead(202).end(); }
    if (message.method === "notifications/initialized") return res.writeHead(202).end();
    if (legacy && message.method === "server/discover") return res.writeHead(400,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:message.id,error:{code:-32601,message:"initialize first"}}));
    let result;
    if (message.method === "initialize") { res.setHeader("mcp-session-id","resume-session"); result={protocolVersion:"2025-11-25",capabilities:{tools:{listChanged:true}}}; }
    else if (message.method === "server/discover") result={supportedVersions:["2026-07-28"]};
    else if (message.method === "tools/list") { record.lists++; result={tools:[{name:"echo",inputSchema:{type:"object"}}]}; }
    else { assert.equal(message.method,"tools/call"); record.calls++; result={content:[],structuredContent:message.params.arguments}; }
    res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:message.id,result}));
  } catch(error) { failure ??= error; res.writeHead(500).end(); }
});
await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${server.address().port}`;
try {
  if (wasi) {
    const {spawn} = await import("node:child_process");
    for (const legacy of [false,true]) for (const auto of [false,true]) {
      const endpoint=`${origin}/mcp?case=wasi-${legacy}-${auto}&legacy=${Number(legacy)}`;
      const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/legacy_resume.wasm",endpoint,legacy?"legacy":"modern",auto?"auto":"explicit"],{stdio:["ignore","pipe","pipe"]});
      let stdout="",stderr=""; child.stdout.on("data",b=>stdout+=b); child.stderr.on("data",b=>stderr+=b);
      const timer=setTimeout(()=>child.kill("SIGKILL"),15000);
      const status=await new Promise((resolve,reject)=>{child.once("exit",resolve);child.once("error",reject);}); clearTimeout(timer);
      assert.equal(status,0,stderr); assert.match(stdout,/PASS legacy resume/); assert.equal(stderr,""); assert.ifError(failure);
      console.log(`PASS Wasmtime legacy resume: ${legacy?"legacy":"modern rejection"}, ${auto?"Auto":"explicit"}`);
    }
  } else {
  const {chromium}=await import("playwright");
  const {default:init,Runtime}=await import("../dist/web/mcpx_web.js");
  const {engine}=await import("../dist/component/mcpx_component.js");
  await init({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
  for (const host of ["wasm","jco"]) for (const legacy of [false,true]) for (const auto of [false,true]) {
    const endpoint=`${origin}/mcp?case=${host}-${legacy}-${auto}&legacy=${Number(legacy)}`;
    const raw=host==="wasm" ? (auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy)) : (auto?engine.createAuto(endpoint):engine.create(endpoint,legacy));
    try { await runLegacyResume(host==="wasm"?raw:componentRuntime(raw),endpoint,legacy); }
    finally { host==="wasm"?raw.free():raw[Symbol.dispose](); }
    assert.ifError(failure); console.log(`PASS legacy resume API: ${host}, ${legacy?"legacy":"modern rejection"}, ${auto?"Auto":"explicit"}`);
  }
  browser=await chromium.launch({headless:true,args:["--no-sandbox"]}); const page=await browser.newPage(); await page.goto(origin.replace("127.0.0.1","localhost"));
  await page.evaluate(async origin=>{
    if (location.origin === origin) throw new Error("Expected cross-origin resume");
    const {default:init,Runtime}=await import("/mcpx_web.js"); const {runLegacyResume}=await import("/legacy-resume-host.mjs"); await init();
    for (const legacy of [false,true]) for (const auto of [false,true]) {
      const endpoint=`${origin}/mcp?case=browser-${legacy}-${auto}&legacy=${Number(legacy)}`;
      const raw=auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy);
      try { await runLegacyResume(raw,endpoint,legacy); } finally { raw.free(); }
    }
  },origin);
  assert.ok(preflights>0);
  }
  assert.ifError(failure); assert.equal(records.size,wasi?4:12);
  for (const record of records.values()) {
    assert.equal(record.gets,record.legacy?3:0); assert.equal(record.calls,record.legacy?1:0);
    assert.equal(record.replies,record.legacy?2:0); assert.equal(record.listens,record.legacy?0:1);
    if (wasi) assert.equal(record.deletes??0,record.legacy?1:0);
  }
  console.log(`PASS ${records.size} real HTTP cases: UTF-8 Last-Event-ID, partial-frame resume, retry timing, cursor reset, frozen credentials, late input isolation, reply completion and no business POST replay${wasi?"; Wasmtime guests exited":"; Chromium CORS"}`);
} finally { await browser?.close(); server.closeAllConnections(); await new Promise(resolve=>server.close(resolve)); }
