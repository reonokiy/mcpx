import assert from "node:assert/strict";
import {createServer} from "node:http";
import {readFile} from "node:fs/promises";
import {chromium} from "playwright";
import init,{Runtime} from "../dist/web/mcpx_web.js";
import {engine} from "../dist/component/mcpx_component.js";
import {componentRuntime} from "../web/component.js";
import {runNotifications} from "./notification-host.mjs";

const records=new Map(),streams=new Map();let browser,preflights=0;
const key=url=>url.searchParams.get("case");
const state=url=>{const id=key(url);if(!records.has(id))records.set(id,{requests:[],replies:0,opens:0,closed:0});return records.get(id);};
const frame=value=>`data: ${JSON.stringify(value)}\r\n\r\n`;
const server=createServer(async(req,res)=>{
  try {
    const url=new URL(req.url,"http://localhost"),legacy=url.searchParams.get("legacy")==="1";
    res.setHeader("access-control-allow-origin","*");res.setHeader("access-control-allow-headers","authorization, content-type, mcp-protocol-version, mcp-session-id, mcp-method, mcp-name");res.setHeader("access-control-expose-headers","mcp-session-id");
    res.setHeader("access-control-allow-methods","GET, POST, OPTIONS");
    if(req.method==="OPTIONS"){preflights++;return res.writeHead(204).end();}
    const files={"/mcpx_web.js":["dist/web/mcpx_web.js","text/javascript"],"/mcpx_web_bg.wasm":["dist/web/mcpx_web_bg.wasm","application/wasm"],"/notification-host.mjs":["scripts/notification-host.mjs","text/javascript"]};
    if(url.pathname==="/")return res.writeHead(200,{"content-type":"text/html"}).end("<!doctype html><title>Notification resources</title>");
    if(files[url.pathname]){const[file,mime]=files[url.pathname];return res.writeHead(200,{"content-type":mime}).end(await readFile(file));}
    if(!["/mcp","/stats","/emit"].includes(url.pathname))return res.writeHead(404).end();
    const record=state(url);
    if(url.pathname==="/stats")return res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify(record));
    if(url.pathname==="/emit") {
      const stream=streams.get(key(url));assert.ok(stream,"No active notification stream");const kind=url.searchParams.get("kind");
      if(kind==="change"||kind==="bad")stream.response.write(frame({jsonrpc:"2.0",method:"notifications/tools/list_changed",params:legacy?{}:{_meta:{"io.modelcontextprotocol/subscriptionId":stream.rpc+(kind==="bad"?2**32:0)}}}));
      else if(kind==="request")stream.response.write(frame({jsonrpc:"2.0",id:-7,method:"roots/list",params:{}}));
      else if(kind==="end") {
        if(!legacy)stream.response.write(frame({jsonrpc:"2.0",id:stream.rpc,result:{resultType:"complete",_meta:{"io.modelcontextprotocol/subscriptionId":stream.rpc}}}));
        stream.response.end();
      }
      return res.writeHead(204).end();
    }
    assert.equal(url.pathname,"/mcp");
    assert.equal(req.headers.authorization,"Bearer notify-snapshot");
    let rpc;
    if(req.method==="POST"){let raw="";for await(const chunk of req)raw+=chunk;rpc=JSON.parse(raw);record.requests.push(rpc);}
    const listening=req.method==="GET"||rpc?.method==="subscriptions/listen";
    if(listening) {
      assert.equal(req.method,legacy?"GET":"POST");assert.equal(req.headers["last-event-id"],undefined);
      if(legacy) {assert.equal(req.headers["mcp-session-id"],"notify-session");assert.equal(req.headers["mcp-method"],undefined);}
      else {assert.equal(req.headers["mcp-session-id"],undefined);assert.equal(req.headers["mcp-method"],"subscriptions/listen");assert.deepEqual(rpc.params.notifications,{toolsListChanged:true});}
      record.opens++;const stream={response:res,rpc:rpc?.id};streams.set(key(url),stream);
      res.once("close",()=>{record.closed++;if(streams.get(key(url))===stream)streams.delete(key(url));});
      res.writeHead(200,{"content-type":"text/event-stream"});res.write(": open\r\n\r\n");
      if(!legacy)res.write(frame({jsonrpc:"2.0",method:"notifications/subscriptions/acknowledged",params:{_meta:{"io.modelcontextprotocol/subscriptionId":rpc.id},notifications:{toolsListChanged:true}}}));
      return;
    }
    if(rpc.method==null) {assert.equal(rpc.id,-7);assert.equal(rpc.error.code,-32601);assert.equal(req.headers["mcp-session-id"],"notify-session");record.replies++;return res.writeHead(202).end();}
    if(legacy&&rpc.method==="server/discover")return res.writeHead(400,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,error:{code:-32601,message:"initialize first"}}));
    if(rpc.method==="notifications/initialized")return res.writeHead(202).end();
    let result;
    if(rpc.method==="initialize"){res.setHeader("mcp-session-id","notify-session");result={protocolVersion:"2025-11-25",capabilities:{tools:{listChanged:true}}};}
    else if(rpc.method==="server/discover")result={supportedVersions:["2026-07-28"],capabilities:{tools:{listChanged:true}}};
    else if(rpc.method==="tools/list")result={tools:[{name:"echo",inputSchema:{type:"object",properties:{message:{type:"string"}}}}]};
    else if(rpc.method==="tools/call")result={content:[],structuredContent:{message:rpc.params.arguments.message}};
    else throw new Error("Unexpected method");
    res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,result}));
  } catch(error) {res.writeHead(500).end();server.emit("fixtureError",error);}
});
let fixtureError;server.on("fixtureError",error=>fixtureError??=error);
await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));const origin=`http://127.0.0.1:${server.address().port}`;
try {
  await init({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
  for(const host of ["wasm","jco"])for(const legacy of [false,true])for(const auto of [false,true]) {
    const endpoint=`${origin}/mcp?legacy=${Number(legacy)}&case=${host}-${legacy}-${auto}`;
    const raw=host==="wasm"?(auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy)):(auto?engine.createAuto(endpoint):engine.create(endpoint,legacy));
    const runtime=host==="wasm"?raw:componentRuntime(raw);
    try {assert.equal((await runNotifications(runtime,endpoint)).legacy,legacy);}finally{host==="wasm"?raw.free():raw[Symbol.dispose]();}
    console.log(`PASS notification resource: Node ${host} ${legacy?"legacy GET":"modern POST"} ${auto?"Auto":"explicit"}`);
  }
  browser=await chromium.launch({headless:true,args:["--no-sandbox"]});const page=await browser.newPage();await page.goto(origin.replace("127.0.0.1","localhost"));
  await page.evaluate(async origin=>{
    if(location.origin===origin)throw new Error("Expected cross-origin notification requests");
    const {default:init,Runtime}=await import("/mcpx_web.js");const {runNotifications}=await import("/notification-host.mjs");await init();
    for(const legacy of [false,true])for(const auto of [false,true]) {
      const endpoint=`${origin}/mcp?legacy=${Number(legacy)}&case=browser-${legacy}-${auto}`;
      const runtime=auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy);
      try {await runNotifications(runtime,endpoint);}finally{runtime.free();}
    }
  },origin);
  assert.ok(preflights>0,"Cross-origin authorization/session headers did not preflight");
  console.log("PASS notification resources: cross-origin Chromium explicit/Auto, both protocol eras, idle invalidation and independent calls");
  await new Promise(resolve=>setTimeout(resolve,20));assert.ifError(fixtureError);assert.equal(streams.size,0);
  for(const record of records.values()){assert.equal(record.opens,2);assert.equal(record.closed,2);assert.equal(record.requests.filter(r=>r.method==="tools/call").length,1);}
  console.log("PASS 12 real HTTP scenarios: frozen credentials, legacy capability replies, exact IDs, stream cleanup and no automatic replay");
} finally {await browser?.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
