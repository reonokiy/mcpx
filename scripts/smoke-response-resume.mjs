import assert from "node:assert/strict";
import {createServer} from "node:http";
import {readFile} from "node:fs/promises";
import {runResponseResume} from "./response-resume-host.mjs";
import {componentRuntime} from "../web/component.js";
const wasi=process.argv.includes("--wasi"),records=new Map();let failure,browser,preflights=0;
const frame=value=>`data: ${JSON.stringify(value)}\n\n`;
const server=createServer(async(req,res)=>{
  try {
    const url=new URL(req.url,"http://localhost");
    res.setHeader("access-control-allow-origin","*");res.setHeader("access-control-allow-headers","authorization,content-type,mcp-protocol-version,mcp-session-id,mcp-method,mcp-name,last-event-id");res.setHeader("access-control-expose-headers","mcp-session-id");res.setHeader("access-control-allow-methods","GET,POST,DELETE,OPTIONS");
    if(req.method==="OPTIONS"){preflights++;return res.writeHead(204).end();}
    if(url.pathname==="/")return res.writeHead(200,{"content-type":"text/html"}).end("<!doctype html><title>Response resumption</title>");
    const files={"/mcpx_web.js":"dist/web/mcpx_web.js","/mcpx_web_bg.wasm":"dist/web/mcpx_web_bg.wasm","/response-resume-host.mjs":"scripts/response-resume-host.mjs"};
    if(files[url.pathname])return res.writeHead(200,{"content-type":url.pathname.endsWith(".wasm")?"application/wasm":"text/javascript"}).end(await readFile(files[url.pathname]));
    assert.equal(url.pathname,"/mcp");assert.equal(req.headers.authorization,"Bearer response-frozen");
    const id=url.searchParams.get("case"),legacy=url.searchParams.get("legacy")==="1";
    if(!records.has(id))records.set(id,{calls:0,gets:0,replies:0,initialized:0,closed:0,streams:0,deleted:0});
    const record=records.get(id);
    if(req.method==="DELETE"){assert.equal(req.headers["mcp-session-id"],"response-session");record.deleted++;return res.writeHead(204).end();}
    if(req.method==="GET") {
      assert.ok(legacy);record.gets++;
      const cursor=Buffer.from(req.headers["last-event-id"],"latin1").toString("utf8");
      assert.equal(cursor,record.gets===1?"游标/first":"cursor/second");assert.equal(req.headers["mcp-session-id"],"response-session");assert.equal(req.headers["mcp-protocol-version"],"2025-11-25");
      assert.equal(record.replies,record.gets,"GET resumed before capability reply finished");
      if(record.gets===1)assert.ok(performance.now()-record.ended>=24,"GET ignored server retry");
      res.writeHead(200,{"content-type":"text/event-stream"});record.streams++;res.once("close",()=>record.closed++);
      if(record.gets===1)return res.end("id: cursor/second\nretry: 0\n"+frame({jsonrpc:"2.0",id:-7,method:"roots/list",params:{}}));
      assert.equal(record.gets,2);
      return res.end(`data: {"jsonrpc":"2.0","id":${record.rpc},"result":{"content":[],"structuredContent":{"value":18446744073709551615}}}\n\n`);
    }
    assert.equal(req.method,"POST");assert.equal(req.headers["last-event-id"],undefined,"Cursor leaked onto POST");
    let raw="";for await(const chunk of req)raw+=chunk;const rpc=JSON.parse(raw);
    const send=result=>res.writeHead(200,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,result}));
    if(rpc.method==null){assert.equal(rpc.id,-7);assert.equal(rpc.error.code,-32601);record.replies++;return res.writeHead(202).end();}
    if(rpc.method==="server/discover")return legacy?res.writeHead(400,{"content-type":"application/json"}).end(JSON.stringify({jsonrpc:"2.0",id:rpc.id,error:{code:-32601,message:"initialize first"}})):send({supportedVersions:["2026-07-28"]});
    if(rpc.method==="initialize"){record.initialized++;res.setHeader("mcp-session-id","response-session");return send({protocolVersion:"2025-11-25"});}
    if(rpc.method==="notifications/initialized")return res.writeHead(202).end();
    if(rpc.method==="tools/list")return send({tools:[{name:"echo",inputSchema:{type:"object"}}]});
    assert.equal(rpc.method,"tools/call");record.calls++;assert.equal(record.calls,1,"Original tool POST replayed");record.rpc=rpc.id;
    res.writeHead(200,{"content-type":"text/event-stream"});record.streams++;res.once("close",()=>record.closed++);
    res.write("id: 游标/first\nretry: 25\ndata:\n\n");if(legacy)res.write(frame({jsonrpc:"2.0",id:-7,method:"roots/list",params:{}}));record.ended=performance.now();res.end("id: uncommitted\ndata: {");
  }catch(error){failure??=error;if(!res.headersSent)res.writeHead(500);res.end();}
});
await new Promise(resolve=>server.listen(0,"127.0.0.1",resolve));const origin=`http://127.0.0.1:${server.address().port}`;
try {
  if(wasi) {
    const {spawn}=await import("node:child_process");
    for(const legacy of [false,true])for(const auto of [false,true]) {
      const endpoint=`${origin}/mcp?legacy=${+legacy}&case=wasi-${legacy}-${auto}`;
      const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/response_resume.wasm",endpoint,legacy?"legacy":"modern",auto?"auto":"explicit"],{stdio:["ignore","pipe","pipe"]});
      let out="",err="";child.stdout.on("data",b=>out+=b);child.stderr.on("data",b=>err+=b);const timer=setTimeout(()=>child.kill("SIGKILL"),15000);
      const status=await new Promise((resolve,reject)=>{child.once("exit",resolve);child.once("error",reject);});clearTimeout(timer);assert.equal(status,0,err);assert.match(out,/PASS response resume/);assert.equal(err,"");
    }
  }else {
    const {default:init,Runtime}=await import("../dist/web/mcpx_web.js"),{engine}=await import("../dist/component/mcpx_component.js"),{chromium}=await import("playwright");await init({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
    for(const host of ["wasm","jco"])for(const legacy of [false,true])for(const auto of [false,true]) {
      const endpoint=`${origin}/mcp?legacy=${+legacy}&case=${host}-${legacy}-${auto}`;
      const raw=host==="wasm"?(auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy)):(auto?engine.createAuto(endpoint):engine.create(endpoint,legacy));
      try{await runResponseResume(host==="wasm"?raw:componentRuntime(raw),endpoint,legacy);}finally{host==="wasm"?raw.free():raw[Symbol.dispose]();}
      console.log(`PASS response resume: ${host}, ${legacy?"legacy":"modern rejection"}, ${auto?"Auto":"explicit"}`);
    }
    browser=await chromium.launch({headless:true,args:["--no-sandbox"]});const page=await browser.newPage();await page.goto(origin.replace("127.0.0.1","localhost"));
    await page.evaluate(async origin=>{
      const {default:init,Runtime}=await import("/mcpx_web.js"),{runResponseResume}=await import("/response-resume-host.mjs");await init();
      for(const legacy of [false,true])for(const auto of [false,true]){
        const endpoint=`${origin}/mcp?legacy=${+legacy}&case=browser-${legacy}-${auto}`,raw=auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy);
        try{await runResponseResume(raw,endpoint,legacy);}finally{raw.free();}
      }
    },origin);assert.ok(preflights>0);
  }
  assert.ifError(failure);
  for(const record of records.values()){assert.equal(record.calls,1);assert.equal(record.gets,record.initialized?2:0);assert.equal(record.replies,record.initialized?2:0);assert.equal(record.closed,record.streams);}
  assert.equal(records.size,wasi?4:12);console.log(`PASS ${records.size} response resumption HTTP cases: unchanged operation/RPC, fresh streams, original credentials/session, reply drain, retry, uint64, no POST replay`);
}finally{await browser?.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
