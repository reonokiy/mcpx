import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {createServer,request} from "node:http";
import {readFile} from "node:fs/promises";
import {componentRuntime} from "../web/component.js";
import {runResponseResume} from "./response-resume-host.mjs";
import {Remote} from "../web/client.js";
const wasi=process.argv.includes("--wasi");
let Runtime,engine;
if(!wasi) {
  const web=await import("../dist/web/mcpx_web.js");Runtime=web.Runtime;
  ({engine}=await import("../dist/component/mcpx_component.js"));
  await web.default({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
}
async function guest(endpoint,host,auto) {
  const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/response_recovery.wasm",endpoint,auto?"auto-legacy":"legacy",host==="wasi-client"?"sdk-client":"sdk-queue"],{stdio:["ignore","pipe","pipe"]});
  let stdout="",stderr="";child.stdout.on("data",b=>stdout+=b);child.stderr.on("data",b=>stderr+=b);const timer=setTimeout(()=>child.kill("SIGKILL"),15000);
  const status=await new Promise((resolve,reject)=>{child.once("exit",resolve);child.once("error",reject);});clearTimeout(timer);assert.equal(status,0,stderr);assert.equal(stderr,"");assert.match(stdout,/PASS WASI response recovery/);
}
for(const host of wasi?["wasi-client","wasi-queue"]:["wasm","jco"])for(const auto of [false,true])for(const highLevel of wasi?[true]:[false,true]) {
  const daemon=spawn("target/debug/examples/sdk_http_server",["legacy","sse-preferred","60000","response-recovery"],{stdio:["pipe","pipe","pipe"]});
  let stdout="",stderr="",ready;
  const readiness=new Promise((resolve,reject)=>{ready=resolve;daemon.once("error",reject);});const exit=new Promise(resolve=>daemon.once("exit",resolve));
  daemon.stdout.on("data",chunk=>{stdout+=chunk;if(stdout.includes("\n"))ready(JSON.parse(stdout.trim()));});daemon.stderr.on("data",chunk=>stderr+=chunk);
  const timer=setTimeout(()=>daemon.kill("SIGKILL"),15000);const upstreams=new Set();let proxy,raw,remote;
  try {
    const {endpoint}=await Promise.race([readiness,exit.then(code=>{throw new Error(`SDK exited ${code}: ${stderr}`);})]);
    const records=[];let failure;
    proxy=createServer(async(req,res)=>{
      try {
        assert.equal(req.headers.authorization,"Bearer response-frozen");let body="";for await(const chunk of req)body+=chunk;
        const rpc=body?JSON.parse(body):undefined,row={method:rpc?.method??req.method,rpc:rpc?.id,cursor:req.headers["last-event-id"],session:req.headers["mcp-session-id"]};records.push(row);
        const fields={...req.headers,host:new URL(endpoint).host};delete fields.connection;delete fields["content-length"];
        const upstream=request(endpoint,{method:req.method,headers:fields,agent:false},response=>{
          response.on("data",chunk=>{row.response=((row.response??"")+chunk).slice(0,2048);});
          res.writeHead(response.statusCode,response.headers);
          if(rpc?.method!=="tools/call"){response.pipe(res);return;}
          let priming=Buffer.alloc(0),ended=false;
          response.on("data",chunk=>{
            if(ended)return;priming=Buffer.concat([priming,chunk]);assert.ok(priming.length<=65536);
            let end=priming.indexOf("\r\n\r\n"),size=4;if(end<0){end=priming.indexOf("\n\n");size=2;}
            // Cut the frontend after the SDK's intact priming. Keep draining
            // its original backend response so the stock SDK cache receives
            // the real progress/result; no event IDs or response bytes change.
            if(end>=0){ended=true;res.end(priming.subarray(0,end+size));}
          });
        });
        upstreams.add(upstream);upstream.once("close",()=>upstreams.delete(upstream));upstream.on("error",error=>{if(!res.destroyed){failure??=error;res.destroy();}});res.once("close",()=>{if(rpc?.method!=="tools/call")upstream.destroy();});upstream.end(body);
      }catch(error){failure??=error;res.destroy();}
    });
    await new Promise(resolve=>proxy.listen(0,"127.0.0.1",resolve));const url=`http://127.0.0.1:${proxy.address().port}/mcp`;
    if(!wasi)raw=host==="wasm"?(auto?Runtime.auto(url):new Runtime(url,true)):(auto?engine.createAuto(url):engine.create(url,true));
    const runtime=wasi?undefined:host==="wasm"?raw:componentRuntime(raw);
    if(highLevel&&!wasi)remote=new Remote(runtime,{token:"response-frozen",responseRecovery:{maxAttempts:2,backoff:0,timeout:3000},timeout:5000});
    if(auto) {
      if(wasi)await guest(url,host,auto);
      else await assert.rejects(highLevel?remote.connect():runResponseResume(runtime,url,true,{sdk:true}),error=>(typeof error==="string"?JSON.parse(error):error).code==="UnsupportedProtocolVersion");
      assert.deepEqual(records.map(r=>r.method),["server/discover"]);assert.equal((await (await fetch(new URL("/stats",endpoint))).json()).calls,0);
      console.log(`PASS official SDK explicit version rejection: ${host} ${highLevel?"Remote":"explicit host"} Auto, no initialize, GET or tool replay`);continue;
    }
    try{
      if(wasi)await guest(url,host,auto);
      else if(highLevel){await remote.connect();const binding=remote.tool("echo");const raw=await binding.callRaw('{"message":"sdk-response-resume"}');assert.equal(JSON.parse(raw).value.raw.structuredContent.message,"sdk-response-resume");binding.dispose();await remote.close();}
      else await runResponseResume(runtime,url,true,{sdk:true});
    }
    catch(error){throw new Error(`${error.message??error}\n${JSON.stringify(records)}`,{cause:error});}
    assert.ifError(failure);assert.equal(records.filter(r=>r.method==="tools/call").length,1);assert.equal(records.filter(r=>r.method==="initialize").length,1);
    const get=records.filter(r=>r.method==="GET");assert.ok(get.length>=1);assert.ok(get.every(r=>r.cursor&&r.session));assert.ok(records.every(r=>r.method==="GET"||r.cursor===undefined));
    assert.ok(!records.find(r=>r.method==="tools/call").response.includes('"result"'),"SDK final response arrived on original POST");
    assert.ok(get.some(r=>r.response?.includes('"result"')),"SDK GET did not deliver the original result");
    const metrics=await (await fetch(new URL("/stats",endpoint))).json();assert.equal(metrics.calls,1);
    const session=get[0].session;assert.ok(get.every(r=>r.session===session));
    if(!highLevel)await fetch(url,{method:"DELETE",headers:{authorization:"Bearer response-frozen","mcp-session-id":session}});
    console.log(`PASS official SDK request-scoped GET recovery: ${host} ${highLevel?"Remote":"explicit host"}, ${auto?"Auto":"legacy"}, one POST/initialize, original session and RPC`);
  }finally {
    await remote?.close().catch(()=>{});if(raw)host==="wasm"?raw.free():raw[Symbol.dispose]();for(const upstream of upstreams)upstream.destroy();if(proxy){proxy.closeAllConnections();await new Promise(resolve=>proxy.close(resolve));}
    if(!daemon.killed)daemon.stdin.end();await exit;clearTimeout(timer);
  }
}
