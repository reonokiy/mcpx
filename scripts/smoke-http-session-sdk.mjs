import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {readFile} from "node:fs/promises";
import init,{Runtime} from "../dist/web/mcpx_web.js";
import {engine} from "../dist/component/mcpx_component.js";
import {componentRuntime} from "../web/component.js";
import {Remote} from "../web/client.js";
import {rejected,until} from "./http-reconnect-host.mjs";
await init({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
for(const host of ["wasm","jco"])for(const auto of [false,true])for(const preference of ["json","sse-preferred"]){
 const daemon=spawn("target/debug/examples/sdk_http_server",["legacy",preference,"60000","session-recovery"],{stdio:["pipe","pipe","pipe"]});
 let stdout="",stderr="",ready;
 const readiness=new Promise((resolve,reject)=>{ready=resolve;daemon.once("error",reject);});const exit=new Promise(resolve=>daemon.once("exit",resolve));
 daemon.stdout.on("data",b=>{stdout+=b;if(stdout.includes("\n"))ready(JSON.parse(stdout.trim()));});daemon.stderr.on("data",b=>stderr+=b);
 const timer=setTimeout(()=>daemon.kill("SIGKILL"),15000);let raw,remote;
 try {
  const {endpoint}=await Promise.race([readiness,exit.then(code=>{throw new Error(`SDK exited ${code}: ${stderr}`);})]);
  raw=host==="wasm"?(auto?Runtime.auto(endpoint):new Runtime(endpoint,true)):(auto?engine.createAuto(endpoint):engine.create(endpoint,true));
  const requests=[];
  remote=new Remote(host==="wasm"?raw:componentRuntime(raw),{timeout:4000,httpSessionRecovery:{maxAttempts:2,backoff:0,timeout:2000},fetch:async(url,options)=>{
   const rpc=options.method==="POST"&&options.body?JSON.parse(options.body):undefined;
   const response=await fetch(url,options);requests.push({method:rpc?.method??options.method,status:response.status,session:response.headers.get("mcp-session-id")});return response;
  }});
  if(auto){
   await rejected(remote.connect(),"UnsupportedProtocolVersion");
   const stats=await fetch(new URL("/stats",endpoint)).then(r=>r.json());assert.equal(stats.calls,0);assert.equal(stats.sessions,0);
   assert.ok(requests.every(r=>r.method==="server/discover"));
   console.log(`PASS stock SDK ${host} Auto ${preference}: explicit version rejection, no legacy initialization, renewal or tool replay`);continue;
  }
  await remote.connect();const b=await remote.freshTool("echo");
  assert.equal((await b.call({message:"before"})).value.raw.structuredContent.message,"before");
  const closed=await fetch(new URL("/expire",endpoint),{method:"POST"}).then(r=>r.json());assert.equal(closed.closed,1);
  await rejected(b.call({message:"expired"}),"SessionExpired","unknown");
  const fresh=await remote.freshTool("echo");await rejected(b.call({message:"never-old"}),"BindingStale");
  assert.equal((await fresh.call({message:"after"})).value.raw.structuredContent.message,"after");
  const stats=()=>fetch(new URL("/stats",endpoint)).then(r=>r.json());
  assert.equal((await stats()).calls,2);assert.equal((await stats()).sessions,1);
  assert.equal(requests.filter(r=>r.method==="initialize").length,2);assert.equal(requests.filter(r=>r.method==="tools/call").length,3);
  assert.equal(requests.filter(r=>r.method==="tools/call"&&r.status===404).length,1);
  const sessions=requests.filter(r=>r.method==="initialize").map(r=>r.session);assert.equal(new Set(sessions).size,2);assert.ok(sessions.every(Boolean));
  await remote.close();await until(stats,s=>s.sessions===0,"SDK final session cleanup");
  assert.equal(requests.filter(r=>r.method==="DELETE").length,2);
  b.dispose();fresh.dispose();console.log(`PASS stock SDK ${host} ${auto?"Auto":"explicit"} ${preference}: actual terminated session, one original failed POST, two business effects, new session and empty manager`);
 }finally{
  try{await remote?.close();}finally{if(raw){if(host==="wasm")raw.free();else raw[Symbol.dispose]();}daemon.stdin.end("stop\n");const code=await exit;clearTimeout(timer);assert.equal(code,0,stderr);}
 }
}
