import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {reconnectFixture} from "./http-reconnect-fixture.mjs";
import {runHttpSession,runSessionGc} from "./http-session-host.mjs";
import init,{Runtime} from "../dist/web/mcpx_web.js";
import {Remote} from "../web/client.js";
import {componentRuntime} from "../web/component.js";
import {engine} from "../dist/component/mcpx_component.js";
import {chromium} from "playwright";
await init({module_or_path:await readFile(new URL("../dist/web/mcpx_web_bg.wasm",import.meta.url))});
const f=reconnectFixture();await new Promise(resolve=>f.server.listen(0,"127.0.0.1",resolve));const origin=`http://127.0.0.1:${f.server.address().port}`;let browser;
try {
 for(const host of ["wasm","jco"])for(const legacy of [true,false])for(const auto of [false,true]){
  const open=async(endpoint,options)=>{
   const raw=host==="wasm"?(auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy)):(auto?engine.createAuto(endpoint):engine.create(endpoint,legacy));
   const remote=new Remote(host==="wasm"?raw:componentRuntime(raw),options),dispose=()=>host==="wasm"?raw.free():raw[Symbol.dispose]();
   try{await remote.connect({signal:options.signal,timeout:options.timeout});return {remote,dispose};}catch(e){await remote.close();dispose();throw e;}
  };
  const n=await runHttpSession(open,`${origin}/mcp?legacy=${+legacy}&case=${host}-${legacy}-${auto}`,{legacy,auto});if(legacy)await runSessionGc(open,`${origin}/mcp?legacy=1&case=${host}-${auto}`,globalThis.gc);f.check();console.log(`PASS ${host} ${legacy?"legacy":"modern"} ${auto?"Auto":"explicit"}: ${n} session lifecycle scenarios`);
 }
 if(!process.argv.includes("--node-only")){
  browser=await chromium.launch({headless:true,args:["--no-sandbox","--js-flags=--expose-gc"]});const page=await browser.newPage();let workers=0,closed=0;
  page.on("worker",w=>{workers++;w.on("close",()=>closed++);});page.on("console",m=>{if(m.text().startsWith("PASS"))console.log(m.text());});await page.goto(origin.replace("127.0.0.1","localhost"));
  await page.evaluate(async endpoint=>{
   const {default:init,Runtime}=await import("/dist/web/mcpx_web.js"),{Remote}=await import("/web/client.js"),{WorkerRemote}=await import("/web/worker.js"),{runHttpSession,runSessionGc}=await import("/scripts/http-session-host.mjs");await init();
   for(const worker of [false,true])for(const legacy of [true,false])for(const auto of [false,true]){
    const open=async(url,options)=>{
     if(worker){const remote=await WorkerRemote.connect(url,{...options,protocol:auto?"auto":legacy?"legacy":"modern"});return {remote,dispose:()=>{}};}
     const raw=auto?Runtime.auto(url):new Runtime(url,legacy),remote=new Remote(raw,options);
     try{await remote.connect({signal:options.signal,timeout:options.timeout});return {remote,dispose:()=>raw.free()};}catch(e){await remote.close();raw.free();throw e;}
    };
    const n=await runHttpSession(open,`${endpoint}?legacy=${+legacy}&case=browser-${worker}-${legacy}-${auto}`,{legacy,auto,parallel:4});if(legacy)await runSessionGc(open,`${endpoint}?legacy=1&case=gc-${worker}-${auto}`,globalThis.gc);console.log(`PASS ${worker?"Worker":"Chromium"} ${legacy?"legacy":"modern"} ${auto?"Auto":"explicit"}: ${n} session lifecycle scenarios`);
   }
   const url=`${endpoint}?legacy=1&case=worker-late-binding`,r=await WorkerRemote.connect(url,{protocol:"legacy",token:"reconnect-first",httpSessionRecovery:{maxAttempts:2,backoff:0,timeout:1500}});
   const receive=r.receive.bind(r);let held,late;
   try{
    const b=await r.freshTool("echo");r.receive=m=>{if(m.kind==="result"&&m.id===late?.operationId){held=m;return;}receive(m);};
    late=r.freshTool("echo");late.catch(()=>{});for(let n=0;!held&&n<200;n++)await new Promise(resolve=>setTimeout(resolve,5));if(!held)throw new Error("No held binding reply");
    const config=new URL(url);config.pathname="/config";config.searchParams.set("expire","1");await fetch(config);
    try{await b.call({message:"expire"});throw new Error("Expected expiry");}catch(e){if(e.code!=="SessionExpired")throw e;}
    const current=await r.freshTool("echo");if((await current.call({message:"after"})).value.raw.structuredContent.message!=="after")throw new Error("Fresh post-renewal call failed");
    r.receive=receive;receive(held);const old=await late,next=r.nextId;
    try{await old.call({message:"never"});throw new Error("Late old binding survived");}catch(e){if(e.code!=="BindingStale")throw e;}
    if(r.nextId!==next)throw new Error("Late binding crossed Worker boundary");await old.dispose();await current.dispose();await b.dispose();
    console.log("PASS Worker late binding reply retains its original epoch and rejects locally after automatic renewal");
   }finally{r.receive=receive;if(held&&r.pending.has(late?.operationId))receive(held);await r.close();}
  },`${origin}/mcp`);
  for(let n=0;closed<workers&&n<200;n++)await new Promise(r=>setTimeout(r,10));assert.equal(closed,workers);console.log(`PASS all ${workers} HTTP session Workers exited`);
 }
 f.check();assert.equal(f.streams.size,0);
}finally{await browser?.close();await f.close();}
