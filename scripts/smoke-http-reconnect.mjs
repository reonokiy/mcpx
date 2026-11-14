import assert from "node:assert/strict";
import {readFile,mkdtemp,rm} from "node:fs/promises";
import {createSecureServer} from "node:http2";
import {execFile} from "node:child_process";
import {promisify} from "node:util";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {chromium} from "playwright";
import init,{Runtime} from "../dist/web/mcpx_web.js";
import {engine} from "../dist/component/mcpx_component.js";
import {componentRuntime} from "../web/component.js";
import {Remote} from "../web/client.js";
import {reconnectFixture} from "./http-reconnect-fixture.mjs";
import {runHttpReconnect,runReconnectGc} from "./http-reconnect-host.mjs";
assert.equal(typeof globalThis.gc,"function","Run with --expose-gc");
const fixture=reconnectFixture();await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));const origin=`http://127.0.0.1:${fixture.server.address().port}`;
let browser,secure,directory;
const edgesOnly=process.argv.includes("--edges-only");
try {
  await init({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
  for(const host of edgesOnly?[]:["wasm","jco"])for(const legacy of [false,true])for(const auto of [false,true]) {
    const open=async(endpoint,options)=>{
      const raw=host==="wasm"?(auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy)):(auto?engine.createAuto(endpoint):engine.create(endpoint,legacy));
      const remote=new Remote(host==="wasm"?raw:componentRuntime(raw),options),dispose=()=>host==="wasm"?raw.free():raw[Symbol.dispose]();
      try{await remote.connect();return {remote,dispose};}catch(error){await remote.close();dispose();throw error;}
    };
    const count=await runHttpReconnect(open,`${origin}/mcp?legacy=${+legacy}&case=${host}-${legacy}-${auto}`,{auto});
    await runReconnectGc(open,`${origin}/mcp?legacy=${+legacy}&case=gc-${host}-${legacy}-${auto}`,globalThis.gc);
    fixture.check();console.log(`PASS ${host} ${legacy?"legacy":"modern"} ${auto?"Auto":"explicit"}: ${count} HTTP reconnect scenarios and GC`);
  }
  if(!process.argv.includes("--node-only")) {
    directory=await mkdtemp(join(tmpdir(),"mcpx-reconnect-"));const key=join(directory,"key.pem"),cert=join(directory,"cert.pem");
    await promisify(execFile)("openssl",["req","-x509","-newkey","rsa:2048","-nodes","-keyout",key,"-out",cert,"-days","1","-subj","/CN=localhost"]);
    const tls={key:await readFile(key),cert:await readFile(cert),allowHTTP1:true};secure=reconnectFixture(handler=>createSecureServer(tls,handler));await new Promise(resolve=>secure.server.listen(0,"127.0.0.1",resolve));
    browser=await chromium.launch({headless:true,args:["--no-sandbox","--js-flags=--expose-gc"]});const page=await browser.newPage({ignoreHTTPSErrors:true});let workers=0,closed=0;
    page.on("worker",worker=>{workers++;worker.on("close",()=>closed++);});page.on("console",m=>{if(m.text().startsWith("PASS"))console.log(m.text());});await page.goto(origin.replace("127.0.0.1","localhost"));
    await page.evaluate(async({endpoint,edgesOnly})=>{
      const {default:init,Runtime}=await import("/dist/web/mcpx_web.js"),{Remote}=await import("/web/client.js"),{WorkerRemote}=await import("/web/worker.js"),{runHttpReconnect,runReconnectGc,until}=await import("/scripts/http-reconnect-host.mjs");await init();
      for(const worker of edgesOnly?[]:[false,true])for(const legacy of [false,true])for(const auto of [false,true]) {
        const open=async(endpoint,options)=>{
          if(worker){const remote=await WorkerRemote.connect(endpoint,{...options,protocol:auto?"auto":legacy?"legacy":"modern"});return {remote,dispose:()=>{}};}
          const raw=auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy),remote=new Remote(raw,options);
          try{await remote.connect();return {remote,dispose:()=>raw.free()};}catch(error){await remote.close();raw.free();throw error;}
        };
        const count=await runHttpReconnect(open,`${endpoint}?legacy=${+legacy}&case=browser-${worker}-${legacy}-${auto}`,{auto});
        await runReconnectGc(open,`${endpoint}?legacy=${+legacy}&case=gc-browser-${worker}-${legacy}-${auto}`,globalThis.gc);
        console.log(`PASS ${worker?"WorkerRemote":"Chromium Remote"} ${legacy?"legacy":"modern"} ${auto?"Auto":"explicit"}: ${count} HTTP reconnect scenarios and GC`);
      }
      const remote=await WorkerRemote.connect(`${endpoint}?legacy=1&case=worker-many-epochs`,{protocol:"legacy",token:"reconnect-first"});
      const old=[];
      try {
        for(let n=0;n<132;n++){old.push(await remote.freshTool("echo"));await remote.reconnect();if((await remote.diagnostics()).bindings!==0||remote.bindings.size!==0)throw new Error("Reconnect retained old binding records");}
        const b=await remote.freshTool("echo");const value=await b.call({message:"after-132"});if(value.value.raw.structuredContent.message!=="after-132")throw new Error("Binding budget not reusable");
      }finally{for(const b of old)await b.dispose();await remote.close();}
      console.log("PASS Worker 132 reconnect epochs release old binding capacity without losing IDs");
      const late=await WorkerRemote.connect(`${endpoint}?legacy=0&case=worker-late-continuation`,{protocol:"modern",token:"reconnect-first",elicitation:{form:true}});
      const receive=late.receive.bind(late);let held,call;
      try {
        const binding=await late.freshTool("echo");
        late.receive=message=>{if(message.kind==="result"&&message.id===call?.operationId){held=message;return;}receive(message);};
        call=binding.call({message:"interactive"});call.catch(()=>{});
        await until(()=>held,Boolean,"held old continuation");
        await late.reconnect();late.receive=receive;receive(held);
        const old=await call;if(old.kind!=="needs_input")throw new Error("Late interaction result lost");const nextId=late.nextId;
        try{await late.resume(old.value,{confirm:{action:"accept",content:{yes:true}}});throw new Error("Late old continuation survived reset");}catch(error){if(error.code!=="InvalidContinuation")throw error;}
        if(late.nextId!==nextId)throw new Error("Old continuation crossed worker boundary");
      }finally{late.receive=receive;if(held&&late.pending.has(call?.operationId))receive(held);await late.close();}
      console.log("PASS Worker preserves a late old result while rejecting its continuation locally after reset");
    },{endpoint:`https://127.0.0.1:${secure.server.address().port}/mcp`,edgesOnly});
    for(let n=0;closed<workers&&n<100;n++)await new Promise(resolve=>setTimeout(resolve,10));assert.equal(closed,workers);secure.check();assert.equal(secure.streams.size,0);console.log(`PASS ${workers} HTTP reconnect Workers exited`);
  }
  fixture.check();assert.equal(fixture.streams.size,0);
}finally{await browser?.close();await secure?.close();await fixture.close();if(directory)await rm(directory,{recursive:true,force:true});}
