import assert from "node:assert/strict";
import {readFile,mkdtemp,rm} from "node:fs/promises";
import {createSecureServer} from "node:http2";
import {execFile} from "node:child_process";
import {promisify} from "node:util";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {chromium} from "playwright";
import init,{Runtime} from "../dist/web/mcpx_web.js";
import {engine} from "../dist/component/mcpx_component.js";
import {componentRuntime} from "../web/component.js";
import {Remote} from "../web/client.js";
import {responseRecoveryPolicy} from "../web/responses.js";
import {responseRecoveryFixture} from "./response-recovery-fixture.mjs";
import {runResponseRecovery,runResponseRecoveryGc} from "./response-recovery-host.mjs";

for(const policy of [{},[],{maxAttempts:0,backoff:0,timeout:1},{maxAttempts:9,backoff:0,timeout:1},{maxAttempts:1,backoff:60001,timeout:1},{maxAttempts:1,backoff:0,timeout:600001},{maxAttempts:1,backoff:0,timeout:1,other:1}])assert.throws(()=>responseRecoveryPolicy(policy));
for(const policy of [{maxAttempts:1,backoff:0,timeout:1},{maxAttempts:8,backoff:60000,timeout:600000}])assert.deepEqual(responseRecoveryPolicy(policy),policy);
assert.equal(typeof globalThis.gc,"function","Run with --expose-gc");
const fixture=responseRecoveryFixture();let secure,browser,directory;
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));const origin=`http://127.0.0.1:${fixture.server.address().port}`;
try {
  await init({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
  for(const host of ["wasm","jco"])for(const auto of [false,true]) {
    const open=async(endpoint,options)=>{
      const legacy=new URL(endpoint).searchParams.get("legacy")!=="0",raw=host==="wasm"?(auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy)):(auto?engine.createAuto(endpoint):engine.create(endpoint,legacy));
      const remote=new Remote(host==="wasm"?raw:componentRuntime(raw),options),dispose=()=>host==="wasm"?raw.free():raw[Symbol.dispose]();
      try{await remote.connect();return {remote,dispose};}catch(error){await remote.close();dispose();throw error;}
    };
    const count=await runResponseRecovery(open,`${origin}/mcp?case=${host}-${auto}`);
    await runResponseRecoveryGc(open,`${origin}/mcp?case=gc-${host}-${auto}`,globalThis.gc);
    fixture.check();console.log(`PASS ${host} ${auto?"Auto":"explicit"}: ${count} response recovery cases, retained promises release owner and credentials`);
  }
  if(process.argv.includes("--node-only")) {fixture.check();assert.equal(fixture.streams.size,0);process.exitCode=0;}
  else {
    directory=await mkdtemp(join(tmpdir(),"mcpx-response-"));const key=join(directory,"key.pem"),cert=join(directory,"cert.pem");
    await promisify(execFile)("openssl",["req","-x509","-newkey","rsa:2048","-nodes","-keyout",key,"-out",cert,"-days","1","-subj","/CN=localhost"]);
    const tls={key:await readFile(key),cert:await readFile(cert),allowHTTP1:true};secure=responseRecoveryFixture(handler=>createSecureServer(tls,handler));
    await new Promise(resolve=>secure.server.listen(0,"127.0.0.1",resolve));const endpoint=`https://127.0.0.1:${secure.server.address().port}/mcp`;
    browser=await chromium.launch({headless:true,args:["--no-sandbox","--js-flags=--expose-gc"]});const page=await browser.newPage({ignoreHTTPSErrors:true});
    let workers=0,closed=0;page.on("worker",worker=>{workers++;worker.on("close",()=>closed++);});page.on("console",message=>{if(message.text().startsWith("PASS"))console.log(message.text());});
    await page.goto(origin.replace("127.0.0.1","localhost"));
    await page.evaluate(async endpoint=>{
      const {default:init,Runtime}=await import("/dist/web/mcpx_web.js"),{Remote}=await import("/web/client.js"),{WorkerRemote}=await import("/web/worker.js");await init();
      const {runResponseRecovery,runResponseRecoveryGc}=await import("/scripts/response-recovery-host.mjs");
      for(const worker of [false,true])for(const auto of [false,true]) {
        const open=async(endpoint,options)=>{
          const legacy=new URL(endpoint).searchParams.get("legacy")!=="0";
          if(worker){const remote=await WorkerRemote.connect(endpoint,{...options,protocol:auto?"auto":legacy?"legacy":"modern"});return {remote,dispose:()=>{}};}
          const raw=auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy),remote=new Remote(raw,options);
          try{await remote.connect();return {remote,dispose:()=>raw.free()};}catch(error){await remote.close();raw.free();throw error;}
        };
        const count=await runResponseRecovery(open,`${endpoint}?case=browser-${worker}-${auto}`);
        await runResponseRecoveryGc(open,`${endpoint}?case=gc-browser-${worker}-${auto}`,globalThis.gc);
        console.log(`PASS ${worker?"WorkerRemote":"Chromium Remote"} ${auto?"Auto":"explicit"}: ${count} response recovery cases and GC`);
      }
      const url=new URL(endpoint);url.searchParams.set("case","worker-response-ack");url.searchParams.set("hang","1");
      const remote=await WorkerRemote.connect(String(url),{protocol:"legacy",token:"response-first",responseRecovery:{maxAttempts:2,backoff:0,timeout:3000},timeout:5000,maxInFlight:1,eventCapacity:6,eventBytes:65536});
      const post=remote.post.bind(remote),acks=[];let call;
      const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
      async function until(read,test){for(let n=0;n<1000;n++){const state=await read();if(test(state))return state;await delay(5);}throw new Error("Response ACK wait");}
      const stats=()=>{const value=new URL(url);value.pathname="/stats";return fetch(value).then(r=>r.json());};
      try{
        const binding=await remote.freshTool("echo");call=binding.call({message:"ACK stalled GET"});call.catch(()=>{});
        await until(stats,s=>s.gets.length===1);await delay(30);const baseline=(await remote.diagnostics()).eventWaits;
        remote.post=(message,transfer)=>message.kind==="ack"?acks.push(message):post(message,transfer);
        const emit=new URL(url);emit.pathname="/emit";await (await fetch(emit)).arrayBuffer();
        await until(()=>remote.diagnostics(),s=>s.eventWaits>baseline);
        call.cancel();try{await call;throw new Error("ACK-blocked call succeeded");}catch(error){if(error.code!=="Cancelled")throw error;}
        const final=await until(stats,s=>s.closed===s.opened);if(final.calls.length!==1||final.gets.length!==1||final.cancelled.length!==1)throw new Error("ACK cancellation replayed/missed original RPC");
        if(remote.io.records.size!==0)throw new Error("ACK cancellation left host I/O alive");
      }finally{call?.cancel();remote.post=post;acks.forEach(message=>post(message));await remote.close();}
      console.log("PASS Worker response GET cancellation drains I/O while event ACKs are paused");
    },endpoint);
    for(let n=0;closed<workers&&n<100;n++)await new Promise(resolve=>setTimeout(resolve,10));assert.equal(closed,workers);assert.ok(secure.preflights>0);secure.check();assert.equal(secure.streams.size,0);
    console.log(`PASS ${workers} response recovery Workers exited; HTTP/2 16-way mixed calls, cross-origin UTF-8 cursors, original RPCs and no POST replay`);
  }
}finally{await browser?.close();await secure?.close();await fixture.close();if(directory)await rm(directory,{recursive:true,force:true});}
