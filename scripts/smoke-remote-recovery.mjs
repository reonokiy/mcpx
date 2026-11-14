import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {chromium} from "playwright";
import init,{Runtime} from "../dist/web/mcpx_web.js";
import {engine} from "../dist/component/mcpx_component.js";
import {componentRuntime} from "../web/component.js";
import {Remote} from "../web/client.js";
import {notificationRecoveryPolicy} from "../web/notifications.js";
import {notificationFixture} from "./remote-notification-fixture.mjs";
import {runRemoteRecovery,runRecoveryGc,runModernRecoveryDenial} from "./remote-recovery-host.mjs";

for(const policy of [{},[],{maxAttempts:0,backoff:0,timeout:1},{maxAttempts:9,backoff:0,timeout:1},{maxAttempts:1,backoff:60001,timeout:1},{maxAttempts:1,backoff:0,timeout:600001},{maxAttempts:1,backoff:0,timeout:1,other:1}])assert.throws(()=>notificationRecoveryPolicy(policy));
const fixture=notificationFixture();let browser;
assert.equal(typeof globalThis.gc,"function","Run with node --expose-gc");
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`;
try {
  await init({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
  for(const host of ["wasm","jco"])for(const auto of [false,true]) {
    const open=async(endpoint,options)=>{
      const legacy=new URL(endpoint).searchParams.get("legacy")==="1";
      const raw=host==="wasm"?(auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy)):(auto?engine.createAuto(endpoint):engine.create(endpoint,legacy));
      const remote=new Remote(host==="wasm"?raw:componentRuntime(raw),options);
      const dispose=()=>host==="wasm"?raw.free():raw[Symbol.dispose]();
      try{await remote.connect({timeout:10000});return {remote,dispose};}catch(error){await remote.close();dispose();throw error;}
    };
    const count=await runRemoteRecovery(open,`${origin}/mcp?legacy=1&case=${host}-${auto}`);
    await runRecoveryGc(open,`${origin}/mcp?legacy=1&recovery=1&case=gc-${host}-${auto}`,globalThis.gc);
    await runModernRecoveryDenial(open,`${origin}/mcp?case=modern-${host}-${auto}`);
    console.log(`PASS ${host} ${auto?"Auto":"legacy"}: ${count} high-level recovery scenarios`);
  }
  browser=await chromium.launch({headless:true,args:["--no-sandbox","--js-flags=--expose-gc"]});const page=await browser.newPage();
  let workers=0,closed=0;page.on("worker",worker=>{workers++;worker.on("close",()=>closed++);});
  await page.goto(origin.replace("127.0.0.1","localhost"));
  page.on("console",message=>{if(message.text().startsWith("PASS"))console.log(message.text());});
  await page.evaluate(async origin=>{
    const {Remote}=await import("/web/client.js"),{WorkerRemote}=await import("/web/worker.js");
    const {default:init,Runtime}=await import("/dist/web/mcpx_web.js");await init();
    const {runRemoteRecovery,runRecoveryGc,runModernRecoveryDenial}=await import("/scripts/remote-recovery-host.mjs");
    for(const worker of [false,true])for(const auto of [false,true]) {
      const open=async(endpoint,options)=>{
        const legacy=new URL(endpoint).searchParams.get("legacy")==="1";
        if(worker){const remote=await WorkerRemote.connect(endpoint,{...options,protocol:auto?"auto":legacy?"legacy":"modern"});return {remote,dispose:()=>{}};}
        const raw=auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy),remote=new Remote(raw,options);
        try{await remote.connect();return {remote,dispose:()=>raw.free()};}catch(error){await remote.close();raw.free();throw error;}
      };
      const count=await runRemoteRecovery(open,`${origin}/mcp?legacy=1&case=browser-${worker}-${auto}`,{concurrency:4});
      await runRecoveryGc(open,`${origin}/mcp?legacy=1&recovery=1&case=gc-browser-${worker}-${auto}`,globalThis.gc);
      await runModernRecoveryDenial(open,`${origin}/mcp?case=modern-browser-${worker}-${auto}`);
      console.log(`PASS ${worker?"WorkerRemote":"Chromium Remote"} ${auto?"Auto":"legacy"}: ${count} high-level recovery scenarios`);
    }
    const endpoint=`${origin}/mcp?legacy=1&recovery=1&case=worker-recovery-ack`;
    const gated=await WorkerRemote.connect(endpoint,{protocol:"legacy",httpNotifications:true,token:"notify-snapshot",timeout:5000,maxInFlight:1,eventCapacity:6,eventBytes:65536});
    const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
    const originalPost=gated.post.bind(gated),acks=[],pending=[];
    async function until(read,predicate){for(let n=0;n<500;n++){const value=await read();if(predicate(value))return value;await delay(5);}throw new Error("Worker recovery ACK timeout");}
    try {
      const first=await until(()=>gated.notificationStatus(),s=>s?.phase==="listening");
      await fetch(endpoint.replace("/mcp?","/emit?")+"&kind=end");await until(()=>gated.notificationStatus(),s=>s?.phase==="failed");
      await delay(20);const baseline=(await gated.diagnostics()).eventWaits;
      gated.post=(message,transfer)=>message.kind==="ack"?acks.push(message):originalPost(message,transfer);
      for(let n=0;n<5;n++){const call=gated.executeLine(`echo --message gated-${n}`);call.catch(()=>{});pending.push(call);}
      await until(()=>gated.diagnostics(),s=>s.eventWaits>baseline);
      const opening=await gated.resumeNotifications(first.resource_id,{timeout:300});
      if(opening.resource_id===first.resource_id)throw new Error("Worker resume reused resource");
      await until(()=>gated.notificationStatus({timeout:300}),s=>s?.phase==="listening");
      if((await gated.stopNotifications({timeout:300})).phase!=="cancelled")throw new Error("Worker resume blocked cleanup");
    }finally {
      pending.forEach(call=>call.cancel());gated.post=originalPost;acks.forEach(message=>originalPost(message));
      await Promise.allSettled(pending);await gated.close();
    }
    console.log("PASS Worker resume/status/stop bypass paused event ACKs");
  },origin);
  for(let n=0;closed<workers&&n<100;n++)await new Promise(resolve=>setTimeout(resolve,10));
  assert.equal(closed,workers);fixture.check();assert.equal(fixture.streams.size,0);
  console.log(`PASS all ${workers} recovery Workers exited; UTF-8 cursors, frozen credentials, full slots, reply drain, budgets, no POST replay`);
  console.log("PASS 8 recovery GC scenarios retain success/error/cancel promises while releasing owners, providers, credentials and backoff timers");
}finally{await browser?.close();fixture.server.closeAllConnections();await new Promise(resolve=>fixture.server.close(resolve));}
