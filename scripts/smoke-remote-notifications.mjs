import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {chromium} from "playwright";
import init,{Runtime} from "../dist/web/mcpx_web.js";
import {engine} from "../dist/component/mcpx_component.js";
import {componentRuntime} from "../web/component.js";
import {Remote} from "../web/client.js";
import {notificationFixture} from "./remote-notification-fixture.mjs";
import {runRemoteNotifications,runNotificationFailure,runNotificationCleanup,runNotificationGc} from "./remote-notification-host.mjs";

const fixture=notificationFixture();let browser;
assert.equal(typeof globalThis.gc,"function","Run with node --expose-gc");
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`;
try {
  await init({module_or_path:await readFile("dist/web/mcpx_web_bg.wasm")});
  for(const host of ["wasm","jco"])for(const legacy of [false,true])for(const auto of [false,true]) {
    const open=async(endpoint,options)=>{
      const raw=host==="wasm"?(auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy)):(auto?engine.createAuto(endpoint):engine.create(endpoint,legacy));
      const remote=new Remote(host==="wasm"?raw:componentRuntime(raw),options);
      try{await remote.connect({timeout:10000});return {remote,dispose:()=>host==="wasm"?raw.free():raw[Symbol.dispose]()};}catch(error){await remote.close();host==="wasm"?raw.free():raw[Symbol.dispose]();throw error;}
    };
    await runRemoteNotifications(open,`${origin}/mcp?legacy=${+legacy}&case=${host}-${legacy}-${auto}`);
    if(legacy&&!auto)await runNotificationCleanup(open,`${origin}/mcp?legacy=1&holdreply=1&case=cleanup-${host}`);
    if(!auto)await runNotificationGc(open,`${origin}/mcp?legacy=${+legacy}&case=gc-${host}-${legacy}`,globalThis.gc);
    if(!legacy&&!auto)for(const [query,expected]of [["noack=1","Timeout"],["reject=1","CapabilityUnavailable"]])await runNotificationFailure(open,`${origin}/mcp?case=${host}-${query}&${query}`,expected);
    console.log(`PASS Remote HTTP notifications: ${host}, ${legacy?"legacy":"modern"}, ${auto?"Auto":"explicit"}`);
  }
  browser=await chromium.launch({headless:true,args:["--no-sandbox","--js-flags=--expose-gc"]});const page=await browser.newPage();
  let workers=0,closed=0;page.on("worker",worker=>{workers++;worker.on("close",()=>closed++);});
  await page.goto(origin.replace("127.0.0.1","localhost"));
  page.on("console",message=>{if(message.text().startsWith("PASS"))console.log(message.text());});
  await page.evaluate(async origin=>{
    const {Remote}=await import("/web/client.js"),{WorkerRemote}=await import("/web/worker.js");
    const {default:init,Runtime}=await import("/dist/web/mcpx_web.js");await init();
    const {runRemoteNotifications,runNotificationFailure,runNotificationCleanup,runNotificationGc}=await import("/scripts/remote-notification-host.mjs");
    for(const worker of [false,true])for(const legacy of [false,true])for(const auto of [false,true]) {
      const open=async(endpoint,options)=>{
        if(worker){
          const remote=new WorkerRemote({...options,protocol:auto?"auto":legacy?"legacy":"modern"});
          try{await remote.connect(endpoint,{timeout:10000});return {remote,dispose:()=>{}};}catch(error){await remote.close();throw error;}
        }
        const raw=auto?Runtime.auto(endpoint):new Runtime(endpoint,legacy),remote=new Remote(raw,options);
        try{await remote.connect({timeout:10000});return {remote,dispose:()=>raw.free()};}catch(error){await remote.close();raw.free();throw error;}
      };
      await runRemoteNotifications(open,`${origin}/mcp?legacy=${+legacy}&case=browser-${worker}-${legacy}-${auto}`,{concurrency:4,flood:worker});
      if(legacy&&!auto)await runNotificationCleanup(open,`${origin}/mcp?legacy=1&holdreply=1&case=cleanup-browser-${worker}`);
      if(!auto)await runNotificationGc(open,`${origin}/mcp?legacy=${+legacy}&case=gc-browser-${worker}-${legacy}`,globalThis.gc);
      if(!legacy&&!auto)for(const [query,expected]of [["noack=1","Timeout"],["reject=1","CapabilityUnavailable"]])await runNotificationFailure(open,`${origin}/mcp?case=browser-${worker}-${query}&${query}`,expected);
      console.log(`PASS ${worker?"WorkerRemote":"Chromium Remote"} notifications: ${legacy?"legacy":"modern"}, ${auto?"Auto":"explicit"}`);
    }
    const gated=await WorkerRemote.connect(`${origin}/mcp?case=worker-ack-gate`,{httpNotifications:true,token:"notify-snapshot",timeout:5000,maxInFlight:1,eventCapacity:6,eventBytes:65536});
    const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
    const originalPost=gated.post.bind(gated),acks=[],pending=[];
    try {
      for(let n=0;(await gated.notificationStatus())?.phase!=="listening";n++){if(n>500)throw new Error("ACK test stream not listening");await pause(5);}
      await pause(20);const baseline=(await gated.diagnostics()).eventWaits;
      gated.post=(message,transfer)=>message.kind==="ack"?acks.push(message):originalPost(message,transfer);
      for(let n=0;n<5;n++){const call=gated.executeLine(`echo --message gated-${n}`);call.catch(()=>{});pending.push(call);}
      for(let n=0;(await gated.diagnostics()).eventWaits<=baseline;n++){if(n>500)throw new Error("Event ACK gate did not pause");await pause(5);}
      const watcher=gated.subscribeCatalog(),change=watcher.next();
      const stopped=await gated.stopNotifications({timeout:300});
      if(stopped.phase!=="cancelled"||(await gated.notificationStatus({timeout:300})).phase!=="cancelled")throw new Error("Cleanup waited for event ACK");
      if(!(await change).value.invalidated)throw new Error("Catalog mailbox waited for event ACK");
      await watcher.return();
    }finally {
      pending.forEach(call=>call.cancel());gated.post=originalPost;acks.forEach(message=>originalPost(message));
      await Promise.allSettled(pending);await gated.close();
    }
    console.log("PASS Worker notification stop/status and catalog invalidation bypass paused event ACKs");
  },origin);
  for(let n=0;closed<workers&&n<100;n++)await new Promise(resolve=>setTimeout(resolve,10));
  assert.equal(workers,10);assert.equal(closed,workers);
  fixture.check();assert.equal(fixture.streams.size,0);
  console.log("PASS 16 automatic HTTP scenarios, 8 isolated failures and 4 manual cleanup scenarios; credentials, reserved cleanup, delayed EOF replies, >9 MiB Worker streams, no replay");
  console.log("PASS 8 live notification GC scenarios: retained control promises and results release owner, provider and credential snapshots");
  console.log("PASS all 10 notification Workers exited");
}finally{await browser?.close();fixture.server.closeAllConnections();await new Promise(resolve=>fixture.server.close(resolve));}
