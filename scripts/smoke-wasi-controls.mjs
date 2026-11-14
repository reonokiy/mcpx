import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {notificationFixture} from "./remote-notification-fixture.mjs";
const fixture=notificationFixture();
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`;
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(predicate){for(let n=0;n<1000;n++){if(predicate())return;await pause(5);}throw new Error("Control fixture did not reach its barrier");}
let count=0;
async function run(protocol,scenario) {
  const legacy=protocol.endsWith("legacy"),id=`controls-${protocol}-${scenario}`;
  const endpoint=new URL(`${origin}/mcp?case=${id}&legacy=${+legacy}`);
  if(["refresh-cancel","refresh-timeout"].includes(scenario))endpoint.searchParams.set("holdlistat","2");
  if(scenario==="refresh-failure")endpoint.searchParams.set("rejectlistat","2");
  if(scenario==="registry")endpoint.searchParams.set("registryref","1");
  let stdout="",stderr="",failure,task=Promise.resolve();const messages=[];
  const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/controls.wasm",String(endpoint),protocol,scenario],{stdio:["pipe","pipe","pipe"]});
  const timer=setTimeout(()=>{failure??=new Error("WASI control guest did not exit");child.kill("SIGKILL");},20000);
  const control=async()=>{const url=new URL(endpoint);url.pathname="/release";const response=await fetch(url);assert.equal(response.status,204);await response.arrayBuffer();};
  const stage=async message=>{
    messages.push(message);
    if(message.stage==="barrier") {
      await until(()=>fixture.records.get(id)?.held===1);
      await pause(30);
      assert.equal(fixture.records.get(id).calls,1);
      assert.equal(fixture.records.get(id).lists,1,"Local barrier performed metadata I/O");
      child.stdin.write("go\n");
    }
    if(message.stage==="metadata") {
      await until(()=>fixture.records.get(id)?.lists===2);
      child.stdin.write("go\n");
    }
    if(message.stage==="release"){await control();child.stdin.write("go\n");}
  };
  child.stdout.on("data",chunk=>{
    stdout+=chunk;let index;
    while((index=stdout.indexOf("\n"))>=0){const line=stdout.slice(0,index);stdout=stdout.slice(index+1);task=task.then(()=>stage(JSON.parse(line))).catch(error=>{failure??=error;child.kill("SIGKILL");});}
  });
  child.stderr.on("data",chunk=>stderr+=chunk);
  const code=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("exit",resolve);});clearTimeout(timer);await task;
  assert.ifError(failure);fixture.check();assert.equal(code,0,`${protocol}/${scenario} last=${JSON.stringify(messages.at(-1))}\n${stderr}`);assert.equal(stdout,"");assert.equal(messages.at(-1).result,scenario);
  const record=fixture.records.get(id);
  if(scenario==="local") {assert.equal(record,undefined,"Local controls performed network I/O");assert.equal(messages.at(-1).resolutions,0);}
  else {
    assert.ok(record);
    assert.equal(record.deleted.length,legacy&&scenario!=="drop"?(scenario==="identity"?2:1):0);
    assert.equal(record.opens,0);
    // Both post-registry refreshes observed the previous context at submission;
    // neither may reuse a publication across that invalidation. TTL=0 also
    // forces each subsequent refresh, giving 1 initial + 3 context + 2 TTL lists.
    const lists={barrier:1,cancel:1,timeout:1,drop:1,registry:1,coalesce:3,"refresh-cancel":3,"refresh-timeout":2,"refresh-failure":3,context:6,identity:4,release:1}[scenario];
    assert.equal(record.lists,lists,"Wrong refresh count (coalescing/context/freshness)");
    assert.equal(record.calls,["barrier","drop","release"].includes(scenario)?1:["cancel","timeout","registry"].includes(scenario)?2:0,"Unexpected business dispatch or replay");
    if(scenario==="refresh-cancel")assert.equal(record.listClosed,1);
    if(scenario==="identity") {
      assert.equal(record.requests.filter(r=>r.method==="tools/list"&&r.token==="Bearer notify-snapshot").length,2);
      assert.equal(record.requests.filter(r=>r.method==="tools/list"&&r.token==="Bearer notify-second").length,2);
      if(legacy)assert.deepEqual(record.deleted,[{token:"Bearer notify-snapshot",session:"notify-session"},{token:"Bearer notify-second",session:"second-session"}]);
    }
  }
  count++;console.log(`PASS WASI controls: ${protocol} ${scenario}`);
}
const filter=process.argv[2];
try {
  const scenarios=["local","barrier","cancel","timeout","drop","registry","coalesce","refresh-cancel","refresh-timeout","refresh-failure","context","identity"];
  for(const protocol of ["modern","legacy","auto-modern","auto-legacy"])for(const scenario of scenarios)if(!filter||filter===scenario)await run(protocol,scenario);
  for(const protocol of ["modern","auto-modern"])if(!filter||filter==="release")await run(protocol,"release");
  assert.equal(fixture.streams.size,0);assert.ok(count>0,"Unknown scenario filter");
  console.log(`PASS ${count} WASI control guests: FIFO barriers, local-only configuration, registry/cache budgets, cancellation/drop, independent refresh deadlines and context-aware coalescing`);
}finally{fixture.server.closeAllConnections();await new Promise(resolve=>fixture.server.close(resolve));}
