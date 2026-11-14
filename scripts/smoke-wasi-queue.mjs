import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {notificationFixture} from "./remote-notification-fixture.mjs";
const fixture=notificationFixture();
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`;
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(predicate){for(let n=0;n<1000;n++){if(predicate())return;await pause(5);}throw new Error("Queue fixture did not reach its barrier");}
async function run(protocol,scenario) {
  const legacy=protocol.endsWith("legacy"),id=`queue-${protocol}-${scenario}`;
  const endpoint=new URL(`${origin}/mcp?case=${id}&legacy=${+legacy}&reverse=1`);
  if(scenario.startsWith("metadata"))endpoint.searchParams.set("holdlist","1");
  if(scenario==="resume")endpoint.searchParams.set("holdresume","1");
  let stdout="",stderr="",failure,task=Promise.resolve();const messages=[];
  const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/queue.wasm",String(endpoint),protocol,scenario],{stdio:["pipe","pipe","pipe"]});
  const timer=setTimeout(()=>{failure??=new Error("WASI queue guest did not exit");child.kill("SIGKILL");},20000);
  const control=async(path,kind)=>{const url=new URL(endpoint);url.pathname=path;if(kind)url.searchParams.set("kind",kind);const response=await fetch(url);assert.equal(response.status,204);await response.arrayBuffer();};
  const stage=async message=>{
    messages.push(message);
    if(["constructed","submitted"].includes(message.stage)){assert.equal(fixture.records.get(id),undefined,"Constructor/submission performed network I/O");child.stdin.write("go\n");}
    if(message.stage==="metadata-pending") {
      await until(()=>fixture.records.get(id)?.lists===1);
      if(scenario==="metadata")await control("/release");
      child.stdin.write("go\n");
    }
    if(message.stage==="metadata-failed")await until(()=>fixture.records.get(id)?.listClosed===1);
    if(message.stage==="barrier") {
      await until(()=>fixture.records.get(id)?.held===16);
      await pause(35);assert.equal(fixture.records.get(id).calls,16,"17th call crossed the 16-call barrier");
      await control("/emit","change");
    }
    if(message.stage==="changed")await control("/release");
    if(message.stage==="resume-barrier"){await until(()=>fixture.records.get(id)?.held===16);assert.equal(fixture.records.get(id).calls,32);await control("/release");}
    if(message.stage==="active"){await until(()=>fixture.records.get(id)?.held===2);child.stdin.write("go\n");}
  };
  child.stdout.on("data",chunk=>{
    stdout+=chunk;let index;
    while((index=stdout.indexOf("\n"))>=0){const line=stdout.slice(0,index);stdout=stdout.slice(index+1);task=task.then(()=>stage(JSON.parse(line))).catch(error=>{failure??=error;child.kill("SIGKILL");});}
  });
  child.stderr.on("data",chunk=>stderr+=chunk);
  const code=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("exit",resolve);});clearTimeout(timer);await task;
  assert.ifError(failure);assert.equal(code,0,stderr);assert.equal(stdout,"");fixture.check();
  const record=fixture.records.get(id);assert.ok(record);await until(()=>record.closed===record.opens);
  assert.equal(record.deleted.length,legacy&&scenario!=="drop"?1:0,"Queue did not close its legacy session exactly once");
  if(scenario==="parallel") {assert.equal(record.calls,17);assert.equal(record.opens,1);assert.equal(record.lists,2);}
  if(scenario==="isolation"){assert.equal(record.calls,5);assert.equal(record.cancellations,legacy?2:0);}
  if(scenario==="drop"){assert.equal(record.calls,3);assert.equal(record.cancellations,0);assert.equal(messages.at(-1).result,"dropped");}
  if(scenario.startsWith("metadata")){assert.equal(record.calls,0);assert.equal(record.lists,scenario==="metadata"?1:2);}
  if(scenario==="resume")assert.equal(record.calls,32);
  if(scenario==="budgets")assert.equal(record.calls,0);
  console.log(`PASS WASI queue: ${protocol} ${scenario}`);
}
try {
  for(const protocol of ["modern","legacy","auto-modern","auto-legacy"])for(const scenario of ["parallel","isolation","drop","metadata","metadata-cancel","metadata-timeout"])await run(protocol,scenario);
  for(const protocol of ["modern","auto-modern"])await run(protocol,"resume");
  for(const protocol of ["modern","legacy"])await run(protocol,"budgets");
  assert.equal(fixture.streams.size,0);
  console.log("PASS 28 Wasmtime queue guests: 16 mixed calls/resumes, queued 17th, async metadata, notification barrier, isolated cancellation, owner drop, input budgets and cross-owner handles");
}finally{fixture.server.closeAllConnections();await new Promise(resolve=>fixture.server.close(resolve));}
