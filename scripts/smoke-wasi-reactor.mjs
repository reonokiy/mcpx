import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {notificationFixture} from "./remote-notification-fixture.mjs";
const fixture=notificationFixture();
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`;
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(predicate){for(let n=0;n<1000;n++){if(predicate())return;await pause(5);}throw new Error("Reactor fixture did not reach its barrier");}
let count=0;
async function run(protocol,scenario) {
  const legacy=protocol.endsWith("legacy"),id=`reactor-${protocol}-${scenario}`;
  const endpoint=new URL(`${origin}/mcp?case=${id}&legacy=${+legacy}&reverse=1`);
  if(scenario==="cleanup")endpoint.searchParams.set("rejectdelete","1");
  let stdout="",stderr="",failure,task=Promise.resolve();const messages=[];
  const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/reactor.wasm",String(endpoint),protocol,scenario],{stdio:["pipe","pipe","pipe"]});
  const timer=setTimeout(()=>{failure??=new Error("WASI reactor guest did not exit");child.kill("SIGKILL");},20000);
  const control=async(path,kind)=>{const url=new URL(endpoint);url.pathname=path;if(kind)url.searchParams.set("kind",kind);const response=await fetch(url);assert.equal(response.status,204);await response.arrayBuffer();};
  const stage=async message=>{
    messages.push(message);
    if(message.stage==="barrier") {
      await until(()=>fixture.records.get(id)?.held===16);
      await pause(30);assert.equal(fixture.records.get(id).calls,16,"17th call crossed the 16-call barrier");
      await control("/release");
    }
    if(message.stage==="active") {
      await until(()=>fixture.records.get(id)?.held===(["drop","close"].includes(scenario)?1:2));
      child.stdin.write("go\n");
    }
    if(message.stage==="release") {
      if(legacy)await until(()=>fixture.records.get(id)?.cancellations===1);
      await control("/release");
    }
    if(message.stage==="listening")await control("/emit","change");
  };
  child.stdout.on("data",chunk=>{
    stdout+=chunk;let index;
    while((index=stdout.indexOf("\n"))>=0){const line=stdout.slice(0,index);stdout=stdout.slice(index+1);task=task.then(()=>stage(JSON.parse(line))).catch(error=>{failure??=error;child.kill("SIGKILL");});}
  });
  child.stderr.on("data",chunk=>stderr+=chunk);
  const code=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("exit",resolve);});clearTimeout(timer);await task;
  assert.ifError(failure);fixture.check();assert.equal(code,0,`${protocol}/${scenario} last=${JSON.stringify(messages.at(-1))} requests=${JSON.stringify(fixture.records.get(id)?.requests)}\n${stderr}`);assert.equal(stdout,"");assert.equal(messages.at(-1).result,scenario);
  const record=fixture.records.get(id);
  if(scenario==="idle") {assert.equal(record,undefined,"Idle future performed network I/O");assert.equal(messages.at(-1).resolutions,0);}
  else {
    assert.ok(record);await until(()=>record.closed===record.opens);
    assert.equal(record.deleted.length,legacy&&scenario!=="drop"?(scenario==="cleanup"?2:1):0);
    assert.equal(record.calls,{workflow:5,parallel:17,cancel:3,timeout:3,"root-error":2,drop:1,close:1,transfer:1,notifications:1,manual:1,"direct-zero":0,cleanup:0,resume:2}[scenario],"Unexpected dispatch or replay");
    assert.equal(record.lists,["workflow","notifications"].includes(scenario)?2:1);
    assert.equal(record.opens,["notifications","direct-zero"].includes(scenario)?1:0);
    assert.equal(record.cancellations,legacy&&["cancel","timeout","root-error","close"].includes(scenario)?1:0);
    if(legacy&&scenario!=="drop")assert.deepEqual(record.deleted,Array.from({length:scenario==="cleanup"?2:1},()=>({token:"Bearer notify-snapshot",session:"notify-session"})));
  }
  count++;console.log(`PASS WASI reactor: ${protocol} ${scenario}`);
}
const filter=process.argv[2];
try {
  const scenarios=["workflow","parallel","cancel","timeout","root-error","drop","close","transfer","notifications","idle","manual","direct-zero"];
  for(const protocol of ["modern","legacy","auto-modern","auto-legacy"])for(const scenario of scenarios)if(!filter||filter===scenario)await run(protocol,scenario);
  for(const protocol of ["modern","auto-modern"])if(!filter||filter==="resume")await run(protocol,"resume");
  for(const protocol of ["legacy","auto-legacy"])if(!filter||filter==="cleanup")await run(protocol,"cleanup");
  assert.equal(fixture.streams.size,0);assert.ok(count>0,"Unknown scenario filter");
  console.log(`PASS ${count} WASI reactor guests: dependent async submissions, weak handles, non-Send borrowing, 16-call barrier, callback reentry, independent cancellation, root failure, owner cleanup and notifications`);
}finally{fixture.server.closeAllConnections();await new Promise(resolve=>fixture.server.close(resolve));}
