import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {notificationFixture} from "./remote-notification-fixture.mjs";
const fixture=notificationFixture();
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`;
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(predicate){for(let n=0;n<1000;n++){if(predicate())return;await pause(5);}throw new Error("Credential fixture did not reach its barrier");}
let count=0;
async function run(protocol,scenario) {
  const legacy=protocol.endsWith("legacy"),id=`credential-${protocol}-${scenario}`;
  const endpoint=new URL(`${origin}/mcp?case=${id}&legacy=${+legacy}`);
  if(["auth-cancel","auth-timeout"].includes(scenario))endpoint.searchParams.set("holddelete","1");
  if(scenario==="auth-failure")endpoint.searchParams.set("rejectdelete","1");
  let stdout="",stderr="",failure,task=Promise.resolve();const messages=[];
  const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/credentials.wasm",String(endpoint),protocol,scenario],{stdio:["pipe","pipe","pipe"]});
  const timer=setTimeout(()=>{failure??=new Error("WASI credential guest did not exit");child.kill("SIGKILL");},20000);
  const control=async(path,kind)=>{const url=new URL(endpoint);url.pathname=path;if(kind)url.searchParams.set("kind",kind);const response=await fetch(url);assert.equal(response.status,204);await response.arrayBuffer();};
  const stage=async message=>{
    messages.push(message);
    if(message.stage==="peers"){await until(()=>fixture.records.get(id)?.held===2);child.stdin.write("go\n");}
    if(message.stage==="identity-waiting") {
      const record=fixture.records.get(id);assert.equal(record.deleted.length,0);assert.ok(!record.requests.some(r=>r.token==="Bearer notify-second"));
      await control("/emit","change");
    }
    if(message.stage==="old-stream")await control("/release");
    if(message.stage==="frozen-stream") {
      await control("/emit",legacy?"request":"change");
      if(legacy){await until(()=>fixture.records.get(id).replies===1);assert.deepEqual(fixture.records.get(id).replyTokens,["Bearer notify-snapshot"]);}
      child.stdin.write("go\n");
    }
    if(message.stage==="peer-active"){await until(()=>fixture.records.get(id)?.held===1);child.stdin.write("go\n");}
    if(message.stage==="provider-pending")await control("/release");
    if(message.stage==="auth-pending"){await until(()=>fixture.records.get(id)?.deleted.length===1);assert.equal(fixture.records.get(id).deleted[0].token,"Bearer notify-snapshot");child.stdin.write("go\n");}
    if(message.stage==="auth-failed") {
      await until(()=>fixture.records.get(id).deleteClosed>=1);
      assert.ok(!fixture.records.get(id).requests.some(r=>r.token==="Bearer notify-second"),"New identity ran before cleanup succeeded");
      child.stdin.write("go\n");
    }
  };
  child.stdout.on("data",chunk=>{
    stdout+=chunk;let index;
    while((index=stdout.indexOf("\n"))>=0){const line=stdout.slice(0,index);stdout=stdout.slice(index+1);task=task.then(()=>stage(JSON.parse(line))).catch(error=>{failure??=error;child.kill("SIGKILL");});}
  });
  child.stderr.on("data",chunk=>stderr+=chunk);
  const code=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("exit",resolve);});clearTimeout(timer);await task;
  assert.ifError(failure);fixture.check();assert.equal(code,0,stderr);assert.equal(stdout,"");assert.equal(messages.at(-1).result,scenario);
  const record=fixture.records.get(id);await until(()=>record.closed===record.opens);
  const second=["identity","binding"].includes(scenario)||scenario.startsWith("auth-");
  assert.equal(record.deleted.length,legacy?(scenario==="provider-drop"?0:scenario.startsWith("auth-")?3:second?2:1):0);
  if(legacy&&record.deleted.length){assert.deepEqual(record.deleted[0],{token:"Bearer notify-snapshot",session:"notify-session"});if(second)assert.deepEqual(record.deleted.at(-1),{token:"Bearer notify-second",session:"second-session"});}
  if(scenario==="rotation"){assert.equal(record.calls,1);assert.deepEqual(record.callTokens,["Bearer notify-second"]);assert.deepEqual(record.listenTokens,["Bearer notify-snapshot","Bearer notify-second"]);}
  if(scenario==="identity"){assert.equal(record.calls,3);assert.deepEqual(record.callTokens,["Bearer notify-snapshot","Bearer notify-snapshot","Bearer notify-second"]);assert.deepEqual(record.listenTokens,["Bearer notify-snapshot","Bearer notify-second"]);}
  if(scenario==="binding")assert.equal(record.calls,0);
  if(scenario==="resume")assert.equal(record.calls,1);
  if(["provider-cancel","provider-timeout"].includes(scenario))assert.equal(record.calls,2);
  if(["provider-error","provider-oversize"].includes(scenario)||scenario.startsWith("auth-"))assert.equal(record.calls,1);
  if(["provider-close","provider-drop","provider-unpolled"].includes(scenario))assert.equal(record.calls,0);
  count++;console.log(`PASS WASI credentials/runner: ${protocol} ${scenario}`);
}
const filter=process.argv[2];
const modes=["rotation","identity","binding","provider-cancel","provider-timeout","provider-error","provider-oversize","provider-close","provider-drop","provider-unpolled"];
try {
  for(const protocol of ["modern","legacy","auto-modern","auto-legacy"])for(const mode of modes)if(!filter||filter===mode)await run(protocol,mode);
  for(const protocol of ["modern","auto-modern"])if(!filter||filter==="resume")await run(protocol,"resume");
  for(const protocol of ["legacy","auto-legacy"])for(const mode of ["auth-cancel","auth-timeout","auth-failure"])if(!filter||filter===mode)await run(protocol,mode);
  assert.equal(fixture.streams.size,0);assert.ok(count>0,"Unknown scenario filter");
  console.log(`PASS ${count} WASI credential guests: pending providers, concurrent peers, exact-once resolution, frozen snapshots, identity barriers, retryable cleanup, redaction and local Future runner`);
}finally{fixture.server.closeAllConnections();await new Promise(resolve=>fixture.server.close(resolve));}
