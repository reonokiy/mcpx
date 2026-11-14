import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {reconnectFixture} from "./http-reconnect-fixture.mjs";
const fixture=reconnectFixture();
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`;
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(predicate){for(let n=0;n<1500;n++){if(predicate())return;await pause(3);}throw new Error("Reconnect peer barrier stalled");}
let count=0;
async function run(protocol,mode){
  const legacy=protocol.endsWith("legacy"),id=`wasi-${protocol}-${mode}`;
  const endpoint=`${origin}/mcp?case=${id}&legacy=${+legacy}`;
  let stdout="",stderr="",failure,task=Promise.resolve();const messages=[],background=[];
  const control=async(path,settings={})=>{
    const u=new URL(endpoint);u.pathname=path;
    for(const [k,v]of Object.entries(settings))u.searchParams.set(k,v);
    const response=await fetch(u);assert.equal(response.status,204);await response.arrayBuffer();
  };
  const record=()=>fixture.records.get(id);
  const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/reconnect.wasm",endpoint,protocol,mode],{stdio:["pipe","pipe","pipe"]});
  const timer=setTimeout(()=>{failure??=new Error(`WASI reconnect ${protocol} ${mode} timed out`);child.kill("SIGKILL");},20000);
  let configuredRequests,initialListeners;
  const stage=async message=>{
    messages.push(message);
    if(!message.stage)return;
    if(message.stage==="configure"){
      configuredRequests=record().requests.length;initialListeners=record().listens.length;
      if(mode==="schema")await control("/config",{ref:1});
      if(mode==="listener-resume")await control("/end");
      if(["barrier","pending-cancel","pending-timeout"].includes(mode))await control("/config",{holdCalls:1});
      if(mode==="delete-failure")await control("/config",{failDelete:1});
      if(mode.startsWith("delete-")&&mode!=="delete-failure")await control("/config",{holdDelete:1});
      if(["metadata-failure","direct-failure","chain"].includes(mode))await control("/config",{failList:1});
      if(mode.startsWith("metadata-")&&mode!=="metadata-failure")await control("/config",{holdList:1});
      if(mode==="switch")await control("/config",{modern:+legacy});
      if(["root-deadline","direct-deadline"].includes(mode)){
        await control("/config",{holdDelete:1,holdList:1});
        background.push((async()=>{
          if(legacy){await until(()=>record().pending.some(p=>p.role==="delete"));await pause(200);await control("/release",{role:"delete"});}
          await until(()=>record().pending.some(p=>p.role==="list"));await pause(400);await control("/release",{role:"list"});
        })().catch(error=>{failure??=error;child.kill("SIGKILL");}));
      }
      child.stdin.write("go\n");return;
    }
    if(message.stage==="peers"){
      await until(()=>record().calls.length===(mode==="barrier"?16:1));
      child.stdin.write("go\n");return;
    }
    if(message.stage==="barrier"){
      await pause(40);
      assert.equal(record().deleted.length,0,"DELETE overtook active calls");
      assert.equal(record().requests.filter(r=>r.method==="tools/list").length,1,"discovery overtook active calls");
      await control("/config",{holdCalls:0,integer:1});await control("/release",{role:"call"});
      child.stdin.write("go\n");return;
    }
    if(message.stage==="release-peers"){
      assert.equal(record().deleted.length,0,"cancelled pending reconnect changed session");
      await control("/config",{holdCalls:0});await control("/release",{role:"call"});
      child.stdin.write("go\n");return;
    }
    if(message.stage==="delete"){
      await until(()=>record().pending.some(p=>p.role==="delete"));
      assert.equal(record().deleted.length,1);
      assert.ok(!record().requests.some(r=>r.token==="Bearer reconnect-second"));
      if(mode.endsWith("close"))await control("/config",{holdDelete:0});
    }
    if(message.stage==="provider"){
      assert.equal(record().requests.slice(configuredRequests).filter(r=>!["DELETE"].includes(r.method)).length,0,"metadata started before provider completed");
      if(legacy)assert.equal(record().deleted.length,1,"provider started before old DELETE");
    }
    if(message.stage==="metadata")await until(()=>record().pending.some(p=>p.role==="list"));
    if(message.stage==="retry"){
      assert.equal(record().calls.length,mode==="direct-failure"||mode==="direct-deadline"?1:0,"business request escaped suspended state");
      await control("/config",{holdDelete:0,holdList:0,failDelete:0,failList:0});
      await control("/release",{role:"delete"});await control("/release",{role:"list"});
    }
    child.stdin.write("go\n");
  };
  child.stdout.on("data",chunk=>{
    stdout+=chunk;let index;
    while((index=stdout.indexOf("\n"))>=0){const line=stdout.slice(0,index);stdout=stdout.slice(index+1);task=task.then(()=>stage(JSON.parse(line))).catch(error=>{failure??=error;child.kill("SIGKILL");});}
  });
  child.stderr.on("data",chunk=>stderr+=chunk);
  const code=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("exit",resolve);});clearTimeout(timer);await task;
  await Promise.all(background);
  assert.ifError(failure);fixture.check();assert.equal(code,0,`${protocol} ${mode}\n${stderr}`);assert.equal(stdout,"");assert.equal(messages.at(-1).result,mode);
  const r=record();await until(()=>r.opened===r.closed);
  if(!mode.endsWith("drop"))assert.equal(Object.keys(r.sessions).length,0,"explicit close leaked a session");
  assert.ok(!r.calls.some(c=>String(c.rpc.params.arguments.message).startsWith("never-")),"stale/suspended call was sent");
  const ids=r.requests.filter(r=>r.rpc!==undefined).map(r=>r.rpc);
  assert.equal(new Set(ids).size,ids.length,"reset reused an RPC identifier");
  if(mode==="rotate"||mode==="identity"){
    if(legacy)assert.equal(r.deleted[0].token,"Bearer reconnect-first");
    assert.ok(r.calls.every(c=>c.token==="Bearer reconnect-second"));
    assert.equal(r.listens.at(-1).token,"Bearer reconnect-second");
  }
  if(mode==="stopped"||mode==="stop-during")assert.equal(r.listens.length,initialListeners,"explicitly stopped listener restarted");
  if(mode==="barrier"){
    assert.equal(r.calls.length,18);assert.equal(r.calls[16].rpc.params.arguments.message,42);assert.equal(r.calls[17].rpc.params.arguments.message,43);
    const rpcIds=r.calls.map(c=>c.rpc.id);assert.equal(new Set(rpcIds).size,rpcIds.length,"reconnect reused a business request ID");
  }
  if(mode==="switch"){
    assert.ok(r.requests.some(r=>r.method==="server/discover"));
    assert.equal(r.requests.filter(r=>r.method==="initialize").length,1);
    assert.equal(r.legacy,!legacy);
  }
  count++;console.log(`PASS WASI reconnect: ${protocol} ${mode}`);
}
const modes=["basic","cold","chain","rotate","identity","schema","barrier","pending-cancel","pending-timeout","provider-error","provider-cancel","provider-timeout","provider-close","provider-drop","metadata-failure","metadata-cancel","metadata-timeout","metadata-close","metadata-drop","stopped","stop-during","zero-ttl","twice","reactor","reactor-drop","retained","root-deadline","direct-basic","direct-failure","direct-deadline"];
const filter=process.argv[2];
try{
  for(const protocol of ["modern","legacy","auto-modern","auto-legacy"]){
    const scenarios=[...modes,...(protocol.endsWith("legacy")?["delete-failure","delete-cancel","delete-timeout","delete-close","delete-drop","listener-resume"]:["resume","retained-resume"]),...(protocol.startsWith("auto")?["switch"]:[])];
    for(const mode of scenarios)if(!filter||filter===mode)await run(protocol,mode);
  }
  assert.ok(count>0,"Unknown scenario");assert.equal(fixture.streams.size,0);
  console.log(`PASS ${count} WASI reconnect guests: FIFO barriers, fresh discovery, credential ownership, suspension/retry, deadlines, cancellation, notifications and Reactor`);
}finally{await fixture.close();}
