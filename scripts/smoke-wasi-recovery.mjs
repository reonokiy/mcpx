import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {notificationFixture} from "./remote-notification-fixture.mjs";
const fixture=notificationFixture();
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`;
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(predicate,label){for(let n=0;n<1600;n++){if(predicate())return;await delay(5);}throw new Error(`Fixture timeout: ${label}`);}
let guests=0;
async function scenario(protocol,mode) {
  const legacy=protocol.endsWith("legacy"),id=`wasi-recovery-${protocol}-${mode}`;
  const endpoint=new URL(`${origin}/mcp?case=${id}&legacy=${+legacy}&recovery=1`);
  if(["manual","client","provider","auto-provider","writer-failure","drain-window"].includes(mode))endpoint.searchParams.set("holdreply","1");
  if(["timeout","client-timeout","identity","drop"].includes(mode))endpoint.searchParams.set("retry","400");
  if(mode==="window")endpoint.searchParams.set("retry","300");
  if(mode==="server-retry")endpoint.searchParams.set("retry","80");
  if(mode==="automatic")endpoint.searchParams.set("flap","1");
  if(mode==="opening")endpoint.searchParams.set("resumehang","1");
  if(["401","404","405"].includes(mode))endpoint.searchParams.set("resumestatus",mode);
  const control=async(path,kind)=>{
    const url=new URL(endpoint);url.pathname=path;if(kind)url.searchParams.set("kind",kind);
    const response=await fetch(url);assert.equal(response.status,204);await response.arrayBuffer();
  };
  const record=()=>fixture.records.get(id);
  const stages=[];let stderr="",buffer="",failure,task=Promise.resolve(),failedAt;
  const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/notification_recovery.wasm",String(endpoint),protocol,mode],{stdio:["pipe","pipe","pipe"]});
  const timer=setTimeout(()=>{failure??=new Error("WASI recovery guest timed out");child.kill("SIGKILL");},20000);
  const stage=async message=>{
    stages.push(message);if(!message.stage)return;
    const name=message.stage;
    if(name==="exhausted"||name==="override") {
      const opens=name==="exhausted"?3:6;
      await until(()=>record()?.opens===opens&&record().closed===opens,name);await delay(80);assert.equal(record().opens,opens);
    }else if(name==="full") {
      await until(()=>record()?.held===16,"16 held calls");await control("/emit","request-end");
    }else if(name==="eof"||name==="short-eof") {
      failedAt=performance.now();
      if(mode==="broken")await control("/emit","broken");
      else {
        if(mode==="reset")await control("/emit","reset");
        await control("/emit",legacy&&endpoint.searchParams.get("holdreply")==="1"?"request-end":"end");
      }
      if(mode==="auto-provider"){await until(()=>record().replies===1,"automatic capability reply");await control("/release");}
    }else if(name==="resume-waiting") {
      if(endpoint.searchParams.get("holdreply")==="1") {
        await until(()=>record().replies===1,"accepted capability reply");
        assert.equal(record().opens,1,"Resumed before the old reply acknowledgment");
      }
      await control("/release");
      if(mode==="client")return;
    }else if(name==="resumed") {
      const gets=record().requests.filter(r=>r.method==="GET");assert.equal(gets.length,2);
      assert.equal(gets[1].lastEventId,"游标/1");assert.equal(gets[1].token,"Bearer notify-snapshot");assert.equal(gets[1].session,"notify-session");
      await control("/emit","request");await until(()=>record().replies===2,"capability reply on resumed stream");
      assert.deepEqual(record().replyTokens,["Bearer notify-snapshot","Bearer notify-snapshot"]);
      await control("/release");
    }else if(name==="drain-window") {
      await until(()=>record().replies===1,"draining reply");await delay(140);await control("/release");
    }else if(name==="healthy") {await delay(160);await control("/emit","end");}
    else throw new Error(`Unknown guest stage ${name}`);
    if(!child.killed)child.stdin.write("go\n");
  };
  child.stdin.on("error",()=>{});
  child.stdout.on("data",chunk=>{
    buffer+=chunk;let newline;
    while((newline=buffer.indexOf("\n"))>=0){const line=buffer.slice(0,newline);buffer=buffer.slice(newline+1);task=task.then(()=>stage(JSON.parse(line))).catch(error=>{failure??=error;child.kill("SIGKILL");});}
  });
  child.stderr.on("data",chunk=>stderr+=chunk);
  const code=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("exit",resolve);});clearTimeout(timer);await task;
  assert.ifError(failure);fixture.check();assert.equal(code,0,`${protocol} ${mode}, last stage ${JSON.stringify(stages.at(-1))}, HTTP ${JSON.stringify(record())}\n${stderr}`);assert.equal(buffer,"");
  const state=record();await until(()=>state.closed===state.opens,"all streams closed");
  const expected=mode==="policy-only"?0:!legacy?1:mode==="automatic"?6:mode==="healthy"?3:["window","drain-window","writer-failure","broken","reset","timeout","client-timeout","drop","owner-drop","disabled"].includes(mode)?1:2;
  assert.equal(state.opens,expected,`${mode} resumed count`);
  assert.equal(state.calls,mode==="manual"?16:mode==="automatic"?1:0,"Tool POST was replayed");
  if(mode==="server-retry")assert.ok(state.requests.filter(r=>r.method==="GET")[1].at-failedAt>=79,"Automatic recovery ignored server retry");
  assert.ok(state.requests.every(r=>r.method==="GET"||r.lastEventId===undefined),"Last-Event-ID leaked onto POST");
  if(!legacy)assert.ok(state.requests.every(r=>r.method!=="GET"));
  else {
    if(mode!=="identity")state.requests.filter(r=>r.method==="GET").slice(1).forEach((request,n)=>assert.equal(request.lastEventId,`游标/${n+1}`));
    if(mode==="identity") {
      const gets=state.requests.filter(r=>r.method==="GET");assert.equal(gets[1].lastEventId,undefined);assert.equal(gets[1].token,"Bearer notify-second");
      assert.equal(state.deleted.length,2);
    }else if(mode==="owner-drop")assert.equal(state.deleted.length,0,"Drop started a session DELETE");
    else {assert.equal(state.deleted.length,1);assert.equal(state.deleted[0].token,"Bearer notify-snapshot");}
    if(state.deleted.length)assert.equal(state.deleted[0].session,"notify-session");
  }
  guests++;console.log(`PASS WASI high-level notification recovery: ${protocol} ${mode}`);
}
try {
  const modes=["manual","client","client-timeout","timeout","provider","auto-provider","automatic","window","drain-window","healthy","opening","401","404","405","reset","broken","writer-failure","identity","drop","reactor","owner-drop","disabled","policy-only","server-retry"];
  for(const protocol of ["legacy","auto-legacy"])for(const mode of modes)if((!process.argv[2]||process.argv[2]===protocol)&&(!process.argv[3]||process.argv[3]===mode))await scenario(protocol,mode);
  for(const protocol of ["modern","auto-modern"])for(const mode of ["modern","client"])if((!process.argv[2]||process.argv[2]===protocol)&&(!process.argv[3]||process.argv[3]===mode))await scenario(protocol,mode);
  assert.ok(guests,"No matching recovery scenarios");
  assert.equal(fixture.streams.size,0);console.log(`PASS ${guests} Wasmtime recovery guests: original credentials/cursor, full slots, reply drain, cancel/deadlines, identity, automatic budgets, no tool replay`);
}finally{fixture.server.closeAllConnections();await new Promise(resolve=>fixture.server.close(resolve));}
