import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {responseRecoveryFixture} from "./response-recovery-fixture.mjs";
const fixture=responseRecoveryFixture();await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`,delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(test,label){for(let n=0;n<1600;n++){if(test())return;await delay(5);}throw new Error(`WASI response fixture wait: ${label}`);}
let guests=0;
async function scenario(protocol,mode) {
  const legacy=protocol.endsWith("legacy"),id=`wasi-response-${protocol}-${mode}`,url=new URL(`${origin}/mcp?legacy=${+legacy}&case=${id}`);
  const q=url.searchParams;
  if(["client","queue","sixteen","provider","client-known","known-cancel","known-close","known-drop","notifications","reply-window","writer-failure"].includes(mode)&&legacy)q.set("caps","1");
  if(["client","queue","sixteen","provider"].includes(mode))q.set("replydelay","30");
  if(mode==="sixteen"){q.set("hold","1");q.set("barrier","16");}
  if(mode==="provider")q.set("hold","1");
  if(mode==="flap")q.set("flap","1");
  if(mode==="window")q.set("retry","300");
  if(mode==="reply-window")q.set("replydelay","250");
  if(mode==="opening")q.set("opening","1");
  if(["reading","cancel","deadline","close","drop","client-deadline"].includes(mode))q.set("hang","1");
  if(mode==="cancel-backoff")q.set("retry","600");
  if(mode==="client-known"||mode.startsWith("known-"))q.set("finaldelay",mode==="known-close"||mode==="known-drop"?"2000":"250");
  if(["401","404","405","503"].includes(mode))q.set("status",mode);
  if(mode==="writer-failure")q.set("replystatus","503");
  if(mode==="no-cursor")q.set("cursor","none");
  if(mode==="reset")q.set("cursor","reset");
  if(mode==="bad")q.set("bad","1");
  if(mode==="json")q.set("json","1");
  if(mode==="raw")q.set("large","1");
  if(mode==="queue")q.set("padding","1");
  const record=()=>fixture.records.get(id),release=async()=>{const control=new URL(url);control.pathname="/release";await(await fetch(control)).arrayBuffer();};
  let buffer="",stderr="",failure,task=Promise.resolve();const stages=[];
  const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/response_recovery.wasm",String(url),protocol,mode],{stdio:["pipe","pipe","pipe"]});
  const timer=setTimeout(()=>{failure??=new Error(`WASI response guest timeout: ${protocol} ${mode}`);child.kill("SIGKILL");},20000);
  const stage=async message=>{
    const name=message.stage;stages.push(name);
    if(name==="full"){await until(()=>record()?.calls.length===16,"16 held POSTs");child.stdin.write("go\n");return;}
    if(name==="release-full"){await release();child.stdin.write("go\n");return;}
    if(name==="root-held"){await until(()=>record()?.calls.length===1,"provider root");child.stdin.write("go\n");return;}
    if(name==="provider-pending"){await release();child.stdin.write("go\n");return;}
    if(name==="second-held"){await until(()=>record()?.calls.length===2,"second credential POST");await release();child.stdin.write("go\n");return;}
    if(name==="controlled") {
      if(mode==="cancel-backoff")await until(()=>record()?.closed===1,"backoff EOF");
      else if(mode.startsWith("known-"))await until(()=>record()?.replies.length===2,"known result reply");
      else await until(()=>record()?.gets.length===1,"resumed GET");
      child.stdin.write("go\n");
    }
  };
  child.stdout.on("data",chunk=>{buffer+=chunk;for(;;){const n=buffer.indexOf("\n");if(n<0)break;const line=buffer.slice(0,n);buffer=buffer.slice(n+1);if(line.startsWith("{"))task=task.then(()=>stage(JSON.parse(line))).catch(error=>{failure??=error;child.kill("SIGKILL");});else if(line.startsWith("PASS"))stages.push(line);}});
  child.stderr.on("data",chunk=>stderr+=chunk);
  const status=await new Promise((resolve,reject)=>{child.once("exit",resolve);child.once("error",reject);});clearTimeout(timer);await task;
  assert.ifError(failure);assert.equal(status,0,`${protocol} ${mode}: ${stderr}`);assert.equal(stderr,"");assert.ok(stages.some(s=>s?.startsWith("PASS WASI response recovery")),JSON.stringify(stages));
  if(mode==="invalid"){assert.equal(record(),undefined);guests++;return;}
  await until(()=>record()?.opened===record()?.closed,"all HTTP streams closed");fixture.check();const r=record();
  const calls=mode==="sixteen"?16:mode==="provider"||mode==="reactor"?2:1;
  assert.equal(r.calls.length,calls);assert.equal(r.initialized,legacy?1:0);assert.equal(r.deleted,mode==="drop"||mode==="known-drop"?0:legacy?1:0);
  assert.equal(new Set(r.calls.map(c=>c.rpc.id)).size,calls,"Repeated business RPC");
  let gets=!legacy||["disabled","no-cursor","reset","bad","json","window","reply-window","writer-failure","cancel-backoff"].includes(mode)?0:mode==="flap"?2:calls;
  assert.equal(r.gets.length,gets,`${mode} GET count`);
  assert.ok(r.requests.every(request=>request.method==="GET"||request.cursor===undefined));
  for(const get of r.gets){const original=r.calls.find(c=>c.rpc.id===get.rpc);assert.equal(get.token,original.token);assert.equal(get.session,original.session);}
  if(mode==="provider"){assert.deepEqual(r.calls.map(c=>c.token),["Bearer response-first","Bearer response-second"]);assert.deepEqual(r.gets.map(c=>c.token),["Bearer response-first","Bearer response-second"]);}
  if(["client","queue","sixteen","provider"].includes(mode)&&legacy) {
    assert.equal(r.replies.length,calls*2);assert.equal(r.acks.length,calls*2);
    for(const get of r.gets){assert.ok(get.at>=r.acks.find(a=>a.rpc===get.rpc).at);assert.ok(r.replies.filter(p=>p.rpc===get.rpc).every(p=>p.token===get.token&&p.session===get.session));}
  }
  if(["cancel","cancel-backoff","deadline","close","flap","window","reply-window","opening","reading","client-deadline"].includes(mode)) {
    assert.equal(r.cancelled.length,1);assert.equal(r.cancelled[0].rpc,r.calls[0].rpc.id);assert.equal(r.cancelled[0].token,"Bearer response-first");
  }else assert.equal(r.cancelled.length,0);
  guests++;console.log(`PASS WASI high-level response recovery: ${protocol} ${mode}`);
}
try {
  const modes=["client","client-deadline","client-known","queue","sixteen","provider","reactor","flap","window","reply-window","opening","reading","401","404","405","503","writer-failure","disabled","no-cursor","reset","bad","json","raw","notifications","cancel","cancel-backoff","deadline","close","drop","known-cancel","known-close","known-drop","invalid"];
  for(const protocol of ["legacy","auto-legacy"])for(const mode of modes)await scenario(protocol,mode);
  for(const protocol of ["modern","auto-modern"])for(const mode of ["client","queue"])await scenario(protocol,mode);
  fixture.check();assert.equal(fixture.streams.size,0);console.log(`PASS ${guests} WASI high-level response recovery guests: one original POST, frozen credentials/session, typed/command 16-way barrier, reply cleanup, fixed windows, cancellation and owner release`);
}finally{await fixture.close();}
