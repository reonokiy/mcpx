import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {notificationFixture} from "./remote-notification-fixture.mjs";

const fixture=notificationFixture();
await new Promise(resolve=>fixture.server.listen(0,"127.0.0.1",resolve));
const origin=`http://127.0.0.1:${fixture.server.address().port}`;
const wait=async predicate=>{for(let n=0;n<1000;n++){if(predicate())return;await new Promise(resolve=>setTimeout(resolve,5));}throw new Error("Fixture condition timed out");};
async function scenario(protocol,mode="normal") {
  const legacy=protocol.endsWith("legacy"),id=`wasi-${protocol}-${mode}`;
  const endpoint=new URL(`${origin}/mcp?case=${id}&legacy=${+legacy}&holdreply=1`);
  if(mode==="noack")endpoint.searchParams.set("noack","1");
  if(mode==="reject")endpoint.searchParams.set("reject","1");
  if(mode==="noheaders")endpoint.searchParams.set("noheaders","1");
  if(mode==="badmime")endpoint.searchParams.set("badmime","1");
  const control=async(path,kind)=>{
    const url=new URL(endpoint);url.pathname=path;if(kind)url.searchParams.set("kind",kind);
    const response=await fetch(url);assert.equal(response.status,204);await response.arrayBuffer();
  };
  const stages=[];let stderr="",buffer="",failure,task=Promise.resolve();
  const child=spawn(process.env.WASMTIME??"target/tools/wasmtime/wasmtime",["run","-S","http","target/wasm32-wasip2/debug/examples/notifications.wasm",String(endpoint),protocol,mode],{stdio:["ignore","pipe","pipe"]});
  const timer=setTimeout(()=>{failure??=new Error("WASI notification guest did not exit");child.kill("SIGKILL");},20000);
  const stage=async message=>{
    stages.push(message);
    if(message.stage==="idle")await control("/emit",mode==="flood"?"flood":"change");
    if(message.stage==="held") {await wait(()=>fixture.records.get(id).held===1);await control("/emit","change");}
    if(message.stage==="during-call"&&mode!=="business-timeout")await control("/release");
    if(message.stage==="timed-out")await control("/release");
    if(message.stage==="eof") {
      if(mode==="reader-failure") {
        await control("/emit","request");await wait(()=>fixture.records.get(id).replies===1);await control("/emit","broken");
      }else await control("/emit",legacy?"request-end":"end");
    }
    if(message.stage==="reply-business"&&legacy) {
      await wait(()=>fixture.records.get(id).replies===1);
      assert.equal(fixture.records.get(id).replyClosed,0,"Tool waited for the notification writer to finish");
      assert.deepEqual(fixture.records.get(id).replyTokens,["Bearer notify-snapshot"]);
      if(mode!=="writer-timeout")await control("/release");
    }
    if(message.stage==="reply-timeout")assert.equal(fixture.records.get(id).replyClosed,1,"Timed-out writer kept its response resource");
    if(message.stage==="bad")await control("/emit","bad");
    if(message.stage==="oversize")await control("/emit","oversize");
  };
  child.stdout.on("data",chunk=>{
    buffer+=chunk;let newline;
    while((newline=buffer.indexOf("\n"))>=0){const line=buffer.slice(0,newline);buffer=buffer.slice(newline+1);task=task.then(()=>stage(JSON.parse(line))).catch(error=>{failure??=error;child.kill("SIGKILL");});}
  });
  child.stderr.on("data",chunk=>stderr+=chunk);
  const code=await new Promise((resolve,reject)=>{child.once("error",reject);child.once("exit",resolve);});clearTimeout(timer);await task;
  assert.ifError(failure);fixture.check();assert.equal(code,0,`${protocol} ${mode}, last stage: ${JSON.stringify(stages.at(-1))}\n${stderr}`);assert.equal(buffer,"");
  const record=fixture.records.get(id);assert.ok(record);
  await wait(()=>record.closed===record.opens);
  const failed=["noack","noheaders","reject","badmime"].includes(mode),business=mode==="business-timeout",oversize=mode==="oversize",context=mode==="context";
  assert.equal(record.opens,failed||business||context?1:oversize?2:3);assert.equal(record.calls,context?0:failed||oversize?1:2,"Business call was replayed");
  assert.equal(stages.at(-1).result,failed?"failure-isolated":business?"business-timeout-isolated":oversize?"oversize-isolated":context?"context-rejected":"notification-flow");
  if(business)assert.equal(record.cancellations,legacy?1:0);
  if(legacy){assert.equal(record.deleted.length,1);assert.equal(record.deleted[0].session,"notify-session");assert.equal(record.deleted[0].token,"Bearer notify-snapshot");}
  console.log(`PASS standalone WASI notifications: ${protocol} ${mode}`);
}
try {
  for(const protocol of ["modern","legacy","auto-modern","auto-legacy"])await scenario(protocol);
  for(const protocol of ["modern","legacy"])for(const mode of ["manual","lifetime","flood"])await scenario(protocol,mode);
  for(const mode of ["noack","reject"])await scenario("modern",mode);
  for(const protocol of ["modern","legacy"])for(const mode of ["noheaders","badmime","business-timeout","oversize"])await scenario(protocol,mode);
  await scenario("legacy","reject");await scenario("legacy","writer-timeout");await scenario("legacy","reader-failure");
  for(const protocol of ["modern","legacy"])await scenario(protocol,"context");
  assert.equal(fixture.streams.size,0);
  console.log("PASS 25 Wasmtime guests: idle/in-call polling, frozen replies after EOF/reader failure, delayed/expired ACK isolation, restart/stop, auth context, exact IDs, deadlines, >9 MiB streams, frame budgets and resource cleanup");
}finally{fixture.server.closeAllConnections();await new Promise(resolve=>fixture.server.close(resolve));}
