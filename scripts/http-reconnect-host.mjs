const check=(ok,message)=>{if(!ok)throw new Error(message);};
export const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
export async function until(read,test,label){for(let n=0;n<1000;n++){const value=await read();if(test(value))return value;await delay(5);}throw new Error(`Reconnect wait: ${label}`);}
export async function rejected(promise,code,certainty="not_sent") {try{await promise;}catch(error){check((Array.isArray(code)?code:[code]).includes(error.code),`Expected ${code}, got ${error.code}: ${error.message}`);check(error.execution_certainty===certainty,`Expected ${certainty}, got ${error.execution_certainty}`);return error;}throw new Error(`Expected ${code}`);}
const message=result=>result.value.raw.structuredContent.message;
export async function runHttpReconnect(open,base,{auto=false}={}) {
  const legacy=new URL(base).searchParams.get("legacy")==="1";let count=0;
  async function scenario(name,options,initial,run) {
    const url=new URL(base);url.searchParams.set("case",`${url.searchParams.get("case")}-${name}`);const endpoint=String(url);
    const read=()=>{const u=new URL(url);u.pathname="/stats";return fetch(u).then(r=>r.json());};
    const control=async(path,settings={})=>{const u=new URL(url);u.pathname=path;for(const [k,v]of Object.entries(settings))u.searchParams.set(k,String(v));const response=await fetch(u);check(response.status===204,"Fixture control failed");await response.arrayBuffer();};
    await control("/config",initial);
    const events=[];
    const {remote,dispose}=await open(endpoint,{token:"reconnect-first",elicitation:{form:true},timeout:5000,onOperationEvent:event=>events.push(event),...options});
    try {await run(remote,{read,configure:s=>control("/config",s),release:role=>control("/release",role?{role}:{}),end:()=>control("/end"),events});count++;}
    catch(error){throw new Error(`${name}: ${error.message}`,{cause:error});}
    finally{await control("/config",{holdDelete:0,holdList:0,holdCalls:0});await control("/release");await remote.close();dispose();}
    const s=await until(read,s=>s.opened===s.closed,"closed streams");
    check(Object.keys(s.sessions).length===0,"Leaked observed session");
    const ids=s.requests.filter(r=>r.rpc!==undefined).map(r=>r.rpc);check(ids.length===new Set(ids).size,"RPC ID reused after reconnect");
    const terminal=events.filter(e=>["completed","failed","cancelled","needs_input"].includes(e.kind));
    for(const id of new Set(terminal.map(e=>e.queue_operation_id)))check(terminal.filter(e=>e.queue_operation_id===id).length===1,"Operation settled twice");
  }
  const registry=[{uri:"https://fixture.invalid/reconnect-word",schemaJson:'{"type":"string","minLength":1}'}];
  let token="reconnect-first",identity="same",resolutions=0;
  await scenario("same-and-new-identity",{token:undefined,schemaRegistry:registry,credentialProvider:async()=>{resolutions++;return {token,authContextId:identity};}},{ref:1},async(r,h)=>{
    const old=await r.freshTool("echo");check(message(await old.call({message:"one"}))==="one","Original call failed");const before=resolutions;
    token="reconnect-second";await r.reconnect();check(resolutions===before+1,"Reconnect queried provider more than once");
    check((await r.schemaRegistryStats()).resources===1,"Same identity registry lost");await rejected(old.call({message:"never"}),"BindingStale");
    const b=await r.freshTool("echo");check(message(await b.call({message:"two"}))==="two","New session call failed");
    identity="new-identity";await h.configure({ref:0,integer:1});await r.reconnect();check((await r.schemaRegistryStats()).resources===0,"Registry crossed authorization identity");
    check(message(await r.executeLine("echo --message 3"))===3,"Command used old input schema");const s=await h.read();check(s.calls.length===3,"Reconnect replayed a call");
    if(legacy)check(s.deleted[0].token==="Bearer reconnect-first"&&s.deleted[1].token==="Bearer reconnect-second","Wrong session cleanup credential");
    await old.dispose();await b.dispose();
  });
  await scenario("full-barrier",{queueCapacity:32},{holdCalls:1},async(r,h)=>{
    const b=await r.freshTool("echo"),calls=[];for(let n=0;n<16;n++){const call=b.call({message:`old-${n}`});call.catch(()=>{});calls.push(call);}
    await until(h.read,s=>s.calls.length===16,"16 calls");const reconnect=r.reconnect();reconnect.catch(()=>{});const next=r.executeLine("echo --message 17");next.catch(()=>{});const stale=b.call({message:"stale"});stale.catch(()=>{});
    await delay(30);const before=await h.read();check(before.deleted.length===0,"Reconnect overtook active calls");
    await h.configure({holdCalls:0,integer:1});await h.release("call");for(let n=0;n<16;n++)check(message(await calls[n])===`old-${n}`,"Lost original result");
    await reconnect;check(message(await next)===17,"Queued command did not rebind");await rejected(stale,"BindingStale");check((await h.read()).calls.length===17,"Queued stale call dispatched");await b.dispose();
  });
  for(const timeout of [false,true])await scenario(`queued-${timeout?"timeout":"cancel"}`,{},{holdCalls:1},async(r,h)=>{
    const b=await r.freshTool("echo"),call=b.call({message:"held"});call.catch(()=>{});await until(h.read,s=>s.calls.length===1,"held call");
    const reconnect=r.reconnect({timeout:timeout?80:5000});reconnect.catch(()=>{});if(!timeout)reconnect.cancel();await rejected(reconnect,timeout?"Timeout":"Cancelled");
    await h.configure({holdCalls:0});await h.release("call");await call;check(message(await b.call({message:"still-valid"}))==="still-valid","Queued cancellation reset bindings");
    check((await h.read()).deleted.length===0,"Queued cancellation sent DELETE");await b.dispose();
  });
  if(legacy) {
    await scenario("failed-cleanup",{},{},async(r,h)=>{const b=await r.freshTool("echo");await h.configure({failDelete:1});await rejected(r.reconnect(),"SessionCleanupFailed");await rejected(r.executeLine("echo --message blocked"),"TransportClosed");let s=await h.read();check(s.epoch===1&&s.calls.length===0,"Failed cleanup dispatched work");await r.reconnect();await rejected(b.call({message:"stale"}),"BindingStale");check(message(await r.executeLine("echo --message recovered"))==="recovered","Cleanup retry failed");s=await h.read();check(s.deleted.length===2&&s.deleted[0].session===s.deleted[1].session,"Cleanup did not retry original session");await b.dispose();});
    for(const timeout of [false,true])await scenario(`cleanup-${timeout?"timeout":"cancel"}`,{},{},async(r,h)=>{await h.configure({holdDelete:1});const reconnect=r.reconnect({timeout:timeout?100:5000});reconnect.catch(()=>{});await until(h.read,s=>s.held.includes("delete"),"cleanup pending");if(!timeout)reconnect.cancel();await rejected(reconnect,timeout?"Timeout":"Cancelled");await rejected(r.executeLine("echo --message blocked"),"TransportClosed");await h.configure({holdDelete:0});await r.reconnect();check(message(await r.executeLine("echo --message retry"))==="retry","Cancelled cleanup not retryable");});
  }
  for(const timeout of [false,true])await scenario(`metadata-${timeout?"timeout":"cancel"}`,{},{},async(r,h)=>{
    await h.configure({holdList:1});const reconnect=r.reconnect({timeout:timeout?120:5000});reconnect.catch(()=>{});await until(h.read,s=>s.held.includes("list"),"metadata pending");
    const waiting=r.executeLine("echo --message waits");waiting.catch(()=>{});
    if(!timeout)reconnect.cancel();await rejected(reconnect,timeout?"Timeout":"Cancelled");await rejected(waiting,"TransportClosed");
    await rejected(r.executeLine("echo --message blocked"),"TransportClosed");await h.configure({holdList:0});await r.reconnect();check(message(await r.executeLine("echo --message retry"))==="retry","Metadata retry failed");
  });
  await scenario("metadata-failure",{},{},async(r,h)=>{await h.configure({failList:1});await rejected(r.reconnect(),"HttpError");await rejected(r.freshTool("echo"),"TransportClosed");await r.reconnect();check(message(await r.executeLine("echo --message retry"))==="retry","Failed discovery not retryable");});
  let fail=false;
  await scenario("provider-failure",{token:undefined,credentialProvider:async()=>{if(fail)throw new Error("provider denied");return {token:"reconnect-first",authContextId:"same"};}},{},async(r,h)=>{fail=true;await rejected(r.reconnect(),"CredentialUnavailable");await rejected(r.executeLine("echo --message blocked"),"TransportClosed");fail=false;await r.reconnect();check(message(await r.executeLine("echo --message provider"))==="provider","Provider retry failed");check((await h.read()).calls.length===1,"Provider failure dispatched tools");});
  await scenario("notifications",{httpNotifications:true},{},async(r,h)=>{
    const first=await until(()=>r.notificationStatus(),s=>s?.phase==="listening","listener");let resume;
    if(legacy){await h.end();await until(()=>r.notificationStatus(),s=>s?.phase==="failed","EOF");resume=r.resumeNotifications(first.resource_id);resume.catch(()=>{});}
    await r.reconnect();if(resume)await rejected(resume,"Cancelled");const second=await until(()=>r.notificationStatus(),s=>s?.phase==="listening","new listener");check(first.resource_id!==second.resource_id,"Listener ID reused");
    if(legacy)await rejected(r.resumeNotifications(first.resource_id),"InvalidHandle");await r.stopNotifications();const count=(await h.read()).listens.length;await r.reconnect();await delay(20);check((await h.read()).listens.length===count,"Reconnect re-enabled stopped listener");
  });
  if(!legacy)await scenario("continuation",{},{},async(r,h)=>{const b=await r.freshTool("echo"),first=await b.call({message:"interactive"});check(first.kind==="needs_input","No continuation");await r.reconnect();await rejected(r.resume(first.value,{confirm:{action:"accept",content:{yes:true}}}),"InvalidContinuation");check((await h.read()).calls.length===1,"Old continuation replayed");await b.dispose();});
  await scenario("failed-call",{},{},async(r,h)=>{const b=await r.freshTool("echo");await h.configure({expire:1});await rejected(b.call({message:"uncertain"}),["HttpError","SessionExpired"],"unknown");await r.reconnect();check(message(await r.executeLine("echo --message explicit-new-call"))==="explicit-new-call","Explicit new call failed");check((await h.read()).calls.length===2,"Failed call replayed");await b.dispose();});
  if(auto)await scenario("protocol-switch",{},{},async(r,h)=>{await h.configure({modern:legacy?1:0});await r.reconnect();check(message(await r.executeLine("echo --message switched"))==="switched","Auto did not reprobe");await h.configure({modern:legacy?0:1});await r.reconnect();check(message(await r.executeLine("echo --message switched-back"))==="switched-back","Auto did not reprobe twice");});
  return count;
}

async function prepareGc(open,base) {
  const token={token:"reconnect-first",authContextId:"gc"},provider=async()=>token;
  const {remote,dispose}=await open(base,{credentialProvider:provider,timeout:5000,httpNotifications:true});
  const retained=[];const url=new URL(base);url.pathname="/config";
  try {
    retained.push(remote.reconnect());await retained.at(-1);
    url.searchParams.set("failList","1");await(await fetch(url)).arrayBuffer();retained.push(remote.reconnect());await rejected(retained.at(-1),"HttpError");
    retained.push(remote.reconnect());await retained.at(-1);await remote.close();
    return {closed:remote,retained,weak:[new WeakRef(remote),new WeakRef(provider),new WeakRef(token)]};
  }finally{await remote.close();dispose();}
}
export async function runReconnectGc(open,base,gc) {
  const state=await prepareGc(open,base);
  async function released(refs){for(let n=0;n<200;n++){await delay(5);gc();await delay(0);if(refs.every(r=>r.deref()===undefined))return;}throw new Error("Reconnect retained owner/provider/credential");}
  await released(state.weak.slice(1));state.closed=undefined;await released(state.weak);for(const result of state.retained)result.cancel();
}
