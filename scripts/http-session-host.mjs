import {delay,until,rejected} from "./http-reconnect-host.mjs";
const check=(value,message)=>{if(!value)throw new Error(message);};
const message=value=>value.value.raw.structuredContent.message;
export async function runHttpSession(open,base,{legacy=true,auto=false,parallel=16}={}) {
  let count=0;
  const policy={maxAttempts:2,backoff:10,timeout:1500};
  async function scenario(name,options,initial,run) {
    const url=new URL(base);url.searchParams.set("case",`${url.searchParams.get("case")}-${name}`);
    const control=async(path,settings={})=>{const u=new URL(url);u.pathname=path;for(const [k,v]of Object.entries(settings))u.searchParams.set(k,String(v));const r=await fetch(u);check(r.status===204,"Fixture control");await r.arrayBuffer();};
    const read=()=>{const u=new URL(url);u.pathname="/stats";return fetch(u).then(r=>r.json());};
    await control("/config",initial);
    const events=[];
    let result;
    try {
      result=await open(String(url),{token:"reconnect-first",timeout:4000,maxInFlight:parallel,httpSessionRecovery:policy,onOperationEvent:e=>events.push(e),...options});
      await run(result.remote,{read,configure:s=>control("/config",s),release:role=>control("/release",role?{role}:{}),events});count++;
    }catch(error){throw new Error(`${name}: ${error.message}`,{cause:error});}
    finally {await control("/config",{holdDelete:0,holdList:0,holdCalls:0});await control("/release");if(result){await result.remote.close();result.dispose();}}
    const s=await until(read,s=>s.opened===s.closed,"all streams closed");
    check(Object.keys(s.sessions).length===0,"Retained session");
    for(const id of new Set(events.map(e=>e.queue_operation_id))) {
      const own=events.filter(e=>e.queue_operation_id===id);
      check(own.filter(e=>e.kind==="started").length<=1,"Repeated started event");
      check(own.filter(e=>["completed","failed","cancelled","needs_input"].includes(e.kind)).length===1,"Missing/duplicate terminal");
    }
  }
  await scenario("renew",{},{expire:1},async(r,h)=>{
    const b=await r.freshTool("echo");
    if(legacy)await h.configure({integer:1});
    await rejected(b.call({message:"expired"}),legacy?"SessionExpired":"HttpError","unknown");
    if(!legacy){await delay(30);check((await h.read()).epoch===0,"Modern 404 renewed");return;}
    check(message(await r.executeLine("echo --message 42"))===42,"Did not bind fresh schema");
    await rejected(b.call({message:"never"}),"BindingStale");
    const s=await h.read();check(s.epoch===2&&s.calls.length===2,"Replayed failed tool or repeated discovery");
    await b.dispose();
  });
  if(!legacy)return count;
  for(const phase of ["expireList","expireAck"])await scenario(`startup-${phase}`,{},{[phase]:1},async(r,h)=>{
    check((await h.read()).epoch===2,"Startup not renewed");
    check(message(await r.executeLine("echo --message initial"))==="initial","Startup new catalog failed");
    check((await h.read()).calls.length===1,"Startup sent business requests");
  });
  await scenario("default-off",{httpSessionRecovery:undefined},{expire:1},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"failed"}),"SessionExpired","unknown");await delay(30);check((await h.read()).epoch===1,"Default policy recovered");
    check(message(await b.call({message:"explicit-next"}))==="explicit-next","Default owner unexpectedly closed");await b.dispose();
  });
  await scenario("metadata",{catalogTtl:0},{},async(r,h)=>{
    await h.configure({expireList:1,integer:1});
    check(message(await r.executeLine("echo --message 7"))===7,"Unsent command did not refresh schema");check((await h.read()).calls.length===1,"Metadata retried tool");
  });
  await scenario("burst",{httpSessionRecovery:{...policy,maxAttempts:1}},{expireList:2},async()=>{throw new Error("Exhausted startup returned a Remote");}).then(()=>{throw new Error("Expected exhausted startup");},e=>{
    check(e.cause?.code==="SessionExpired"||e.cause?.code==="ReconnectExhausted",`Unexpected startup failure: ${e.cause?.code}`);count++;
  });
  await scenario("retained-budget",{httpSessionRecovery:{...policy,maxAttempts:1}},{},async(r,h)=>{
    const b=await r.freshTool("echo");await h.configure({expire:1});await rejected(b.call({message:"one"}),"SessionExpired","unknown");
    const fresh=await r.freshTool("echo");await h.configure({expire:1});await rejected(fresh.call({message:"two"}),"SessionExpired","unknown");
    await rejected(r.executeLine("echo --message blocked"),"TransportClosed");check((await h.read()).epoch===2,"Metadata replenished burst");
    await r.reconnect();check(message(await r.executeLine("echo --message retry"))==="retry","Explicit retry failed");await b.dispose();await fresh.dispose();
  });
  await scenario("stop-and-status",{httpNotifications:true},{expire:1,holdDelete:1},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"one"}),"SessionExpired","unknown");
    await until(h.read,s=>s.held.includes("delete"),"recovery DELETE");
    await r.stopNotifications();await r.notificationStatus();
    const cancelled=r.executeLine("echo --message cancel");cancelled.catch(()=>{});cancelled.cancel();await rejected(cancelled,"Cancelled");
    const timed=r.executeLine("echo --message timeout",{timeout:60});await rejected(timed,"Timeout");
    await h.configure({holdDelete:0});await h.release();check(message(await r.executeLine("echo --message ok"))==="ok","Waiting command failed");
    const s=await h.read();check(s.calls.length===2&&s.listens.length===1,"Stop was lost or cancelled work dispatched");await b.dispose();
  });
  await scenario("cleanup-failure",{},{expire:1,failDelete:1},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"one"}),"SessionExpired","unknown");
    await rejected(r.executeLine("echo --message blocked"),"TransportClosed");
    await r.reconnect();check(message(await r.executeLine("echo --message retry"))==="retry","Cleanup retry failed");
    const s=await h.read();check(s.deleted[0].session===s.deleted[1].session&&s.calls.length===2,"Cleanup lost original snapshot or replayed");await b.dispose();
  });
  for(const status of [401,503])await scenario(`no-renew-${status}`,{},{expire:1,expireStatus:status},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"failed"}),status===401?"AuthFailed":"HttpError","unknown");
    await delay(30);check((await h.read()).epoch===1,"Non-session failure renewed");await b.dispose();
  });
  await scenario("stateless",{},{stateless:1,expire:1},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"failed"}),"HttpError","unknown");await delay(30);check((await h.read()).epoch===1,"Stateless renewed");await b.dispose();
  });
  await scenario("peer-drain",{queueCapacity:64},{expire:1,holdExpired:1,holdCalls:1},async(r,h)=>{
    const b=await r.freshTool("echo"),calls=[];for(let n=0;n<parallel;n++){const p=b.call({message:`peer-${n}`});p.catch(()=>{});calls.push(p);}
    await until(h.read,s=>s.calls.length===parallel,"all active peers");
    const next=r.executeLine("echo --message 17");next.catch(()=>{});const stale=b.call({message:"never"});stale.catch(()=>{});
    await h.configure({integer:1});await h.release("expired");await delay(40);
    const before=await h.read();check(before.epoch===1&&before.deleted.length===0&&before.calls.length===parallel,"Recovery overtook peers");
    await h.configure({holdCalls:0});await h.release();
    const outcomes=await Promise.allSettled(calls);check(outcomes.filter(o=>o.status==="fulfilled").length===parallel-1,"Positive peer result lost");
    const failed=outcomes.find(o=>o.status==="rejected").reason;check(failed.code==="SessionExpired"&&failed.execution_certainty==="unknown","Original failed attempt changed");
    check(message(await next)===17,"Queued command failed to rebind");await rejected(stale,"BindingStale");check((await h.read()).calls.length===parallel+1,"Failed or stale peer replayed");await b.dispose();
  });
  await scenario("drain-window",{httpSessionRecovery:{...policy,timeout:120}},{expire:1,holdExpired:1,holdCalls:1},async(r,h)=>{
    const b=await r.freshTool("echo"),calls=[];for(let n=0;n<parallel;n++){const p=b.call({message:`held-${n}`});p.catch(()=>{});calls.push(p);}
    await until(h.read,s=>s.calls.length===parallel,"drain window peers");const next=r.executeLine("echo --message never");next.catch(()=>{});await h.release("expired");
    await rejected(next,"TransportClosed");const results=await Promise.allSettled(calls);check(results.every(o=>o.status==="rejected"),"Held peer unexpectedly succeeded");
    check(results.filter(o=>o.reason.code==="SessionExpired").length===1,"Lost original expiry");const s=await h.read();check(s.epoch===1&&s.calls.length===parallel,"Drain window reset or replayed");await b.dispose();
  });
  await scenario("notification-burst",{httpNotifications:true,httpSessionRecovery:{...policy,timeout:1000}},{expireListen:3},async(r,h)=>{
    await until(h.read,s=>s.requests.filter(r=>r.method==="GET").length===3,"notification renewal budget");
    await delay(30);await rejected(r.executeLine("echo --message blocked"),"TransportClosed");const s=await h.read();check(s.epoch===3&&s.calls.length===0,"Idle notification loop got more budget");
    await r.stopNotifications();await r.reconnect();check(message(await r.executeLine("echo --message retry"))==="retry","Explicit notification recovery failed");
  });
  await scenario("result-resets",{httpSessionRecovery:{...policy,maxAttempts:1}},{expire:1},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"first"}),"SessionExpired","unknown");check(message(await r.executeLine("echo --message healthy"))==="healthy","New result failed");
    const fresh=await r.freshTool("echo");await h.configure({expire:1});await rejected(fresh.call({message:"second"}),"SessionExpired","unknown");await r.freshTool("echo");check((await h.read()).epoch===3,"Real result failed to reset budget");await b.dispose();await fresh.dispose();
  });
  await scenario("idle-resets",{httpSessionRecovery:{maxAttempts:1,backoff:0,timeout:160}},{expire:1},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"first"}),"SessionExpired","unknown");const fresh=await r.freshTool("echo");await delay(200);
    await h.configure({expire:1});await rejected(fresh.call({message:"second"}),"SessionExpired","unknown");await r.freshTool("echo");check((await h.read()).epoch===3,"Healthy interval failed to reset budget");await b.dispose();await fresh.dispose();
  });
  await scenario("retry-backoff",{httpSessionRecovery:{maxAttempts:3,backoff:45,timeout:1500}},{expireList:2},async(r,h)=>{
    const s=await h.read();check(s.epoch===3&&s.calls.length===0,"Startup rounds not shared");const init=s.requests.filter(r=>r.method==="initialize");check(init[2].at-init[1].at>=35,"No recovery backoff");
  });
  let token="reconnect-first",resolutions=0;
  await scenario("frozen-provider",{token:undefined,credentialProvider:async()=>{resolutions++;return {token,authContextId:"same"};}},{expire:1,holdDelete:1},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"old"}),"SessionExpired","unknown");await until(h.read,s=>s.held.includes("delete"),"old credential DELETE");
    const before=resolutions;token="reconnect-second";await h.configure({holdDelete:0});await h.release();check(message(await r.executeLine("echo --message current"))==="current","New credential command failed");
    const s=await h.read();check(s.deleted[0].token==="Bearer reconnect-first"&&s.requests.filter(r=>r.method==="initialize")[1].token==="Bearer reconnect-second","Credential snapshot crossed sessions");check(resolutions===before+2,"Renewal resolved provider more than once");await b.dispose();
  });
  for(const phase of ["provider","metadata"]) {
    let resolves=0,entered=false,aborted=false;
    const options=phase==="provider"?{token:undefined,credentialProvider:({signal})=>{
      resolves++;if(resolves===4){entered=true;return new Promise((_,reject)=>signal.addEventListener("abort",()=>{aborted=true;reject(new Error("interrupted"));},{once:true}));}
      return {token:"reconnect-first",authContextId:"same"};
    }}:{};
    await scenario(`close-${phase}`,options,{expire:1},async(r,h)=>{
      const b=await r.freshTool("echo");if(phase==="metadata")await h.configure({holdList:1});await rejected(b.call({message:"one"}),"SessionExpired","unknown");
      if(phase==="provider")await until(()=>entered,Boolean,"pending recovery provider");else await until(h.read,s=>s.held.includes("list"),"pending recovery metadata");
      const next=r.executeLine("echo --message never");next.catch(()=>{});const start=performance.now();await r.close();await rejected(next,"TransportClosed");check(performance.now()-start<1200,"Close waited for blocked recovery");
      if(phase==="provider")check(aborted,"Provider abort was lost");check((await h.read()).calls.length===1,"Close dispatched waiting work");await b.dispose();
    });
  }
  async function failedStartup(name,options,initial,code) {
    const start=performance.now();
    await scenario(name,options,initial,async()=>{throw new Error("Failed startup returned a Remote");}).then(()=>{throw new Error("Startup unexpectedly succeeded");},e=>{
      check(e.cause?.code===code,`${name}: expected ${code}, got ${e.cause?.code}`);count++;
    });
    check(performance.now()-start<1500,"Startup cancellation was not bounded");
    const url=new URL(base);url.pathname="/stats";url.searchParams.set("case",`${url.searchParams.get("case")}-${name}`);const s=await fetch(url).then(r=>r.json());
    check(s.calls.length===0&&Object.keys(s.sessions).length===0,"Cancelled startup retained session or sent business");
  }
  for(const backoff of [false,true]){
    const controller=new AbortController();let resolves=0,aborted=false;
    await failedStartup(`startup-cancel-${backoff?"backoff":"provider"}`,{token:undefined,signal:controller.signal,httpSessionRecovery:{maxAttempts:3,backoff:backoff?30000:0,timeout:45000},credentialProvider:({signal})=>{
      if(++resolves===2){setTimeout(()=>controller.abort(),40);if(!backoff)return new Promise((_,reject)=>signal.addEventListener("abort",()=>{aborted=true;reject(new Error("interrupted"));},{once:true}));}
      return {token:"reconnect-first",authContextId:"same"};
    }},{expireList:backoff?3:1},"Cancelled");
    check(resolves===2&&(!backoff?aborted:true),"Startup cancellation lost original signal");
  }
  let identity="old";
  await scenario("identity-registry",{token:undefined,schemaRegistry:[{uri:"https://fixture.invalid/reconnect-word",schemaJson:'{"type":"string"}'}],credentialProvider:()=>({token:"reconnect-first",authContextId:identity})},{ref:1,expire:1},async(r,h)=>{
    const b=await r.freshTool("echo");await h.configure({holdDelete:1});await rejected(b.call({message:"old"}),"SessionExpired","unknown");await until(h.read,s=>s.held.includes("delete"),"identity cleanup");
    identity="new";await h.configure({holdDelete:0,ref:0,integer:1});await h.release();check(message(await r.executeLine("echo --message 8"))===8,"New identity did not bind new schema");check((await r.schemaRegistryStats()).resources===0,"Schema registry crossed identity");await rejected(b.call({message:"never"}),"BindingStale");await b.dispose();
  });
  for(const body of [false,true])await scenario(`transport-retry-${body}`,{},{expire:1,freshConnections:1},async(r,h)=>{
    const b=await r.freshTool("echo");await h.configure({[body?"dropListBody":"dropList"]:1});await rejected(b.call({message:"failed"}),"SessionExpired","unknown");
    check(message(await r.executeLine("echo --message retry"))==="retry","Transport renewal round did not retry");const s=await h.read();check(s.epoch===3&&s.calls.length===2,`Transport retry evidence epoch=${s.epoch}, calls=${s.calls.length}, methods=${s.requests.map(r=>r.method).join(",")}`);await b.dispose();
  });
  await scenario("server-error-terminal",{},{expire:1},async(r,h)=>{
    const b=await r.freshTool("echo");await h.configure({failList:1});await rejected(b.call({message:"failed"}),"SessionExpired","unknown");await rejected(r.executeLine("echo --message never"),"TransportClosed");
    check((await h.read()).epoch===2,"Server rejection retried renewal");await r.reconnect();check(message(await r.executeLine("echo --message explicit"))==="explicit","Terminal failure could not be retried explicitly");await b.dispose();
  });
  await scenario("startup-404-body",{},{expireList:1,pending404:1},async(r,h)=>{
    check((await h.read()).epoch===2,"404 header waited for stalled error body");check(message(await r.executeLine("echo --message after"))==="after","404 body renewal failed");
  });
  await scenario("partial-pagination",{},{page:1},async(r,h)=>{
    await rejected(r.freshTool("obsolete"),"ToolNotFound");check(message(await r.executeLine("echo --message fresh"))==="fresh","Catalog retained old partial page");check((await h.read()).epoch===2,"Pagination renewal count");
  });
  let pendingEntered=false,pendingAborted=false,preflights=0;
  await scenario("provider-blocks-drain",{token:undefined,httpSessionRecovery:{maxAttempts:2,backoff:0,timeout:180},credentialProvider:({signal})=>{
    if(++preflights===4){pendingEntered=true;return new Promise((_,reject)=>signal.addEventListener("abort",()=>{pendingAborted=true;reject(new Error("interrupted"));},{once:true}));}
    return {token:"reconnect-first",authContextId:"same"};
  }},{expire:1,holdExpired:1},async(r,h)=>{
    const b=await r.freshTool("echo"),first=b.call({message:"expired"});first.catch(()=>{});await until(h.read,s=>s.held.includes("expired"),"held failure");
    const blocked=r.executeLine("echo --message never");blocked.catch(()=>{});await until(()=>pendingEntered,Boolean,"old preflight provider");await h.release("expired");
    await rejected(first,"SessionExpired","unknown");const start=performance.now();await rejected(blocked,"TransportClosed");check(performance.now()-start<1000&&pendingAborted,"Recovery failed to interrupt unsent active preflight");
    check((await h.read()).epoch===1,"Recovery reset behind active provider");await r.reconnect();check(message(await r.executeLine("echo --message explicit"))==="explicit","Provider cancellation did not free its slot");await b.dispose();
  });
  for(const phase of ["delete","provider","metadata"]){
    let calls=0,entered=false,aborted=false;
    const options={httpSessionRecovery:{maxAttempts:2,backoff:0,timeout:250},...(phase==="provider"?{token:undefined,credentialProvider:({signal})=>{
      if(++calls===4){entered=true;return new Promise((_,reject)=>signal.addEventListener("abort",()=>{aborted=true;reject(new Error("interrupted"));},{once:true}));}
      return {token:"reconnect-first",authContextId:"same"};
    }}:{})};
    await scenario(`phase-window-${phase}`,options,{expire:1},async(r,h)=>{
      const b=await r.freshTool("echo");if(phase!=="provider")await h.configure({[phase==="delete"?"holdDelete":"holdList"]:1});
      await rejected(b.call({message:"expired"}),"SessionExpired","unknown");
      if(phase==="provider")await until(()=>entered,Boolean,"bounded provider");else await until(h.read,s=>s.held.includes(phase==="delete"?"delete":"list"),"bounded recovery phase");
      await rejected(r.executeLine("echo --message never"),"TransportClosed");if(phase==="provider")check(aborted,"Recovery timeout did not abort provider");
      check((await h.read()).calls.length===1,"Timed out recovery dispatched tool");await h.configure({holdDelete:0,holdList:0});await h.release();await r.reconnect();check(message(await r.executeLine("echo --message explicit"))==="explicit","Failed recovery phase was not explicitly retryable");await b.dispose();
    });
  }
  await scenario("resumed-tool-expiry",{responseRecovery:{maxAttempts:1,backoff:0,timeout:1000}},{resume404:1},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"resume-expired"}),"SessionExpired","unknown");
    check(message(await r.executeLine("echo --message next"))==="next","Response GET expiry did not renew session");const s=await h.read();check(s.epoch===2&&s.calls.length===2&&s.requests.filter(q=>q.method==="GET").length===1,"Resumed tool expiry replayed a POST or GET");await b.dispose();
  });
  await scenario("suspended-is-not-healthy",{httpSessionRecovery:{maxAttempts:1,backoff:0,timeout:240}},{expire:1},async(r,h)=>{
    const b=await r.freshTool("echo");await rejected(b.call({message:"first"}),"SessionExpired","unknown");await r.freshTool("echo");
    await h.configure({failDelete:1});await rejected(r.reconnect(),"SessionCleanupFailed");await delay(280);
    await r.reconnect();const fresh=await r.freshTool("echo");await h.configure({expire:1});await rejected(fresh.call({message:"second"}),"SessionExpired","unknown");
    await rejected(r.executeLine("echo --message never"),"TransportClosed");check((await h.read()).epoch===3,"Suspended time replenished automatic budget");await b.dispose();await fresh.dispose();
  });
  return count;
}

export async function runSessionGc(open,base,gc) {
  if(typeof gc!=="function")return;
  const url=new URL(base);url.searchParams.set("case",`${url.searchParams.get("case")}-gc`);
  const config=new URL(url);config.pathname="/config";config.searchParams.set("expire","1");await fetch(config);
  const retained=[],weak=[];
  await (async()=>{
    const marker={token:"reconnect-first"};
    const provider=async()=>({token:marker.token,authContextId:"gc"});weak.push(new WeakRef(provider),new WeakRef(marker));
    const {remote,dispose}=await open(String(url),{timeout:4000,httpSessionRecovery:{maxAttempts:2,backoff:0,timeout:1500},credentialProvider:provider});weak.push(new WeakRef(remote));
    try{
      const b=await remote.freshTool("echo");const failed=b.call({message:"expired"});retained.push(failed);await rejected(failed,"SessionExpired","unknown");
      const success=remote.executeLine("echo --message after");retained.push(success);check(message(await success)==="after","GC recovered result lost");
    }finally{const closing=remote.close();retained.push(closing);await closing;dispose();}
  })();
  for(let n=0;n<100;n++){await delay(0);gc();await delay(2);if(weak.every(w=>w.deref()===undefined))break;}
  check(weak.every(w=>w.deref()===undefined),"Retained renewal promises keep owner/provider/credentials");
  check((await retained[1]).value.raw.structuredContent.message==="after","Settled value was not retained");
  const stats=new URL(url);stats.pathname="/stats";const s=await fetch(stats).then(r=>r.json());check(s.calls.length===2&&Object.keys(s.sessions).length===0,"GC session leak/replay");
  console.log(`PASS renewal GC ${url.searchParams.get("case")}: retained error/result/close promises release owner, provider and credentials`);
}
