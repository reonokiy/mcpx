const check=(ok,message)=>{if(!ok)throw new Error(message);};
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function rejects(promise,code){try{await promise;}catch(error){check(error.code===code,`Expected ${code}, got ${error.code}: ${error.message}`);return;}throw new Error(`Expected ${code}`);}
async function until(read,predicate,label){for(let n=0;n<500;n++){const value=await read();if(predicate(value))return value;await delay(5);}throw new Error(`Timed out: ${label}`);}
export async function runRemoteNotifications(open,endpoint,{concurrency=16,flood=false}={}) {
  const url=new URL(endpoint),legacy=url.searchParams.get("legacy")==="1";
  const control=async(path,kind)=>{const next=new URL(endpoint);next.pathname=path;if(kind)next.searchParams.set("kind",kind);const response=await fetch(next);check(response.ok,`Fixture ${path} failed`);return path==="/stats"?response.json():response.arrayBuffer();};
  const stats=()=>control("/stats"),emit=kind=>control("/emit",kind);
  const listen=remote=>until(()=>remote.notificationStatus(),value=>value?.phase==="listening","notification acknowledgment");
  let current={token:"notify-snapshot",authContextId:"first"},providerCalls=0,block=false,unblock;
  const {remote,dispose}=await open(endpoint,{httpNotifications:true,maxInFlight:concurrency,queueCapacity:concurrency+2,timeout:5000,credentialProvider:async()=>{providerCalls++;if(block)await new Promise(resolve=>unblock=resolve);return current;}});
  let watcher;
  try {
    const first=await listen(remote);check((await remote.startNotifications()).resource_id===first.resource_id,"Start was not idempotent");
    const before=providerCalls;await remote.notificationStatus();await delay(10);check(providerCalls===before,"Status resolved credentials");
    watcher=remote.subscribeCatalog();const changed=watcher.next();await emit(flood?"flood":"change");
    const notice=await changed;check(!notice.done&&notice.value.invalidated,"Idle stream did not invalidate catalog");
    check((await stats()).calls===0,"Notification replayed a business call");
    const binding=await remote.freshTool("echo");
    check((await binding.call({message:"while-listening"})).value.raw.structuredContent.message==="while-listening","Notification blocked ordinary call");
    const calls=Array.from({length:concurrency},(_,n)=>binding.call({message:`held-${n}`}));
    calls.forEach(p=>p.catch(()=>{}));await until(stats,s=>s.held===concurrency,"full business slots");
    const queuedStart=remote.startNotifications();const cancelled=rejects(queuedStart,"Cancelled");
    const aborted=new AbortController();aborted.abort();
    await rejects(remote.stopNotifications({signal:aborted.signal}),"Cancelled");
    check((await remote.notificationStatus()).phase==="listening","Rejected stop changed listener state");
    const stopped=await remote.stopNotifications({timeout:1000});check(stopped.phase==="cancelled","Stop blocked behind calls");await cancelled;
    check((await remote.notificationStatus({timeout:1000})).phase==="cancelled","Status blocked behind calls");
    await control("/release");await Promise.all(calls);
    // A newer start must survive a stop admitted in the same turn.
    const stop=remote.stopNotifications(),start=remote.startNotifications();await stop;await start;
    const restarted=await listen(remote);check(restarted.resource_id!==first.resource_id,"Restart reused resource ID");
    current={token:"notify-second",authContextId:"second"};await remote.refresh();await listen(remote);
    await until(stats,s=>s.opens===3&&s.closed>=2,"identity stream cleanup");
    const rotated=await stats();check(rotated.listenTokens.join(",")==="Bearer notify-snapshot,Bearer notify-snapshot,Bearer notify-second","Listener did not freeze credentials");
    if(legacy)check(rotated.deleted[0]?.token==="Bearer notify-snapshot"&&rotated.deleted[0]?.session==="notify-session","DELETE used new credentials");
    block=true;const pending=remote.refresh();pending.catch(()=>{});
    // Wait until this particular callback has entered its unresolved await.
    await until(()=>Boolean(unblock),Boolean,"blocked provider");
    await remote.stopNotifications({timeout:1000});await remote.notificationStatus({timeout:1000});
    pending.cancel();await rejects(pending,"Cancelled");block=false;unblock();await delay(10);
    await remote.startNotifications();await listen(remote);await emit(legacy?"request-end":"end");
    const ended=await until(()=>remote.notificationStatus(),s=>s?.phase===(legacy?"failed":"closed"),"notification ending");
    if(legacy){check(ended.error_code==="SubscriptionDisconnected","EOF lost diagnostic");await until(stats,s=>s.replies===1,"capability reply after EOF");}
    await remote.refresh();check((await stats()).opens===4,"Ended listener restarted automatically");
    await remote.startNotifications();await listen(remote);
    if(!legacy){await emit("bad");const bad=await until(()=>remote.notificationStatus(),s=>s?.phase==="failed","wrong ID");check(bad.error_code==="ProtocolError","Wrong ID was not rejected");}
    await binding.dispose();
  }finally{await watcher?.return();await remote.close();dispose();}
  await until(stats,s=>s.closed===s.opens,"listener close");
}

export async function runNotificationFailure(open,endpoint,expected) {
  const {remote,dispose}=await open(endpoint,{httpNotifications:true,token:"notify-snapshot",timeout:300});
  try {
    const status=await until(()=>remote.notificationStatus(),s=>s?.phase==="failed","listener failure");
    check(status.error_code===expected,`Expected ${expected}, got ${status.error_code}`);
    check((await remote.executeLine("echo --message after-failure",{timeout:5000})).value.raw.structuredContent.message==="after-failure","Listener failure broke ordinary call");
  }finally{await remote.close();dispose();}
}

export async function runNotificationCleanup(open,endpoint) {
  const call=async(path,kind)=>{const url=new URL(endpoint);url.pathname=path;if(kind)url.searchParams.set("kind",kind);const response=await fetch(url);check(response.ok,"Fixture control failed");return path==="/stats"?response.json():response.arrayBuffer();};
  const {remote,dispose}=await open(endpoint,{token:"notify-snapshot",timeout:5000});
  try {
    check(await remote.notificationStatus()===null,"Notifications defaulted on");
    check((await call("/stats")).opens===0,"Default opened a stream");
    await remote.startNotifications();await until(()=>remote.notificationStatus(),s=>s?.phase==="listening","manual start");
    await call("/emit","request-end");await until(()=>call("/stats"),s=>s.replies===1,"pending reply");
    // The fixture withholds the reply's HTTP acknowledgment. The business
    // request must complete before that independent writer is released.
    check((await remote.executeLine("echo --message before-reply-ack",{timeout:1000})).value.raw.structuredContent.message==="before-reply-ack","Capability writer blocked business calls");
    check((await call("/stats")).replyClosed===0,"Business call waited for the reply writer to finish or time out");
    await call("/release");await remote.stopNotifications();
    await remote.startNotifications();await until(()=>remote.notificationStatus(),s=>s?.phase==="listening","restart after writer");
    // The direct owner deliberately pauses its event gate. Cleanup bypasses
    // event and admission waits; Worker ACK gating has its own regression.
    if(remote.notifications) {
      remote.eventGate=signal=>new Promise(resolve=>signal.addEventListener("abort",resolve,{once:true}));
      const pending=remote.refresh();pending.catch(()=>{});await delay(10);
      await remote.stopNotifications({timeout:300});await remote.notificationStatus({timeout:300});
      pending.cancel();await rejects(pending,"Cancelled");
    }
  }finally{await remote.close();dispose();}
}

async function prepareNotificationGc(open,endpoint) {
  const credential={token:"notify-snapshot",authContextId:"notification-gc"};
  const provider=async()=>credential;
  const {remote,dispose}=await open(endpoint,{httpNotifications:true,credentialProvider:provider,timeout:5000});
  const retained=[],watcher=remote.subscribeCatalog();
  try {
    await until(()=>remote.notificationStatus(),s=>s?.phase==="listening","live stream for GC");
    retained.push(remote.startNotifications());await retained.at(-1);
    retained.push(remote.notificationStatus());await retained.at(-1);
    retained.push(remote.executeLine("echo --message retained-notification-result"));const result=await retained.at(-1);
    retained.push(remote.stopNotifications());await retained.at(-1);
    retained.push(remote.startNotifications());await retained.at(-1);
    await until(()=>remote.notificationStatus(),s=>s?.phase==="listening","restarted stream for GC");
    await remote.close();
    return {closed:remote,retained,watcher,positive:new WeakRef(result),weak:[new WeakRef(remote),new WeakRef(provider),new WeakRef(credential)]};
  }finally{await remote.close();dispose();}
}
export async function runNotificationGc(open,endpoint,collect) {
  const state=await prepareNotificationGc(open,endpoint);
  async function released(refs) {
    for(let n=0;n<200;n++){await delay(5);await collect();await delay(0);if(refs.every(ref=>ref.deref()===undefined))return;}
    throw new Error("Notification promises retained owner/provider/credentials");
  }
  await released(state.weak.slice(1));state.closed=undefined;await released(state.weak);
  check(state.positive.deref()!==undefined,"Retained promise lost its result");
  check((await state.retained[2]).value.raw.structuredContent.message==="retained-notification-result","Retained notification call changed result");
  for(const promise of state.retained)promise.cancel?.();
  while(state.watcher.tryNext()?.done===false){}
  check((await state.watcher.next()).done,"Closed stream retained a watcher read");
}
