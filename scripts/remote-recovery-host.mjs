const check=(ok,message)=>{if(!ok)throw new Error(message);};
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(read,predicate,label) {
  for(let n=0;n<600;n++){const value=await read();if(predicate(value))return value;await delay(5);}
  throw new Error(`Timed out: ${label}`);
}
async function rejects(promise,code) {
  try{await promise;}catch(error){check(error.code===code,`Expected ${code}, got ${error.code}: ${error.message}`);return error;}
  throw new Error(`Expected ${code}`);
}
export async function runRemoteRecovery(open,endpoint,{concurrency=16}={}) {
  let count=0;
  async function scenario(name,query,options,body) {
    const url=new URL(endpoint);url.searchParams.set("case",`${url.searchParams.get("case")}-${name}`);
    url.searchParams.set("recovery","1");
    for(const [key,value]of Object.entries(query))url.searchParams.set(key,String(value));
    const control=async(path,kind)=>{
      const next=new URL(url);next.pathname=path;if(kind)next.searchParams.set("kind",kind);
      const response=await fetch(next);check(response.ok,`Fixture ${name}/${path} failed`);
      return path==="/stats"?response.json():response.arrayBuffer();
    };
    const {remote,dispose}=await open(String(url),{token:"notify-snapshot",httpNotifications:true,timeout:5000,...options});
    const helpers={stats:()=>control("/stats"),emit:kind=>control("/emit",kind),release:()=>control("/release"),
      listening:()=>until(()=>remote.notificationStatus(),s=>s?.phase==="listening",`${name} listening`),
      failed:()=>until(()=>remote.notificationStatus(),s=>s?.phase==="failed",`${name} failed`)};
    try{await body(remote,helpers);count++;}
    finally{await control("/release");await remote.close();dispose();}
    await until(helpers.stats,s=>s.opens===s.closed,`${name} streams closed`);
    const requests=(await helpers.stats()).requests;
    check(requests.every(r=>r.method==="GET"||r.lastEventId===undefined),`${name}: Last-Event-ID leaked onto POST`);
  }
  await scenario("manual",{holdreply:1},{},async(remote,h)=>{
    const first=await h.listening();
    const calls=Array.from({length:concurrency},(_,n)=>remote.executeLine(`echo --message held-${n}`));calls.forEach(p=>p.catch(()=>{}));
    try {
      await until(h.stats,s=>s.held===concurrency,"business slots");
      await h.emit("request-end");await h.failed();await until(h.stats,s=>s.replies===1,"accepted capability reply");
      const resume=remote.resumeNotifications(first.resource_id);resume.catch(()=>{});
      await rejects(remote.resumeNotifications(first.resource_id),"QueueFull");
      check((await remote.notificationStatus({timeout:300})).phase==="failed","Status waited for resume/calls");
      check((await h.stats()).opens===1,"Resume did not drain accepted reply");
      await h.release();await Promise.all(calls);const opening=await resume;
      check(opening.resource_id!==first.resource_id,"Resume reused resource ID");await h.listening();
      const state=await h.stats(),gets=state.requests.filter(r=>r.method==="GET");
      check(state.calls===concurrency&&state.opens===2,"Resume replayed business work");
      check(gets[1].lastEventId==="游标/1"&&gets[1].session==="notify-session","Resume changed UTF-8 cursor/session");
    }finally{await h.release();await Promise.allSettled(calls);}
  });
  await scenario("interrupt",{retry:400},{},async(remote,h)=>{
    const first=await h.listening();await h.emit("end");await h.failed();
    const error=await rejects(remote.resumeNotifications(first.resource_id,{timeout:25}),"Timeout");
    check(error.execution_certainty==="not_sent","Waiting timeout claimed business execution");
    const pending=remote.resumeNotifications(first.resource_id);const cancelled=rejects(pending,"Cancelled");pending.cancel();await cancelled;
    const superseded=remote.resumeNotifications(first.resource_id);const stopped=rejects(superseded,"Cancelled");
    await remote.stopNotifications({timeout:300});await stopped;
    check((await h.stats()).opens===1,"Interrupted resume sent a GET");
    await rejects(remote.resumeNotifications(first.resource_id),"ResumeUnavailable");
    await remote.startNotifications();await h.listening();
    const gets=(await h.stats()).requests.filter(r=>r.method==="GET");check(gets.length===2&&gets[1].lastEventId===undefined,"Fresh start reused cursor");
  });
  let current={token:"notify-snapshot",authContextId:"stable"},providerCalls=0,blocked=false,unblock;
  await scenario("snapshot",{}, {token:undefined,credentialProvider:async()=>{providerCalls++;if(blocked)await new Promise(resolve=>unblock=resolve);return current;}},async(remote,h)=>{
    const first=await h.listening();current={token:"notify-second",authContextId:"stable"};await remote.refresh();
    blocked=true;const pending=remote.refresh();pending.catch(()=>{});
    try {
      await until(()=>Boolean(unblock),Boolean,"blocked provider");
      await h.emit("request-end");await h.failed();const before=providerCalls;
      await remote.resumeNotifications(first.resource_id,{timeout:1000});await h.listening();await h.emit("request");
      await until(h.stats,s=>s.replies===2,"reply on resumed stream");
      const state=await h.stats();check(providerCalls===before,"Resume resolved provider");
      check(state.listenTokens.every(t=>t==="Bearer notify-snapshot")&&state.replyTokens.every(t=>t==="Bearer notify-snapshot"),"Recovery replaced frozen credential");
    }finally{pending.cancel();await rejects(pending,"Cancelled");blocked=false;unblock?.();}
  });
  const policy={maxAttempts:2,backoff:20,timeout:800};
  let resumeProviderCalls=0,providerBlocked=false,releaseProvider;
  await scenario("automatic-provider",{}, {token:undefined,notificationRecovery:policy,credentialProvider:async()=>{resumeProviderCalls++;if(providerBlocked)await new Promise(resolve=>releaseProvider=resolve);return {token:"notify-snapshot",authContextId:"auto-provider"};}},async(remote,h)=>{
    await h.listening();providerBlocked=true;const pending=remote.refresh();pending.catch(()=>{});
    try {
      await until(()=>Boolean(releaseProvider),Boolean,"automatic blocked provider");const before=resumeProviderCalls;
      await h.emit("request-end");await until(h.stats,s=>s.opens===2&&s.replies===1,"automatic recovery during provider wait");await h.listening();
      check(resumeProviderCalls===before,"Automatic recovery resolved provider");await remote.stopNotifications({timeout:300});
    }finally{pending.cancel();await rejects(pending,"Cancelled");providerBlocked=false;releaseProvider?.();}
  });
  let identity={token:"notify-snapshot",authContextId:"identity-one"};
  await scenario("identity",{retry:300},{token:undefined,credentialProvider:async()=>identity},async(remote,h)=>{
    const first=await h.listening();await h.emit("end");await h.failed();
    const pending=remote.resumeNotifications(first.resource_id),rejected=rejects(pending,"ResumeUnavailable");
    identity={token:"notify-second",authContextId:"identity-two"};await remote.refresh();await h.listening();await rejected;
    const gets=(await h.stats()).requests.filter(r=>r.method==="GET");
    check(gets.length===2&&gets[1].lastEventId===undefined&&gets[1].token==="Bearer notify-second","Old resume retargeted new identity");
  });
  await scenario("bounded",{flap:1},{notificationRecovery:policy},async(remote,h)=>{
    await until(h.stats,s=>s.opens===3&&s.closed===3,"bounded automatic retries");await delay(100);
    check((await h.stats()).opens===3,"Retry budget reset after short EOF");
    await remote.executeLine("echo --message after-exhaustion");check((await h.stats()).calls===1,"Automatic recovery replayed POST");
    const failed=await h.failed();await remote.resumeNotifications(failed.resource_id);
    await until(h.stats,s=>s.opens===6&&s.closed===6,"manual override starts new bounded burst");
    await remote.stopNotifications();await delay(40);check((await h.stats()).opens===6,"Stop left automatic timer live");
  });
  await scenario("disabled",{}, {},async(remote,h)=>{
    await h.listening();await h.emit("end");await h.failed();await delay(60);
    check((await h.stats()).opens===1,"Automatic recovery defaulted on");
  });
  await scenario("policy-only",{}, {httpNotifications:false,notificationRecovery:policy},async(remote,h)=>{
    check(await remote.notificationStatus()===null&&(await h.stats()).opens===0,"Policy opened listener");
  });
  await scenario("window",{retry:200},{notificationRecovery:{...policy,timeout:80}},async(remote,h)=>{
    await h.listening();await h.emit("end");await h.failed();await delay(120);
    check((await h.stats()).opens===1,"Retry exceeded total window");
  });
  await scenario("opening",{resumehang:1},{notificationRecovery:{...policy,timeout:100}},async(remote,h)=>{
    await h.listening();await h.emit("end");
    const status=await until(()=>remote.notificationStatus(),s=>s?.phase==="failed"&&s.error_code==="Timeout","remaining opening window");
    check(status.error_code==="Timeout"&&(await h.stats()).opens===2,"Opening ignored recovery window");
  });
  await scenario("healthy",{},{notificationRecovery:{maxAttempts:1,backoff:10,timeout:120}},async(remote,h)=>{
    await h.listening();await h.emit("end");await until(h.stats,s=>s.opens===2,"first recovery");await h.listening();
    await delay(160);await h.emit("end");await until(h.stats,s=>s.opens===3,"healthy reset");await h.listening();
    await h.emit("end");await h.failed();await delay(60);check((await h.stats()).opens===3,"Short stream reset healthy budget");
  });
  for(const status of [401,404,405])await scenario(`reject-${status}`,{resumestatus:status},{notificationRecovery:policy},async(remote,h)=>{
    await h.listening();await h.emit("end");await until(h.stats,s=>s.opens===2&&s.closed===2,"resume rejection");await h.failed();await delay(80);
    check((await h.stats()).opens===2,"Rejected endpoint retried");
  });
  for(const kind of ["reset","broken"])await scenario(kind,{}, {notificationRecovery:policy},async(remote,h)=>{
    const first=await h.listening();await h.emit(kind);if(kind==="reset")await h.emit("end");await h.failed();await delay(60);
    check((await h.stats()).opens===1,`${kind} allowed recovery`);await rejects(remote.resumeNotifications(first.resource_id),"ResumeUnavailable");
  });
  await scenario("writer-failure",{holdreply:1},{notificationRecovery:policy},async(remote,h)=>{
    const first=await h.listening();await h.emit("request-end");await h.failed();
    await until(h.stats,s=>s.replyClosed===1,"failed reply writer");await delay(80);
    check((await h.stats()).opens===1,"Failed writer allowed automatic recovery");
    await rejects(remote.resumeNotifications(first.resource_id),"ResumeUnavailable");
  });
  return count;
}

async function prepareRecoveryGc(open,endpoint) {
  const credential={token:"notify-snapshot",authContextId:"recovery-gc"},provider=async()=>credential;
  const {remote,dispose}=await open(endpoint,{httpNotifications:true,credentialProvider:provider,timeout:5000,notificationRecovery:{maxAttempts:2,backoff:1500,timeout:3000}});
  const retained=[];
  const control=async kind=>{const url=new URL(endpoint);url.pathname="/emit";url.searchParams.set("kind",kind);const response=await fetch(url);check(response.ok,"GC emit failed");await response.arrayBuffer();};
  try {
    const first=await until(()=>remote.notificationStatus(),s=>s?.phase==="listening","GC stream");
    await control("end");await until(()=>remote.notificationStatus(),s=>s?.phase==="failed","GC failure");
    retained.push(remote.resumeNotifications(first.resource_id));await retained.at(-1);
    await until(()=>remote.notificationStatus(),s=>s?.phase==="listening","GC resumed stream");
    retained.push(remote.resumeNotifications(first.resource_id));await rejects(retained.at(-1),"InvalidHandle");
    await control("end");const failed=await until(()=>remote.notificationStatus(),s=>s?.phase==="failed","GC second failure");
    retained.push(remote.resumeNotifications(failed.resource_id));const cancelled=rejects(retained.at(-1),"Cancelled");retained.at(-1).cancel();await cancelled;
    // Closing must release a pending automatic backoff and all frozen credentials.
    await remote.close();
    return {closed:remote,retained,weak:[new WeakRef(remote),new WeakRef(provider),new WeakRef(credential)]};
  }finally{await remote.close();dispose();}
}
export async function runRecoveryGc(open,endpoint,collect) {
  const url=new URL(endpoint);url.searchParams.set("retry","400");endpoint=String(url);
  const state=await prepareRecoveryGc(open,endpoint);
  async function released(refs) {
    for(let n=0;n<200;n++){await delay(5);await collect();await delay(0);if(refs.every(ref=>ref.deref()===undefined))return;}
    throw new Error("Recovery promises/timers retained owner/provider/credentials");
  }
  await released(state.weak.slice(1));state.closed=undefined;await released(state.weak);
  check((await state.retained[0]).phase==="opening","Retained recovery result changed");
  for(const promise of state.retained)promise.cancel?.();
}

export async function runModernRecoveryDenial(open,endpoint) {
  const {remote,dispose}=await open(endpoint,{token:"notify-snapshot",httpNotifications:true,timeout:5000,notificationRecovery:{maxAttempts:2,backoff:0,timeout:500}});
  try {
    for(const id of [0,-1,1.5,Number.MAX_SAFE_INTEGER+1,"1"])await rejects(remote.resumeNotifications(id),"InvalidHandle");
    const first=await until(()=>remote.notificationStatus(),s=>s?.phase==="listening","modern listening");
    const url=new URL(endpoint);url.pathname="/emit";url.searchParams.set("kind","end");const response=await fetch(url);check(response.ok,"Modern end failed");await response.arrayBuffer();
    await until(()=>remote.notificationStatus(),s=>s?.phase==="closed","modern completion");
    await rejects(remote.resumeNotifications(first.resource_id),"ResumeUnavailable");await delay(30);
    url.pathname="/stats";const state=await (await fetch(url)).json();
    check(state.opens===1&&state.requests.every(r=>r.method!=="GET"),"Modern completion used legacy recovery");
  }finally{await remote.close();dispose();}
}
