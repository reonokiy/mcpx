const check=(ok,message)=>{if(!ok)throw new Error(message);};
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function rejects(promise,code,certainty) {try{await promise;}catch(error){check(error.code===code,`Expected ${code}, got ${error.code}: ${error.message}`);if(certainty)check(error.execution_certainty===certainty,`Expected ${certainty}, got ${error.execution_certainty}`);return error;}throw new Error(`Expected ${code}`);}
async function until(read,predicate,label) {for(let n=0;n<1000;n++){const value=await read();if(predicate(value))return value;await delay(5);}throw new Error(`Response recovery wait: ${label}`);}
const message=outcome=>outcome.value.raw.structuredContent.message;
const policy={maxAttempts:2,backoff:10,timeout:2000};
export async function runResponseRecovery(open,base) {
  let count=0;
  async function scenario(name,query,options,run) {
    const url=new URL(base);url.searchParams.set("case",`${url.searchParams.get("case")}-${name}`);
    for(const [name,value]of Object.entries(query))url.searchParams.set(name,String(value));
    const endpoint=String(url),stats=()=>{const url=new URL(endpoint);url.pathname="/stats";return fetch(url).then(r=>r.json());};
    const control=async()=>{const url=new URL(endpoint);url.pathname="/release";const r=await fetch(url);await r.arrayBuffer();};
    const {remote,dispose}=await open(endpoint,{token:"response-first",responseRecovery:policy,timeout:5000,...options});
    try{await run(remote,stats,control);count++;}catch(error){throw new Error(`${name}: ${error.message}`,{cause:error});}
    finally{await remote.close();dispose();}
    const final=await until(stats,s=>s.opened===s.closed,"HTTP cleanup");check(final.calls.length===new Set(final.calls.map(c=>c.rpc.id)).size,"Tool RPC replayed");
  }
  await scenario("normal",{caps:1,replydelay:30,retry:25,padding:1},{},async(r,stats)=>{
    const binding=await r.freshTool("echo");check(message(await binding.call({message:"normal"}))==="normal","Wrong result");await binding.dispose();
    const s=await stats();check(s.calls.length===1&&s.gets.length===1&&s.replies.length===2&&s.acks.length===2,"Wrong I/O counts");check(s.gets[0].at>=s.acks[0].at,"GET overtook reply");check(s.requests.every(q=>q.method==="GET"||q.cursor===undefined),"POST carried cursor");
  });
  await scenario("sixteen",{barrier:16,hold:1,caps:1,replydelay:25},{queueCapacity:16},async(r,stats,control)=>{
    const b=await r.freshTool("echo"),calls=[];
    for(let n=0;n<16;n++){const call=n%3===0?b.call({message:`mixed-${n}`}):n%3===1?r.execute("echo",["--message",`mixed-${n}`]):r.executeLine(`echo --message mixed-${n}`);call.catch(()=>{});calls.push(call);}
    await until(stats,s=>s.calls.length===16,"16 original calls");await rejects(b.call({message:"overflow"}),"QueueFull","not_sent");await control();
    for(let n=0;n<16;n++)check(message(await calls[n])===`mixed-${n}`,"Concurrent result misrouted");
    const s=await stats();check(s.calls.length===16&&s.gets.length===16&&s.replies.length===32,"16 calls did not share recovery slots");await b.dispose();
  });
  let blocked=false,entered,release,calls=0;
  await scenario("provider",{hold:1,caps:1,replydelay:20},{token:undefined,credentialProvider:async()=>{calls++;if(blocked){entered?.();await new Promise(resolve=>release=resolve);}return {token:blocked?"response-second":"response-first",authContextId:"same"};}},async(r,stats,control)=>{
    const b=await r.freshTool("echo"),root=b.call({message:"frozen"});root.catch(()=>{});await until(stats,s=>s.calls.length===1,"root POST");
    blocked=true;const waiting=new Promise(resolve=>entered=resolve),peer=b.call({message:"pending-provider"});peer.catch(()=>{});await waiting;const before=calls;
    await control();check(message(await root)==="frozen","Provider blocked resumed call");check(calls===before,"Recovery re-resolved provider");
    const s=await stats();check(s.gets[0].token==="Bearer response-first"&&s.replies.every(p=>p.token==="Bearer response-first"),"Frozen token changed");
    peer.cancel();await rejects(peer,"Cancelled","not_sent");blocked=false;release();await b.dispose();
  });
  await scenario("exhausted",{flap:1},{},async(r,stats)=>{const b=await r.freshTool("echo");await rejects(b.call({message:"flap"}),"ResponseRecoveryExhausted","unknown");const s=await stats();check(s.gets.length===2&&s.calls.length===1&&s.cancelled.length===1,"Unbounded GETs or wrong cancellation");await b.dispose();});
  for(const [name,query]of [["retry-window",{retry:300}],["reply-window",{caps:1,replydelay:200}],["opening-window",{opening:1}],["reading-window",{hang:1}]])await scenario(name,query,{responseRecovery:{...policy,timeout:100}},async(r,stats)=>{const b=await r.freshTool("echo");await rejects(b.call({message:name}),"Timeout","unknown");const s=await stats();check(s.gets.length===(name.startsWith("retry")||name.startsWith("reply")?0:1),"GET crossed deadline");await b.dispose();});
  for(const status of [401,404,405,503])await scenario(`status-${status}`,{status},{},async(r,stats)=>{const b=await r.freshTool("echo");await rejects(b.call({message:"reject"}),status===401?"AuthFailed":status===404?"SessionExpired":"HttpError","unknown");check((await stats()).gets.length===1,"Rejected GET retried");await b.dispose();});
  for(const [name,query,options]of [["disabled",{}, {responseRecovery:undefined}],["missing-cursor",{cursor:"none"},{}],["reset-cursor",{cursor:"reset"},{}],["broken",{bad:1},{}],["json",{json:1},{}],["modern",{legacy:0},{}]])await scenario(name,query,options,async(r,stats)=>{const b=await r.freshTool("echo"),call=b.call({message:name});if(name==="json")check(message(await call)==="json","JSON failed");else await rejects(call,"ProtocolError","unknown");check((await stats()).gets.length===0,"Ineligible call resumed");await b.dispose();});
  await scenario("writer-failed",{caps:1,replystatus:503},{},async(r,stats)=>{const b=await r.freshTool("echo");await rejects(b.call({message:"writer"}),"HostError","unknown");check((await stats()).gets.length===0,"Writer failure resumed");await b.dispose();});
  for(const phase of ["retry","read"])await scenario(`cancel-${phase}`,phase==="retry"?{retry:600}:{hang:1},{},async(r,stats)=>{const b=await r.freshTool("echo"),call=b.call({message:phase});call.catch(()=>{});await until(stats,s=>phase==="retry"?s.closed===1:s.gets.length===1,"cancel boundary");call.cancel();await rejects(call,"Cancelled","unknown");const s=await stats();check(s.cancelled.length===1&&s.cancelled[0].rpc===s.calls[0].rpc.id&&s.cancelled[0].token==="Bearer response-first","Wrong cancelled RPC/token");check(s.gets.length===(phase==="retry"?0:1),"Cancel opened another GET");await b.dispose();});
  await scenario("known-result",{caps:1,finaldelay:250},{},async(r,stats)=>{const b=await r.freshTool("echo"),call=b.call({message:"known"},{timeout:180});call.catch(()=>{});await until(stats,s=>s.replies.length===2,"final reply pending");call.cancel();check(message(await call)==="known","Known result overwritten");check((await stats()).cancelled.length===0,"Known result cancelled on wire");await b.dispose();});
  await scenario("known-close",{caps:1,finaldelay:2000},{},async(r,stats)=>{const b=await r.freshTool("echo"),call=b.call({message:"known-close"});call.catch(()=>{});await until(stats,s=>s.replies.length===2,"closing final reply");await r.close();check(message(await call)==="known-close","Close discarded result");check((await stats()).cancelled.length===0,"Close cancelled known result");});
  await scenario("raw-uint64",{large:1},{},async(r)=>{const b=await r.freshTool("echo");const raw=await b.callRaw('{"message":"raw"}');check(raw.includes("18446744073709551615"),"GET rounded uint64");await rejects(b.call({message:"parsed"}),"NumberOutOfRange","result_received");await b.dispose();});
  await scenario("notifications",{caps:1},{httpNotifications:true},async(r,stats)=>{const b=await r.freshTool("echo");check(message(await b.call({message:"notification peer"}))==="notification peer","Notification interfered with recovery");check((await stats()).listens===1,"No independent listener");await b.dispose();});
  return count;
}

async function prepareGc(open,base) {
  const url=new URL(base);url.searchParams.set("retry","500");const endpoint=String(url);
  const credential={token:"response-first",authContextId:"gc"},provider=async()=>credential;
  const {remote,dispose}=await open(endpoint,{credentialProvider:provider,responseRecovery:policy,timeout:5000});
  const b=await remote.freshTool("echo"),retained=[];
  try {
    retained.push(b.call({message:"gc-success"}));await retained.at(-1);
    retained.push(b.call({message:"gc-cancel"}));retained.at(-1).catch(()=>{});
    url.pathname="/stats";await until(()=>fetch(url).then(r=>r.json()),s=>s.calls.length===2&&s.closed>=3,"GC backoff");
    retained.at(-1).cancel();await rejects(retained.at(-1),"Cancelled");
    retained.push(b.call({message:"gc-failure"}));await rejects(retained.at(-1),"ResponseRecoveryExhausted");
    retained.push(b.call({message:"gc-close"}));retained.at(-1).catch(()=>{});
    await until(()=>fetch(url).then(r=>r.json()),s=>s.calls.length===4,"GC active");await remote.close();await rejects(retained.at(-1),"TransportClosed");
    return {closed:remote,retained,weak:[new WeakRef(remote),new WeakRef(provider),new WeakRef(credential)]};
  }finally{await remote.close();dispose();}
}
export async function runResponseRecoveryGc(open,base,collect) {
  const state=await prepareGc(open,base);
  async function released(refs) {for(let n=0;n<200;n++){await delay(5);collect();await delay(0);if(refs.every(ref=>ref.deref()===undefined))return;}throw new Error("Response recovery retained owner/provider/credential");}
  await released(state.weak.slice(1));state.closed=undefined;await released(state.weak);
  check(message(await state.retained[0])==="gc-success","Retained success changed");for(const result of state.retained)result.cancel();
}
