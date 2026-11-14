// Shared low-level host scenario; no high-level Remote owns this runtime.
const check = (ok, message) => { if (!ok) throw new Error(message); };
const code = error => typeof error === "string" ? JSON.parse(error).code : error.code;
function fails(fn, expected) { try { fn(); } catch(error) { check(code(error)===expected,`Expected ${expected}, got ${code(error)}`); return; } throw new Error(`Expected ${expected}`); }
const parse = value => typeof value === "string" ? JSON.parse(value) : value;
export async function runNotifications(runtime, endpoint, fetchImpl=globalThis.fetch) {
  const headers = values => ({...values, authorization:"Bearer notify-snapshot"});
  async function drive() {
    while (runtime.busy()) {
      const actions=parse(runtime.actions());check(actions.length>0,"No ordinary action");
      for (const action of actions) {
        const response=await fetchImpl(action.url,{method:"POST",headers:headers(action.headers),body:action.body,redirect:"error"});
        const finished=runtime.begin_response(action.id,response.status,JSON.stringify(Object.fromEntries(response.headers)));
        if (!finished) {
          const reader=response.body.getReader();let done=false;
          try { while (!done) {const chunk=await reader.read();if(chunk.done){runtime.finish_response_for(action.id);break;}done=runtime.push_response_for(action.id,chunk.value);} }
          finally {await reader.cancel();reader.releaseLock();}
        } else await response.body?.cancel();
      }
    }
    runtime.events();
  }
  const emit = async kind => {
    const url=new URL(endpoint);url.pathname="/emit";url.searchParams.set("kind",kind);
    const response=await fetchImpl(url);check(response.ok,"Fixture emit failed");await response.arrayBuffer();
  };
  async function open() {
    const plan=parse(runtime.start_notifications()), id=plan.resource_id;
    fails(()=>runtime.begin_notifications(id+2**32,200,"text/event-stream"),"InvalidHandle");
    for (const status of ["200",true,200.5,65536+200,NaN]) fails(()=>runtime.begin_notifications(id,status,"text/event-stream"),"InvalidResponse");
    check(parse(runtime.notification_status()).phase==="opening","Rejected ABI arguments changed the live resource");
    const controller=new AbortController();let cause,readyResolve,readyReject,changeResolve;
    const ready=new Promise((resolve,reject)=>{readyResolve=resolve;readyReject=reject;});ready.catch(()=>{});
    const changed=new Promise(resolve=>{changeResolve=resolve;});
    const timer=setTimeout(()=>{cause="Timeout";controller.abort();},5000);
    const publish=()=>{const notice=parse(runtime.catalog_change());if(notice.kind==="changed"&&notice.invalidated)changeResolve(notice);if(parse(runtime.notification_status()).phase==="listening")readyResolve();};
    const closed=(async()=>{
      let reader;
      try {
        const response=await fetchImpl(plan.url,{method:plan.method,headers:headers(plan.headers),body:plan.body===null?undefined:JSON.stringify(plan.body),signal:controller.signal,redirect:"error"});
        runtime.begin_notifications(id,response.status,response.headers.get("content-type")??"");publish();
        reader=response.body.getReader();
        for (;;) {
          const chunk=await reader.read();
          if(chunk.done){runtime.finish_notifications(id);throw new Error("Unexpected successful EOF");}
          let done=false;
          for(let offset=0;offset<chunk.value.byteLength;offset+=65536) {
            done=runtime.push_notifications(id,chunk.value.subarray(offset,offset+65536));publish();
            if(done)break;
            for(const reply of parse(runtime.notification_replies(id))) {
              const response=await fetchImpl(plan.url,{method:"POST",headers:headers({...plan.headers,accept:"application/json, text/event-stream","content-type":"application/json"}),body:JSON.stringify(reply),signal:controller.signal,redirect:"error"});
              check(response.status===202,"Legacy capability response was not accepted");await response.body?.cancel();
            }
          }
          if(done)return parse(runtime.notification_status());
        }
      } catch(error) {
        const known=code(error);
        if(cause==="Cancelled")return parse(runtime.notification_status());
        try {runtime.abort_notifications(id,cause==="Timeout");}catch{}
        readyReject(error);
        throw Object.assign(new Error("Notification stream failed"),{code:cause??known??"TransportError"});
      } finally {clearTimeout(timer);controller.abort();if(reader){await reader.cancel().catch(()=>{});reader.releaseLock();}}
    })();closed.catch(()=>{});
    return {id,plan,ready,changed,closed,stop:async()=>{cause="Cancelled";readyReject(Object.assign(new Error("Notification cancelled"),{code:"Cancelled"}));try{runtime.stop_notifications(id);}catch{}controller.abort();await closed.catch(()=>{});}};
  }
  runtime.accept_authorization("notify-scope");runtime.discover();await drive();runtime.catalog_change();
  let listener;
  try {
    listener=await open();await listener.ready;
    const legacy=listener.plan.method==="GET";
    check(!runtime.busy(),"Listener occupied an ordinary call slot");
    check(legacy?!listener.plan.headers["mcp-method"]:!listener.plan.headers["mcp-session-id"],"Version routing leaked across eras");
    await emit("change");await listener.changed;
    check(parse(runtime.catalog_status("notify-scope")).needs_refresh,"Idle notification did not invalidate catalog");
    runtime.refresh_catalog();await drive();
    const binding=runtime.bind("echo"),operation=runtime.call_concurrent(binding,'{"message":"while-listening"}');await drive();
    check(parse(runtime.completion(operation)).value.raw.structuredContent.message==="while-listening","Listener blocked an ordinary call");
    if(legacy) {
      await emit("request");
      const stats=new URL(endpoint);stats.pathname="/stats";
      let replies=0;for(let i=0;i<100&&replies===0;i++){replies=(await(await fetchImpl(stats)).json()).replies;if(!replies)await new Promise(resolve=>setTimeout(resolve,1));}
      check(replies===1,"Legacy request did not receive a capability error");
      await emit("end");
      try{await listener.closed;throw new Error("EOF accepted as success");}catch(error){check(error.code==="SubscriptionDisconnected","Legacy disconnect was not explicit");}
    } else {await emit("end");check((await listener.closed).phase==="closed","Modern final result was not accepted");}
    listener=await open();await listener.ready;
    if(!legacy) {
      await emit("bad");
      try{await listener.closed;throw new Error("Wrong subscription ID accepted");}catch(error){check(error.code==="ProtocolError","Wrong subscription ID was not rejected");}
    } else await listener.stop();
    return {legacy};
  } finally {await listener?.stop();}
}
