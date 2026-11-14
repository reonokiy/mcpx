// Explicit host for a runtime without a high-level Remote owner.
const parse=value=>typeof value==="string"?JSON.parse(value):value;
const check=(ok,message)=>{if(!ok)throw new Error(message);};
const code=e=>typeof e==="string"?JSON.parse(e).code:e.code;
function fails(fn,expected){try{fn();}catch(error){check(code(error)===expected,`Expected ${expected}, got ${code(error)}`);return;}throw new Error(`Expected ${expected}`);}
export async function runResponseResume(runtime,endpoint,legacy,{sdk=false}={}) {
  let frozen="Bearer response-frozen",rpcs=0;
  const headers=(fields,get=false)=>{
    const result=new Headers();
    for(const [name,value]of Object.entries(fields)) {
      if(name.toLowerCase()==="last-event-id"){if(get)result.set(name,String.fromCharCode(...new TextEncoder().encode(value)));}
      else result.set(name,value);
    }
    result.set("authorization",frozen);return result;
  };
  const origin=performance.now();const tick=()=>runtime.advance_time(Math.floor(performance.now()-origin));
  async function read(action,enable=false) {
    if(enable)runtime.enable_response_resumption(action.id);
    const response=await fetch(action.url,{method:action.method??"POST",headers:headers(action.headers,action.method==="GET"),body:action.body??undefined,redirect:"error"});
    tick();if(runtime.begin_response(action.id,response.status,JSON.stringify(Object.fromEntries(response.headers)))) {await response.body?.cancel();return true;}
    const reader=response.body.getReader();let complete=false;
    try {
      for(;;){const part=await reader.read();tick();if(part.done)break;
        for(let offset=0;offset<part.value.length;offset+=65536){complete=runtime.push_response_for(action.id,part.value.subarray(offset,offset+65536));if(complete)break;}
        if(complete)break;
      }
    }finally{await reader.cancel().catch(()=>{});reader.releaseLock();}
    if(!enable&&!action.resumed) {if(!complete)runtime.finish_response_for(action.id);return complete;}
    if(!complete)runtime.interrupt_response_for(action.id);
    if(!complete&&!sdk)fails(()=>runtime.response_resume(action.id),"Busy");
    for(const reply of parse(runtime.response_replies(action.id))) {
      const fields={...action.headers,"content-type":"application/json",accept:"application/json, text/event-stream"};
      const response=await fetch(action.url,{method:"POST",headers:headers(fields),body:JSON.stringify(reply),redirect:"error"});
      check(response.status===202,"Accepted capability reply failed");await response.body?.cancel();
    }
    return complete;
  }
  runtime.accept_authorization("response-scope");runtime.discover();
  while(runtime.busy())for(const action of parse(runtime.actions()))await read(action);
  runtime.events();
  const binding=runtime.bind("echo"),operation=Number(runtime.call_concurrent(binding,JSON.stringify({message:sdk?"sdk-response-resume":"resumable"})));
  const [original]=parse(runtime.actions());check(original.body&&JSON.parse(original.body).method==="tools/call","Missing original business POST");rpcs++;
  if(!legacy) {
    fails(()=>runtime.enable_response_resumption(original.id),"ResumeUnavailable");
    try{await read(original);}catch(error){check(code(error)==="ProtocolError","Wrong modern EOF error");}
    fails(()=>runtime.completion(operation),"ProtocolError");
    runtime.release_binding(binding);return;
  }
  let current=original;check(!await read(current,true),"Expected initial SSE disconnection");
  const old=current.id;
  for(let attempt=0;attempt<4;attempt++) {
    tick();const info=parse(runtime.response_resume(current.id));check(info.operation_id===operation,"Operation changed during response resume");
    if(info.retry_after_ms>0)fails(()=>runtime.resume_response(current.id),"RetryLater");
    await new Promise(resolve=>setTimeout(resolve,info.retry_after_ms+1));tick();
    const plan=parse(runtime.resume_response(current.id));check(plan.method==="GET"&&plan.body==null,"Response recovery replayed a POST");
    check(plan.action_id!==current.id&&plan.rpc_id===info.rpc_id&&plan.operation_id===operation,"Response identity changed");
    fails(()=>runtime.begin_response(current.id,200,'{"content-type":"text/event-stream"}'),"UnexpectedResponse");
    current={...plan,id:plan.action_id,resumed:true};
    if(await read(current))break;
    check(attempt<3,"Response resume did not finish");
  }
  check(rpcs===1&&parse(runtime.actions()).length===0,"Generated a new business action");
  fails(()=>runtime.response_resume(old),"InvalidHandle");
  const raw=runtime.completion(operation);
  if(!sdk)check(raw.includes("18446744073709551615"),"Resumed result lost uint64 precision");
  const outcome=parse(raw);check(outcome.kind==="completed"&&outcome.value.operation_id===operation,"Lost original operation result");
  const events=parse(runtime.events());check(events.events.filter(e=>["completed","tool_failed","failed","cancelled"].includes(e.kind)).length===1,"Duplicate/missing original terminal");
  runtime.release_binding(binding);
  // Session termination uses the same frozen credential snapshot as initialize.
  if(legacy&&!sdk){const response=await fetch(endpoint,{method:"DELETE",headers:headers({"mcp-session-id":"response-session"}),redirect:"error"});check(response.status===204,"Session cleanup failed");await response.body?.cancel();}
}
