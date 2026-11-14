import { abortable } from "./credentials.js";

const failure=(code,message)=>Object.assign(new Error(message),{code});
const decode=value=>typeof value==="string"?JSON.parse(value):value;
const errorCode=error=>{try{return decode(error)?.code;}catch{return undefined;}};

export function responseRecoveryPolicy(value) {
  if(value==null)return undefined;
  if(typeof value!=="object"||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value))
    ||Object.keys(value).some(key=>!["maxAttempts","backoff","timeout"].includes(key)))throw new TypeError("Invalid responseRecovery policy");
  const {maxAttempts,backoff,timeout}=value;
  for(const [name,n,lo,hi] of [["maxAttempts",maxAttempts,1,8],["backoff",backoff,0,60000],["timeout",timeout,1,600000]])
    if(!Number.isSafeInteger(n)||n<lo||n>hi)throw new RangeError(`responseRecovery.${name} must be an integer in ${lo}..${hi}`);
  return Object.freeze({maxAttempts,backoff,timeout});
}
export const resumableResponse=action=>action.rpc_method==="tools/call"&&action.headers?.["mcp-protocol-version"]==="2025-11-25";
function fields(values,get=false) {
  const headers=new Headers();
  for(const [name,value]of Object.entries(values)) {
    if(name.toLowerCase()==="last-event-id") {
      if(get)headers.set(name,String.fromCharCode(...new TextEncoder().encode(value)));
    }else headers.set(name,value);
  }
  return headers;
}
function pause(ms,signal) {
  return new Promise((resolve,reject)=>{
    let timer;
    const abort=()=>{clearTimeout(timer);signal.removeEventListener("abort",abort);reject(failure("Cancelled","Response recovery interrupted"));};
    if(signal.aborted){abort();return;}
    signal.addEventListener("abort",abort,{once:true});
    timer=setTimeout(()=>{signal.removeEventListener("abort",abort);resolve();},Math.min(Math.max(0,ms),2147483647));
  });
}

// One original call owns these I/O tasks. Rust retains all MCP state, JSON and
// cursor validation. No promise returned to callers retains this owner on settle.
export class ToolResponse {
  constructor(owner,job,action,policy) {
    this.owner=owner;this.job=job;this.original=action;this.id=action.id;this.policy=policy;
    this.fetch=owner.fetch;this.credential=job.credential;
    this.pending=[];this.reply=new AbortController();this.stopping=new AbortController();
    this.closed=new Promise(resolve=>this.resolveClosed=resolve);
    this.abort=()=>{void this.stop();};job.controller.signal.addEventListener("abort",this.abort,{once:true});
    job.cancelAction=action;
    job.cancelFetch=this.fetch;
  }
  check() {
    if(this.job.accepted)return;
    this.owner.ensure(this.job);
    if(this.budget&&performance.now()>=this.budget.deadline) {
      this.owner.cancelJob(this.job,failure("Timeout","Response recovery window exceeded; execution may be unknown"));
      throw this.job.cancelCause;
    }
    if(this.writerError)throw this.writerError;
    if(this.stopping.signal.aborted)throw this.job.cancelCause??failure("TransportClosed","Response owner is closing");
  }
  observe() {
    this.owner.collect(this.job);
    if(this.job.accepted&&!this.cleanupDeadline) {
      this.cleanupDeadline=performance.now()+500;
      this.cleanupTimer=setTimeout(()=>this.reply.abort(),500);
    }
    let replies;
    try{replies=decode(this.owner.runtime.response_replies(this.id));}
    catch(error){if(errorCode(error)!=="InvalidHandle")throw error;replies=[];}
    if(!this.job.accepted?.error) {
      if(this.pending.length+replies.length>16)throw failure("LimitExceeded","Response reply queue exceeded");
      this.pending.push(...replies);this.flush();
    }
    this.owner.publish();
  }
  flush() {
    if(!this.writer&&this.pending.length&&!this.stopping.signal.aborted) {
      this.writer=this.write().finally(()=>{this.writer=undefined;});
      this.writer.catch(()=>{});
    }
  }
  async write() {
    while(this.pending.length&&!this.stopping.signal.aborted) {
      const body=JSON.stringify(this.pending.shift()),controller=new AbortController();
      const abort=()=>controller.abort();this.reply.signal.addEventListener("abort",abort,{once:true});
      if(this.reply.signal.aborted)controller.abort();
      const deadline=Math.min(performance.now()+500,this.cleanupDeadline??Math.min(this.job.deadline,this.budget?.deadline??Infinity));
      const timer=setTimeout(abort,Math.max(0,deadline-performance.now()));
      try {
        const headers=fields(this.original.headers);headers.set("content-type","application/json");headers.set("accept","application/json, text/event-stream");
        if(this.credential.token)headers.set("authorization",`Bearer ${this.credential.token}`);
        const response=await this.fetch(this.original.url,{method:"POST",headers,body,redirect:"error",signal:controller.signal});
        await response.body?.cancel();
        if(response.status!==202)throw failure("HostError","Response capability reply rejected");
      }catch {
        this.pending.length=0;
        if(!this.job.accepted&&!this.stopping.signal.aborted) {
          this.writerError=failure("HostError","Response capability reply failed");this.read?.abort();
        }
        break;
      }finally{clearTimeout(timer);this.reply.signal.removeEventListener("abort",abort);}
    }
  }
  async readOnce(plan) {
    this.check();this.read=new AbortController();
    let response,reader;
    try {
      const headers=fields(plan.headers,plan.method==="GET");
      if(this.credential.token)headers.set("authorization",`Bearer ${this.credential.token}`);
      this.job.sent=true;
      try{response=await this.fetch(plan.url,{method:plan.method??"POST",headers,body:plan.method==="GET"?undefined:plan.body,redirect:"error",signal:this.read.signal});}
      catch(error){this.check();return {ended:performance.now(),error};}
      this.check();this.owner.tick();
      const done=this.owner.runtime.begin_response(this.id,response.status,JSON.stringify(Object.fromEntries(response.headers)));
      if(response.headers.has("mcp-session-id"))this.owner.session={id:response.headers.get("mcp-session-id"),credential:this.credential};
      this.observe();if(done)return;
      reader=response.body?.getReader();if(!reader)throw failure("HostError","Missing response body");
      for(;;) {
        this.check();
        try{await abortable(Promise.resolve(this.owner.eventGate(this.read.signal)),this.read.signal);}
        catch(error){this.check();throw error;}
        this.check();
        let chunk;
        try{chunk=await reader.read();}catch(error){this.check();return {ended:performance.now(),error};}
        const ended=chunk.done?performance.now():undefined;
        this.check();this.owner.tick();if(chunk.done)return {ended};
        for(let offset=0;offset<chunk.value.byteLength;offset+=65536) {
          this.check();
          const complete=this.owner.runtime.push_response_for(this.id,chunk.value.subarray(offset,offset+65536));
          // Collect before callbacks can cancel a final result in this chunk.
          this.observe();if(complete)return;
        }
      }
    }finally{
      this.read.abort();
      if(reader){try{await reader.cancel();}catch{}finally{reader.releaseLock();}}
      else {try{await response?.body?.cancel();}catch{}}
      this.read=undefined;
    }
  }
  async drain() {while(this.writer){await this.writer;this.check();this.flush();}}
  async run() {
    try {
      this.owner.runtime.enable_response_resumption(this.id);
      let plan=this.original;
      while(!this.job.accepted) {
        const disconnected=await this.readOnce(plan);
        if(this.job.accepted)break;
        if(!disconnected)break;
        this.check();this.owner.tick();
        try{this.owner.runtime.interrupt_response_for(this.id);}
        catch {
          if(disconnected.error)throw disconnected.error;
          this.owner.runtime.finish_response_for(this.id);this.observe();break;
        }
        const ended=disconnected.ended;
        if(!this.budget) {
          this.budget={deadline:Math.min(this.job.deadline,ended+this.policy.timeout),remaining:this.policy.maxAttempts};
          this.windowTimer=setTimeout(()=>this.owner.cancelJob(this.job,failure("Timeout","Response recovery window exceeded; execution may be unknown")),Math.max(0,this.budget.deadline-performance.now()));
        }
        const next=ended+this.policy.backoff;
        this.observe();await this.drain();
        for(;;) {
          this.check();this.owner.tick();
          const info=decode(this.owner.runtime.response_resume(this.id));
          if(this.budget.remaining===0) {
            this.owner.runtime.exhaust_response_resumption(this.id);this.observe();
            await this.owner.cancelWire(this.job);return;
          }
          const now=performance.now(),due=Math.max(now+info.retry_after_ms,next);
          if(due>=this.budget.deadline){await pause(this.budget.deadline-now+1,this.stopping.signal);this.check();continue;}
          if(due>now){await pause(due-now,this.stopping.signal);continue;}
          break;
        }
        this.check();this.owner.tick();plan=decode(this.owner.runtime.resume_response(this.id));
        this.id=plan.action_id;this.job.action={...this.original,id:this.id};this.budget.remaining--;
      }
      await this.drain();
    }finally{
      clearTimeout(this.windowTimer);
      if(!this.job.accepted||this.job.accepted.error)this.reply.abort();
      this.pending.length=0;
      if(this.writer)await this.writer;
      clearTimeout(this.cleanupTimer);this.reply.abort();this.stopping.abort();
      this.job.controller.signal.removeEventListener("abort",this.abort);
      this.resolveClosed();
    }
  }
  stop() {
    this.stopping.abort();this.read?.abort();this.reply.abort();this.pending.length=0;
    return this.closed;
  }
}
