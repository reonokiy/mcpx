// The Rust runtime owns protocol validation; this object only drives bounded I/O.
const failure = (code, message) => Object.assign(new Error(message), {code});
const decode = value => typeof value === "string" ? JSON.parse(value) : value;
const active = status => status && ["opening", "listening"].includes(status.phase);

export function notificationRecoveryPolicy(value) {
  if(value==null)return undefined;
  if(typeof value!=="object"||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value))
    ||Object.keys(value).some(key=>!["maxAttempts","backoff","timeout"].includes(key)))throw new TypeError("Invalid notificationRecovery policy");
  const {maxAttempts,backoff,timeout}=value;
  for(const [name,n,lo,hi] of [["maxAttempts",maxAttempts,1,8],["backoff",backoff,0,60000],["timeout",timeout,1,600000]])
    if(!Number.isSafeInteger(n)||n<lo||n>hi)throw new RangeError(`notificationRecovery.${name} must be an integer in ${lo}..${hi}`);
  return Object.freeze({maxAttempts,backoff,timeout});
}
function headers(fields, get=false) {
  const result=new Headers();
  for(const [name,value] of Object.entries(fields)) {
    if(name.toLowerCase()==="last-event-id") {
      if(get)result.set(name,String.fromCharCode(...new TextEncoder().encode(value)));
    } else result.set(name,value);
  }
  return result;
}
function pause(ms,signal) {
  return new Promise((resolve,reject)=>{
    let timer;
    const abort=()=>{clearTimeout(timer);signal.removeEventListener("abort",abort);reject(failure("Cancelled","Notification recovery interrupted"));};
    if(signal.aborted){abort();return;}
    signal.addEventListener("abort",abort,{once:true});
    timer=setTimeout(()=>{signal.removeEventListener("abort",abort);resolve();},Math.min(ms,2147483647));
  });
}

export class HttpNotifications {
  constructor(owner,policy) { this.owner = owner; this.policy=policy; }
  status() { return decode(this.owner.runtime.notification_status()); }
  current(record) { const status=this.status(); return status?.resource_id===record.id ? status : undefined; }
  fail(record, timeout=false) {
    if (active(this.current(record))) this.owner.runtime.abort_notifications(record.id,timeout);
    this.owner.publish();
  }
  ensure(credential, force=false) {
    if (this.record) return this.status();
    const previous=this.status();
    if (!force && previous && !(previous.phase==="cancelled" && previous.error_code==="BindingStale")) return previous;
    let scope=decode(this.owner.runtime.catalog_status(credential.authContextId));
    if (scope.authorization_changed) return previous;
    let plan;
    try { plan=decode(this.owner.runtime.start_notifications()); }
    catch(error) { if (decodeError(error)?.code==="NotConnected") return previous; throw error; }
    this.clearTimer();this.budget=undefined;
    return this.open(plan,credential,Symbol(),this.owner.timeout);
  }
  open(plan,credential,root,timeout) {
    const health={};
    const source=plan.method==="GET"?{id:plan.resource_id,credential,root,health}:undefined;
    this.source=source;
    const record={id:plan.resource_id,plan,credential,health,read:new AbortController(),reply:new AbortController(),pending:[],deadline:performance.now()+timeout};
    this.record=record;
    record.timer=setTimeout(()=>{
      if (this.current(record)?.phase!=="opening") return;
      record.timeout=true;this.fail(record,true);record.read.abort();
    },timeout);
    record.finished=this.run(record).finally(()=>{
      clearTimeout(record.timer);
      if (this.record===record) this.record=undefined;
      // A retained control promise must not keep credentials, buffers or the owner.
      record.plan=record.credential=record.pending=record.writer=undefined;
      if(this.source===source) {
        if(record.stopped||record.writerFailed||!this.usable(source))this.source=undefined;
        this.scheduleRecovery();
      }
    });
    record.finished.catch(()=>{});
    return this.status();
  }
  info(source) {
    this.owner.tick();
    if(!source||this.source!==source)throw failure("ResumeUnavailable","No matching frozen notification credentials");
    const scope=decode(this.owner.runtime.catalog_status(source.credential.authContextId));
    if(scope.authorization_changed)throw failure("ResumeUnavailable","Notification authorization changed");
    return decode(this.owner.runtime.notification_resume(source.id));
  }
  usable(source) {
    try{this.info(source);return true;}catch(error){return decodeError(error)?.code==="Busy";}
  }
  holdResume() { this.manual=true;this.clearTimer(); }
  releaseResume() { this.manual=false;this.scheduleRecovery(); }
  async resume(id,signal,ensure,marker) {
    this.owner.tick();
    try{this.owner.runtime.notification_resume(id);}catch(error){if(decodeError(error)?.code!=="Busy")throw error;}
    const source=this.source;
    if(!source||source.id!==id)throw failure("ResumeUnavailable","No matching frozen notification credentials");
    const record=this.record;
    if(record?.id===id)await abortableRecord(record.finished,signal);
    for(;;) {
      ensure();
      const info=this.info(source);
      if(info.retry_after_ms===0)break;
      await pause(info.retry_after_ms,signal);
    }
    ensure();
    const plan=decode(this.owner.runtime.resume_notifications(id));
    marker.id=plan.resource_id;
    this.budget=undefined;
    return this.open(plan,source.credential,source.root,this.owner.timeout);
  }
  async cancelResume(marker) {
    if(marker.id!==undefined&&this.status()?.resource_id===marker.id)await this.stop();
  }
  clearTimer() { clearTimeout(this.recoveryTimer);this.recoveryTimer=undefined; }
  scheduleRecovery() {
    this.clearTimer();
    const source=this.source,policy=this.policy;
    if(this.owner.closed||!this.owner.httpNotifications||this.manual||this.record||!source||!policy)return;
    let info;try{info=this.info(source);}catch{return;}
    const now=performance.now(),health=source.health;
    if(health.opened!==undefined&&health.ended-health.opened>=policy.timeout&&this.resetId!==source.id) {
      this.budget=undefined;this.resetId=source.id;
    }
    const failedAt=health.ended??now;
    if(this.budget?.root!==source.root)this.budget={root:source.root,failedId:source.id,remaining:policy.maxAttempts,deadline:failedAt+policy.timeout,next:failedAt+policy.backoff};
    const budget=this.budget;
    if(budget.failedId!==source.id){budget.failedId=source.id;budget.next=failedAt+policy.backoff;}
    if(budget.remaining===0||now>=budget.deadline)return;
    const due=Math.max(now+info.retry_after_ms,budget.next);
    if(due>=budget.deadline)return;
    this.recoveryTimer=setTimeout(()=>{
      this.recoveryTimer=undefined;
      if(this.source!==source||this.owner.closed||!this.owner.httpNotifications||this.manual||this.record)return;
      try {
        const info=this.info(source),now=performance.now();
        if(now>=budget.deadline||budget.remaining===0)return;
        if(info.retry_after_ms>0||now<budget.next){this.scheduleRecovery();return;}
        budget.remaining--;
        const plan=decode(this.owner.runtime.resume_notifications(source.id));
        this.open(plan,source.credential,source.root,Math.min(this.owner.timeout,budget.deadline-now));
      }catch{budget.remaining=0;}
    },Math.min(Math.max(0,due-now),2147483647));
  }
  check(record) {
    if (record.stopped) throw failure("Cancelled","Notification stream stopped");
    const status=this.current(record);
    if (status?.phase==="opening" && performance.now()>=record.deadline) {
      record.timeout=true;this.fail(record,true);record.read.abort();
      throw failure("Timeout","Notification acknowledgment deadline exceeded");
    }
    return status;
  }
  observe(record) {
    const status=this.current(record);
    if (status?.phase!=="opening") clearTimeout(record.timer);
    this.owner.publish();
  }
  replies(record) {
    const status=this.current(record);
    if (record.stopped || !status || status.phase==="cancelled" || status.phase==="closed") return;
    const replies=decode(this.owner.runtime.notification_replies(record.id));
    if (record.pending.length+replies.length>16) throw failure("LimitExceeded","Notification reply queue is full");
    record.pending.push(...replies);
    this.flush(record);
  }
  flush(record) {
    if (!record.writer && record.pending.length) {
      record.writer=this.write(record).finally(()=>{ record.writer=undefined; if(!record.stopped)this.flush(record); });
      record.writer.catch(()=>{});
    }
  }
  async write(record) {
    while (record.pending.length && !record.stopped) {
      const body=JSON.stringify(record.pending.shift());
      const controller=new AbortController();
      const abort=()=>controller.abort();
      record.reply.signal.addEventListener("abort",abort,{once:true});
      if(record.reply.signal.aborted)controller.abort();
      const timer=setTimeout(abort,500);
      try {
        const fields=headers(record.plan.headers);
        fields.set("content-type","application/json");fields.set("accept","application/json, text/event-stream");
        if(record.credential.token)fields.set("authorization",`Bearer ${record.credential.token}`);
        const response=await this.owner.fetch(record.plan.url,{method:"POST",headers:fields,body,redirect:"error",signal:controller.signal});
        await response.body?.cancel();
        if(response.status!==202)throw failure("TransportError","Notification reply was rejected");
      } catch {
        record.writerFailed=true;
        record.pending.length=0;
        if(!record.stopped){this.fail(record);record.read.abort();}
        break;
      } finally {clearTimeout(timer);record.reply.signal.removeEventListener("abort",abort);}
    }
  }
  async run(record) {
    let reader,response;
    try {
      const fields=headers(record.plan.headers,true);
      if(record.credential.token)fields.set("authorization",`Bearer ${record.credential.token}`);
      response=await this.owner.fetch(record.plan.url,{method:record.plan.method,headers:fields,body:record.plan.body==null?undefined:JSON.stringify(record.plan.body),signal:record.read.signal,redirect:"error",notificationStream:true});
      this.owner.tick();this.check(record);
      this.owner.runtime.begin_notifications(record.id,response.status,response.headers.get("content-type")??"");this.observe(record);
      record.health.opened=performance.now();
      reader=response.body?.getReader();
      if(!reader)throw failure("TransportError","Notification response has no body");
      let complete=false;
      while(!complete) {
        await this.owner.waitNotificationEvents(record.read.signal);
        const chunk=await reader.read();this.owner.tick();this.check(record);
        if(chunk.done){record.health.ended=performance.now();this.owner.runtime.finish_notifications(record.id);break;}
        for(let offset=0;offset<chunk.value.byteLength;offset+=65536) {
          this.check(record);
          complete=this.owner.runtime.push_notifications(record.id,chunk.value.subarray(offset,offset+65536));
          this.observe(record);
          if(complete)break;
          this.replies(record);
        }
      }
    } catch {
      record.health.ended??=performance.now();
      if(!record.stopped)this.fail(record,Boolean(record.timeout));
    } finally {
      clearTimeout(record.timer);
      record.read.abort();
      if(reader){try{await reader.cancel();}catch{}finally{reader.releaseLock();}}
      else {try{await response?.body?.cancel();}catch{}}
      if(!record.stopped) {
        // Complete legacy requests remain answerable even after a GET disconnect.
        try{this.replies(record);}catch{this.fail(record);}
      }
      while(record.writer) await record.writer;
      record.reply.abort();
      this.owner.publish();
    }
  }
  async stop() {
    this.clearTimer();this.source=this.budget=undefined;this.resetId=undefined;
    const record=this.record;
    const status=this.status();
    if(status)this.owner.runtime.stop_notifications(status.resource_id);
    const stopped=this.status();
    if(!record)return stopped;
    record.stopped=true;record.pending.length=0;
    record.read.abort();record.reply.abort();
    await record.finished;
    return stopped;
  }
}
function decodeError(error) { try{return typeof error==="string"?JSON.parse(error):error;}catch{return undefined;} }
function abortableRecord(promise,signal) {
  let abort;
  const interrupted=new Promise((_,reject)=>{
    abort=()=>reject(failure("Cancelled","Notification recovery interrupted"));
    if(signal.aborted)abort();else signal.addEventListener("abort",abort,{once:true});
  });
  return Promise.race([promise,interrupted]).finally(()=>signal.removeEventListener("abort",abort));
}
