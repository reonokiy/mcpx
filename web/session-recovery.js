import {abortable} from "./credentials.js";
import {contextError} from "./errors.js";

const failure=(code,message)=>Object.assign(new Error(message),{code});
const decode=value=>typeof value==="string"?JSON.parse(value):value;
const errorOf=value=>contextError(value,{certainty:"not_sent"});

export function httpSessionRecoveryPolicy(value) {
  if(value==null)return undefined;
  if(typeof value!=="object"||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value))
    ||Object.keys(value).some(key=>!["maxAttempts","backoff","timeout"].includes(key)))throw new TypeError("Invalid httpSessionRecovery policy");
  const {maxAttempts,backoff,timeout}=value;
  for(const [name,n,lo,hi]of [["maxAttempts",maxAttempts,1,8],["backoff",backoff,0,60000],["timeout",timeout,1,600000]])
    if(!Number.isSafeInteger(n)||n<lo||n>hi)throw new RangeError(`httpSessionRecovery.${name} must be an integer in ${lo}..${hi}`);
  return Object.freeze({maxAttempts,backoff,timeout});
}
const WAIT_FOR_SESSION=Symbol("wait for session renewal");
function pause(ms,signal) {
  let timer;
  return abortable(new Promise(resolve=>{timer=setTimeout(resolve,ms);}),signal).finally(()=>clearTimeout(timer));
}

// One connection owner manages recovery; business jobs keep their original IDs,
// timers, input reservations and cancellation. No tools/call is ever resubmitted.
export class HttpSessionRecovery {
  constructor(owner,policy) {
    this.owner=owner;this.policy=policy;
    if(policy&&typeof owner.runtime.http_session_status!=="function")throw new TypeError("Rebuild the runtime and adapter for HTTP session recovery");
  }
  observe() {
    if(!this.policy||this.owner.closed)return;
    const status=decode(this.owner.runtime.http_session_status());
    const changed=this.lastResult!==status.last_tool_result;
    this.lastResult=status.last_tool_result;
    if(changed&&status.expired_at_ms==null&&!this.pending)this.budget=undefined;
    if(status.expired_at_ms==null||this.pending||this.owner.suspended)return;
    const now=performance.now();
    if(this.budget&&now-this.budget.healthySince>=this.policy.timeout)this.budget=undefined;
    this.budget??={remaining:this.policy.maxAttempts,deadline:this.owner.clockOrigin+status.expired_at_ms+this.policy.timeout,healthySince:now};
    this.pending=true;
    // Let the I/O that observed expiry accept its own original terminal first.
    this.finished=Promise.resolve().then(()=>this.run());
    this.finished.catch(()=>{});
  }
  wait(job) {
    this.observe();
    if(this.pending&&!job.cleanup&&!job.internalRecovery)throw WAIT_FOR_SESSION;
  }
  defer(job) {
    return this.pending&&!job.cleanup&&!job.reconnect&&!job.internalRecovery&&!job.cancelCause&&!job.accepted
      &&job.coreId===undefined&&(!job.started||job.metadata||job.connect);
  }
  async run() {
    const owner=this.owner,policy=this.policy,budget=this.budget;
    const job=this.job={internalRecovery:true,automatic:true,reconnect:true,connect:true,metadata:true,deadline:Math.min(budget.deadline,this.origin?.deadline??Infinity),controller:new AbortController()};
    const interrupt=code=>{job.cancelCause??=failure(code,"HTTP session renewal interrupted");job.controller.abort();};
    job.timer=setTimeout(()=>interrupt("Timeout"),Math.max(0,job.deadline-performance.now()));
    if(this.originCancelled)interrupt(this.originCancelled.code);
    let unlock;
    try {
      owner.ensure(job);
      const stopping=owner.notifications.stop();
      unlock=await owner.admission.acquire(job.controller.signal);
      owner.ensure(job);
      const peers=[...owner.active].filter(peer=>!peer.cleanup&&(peer.started||peer.metadata||peer.sent));
      await abortable(Promise.allSettled([stopping,...peers.map(peer=>peer.promise)]),job.controller.signal);
      owner.ensure(job);
      for(;;) {
        owner.ensure(job);
        if(!budget.remaining)throw failure("ReconnectExhausted","HTTP session renewal rounds exhausted");
        const delayed=budget.remaining<policy.maxAttempts;
        budget.remaining--;
        if(delayed)await pause(policy.backoff,job.controller.signal);
        owner.ensure(job);
        try {
          await owner.resetTransport(job);
          job.credential=await owner.credentials.resolve(owner.runtime.endpoint(),job.controller.signal);
          owner.ensure(job);owner.tick();
          owner.runtime.accept_authorization(job.credential.authContextId);
          owner.runtime.discover();
          while(owner.runtime.busy()) {
            owner.ensure(job);
            const actions=decode(owner.runtime.actions()).filter(action=>action.kind!=="cancel");
            if(!actions.length)throw failure("HostError","Session discovery has no host action");
            for(const action of actions){job.action=action;await owner.io(job,action);}
          }
          owner.ensure(job);
          budget.healthySince=performance.now();
          owner.suspended=false;
          for(const pending of owner.queue)if(pending.connect&&!pending.reconnect){pending.recoveredConnect=true;pending.credential=job.credential;}
          owner.ensureNotifications(job.credential,true);
          return;
        }catch(value) {
          const error=errorOf(value);
          if(owner.runtime.busy())owner.runtime.fail("Session discovery failed");
          owner.publish();
          if(job.controller.signal.aborted)throw job.cancelCause??error;
          if(!budget.remaining||!["SessionExpired","TransportError","TransportClosed","Timeout"].includes(error.code))throw error;
        }
      }
    }catch(value) {
      const cause=job.cancelCause??errorOf(value);
      owner.suspended=true;
      // Drain expiry is bounded too. A previously accepted result still wins.
      for(const peer of [...owner.active])if(!peer.cleanup)
        owner.cancelJob(peer,peer.coreId!==undefined?cause:failure("TransportClosed","HTTP session renewal failed before admission"));
      for(const pending of [...owner.queue])if(!pending.cleanup&&!pending.reconnect)
        owner.cancelJob(pending,pending.connect?cause:failure("TransportClosed","HTTP session renewal failed; reconnect explicitly"));
    }finally {
      clearTimeout(job.timer);
      if(owner.runtime.busy()&&owner.suspended)owner.runtime.fail("Session renewal interrupted");
      if(owner.reconnectJob===job)owner.reconnectJob=undefined;
      this.job=this.origin=this.originCancelled=undefined;this.pending=false;
      unlock?.();owner.publish();owner.schedule();
    }
  }
  stop() {
    if(this.job){this.job.cancelCause=failure("TransportClosed","Remote closed during session renewal");this.job.controller.abort();}
    return this.finished;
  }
  cancelConnect(job,cause) {
    if(this.origin!==job)return;
    this.originCancelled=cause;
    if(this.job){this.job.cancelCause=cause;this.job.controller.abort();}
  }
  reconnected() {
    // Time spent suspended is not evidence of a healthy new connection.
    // Manual metadata success starts health observation without adding rounds.
    if(this.budget)this.budget.healthySince=performance.now();
  }
}
