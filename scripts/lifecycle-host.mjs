const turn = () => new Promise(resolve => setTimeout(resolve, 0));
function check(condition, message) { if (!condition) throw new Error(message); }
async function rejects(promise, code) {
  try { await promise; } catch (error) {
    check(error.code === code || code === "RangeError" && error instanceof RangeError || code === "TypeError" && error instanceof TypeError, `Expected ${code}, got ${error.code ?? error.name}`);
    if (code === "ValidationFailed") check(error.issues.some(issue => issue.instance_path === "/message"), "Validation pointers were lost");
    if (code === "RangeError") check(error.raw.includes("18446744073709551615"), "Unsafe-number raw JSON was lost");
    return;
  }
  throw new Error(`Expected ${code}`);
}

async function prepare(open, legacy) {
  // Each retained promise used to keep its cancellation closure, job and owner.
  const provider = async () => ({ authContextId: "lifecycle-owner" });
  const operations=[];
  const { remote, dispose } = await open({ credentialProvider: provider, maxInFlight: 1,onOperationEvent:event=>operations.push(event) });
  const retained = [];
  const catalog = remote.subscribeCatalog();
  const catalogRead = catalog.next();
  try {
    const binding = await remote.freshTool("echo");
    const success = binding.call({ message: "lifecycle-success" });
    retained.push(success);
    const outcome = await success;
    const positive = new WeakRef(outcome);
    retained.push(binding.callRaw('{"message":"lifecycle-raw"}'));
    check((await retained.at(-1)).includes("lifecycle-raw"), "Raw result changed");
    retained.push(binding.call({ message: 3 }));
    await rejects(retained.at(-1), "ValidationFailed");
    retained.push(binding.call({ message: "raw-number" }));
    await rejects(retained.at(-1), "RangeError");
    retained.push(binding.call({ message: 1n }));
    await rejects(retained.at(-1), "TypeError");
    retained.push(remote.executeLine("echo --help"));
    check((await retained.at(-1)).kind === "help", "Help failed");
    retained.push(remote.completeLine("ec", 2));
    check((await retained.at(-1)).candidates[0].value === "echo", "Metadata failed");
    retained.push(remote.refresh()); await retained.at(-1);
    const active = binding.call({ message: "slow-lifecycle-cancel" });
    retained.push(active);
    const cancelled = rejects(active, "Cancelled");
    const queued = remote.executeLine("echo --message lifecycle-never-sent");
    retained.push(queued);
    const unsent = rejects(queued, "Cancelled");
    queued.cancel();
    await new Promise(resolve => setTimeout(resolve, 20));
    active.cancel(); await Promise.all([cancelled, unsent]);
    const timeout = binding.call({ message: "slow-lifecycle-timeout" }, { timeout: 30 });
    retained.push(timeout); await rejects(timeout, "Timeout");
    if (!legacy) {
      const pending = binding.call({ message: "interactive" });
      retained.push(pending);
      const input = await pending;
      const resumed = remote.resume(input.value, {confirm:{action:"accept",content:{yes:true}}});
      retained.push(resumed);
      check((await resumed).value.raw.structuredContent.message === "interactive", "Resume failed");
    }
    await binding.dispose();
    for(const operation of [()=>remote.notificationStatus(),()=>remote.startNotifications(),()=>remote.stopNotifications()]) {
      retained.push(operation());await retained.at(-1);
    }
    await remote.close();
    retained.push(remote.executeLine("echo --message lifecycle-after-close"));
    await rejects(retained.at(-1), "TransportClosed");
    // A settled promise must be safe even after the explicit low-level runtime
    // was freed. Only its own result/error and operation ID may remain live.
    for (const promise of retained) {
      if (promise.cancel) {
        check(Number.isSafeInteger(promise.operationId), "Operation identity was lost");
        promise.cancel(); promise.cancel();
      }
    }
    await catalogRead;
    while (catalog.tryNext()?.done === false) {}
    check((await catalog.next()).done, "Closed catalog subscription retained a pending read");
    return { catalog, catalogRead, operations, closed:remote, retained, positive, weak: [{name:"remote",ref:new WeakRef(remote)}, {name:"provider",ref:new WeakRef(provider)}] };
  } finally { await remote.close(); dispose(); }
}

export async function verifySettlement(open, collect, { legacy = false } = {}) {
  const state = await prepare(open, legacy);
  check(state.operations.length>0,"Lifecycle observer was not exercised");
  check(state.operations.every(event=>!("raw" in event.payload)),"Lifecycle journal retained a raw result");
  // WeakRef dereference keeps targets alive through the current JS job. Yield
  // before every collection rather than repeatedly collecting in one job.
  const released = async probes => {
    for (let attempt=0; attempt<100; attempt++) {
      await turn(); await collect(); await turn();
      if (probes.every(({ref}) => ref.deref() === undefined)) return;
    }
    throw new Error(`Settled promises retained ${probes.filter(({ref}) => ref.deref()).map(({name}) => name).join(", ")}`);
  };
  // close must release the configured provider even while the application
  // deliberately keeps its closed Remote object and all settled promises.
  await released(state.weak.filter(({name})=>name === "provider"));
  state.closed = undefined;
  await released(state.weak);
  check((await state.catalog.next()).done, "Retained catalog watcher did not remain closed after owner GC");
  check(state.positive.deref() !== undefined, "GC control lost a result still owned by a promise");
  check((await state.retained[0]).value.raw.structuredContent.message === "lifecycle-success", "Settled promise lost its result");
  for (const promise of state.retained) promise.cancel?.();
  return { retained: state.retained.length, collected: state.weak.map(({name}) => name) };
}
