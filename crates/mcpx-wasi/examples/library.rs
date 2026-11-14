#[cfg(all(target_os = "wasi", target_env = "p2"))]
fn main() -> mcpx_core::Result<()> {
    use mcpx_command::{ClientCommandExt, Execution};
    use mcpx_core::{CallOutcome, Client, Credential, Dialect, HostRuntime};
    use mcpx_wasi::{WasiDriver, run_blocking};
    use serde::Serialize;
    use serde_json::json;
    use std::time::Duration;
    #[derive(Serialize)]
    struct Echo<'a> {
        message: &'a str,
    }
    #[derive(serde::Deserialize, Serialize)]
    struct EchoResult {
        message: String,
    }
    run_blocking(async {
        let mut args = std::env::args().skip(1);
        let endpoint = args.next().expect("endpoint argument");
        let legacy = args.next().as_deref() == Some("legacy");
        let mut runtime = HostRuntime::with_dialect(
            endpoint,
            if legacy {
                Dialect::Legacy
            } else {
                Dialect::Modern
            },
        )?;
        runtime.set_elicitation(true, false)?;
        let driver = WasiDriver::new(Credential::anonymous(), Duration::from_secs(5))?;
        let mut remote = Client::new(driver, runtime);
        let result=async {
            let binding=remote.fresh_tool("echo").await?;
            let args=binding.args().set("message","wasi-typed")?.finish()?;
            let typed=remote.call(&binding,&args).await?;
            let serde=remote.call_typed(&binding,Echo {message:"wasi-serde"}).await?;
            let decoded=match &serde {CallOutcome::Completed(response)=>Some(response.deserialize::<EchoResult>()?),CallOutcome::NeedsInput(_)=>None};
            let string=remote.execute("echo","--message 'wasi string'").await?;
            let argv=remote.execute_argv("echo",&["--message".into(),"wasi argv".into()]).await?;
            let unwrap=|execution|match execution {Execution::Outcome(value)=>value,_=>panic!("expected a call")};
            let resumed=if legacy {None} else {
                let CallOutcome::NeedsInput(handle)=remote.call_json(&binding,json!({"message":"interactive"})).await? else {panic!("expected an interaction")};
                Some(remote.resume(&handle,json!({"confirm":{"action":"accept","content":{"yes":true}}})).await?)
            };
            println!("{}",json!({"typed":typed,"serde":serde,"decoded":decoded,"string":unwrap(string),"argv":unwrap(argv),"resumed":resumed}));
            Ok(())
        }.await;
        let closed = match remote.close().await {
            Err(error) if error.code == "SessionCleanupFailed" => remote.close().await,
            result => result,
        };
        result?;
        closed
    })
}
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("Build this example with --target wasm32-wasip2");
}
