use mcpx_core::{CallOutcome, Error, Result};
use mcpx_native::{ProtocolMode, QueueOptions, QueuedRemote, ServerConfig};

#[derive(serde::Serialize)]
struct EchoInput {
    message: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let endpoint = std::env::args()
        .nth(1)
        .ok_or_else(|| Error::new("Usage", "queued <endpoint>"))?;
    let (remote, mut events) = QueuedRemote::connect_with_events(
        ServerConfig::Http {
            url: endpoint,
            protocol: ProtocolMode::Auto,
            credential: None,
        },
        QueueOptions::default(),
        mcpx_core::EventLimits::default(),
    )
    .await?;
    let listener = tokio::spawn(async move {
        while let Some(batch) = events.next_batch().await {
            eprintln!("{}", serde_json::json!({"type":"operations","batch":batch}));
        }
    });
    let tool = remote.tool("echo").await?;
    let mut callers = Vec::new();
    for index in 0..2 {
        let tool = tool.clone();
        callers.push(tokio::spawn(async move {
            tool.call_typed(EchoInput {
                message: format!("queued-{index}"),
            })
            .await
        }));
    }
    let mut results = Vec::new();
    for caller in callers {
        match caller
            .await
            .map_err(|_| Error::new("TaskFailed", "caller task failed"))??
        {
            CallOutcome::Completed(response) => results.push(response.raw),
            CallOutcome::NeedsInput(_) => {
                return Err(Error::new(
                    "NeedsInput",
                    "example requires a noninteractive echo tool",
                ));
            }
        }
    }
    remote.close().await?;
    listener
        .await
        .map_err(|_| Error::new("TaskFailed", "event listener failed"))?;
    println!(
        "{}",
        serde_json::to_string(&results).expect("serializable results")
    );
    Ok(())
}
