use mcpx_core::Dialect;
use mcpx_native::{Client, EventStreamItem};

#[derive(serde::Serialize)]
struct EchoInput<'a> {
    message: &'a str,
}
#[derive(serde::Deserialize)]
struct EchoResult {
    message: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::args()
        .nth(1)
        .ok_or("usage: library HTTP_MCP_ENDPOINT [MESSAGE]")?;
    let message = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "hello from Rust".into());
    let (client, mut events) = Client::new()?.with_event_stream(32)?;
    let consumer = tokio::spawn(async move {
        while let Some(item) = events.next().await {
            match item {
                EventStreamItem::Event(event) => {
                    eprintln!("operation {}: {}", event.operation_id, event.kind)
                }
                EventStreamItem::Lagged { dropped } => {
                    eprintln!("skipped {dropped} progress/log records")
                }
            }
        }
    });
    let mut remote = client.connect(&endpoint, Dialect::Auto).await?;
    let result = async {
        let echo = remote.fresh_tool("echo").await?;
        let args = echo.args_from(EchoInput { message: &message })?;
        remote
            .call_checked(&echo, &args)
            .await
            .map_err(|error| error.error)
    }
    .await;
    let shutdown = remote.close().await;
    consumer.await?;
    let response = result?;
    shutdown?;
    let decoded: EchoResult = response.deserialize()?;
    eprintln!("decoded {} bytes of message text", decoded.message.len());
    println!("{}", serde_json::to_string_pretty(&response.raw)?);
    Ok(())
}
