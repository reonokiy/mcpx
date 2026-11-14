use mcpx_core::{CallOutcome, Dialect, OutputValidation};
use mcpx_native::{Client, Engine, Execution};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn server(legacy: bool, sse: bool) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        let count = if legacy { 5 } else { 4 };
        for _ in 0..count {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (header_end, content_length) = loop {
                let mut chunk = [0; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                assert_ne!(n, 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let text = String::from_utf8_lossy(&bytes[..end]);
                    let length = text
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|v| v.parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end + 4, length);
                }
            };
            while bytes.len() < header_end + content_length {
                let mut chunk = [0; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                assert_ne!(n, 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let request: Value =
                serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap();
            let method = request["method"].as_str().unwrap();
            let result = match method {
                "initialize" => json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}}}),
                "server/discover" => {
                    json!({"supportedVersions":["2026-07-28"],"capabilities":{"tools":{}}})
                }
                "notifications/initialized" => Value::Null,
                "tools/list" => {
                    json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false},"outputSchema":{"type":"object","required":["message"]}}]})
                }
                "tools/call" => {
                    json!({"content":[{"type":"text","text":request["params"]["arguments"]["message"]}],"structuredContent":request["params"]["arguments"]})
                }
                _ => panic!("unexpected request"),
            };
            let (status, mime, body) = if method == "notifications/initialized" {
                (202, "application/json", String::new())
            } else {
                let response = json!({"jsonrpc":"2.0","id":request["id"],"result":result});
                if method == "tools/call" && sse {
                    (
                        200,
                        "text/event-stream",
                        format!(
                            "data: {{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{{\"progress\":1}}}}\n\ndata: {response}\n\n"
                        ),
                    )
                } else {
                    (200, "application/json", response.to_string())
                }
            };
            let headers = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            for bytes in body.as_bytes().chunks(7) {
                socket.write_all(bytes).await.unwrap();
            }
            requests.push(request);
        }
        requests
    });
    (endpoint, task)
}

async fn roundtrip(legacy: bool, sse: bool) {
    let (endpoint, server) = server(legacy, sse).await;
    let mut remote = Client::new()
        .unwrap()
        .connect(
            &endpoint,
            if legacy {
                Dialect::Legacy
            } else {
                Dialect::Modern
            },
        )
        .await
        .unwrap();
    let binding = remote.tool("echo").unwrap();
    assert!(
        remote
            .call_json(&binding, json!({"message":10}))
            .await
            .is_err()
    );
    let args = binding
        .args()
        .set("message", "hello")
        .unwrap()
        .finish()
        .unwrap();
    let CallOutcome::Completed(response) = remote.call(&binding, &args).await.unwrap() else {
        panic!()
    };
    assert!(matches!(
        response.output_validation,
        OutputValidation::Valid
    ));
    assert_eq!(response.raw["structuredContent"]["message"], "hello");
    let mut engine = Engine::new();
    engine.register("test", remote).unwrap();
    let Execution::Outcome(CallOutcome::Completed(response)) = engine
        .execute("test echo --message 'command value'")
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        response.raw["structuredContent"]["message"],
        "command value"
    );
    let requests = server.await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "tools/call")
            .count(),
        2
    );
    if sse {
        assert!(
            engine
                .remote_mut("test")
                .unwrap()
                .drain_events()
                .events
                .iter()
                .any(|e| e.kind == "progress")
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn modern_http_json() {
    roundtrip(false, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn modern_http_sse() {
    roundtrip(false, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn legacy_http_sse() {
    roundtrip(true, true).await;
}
