use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use mcpx_auth::{Grant, ServerMetadata, Tokens};
use mcpx_core::{CallOutcome, CredentialProvider, CredentialRequest};
use mcpx_native::{
    Client, ProtocolMode, QueueOptions, QueuedRemote, ServerConfig, oauth::OAuthCredentialProvider,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
struct Fixture {
    origin: String,
    refresh_barrier: tokio::sync::Barrier,
    tool_barrier: tokio::sync::Barrier,
    alice: AtomicUsize,
    bob: AtomicUsize,
    calls: std::sync::Mutex<Vec<String>>,
}
async fn token(State(f): State<Arc<Fixture>>, body: String) -> Json<Value> {
    let form: HashMap<_, _> = url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect();
    assert_eq!(form["grant_type"], "refresh_token");
    assert_eq!(form["resource"], format!("{}/mcp", f.origin));
    let user = match form["refresh_token"].as_str() {
        "alice-r0" => {
            f.alice.fetch_add(1, Ordering::SeqCst);
            "alice"
        }
        "bob-r0" => {
            f.bob.fetch_add(1, Ordering::SeqCst);
            "bob"
        }
        _ => panic!("refresh reused or mixed across users"),
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), f.refresh_barrier.wait())
        .await
        .unwrap();
    Json(
        json!({"access_token":format!("{user}-access"),"refresh_token":format!("{user}-r1"),"expires_in":3600,"scope":"tools.read","token_type":"Bearer"}),
    )
}
async fn mcp(
    State(f): State<Arc<Fixture>>,
    headers: HeaderMap,
    Json(rpc): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let user = match headers.get("authorization").and_then(|v| v.to_str().ok()) {
        Some("Bearer alice-access") => "alice",
        Some("Bearer bob-access") => "bob",
        _ => return Err(StatusCode::UNAUTHORIZED),
    };
    let result = match rpc["method"].as_str().unwrap() {
        "server/discover" => {
            json!({"supportedVersions":["2026-07-28"],"capabilities":{"tools":{}}})
        }
        "tools/list" => {
            json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"message":{"type":if user=="alice"{"string"}else{"integer"}}},"required":["message"]}}]})
        }
        "tools/call" => {
            let value = &rpc["params"]["arguments"]["message"];
            if user == "alice" {
                assert!(value.is_string());
            } else {
                assert!(value.is_i64());
            }
            f.calls.lock().unwrap().push(user.into());
            tokio::time::timeout(std::time::Duration::from_secs(5), f.tool_barrier.wait())
                .await
                .unwrap();
            json!({"content":[],"structuredContent":{"user":user,"message":value}})
        }
        _ => panic!("unexpected MCP request"),
    };
    Ok(Json(
        json!({"jsonrpc":"2.0","id":rpc["id"],"result":result}),
    ))
}
fn grant(origin: &str, user: &str) -> Grant {
    Grant {
        resource: format!("{origin}/mcp"),
        context_id: format!("{user}-grant"),
        client_id: format!("{user}-client"),
        server: ServerMetadata {
            issuer: origin.into(),
            authorization_endpoint: format!("{origin}/authorize"),
            token_endpoint: format!("{origin}/token"),
            registration_endpoint: None,
            revocation_endpoint: None,
            code_challenge_methods_supported: vec!["S256".into()],
            client_id_metadata_document_supported: false,
            authorization_response_iss_parameter_supported: false,
        },
        tokens: Tokens {
            access_token: format!("{user}-expired"),
            refresh_token: Some(format!("{user}-r0")),
            expires_at: Some(1),
            scopes: vec!["tools.read".into()],
        },
    }
}
#[tokio::test]
async fn different_users_run_concurrently_and_clones_refresh_only_their_own_grant() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let endpoint = format!("{origin}/mcp");
    let fixture = Arc::new(Fixture {
        origin: origin.clone(),
        refresh_barrier: tokio::sync::Barrier::new(2),
        tool_barrier: tokio::sync::Barrier::new(2),
        alice: AtomicUsize::new(0),
        bob: AtomicUsize::new(0),
        calls: std::sync::Mutex::new(vec![]),
    });
    let app = Router::new()
        .route("/mcp", post(mcp))
        .route("/token", post(token))
        .with_state(fixture.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let alice = OAuthCredentialProvider::from_grant(grant(&origin, "alice")).unwrap();
    let mut alice_clone = alice.clone();
    let bob = OAuthCredentialProvider::from_grant(grant(&origin, "bob")).unwrap();
    let mut bob_observer = bob.clone();
    let config = ServerConfig::Http {
        url: endpoint.clone(),
        protocol: ProtocolMode::Modern,
        credential: None,
    };
    let a = QueuedRemote::spawn_with(config.clone(), QueueOptions::default(), move || {
        Ok(Client::new()?.with_credential_provider(alice))
    });
    let b = QueuedRemote::spawn_with(config, QueueOptions::default(), move || {
        Ok(Client::new()?.with_credential_provider(bob))
    });
    let extra = alice_clone.resolve(CredentialRequest {
        endpoint: &endpoint,
        reference: None,
    });
    let (a, b, alice_credential) = tokio::join!(a, b, extra);
    let a = a.unwrap();
    let b = b.unwrap();
    let alice_credential = alice_credential.unwrap();
    assert_eq!(alice_credential.token(), Some("alice-access"));
    assert_eq!(alice_credential.auth_context_id(), "oauth:alice-grant");
    assert_eq!(fixture.alice.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.bob.load(Ordering::SeqCst), 1);
    let (alice_tool, bob_tool) = tokio::join!(a.tool("echo"), b.tool("echo"));
    let alice_tool = alice_tool.unwrap();
    let bob_tool = bob_tool.unwrap();
    let (ra, rb) = tokio::join!(
        alice_tool.submit_json(json!({"message":"hello"})).unwrap(),
        bob_tool.submit_json(json!({"message":42})).unwrap()
    );
    for (result, expected) in [(ra.unwrap(), "alice"), (rb.unwrap(), "bob")] {
        let CallOutcome::Completed(response) = result else {
            panic!()
        };
        assert_eq!(response.raw["structuredContent"]["user"], expected);
    }
    assert_eq!(fixture.calls.lock().unwrap().len(), 2);
    alice_clone.forget().await.unwrap();
    assert_eq!(
        alice_clone
            .resolve(CredentialRequest {
                endpoint: &endpoint,
                reference: None
            })
            .await
            .err()
            .unwrap()
            .code,
        "LoginRequired"
    );
    assert_eq!(a.tool("echo").await.err().unwrap().code, "LoginRequired");
    assert_eq!(
        bob_observer
            .resolve(CredentialRequest {
                endpoint: &endpoint,
                reference: None
            })
            .await
            .unwrap()
            .token(),
        Some("bob-access")
    );
    let (ra, rb) = tokio::join!(a.close(), b.close());
    ra.unwrap();
    rb.unwrap();
    server.abort();
}
