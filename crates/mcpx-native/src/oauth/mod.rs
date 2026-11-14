//! Native OAuth browser callback, credential storage and refresh provider.
use mcpx_auth::error;
use mcpx_core::{
    Credential, CredentialFuture, CredentialProvider, CredentialRequest, Error, Result,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
mod store;
pub use mcpx_auth::{
    Authorization, Discovery, Grant, Http, Request, Response, ServerMetadata, Tokens, client_id,
    discover,
};
pub use store::{StoreKind, TokenStore};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn login_required() -> Error {
    error(
        "LoginRequired",
        "OAuth login required; run mcpx auth login SERVER (or --url ENDPOINT auth login)",
    )
}
#[derive(Clone)]
pub struct OAuthHttpClient {
    client: reqwest::Client,
}
impl OAuthHttpClient {
    pub fn new() -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30))
                .user_agent(concat!("mcpx/", env!("CARGO_PKG_VERSION")))
                .build()
                .map_err(|_| error("OAuthTransportError", "cannot create OAuth HTTP client"))?,
        })
    }
}
impl Http for OAuthHttpClient {
    async fn send(&mut self, request: Request) -> Result<Response> {
        mcpx_auth::validate_url(&request.url)?;
        let headers_only = request.headers_only;
        let mut builder = if request.form.is_some() {
            self.client.post(&request.url)
        } else {
            self.client.get(&request.url)
        };
        if let Some(form) = request.form {
            if form
                .first()
                .is_some_and(|(name, _)| name == "__registration_json")
            {
                builder = builder
                    .header("content-type", "application/json")
                    .body(form[0].1.clone());
            } else {
                let body = url::form_urlencoded::Serializer::new(String::new())
                    .extend_pairs(form)
                    .finish();
                builder = builder
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(body);
            }
        }
        let mut response = builder
            .send()
            .await
            .map_err(|_| error("OAuthTransportError", "OAuth HTTP request failed"))?;
        let status = response.status().as_u16();
        let challenges = response
            .headers()
            .get_all("www-authenticate")
            .iter()
            .filter_map(|v| v.to_str().ok().map(str::to_owned))
            .collect();
        if headers_only || status == 401 || status == 403 {
            return Ok(Response {
                status,
                challenges,
                body: vec![],
            });
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error("OAuthTransportError", "OAuth response failed"))?
        {
            if body.len() + chunk.len() > 1024 * 1024 {
                return Err(error(
                    "OAuthMetadataInvalid",
                    "OAuth response exceeds 1 MiB",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Response {
            status,
            challenges,
            body,
        })
    }
}
#[derive(Default)]
pub struct LoginOptions {
    pub client_id: Option<String>,
    pub scopes: Vec<String>,
    pub no_browser: bool,
    pub store: StoreKind,
    pub redirect_port: Option<u16>,
    pub profile: Option<String>,
}
/// Authorize one endpoint. The caller displays the URL; a library never opens UI
/// implicitly during credential resolution.
pub async fn login(
    endpoint: &str,
    options: LoginOptions,
    display: impl FnOnce(&str),
    timeout: Duration,
) -> Result<()> {
    let operation = async {
        let mut http = OAuthHttpClient::new()?;
        let discovery = mcpx_auth::discover(&mut http, endpoint).await?;
        let listener = tokio::net::TcpListener::bind((
            std::net::Ipv4Addr::LOCALHOST,
            options
                .redirect_port
                .unwrap_or(mcpx_auth::DEFAULT_REDIRECT_PORT),
        ))
        .await
        .map_err(|_| {
            error(
                "OAuthCallbackUnavailable",
                "cannot listen on the loopback callback port; another login may be active",
            )
        })?;
        let redirect = format!(
            "http://127.0.0.1:{}/callback",
            listener
                .local_addr()
                .map_err(|_| error(
                    "OAuthCallbackUnavailable",
                    "cannot determine callback address"
                ))?
                .port()
        );
        let client = mcpx_auth::client_id(
            &mut http,
            &discovery,
            options.client_id.as_deref(),
            &redirect,
        )
        .await?;
        let transaction =
            Authorization::begin(discovery, client, redirect.clone(), options.scopes)?;
        display(&transaction.url);
        if !options.no_browser {
            let _ = open::that(&transaction.url);
        }
        let callback = loop {
            let (mut stream, peer) = listener.accept().await.map_err(|_| {
                error(
                    "OAuthCallbackUnavailable",
                    "authorization callback listener failed",
                )
            })?;
            if !peer.ip().is_loopback() {
                continue;
            }
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 1024];
            let received = tokio::time::timeout(Duration::from_secs(3), async {
                while !bytes.windows(4).any(|b| b == b"\r\n\r\n") {
                    let n = stream.read(&mut chunk).await?;
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..n]);
                    if bytes.len() > 16384 {
                        break;
                    }
                }
                Ok::<_, std::io::Error>(())
            })
            .await;
            if !matches!(received, Ok(Ok(()))) || bytes.len() > 16384 {
                continue;
            }
            let request = std::str::from_utf8(&bytes).unwrap_or("");
            let target = request
                .lines()
                .next()
                .and_then(|l| l.strip_prefix("GET "))
                .and_then(|l| l.strip_suffix(" HTTP/1.1"));
            if target.is_none_or(|t| !t.starts_with("/callback?") || t.starts_with("//")) {
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
                continue;
            }
            let callback = format!(
                "http://127.0.0.1:{}{}",
                listener
                    .local_addr()
                    .map_err(|_| error(
                        "OAuthCallbackUnavailable",
                        "cannot determine callback address"
                    ))?
                    .port(),
                target.unwrap()
            );
            if let Err(error) = transaction.validate_callback(&callback)
                && error.code == "OAuthCallbackInvalid"
            {
                let text = "This callback does not match the pending login. Use the latest authorization URL from your terminal.";
                let response = format!(
                    "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                    text.len()
                );
                let _ = tokio::time::timeout(
                    Duration::from_secs(1),
                    stream.write_all(response.as_bytes()),
                )
                .await;
                continue;
            }
            let result = transaction.complete(&mut http, &callback, now()).await;
            let text = if result.is_ok() {
                "Authorization received. Return to your terminal."
            } else {
                "Authorization failed. Return to your terminal."
            };
            let response = format!(
                "HTTP/1.1 {}\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                if result.is_ok() {
                    "200 OK"
                } else {
                    "400 Bad Request"
                },
                text.len()
            );
            let _ = tokio::time::timeout(
                Duration::from_secs(1),
                stream.write_all(response.as_bytes()),
            )
            .await;
            break result?;
        };
        let store =
            TokenStore::for_profile(endpoint, options.profile.as_deref().unwrap_or("default"))?;
        let _lock = store.lock().await?;
        store.save(&callback, Some(options.store))?;
        Ok(())
    };
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| error("Timeout", "OAuth login timed out"))?
}
pub async fn logout(endpoint: &str) -> Result<()> {
    logout_profile(endpoint, "default").await
}
pub async fn logout_profile(endpoint: &str, profile: &str) -> Result<()> {
    let store = TokenStore::for_profile(endpoint, profile)?;
    let _lock = store.lock().await?;
    let grant = match store.load() {
        Ok(grant) => Some(grant),
        Err(e) if e.code == "LoginRequired" => None,
        Err(e) => return Err(e),
    };
    store.delete()?;
    if let Some(grant) = grant
        && let Some(endpoint) = grant.server.revocation_endpoint
    {
        let (token, hint) = grant
            .tokens
            .refresh_token
            .map(|t| (t, "refresh_token"))
            .unwrap_or((grant.tokens.access_token, "access_token"));
        let response = OAuthHttpClient::new()?
            .send(Request {
                url: endpoint,
                headers_only: false,
                form: Some(vec![
                    ("token".into(), token),
                    ("token_type_hint".into(), hint.into()),
                    ("client_id".into(), grant.client_id),
                ]),
            })
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(error(
                "OAuthTokenFailed",
                "logged out locally, but the authorization server could not revoke the token",
            ));
        }
    }
    Ok(())
}
pub async fn status(endpoint: &str) -> Result<serde_json::Value> {
    status_profile(endpoint, "default").await
}
pub async fn status_profile(endpoint: &str, profile: &str) -> Result<serde_json::Value> {
    let store = TokenStore::for_profile(endpoint, profile)?;
    let _lock = store.lock().await?;
    match store.load() {
        Ok(grant) => Ok(
            serde_json::json!({"logged_in":true,"issuer":grant.server.issuer,"resource":grant.resource,"expires_at":grant.tokens.expires_at,"scopes":grant.tokens.scopes,"refresh_available":grant.tokens.refresh_token.is_some()}),
        ),
        Err(e) if e.code == "LoginRequired" => Ok(serde_json::json!({"logged_in":false})),
        Err(e) => Err(e),
    }
}
/// One identity's OAuth state. Clone shares refresh state for that identity;
/// construct distinct providers/grants for distinct users. Never select a user
/// by mutating a process-global bearer token.
#[derive(Clone)]
pub struct OAuthCredentialProvider {
    resource: String,
    state: State,
    http: OAuthHttpClient,
}
#[derive(Clone)]
enum State {
    Persistent(TokenStore),
    Memory(std::sync::Arc<tokio::sync::Mutex<Option<Grant>>>),
}
impl OAuthCredentialProvider {
    pub fn new(endpoint: &str) -> Result<Self> {
        Self::for_profile(endpoint, "default")
    }
    pub fn for_profile(endpoint: &str, profile: &str) -> Result<Self> {
        Ok(Self {
            resource: mcpx_auth::canonical_resource(endpoint)?,
            state: State::Persistent(TokenStore::for_profile(endpoint, profile)?),
            http: OAuthHttpClient::new()?,
        })
    }
    /// Use an application-owned authorization grant without OS/global storage.
    /// Clones share one refresh lock; unrelated users have independent locks.
    pub fn from_grant(grant: Grant) -> Result<Self> {
        Ok(Self {
            resource: mcpx_auth::canonical_resource(&grant.resource)?,
            state: State::Memory(std::sync::Arc::new(tokio::sync::Mutex::new(Some(grant)))),
            http: OAuthHttpClient::new()?,
        })
    }
    pub fn has_login(endpoint: &str) -> Result<bool> {
        Self::has_profile(endpoint, "default")
    }
    pub fn has_profile(endpoint: &str, profile: &str) -> Result<bool> {
        match TokenStore::for_profile(endpoint, profile) {
            Ok(store) => Ok(store.exists()),
            // An optional lookup must not apply OAuth's HTTPS policy to an
            // anonymous connection. Explicit OAuth providers still validate it.
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "TokenStoreUnavailable" | "OAuthMetadataInvalid"
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
    /// Clear this identity in every clone. Existing I/O retains its already
    /// prepared credential snapshot; this cannot roll back an executed tool.
    pub async fn forget(&self) -> Result<()> {
        match &self.state {
            State::Memory(state) => {
                *state.lock().await = None;
                Ok(())
            }
            State::Persistent(store) => {
                let _lock = store.lock().await?;
                store.delete()
            }
        }
    }
}
fn check_resource(grant: &Grant, resource: &str) -> Result<()> {
    if mcpx_auth::canonical_resource(&grant.resource)? != resource {
        return Err(error(
            "CredentialUnavailable",
            "OAuth grant belongs to another MCP resource",
        ));
    }
    Ok(())
}
fn credential(grant: &Grant) -> Result<Credential> {
    Credential::bearer(
        grant.tokens.access_token.clone(),
        format!("oauth:{}", grant.context_id),
    )
}
impl CredentialProvider for OAuthCredentialProvider {
    fn resolve<'a>(&'a mut self, request: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(async move {
            if mcpx_auth::canonical_resource(request.endpoint)? != self.resource {
                return Err(error(
                    "CredentialUnavailable",
                    "OAuth provider belongs to a different MCP resource",
                ));
            }
            match &self.state {
                State::Persistent(store) => {
                    let _lock = store.lock().await?;
                    let mut grant = store.load()?;
                    check_resource(&grant, &self.resource)?;
                    if grant.needs_refresh(now()) {
                        match grant.refresh(&mut self.http, now()).await {
                            Ok(()) => store.save(&grant, None)?,
                            Err(e) if e.code == "LoginRequired" => {
                                store.delete()?;
                                return Err(e);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    credential(&grant)
                }
                State::Memory(state) => {
                    let mut slot = state.lock().await;
                    let grant = slot.as_mut().ok_or_else(login_required)?;
                    check_resource(grant, &self.resource)?;
                    if grant.needs_refresh(now()) {
                        match grant.refresh(&mut self.http, now()).await {
                            Ok(()) => {}
                            Err(e) if e.code == "LoginRequired" => {
                                *slot = None;
                                return Err(e);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    credential(slot.as_ref().ok_or_else(login_required)?)
                }
            }
        })
    }
}
