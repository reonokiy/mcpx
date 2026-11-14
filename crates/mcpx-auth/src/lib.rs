//! Host-independent MCP OAuth discovery and authorization transactions.
//! Hosts supply HTTP, storage, browser interaction and the callback transport.
use mcpx_core::{Error, Result};
use oauth2::{CsrfToken, PkceCodeChallenge, PkceCodeVerifier, TokenResponse};
use serde::{Deserialize, Serialize};
use url::Url;

pub const DEFAULT_CLIENT_ID: &str = "https://reonokiy.github.io/mcpx/oauth/native.json";
pub const DEFAULT_REDIRECT_PORT: u16 = 48721;

pub struct Request {
    pub url: String,
    pub form: Option<Vec<(String, String)>>,
    pub headers_only: bool,
}
pub struct Response {
    pub status: u16,
    pub challenges: Vec<String>,
    pub body: Vec<u8>,
}
#[allow(async_fn_in_trait)]
pub trait Http {
    async fn send(&mut self, request: Request) -> Result<Response>;
}
pub fn error(code: &str, message: &str) -> Error {
    Error::local(code, message)
}
pub fn validate_url(text: &str) -> Result<Url> {
    let url = Url::parse(text).map_err(|_| error("OAuthMetadataInvalid", "invalid OAuth URL"))?;
    let local = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if url.cannot_be_a_base()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !(url.scheme() == "https" || url.scheme() == "http" && local)
    {
        return Err(error(
            "OAuthMetadataInvalid",
            "OAuth URLs require HTTPS (HTTP is allowed only on loopback)",
        ));
    }
    Ok(url)
}
pub fn canonical_resource(text: &str) -> Result<String> {
    let mut url = validate_url(text)?;
    if url.path() == "/" && url.query().is_none() {
        let text = url.as_str().trim_end_matches('/').to_owned();
        return Ok(text);
    }
    url.set_fragment(None);
    Ok(url.into())
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct Challenge {
    pub resource_metadata: Option<String>,
    pub scope: Option<String>,
}
/// Split challenges without splitting quoted commas or treating Basic as Bearer.
pub fn challenge(headers: &[String]) -> Result<Challenge> {
    let mut result = Challenge::default();
    for header in headers {
        if header.len() > 16384 {
            return Err(error(
                "OAuthMetadataInvalid",
                "authorization challenge exceeds limit",
            ));
        }
        let mut parts = Vec::new();
        let mut start = 0;
        let mut quoted = false;
        let mut escaped = false;
        for (i, ch) in header.char_indices() {
            if escaped {
                escaped = false;
                continue;
            }
            if quoted && ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == '"' {
                quoted = !quoted;
            }
            if ch == ',' && !quoted {
                parts.push(&header[start..i]);
                start = i + 1;
            }
        }
        parts.push(&header[start..]);
        let mut bearer = false;
        for part in parts {
            let mut part = part.trim();
            if part
                .get(..6)
                .is_some_and(|s| s.eq_ignore_ascii_case("bearer"))
                && part.as_bytes().get(6).is_some_and(u8::is_ascii_whitespace)
            {
                bearer = true;
                part = part[6..].trim();
            } else if part
                .split_once('=')
                .is_none_or(|(key, _)| key.trim().contains(char::is_whitespace))
            {
                bearer = false;
            }
            if !bearer {
                continue;
            }
            let Some((key, value)) = part.split_once('=') else {
                continue;
            };
            let value = value.trim();
            let value = if value.starts_with('"') {
                serde_json::from_str::<String>(value).map_err(|_| {
                    error(
                        "OAuthMetadataInvalid",
                        "invalid quoted authorization parameter",
                    )
                })?
            } else {
                value.to_owned()
            };
            match key.trim().to_ascii_lowercase().as_str() {
                "resource_metadata" => result.resource_metadata = Some(value),
                "scope" => result.scope = Some(value),
                _ => {}
            }
        }
    }
    Ok(result)
}
#[derive(Clone, Serialize, Deserialize)]
pub struct ServerMetadata {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default)]
    pub registration_endpoint: Option<String>,
    #[serde(default)]
    pub revocation_endpoint: Option<String>,
    #[serde(default)]
    pub code_challenge_methods_supported: Vec<String>,
    #[serde(default)]
    pub client_id_metadata_document_supported: bool,
    #[serde(default)]
    pub authorization_response_iss_parameter_supported: bool,
}
#[derive(Deserialize)]
struct ResourceMetadata {
    resource: String,
    authorization_servers: Vec<String>,
    #[serde(default)]
    scopes_supported: Vec<String>,
}
pub struct Discovery {
    pub resource: String,
    pub server: ServerMetadata,
    pub scopes: Vec<String>,
}
fn json<T: for<'a> Deserialize<'a>>(response: &Response) -> Result<T> {
    if response.status != 200 {
        return Err(error(
            "OAuthDiscoveryFailed",
            "authorization metadata request failed",
        ));
    }
    serde_json::from_slice(&response.body)
        .map_err(|_| error("OAuthMetadataInvalid", "invalid authorization metadata"))
}
fn get(url: String) -> Request {
    Request {
        url,
        form: None,
        headers_only: false,
    }
}
fn well_known(issuer: &Url, kind: &str, append: bool) -> String {
    let mut result = issuer.clone();
    let path = issuer.path().trim_end_matches('/');
    result.set_path(&if append {
        format!("{path}/.well-known/{kind}")
    } else {
        format!("/.well-known/{kind}{path}")
    });
    result.set_query(None);
    result.into()
}
pub async fn discover(http: &mut impl Http, endpoint: &str) -> Result<Discovery> {
    let resource = canonical_resource(endpoint)?;
    // A metadata probe, never a business tool call. Never attach an old bearer.
    let probe = http
        .send(Request {
            url: endpoint.into(),
            form: None,
            headers_only: true,
        })
        .await?;
    let hint = challenge(&probe.challenges)?;
    let endpoint_url = validate_url(endpoint)?;
    let locations = if let Some(url) = hint.resource_metadata {
        vec![url]
    } else {
        let mut urls = vec![well_known(&endpoint_url, "oauth-protected-resource", false)];
        let mut root = endpoint_url.clone();
        root.set_path("/.well-known/oauth-protected-resource");
        root.set_query(None);
        if root.as_str() != urls[0] {
            urls.push(root.into());
        }
        urls
    };
    let mut metadata = None;
    for location in locations {
        validate_url(&location)?;
        let response = http.send(get(location)).await?;
        if response.status == 404 {
            continue;
        }
        metadata = Some(json::<ResourceMetadata>(&response)?);
        break;
    }
    let metadata = metadata.ok_or_else(|| {
        error(
            "OAuthDiscoveryFailed",
            "MCP protected resource metadata was not found",
        )
    })?;
    if canonical_resource(&metadata.resource)? != resource {
        return Err(error(
            "OAuthMetadataInvalid",
            "protected resource does not match the MCP endpoint",
        ));
    }
    let issuer = metadata
        .authorization_servers
        .first()
        .ok_or_else(|| error("OAuthMetadataInvalid", "no authorization server advertised"))?;
    let issuer_url = validate_url(issuer)?;
    let mut locations = vec![
        well_known(&issuer_url, "oauth-authorization-server", false),
        well_known(&issuer_url, "openid-configuration", false),
    ];
    if issuer_url.path() != "/" {
        locations.push(well_known(&issuer_url, "openid-configuration", true));
    }
    let mut server = None;
    for location in locations {
        let response = http.send(get(location)).await?;
        if response.status == 404 {
            continue;
        }
        let parsed: ServerMetadata = json(&response)?;
        if parsed.issuer != *issuer {
            return Err(error(
                "OAuthMetadataInvalid",
                "authorization metadata issuer mismatch",
            ));
        }
        validate_url(&parsed.authorization_endpoint)?;
        validate_url(&parsed.token_endpoint)?;
        if !parsed
            .code_challenge_methods_supported
            .iter()
            .any(|s| s == "S256")
        {
            return Err(error(
                "OAuthPkceRequired",
                "authorization server must advertise PKCE S256",
            ));
        }
        server = Some(parsed);
        break;
    }
    Ok(Discovery {
        resource: metadata.resource,
        server: server.ok_or_else(|| {
            error(
                "OAuthDiscoveryFailed",
                "authorization server metadata was not found",
            )
        })?,
        scopes: hint
            .scope
            .map(|s| s.split_whitespace().map(str::to_owned).collect())
            .unwrap_or(metadata.scopes_supported),
    })
}

pub async fn client_id(
    http: &mut impl Http,
    discovery: &Discovery,
    pre_registered: Option<&str>,
    redirect: &str,
) -> Result<String> {
    if let Some(value) = pre_registered {
        if value.is_empty() || value.len() > 4096 {
            return Err(error("OAuthClientInvalid", "invalid client ID"));
        }
        return Ok(value.into());
    }
    if discovery.server.client_id_metadata_document_supported
        && redirect == format!("http://127.0.0.1:{DEFAULT_REDIRECT_PORT}/callback")
    {
        return Ok(DEFAULT_CLIENT_ID.into());
    }
    let endpoint = discovery
        .server
        .registration_endpoint
        .as_ref()
        .ok_or_else(|| {
            error(
                "OAuthRegistrationRequired",
                "server requires a registered client; use auth login --client-id ID",
            )
        })?;
    validate_url(endpoint)?;
    // Registration uses JSON; represented separately so hosts never confuse it
    // with the form-encoded token endpoint.
    let registration = serde_json::json!({"client_name":"mcpx", "redirect_uris":[redirect], "grant_types":["authorization_code","refresh_token"], "response_types":["code"], "token_endpoint_auth_method":"none", "application_type":"native"});
    let response = http
        .send(Request {
            url: endpoint.clone(),
            headers_only: false,
            form: Some(vec![(
                "__registration_json".into(),
                registration.to_string(),
            )]),
        })
        .await?;
    if response.status != 201 && response.status != 200 {
        return Err(error(
            "OAuthRegistrationFailed",
            "public OAuth client registration failed; use --client-id for a pre-registered client",
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(&response.body).map_err(|_| {
        error(
            "OAuthRegistrationFailed",
            "invalid client registration response",
        )
    })?;
    if value
        .get("token_endpoint_auth_method")
        .and_then(|v| v.as_str())
        .is_some_and(|v| v != "none")
    {
        return Err(error(
            "OAuthClientInvalid",
            "this CLI requires a public PKCE client",
        ));
    }
    value
        .get("client_id")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty() && v.len() <= 4096)
        .map(str::to_owned)
        .ok_or_else(|| {
            error(
                "OAuthRegistrationFailed",
                "registration response has no client ID",
            )
        })
}

pub struct Authorization {
    pub url: String,
    pub state: String,
    verifier: PkceCodeVerifier,
    redirect: String,
    client_id: String,
    discovery: Discovery,
}
impl Authorization {
    pub fn begin(
        discovery: Discovery,
        client_id: String,
        redirect: String,
        scopes: Vec<String>,
    ) -> Result<Self> {
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let state = CsrfToken::new_random().secret().clone();
        let mut url = validate_url(&discovery.server.authorization_endpoint)?;
        url.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("client_id", &client_id),
            ("redirect_uri", &redirect),
            ("state", &state),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("resource", &discovery.resource),
        ]);
        let mut required = discovery.scopes.clone();
        required.extend(scopes);
        let mut scopes = required;
        scopes.sort();
        scopes.dedup();
        if !scopes.is_empty() {
            url.query_pairs_mut()
                .append_pair("scope", &scopes.join(" "));
        }
        let mut discovery = discovery;
        discovery.scopes = scopes;
        Ok(Self {
            url: url.into(),
            state,
            verifier,
            redirect,
            client_id,
            discovery,
        })
    }
    /// Validate a loopback response without consuming the pending transaction.
    pub fn validate_callback(&self, callback: &str) -> Result<String> {
        let callback = Url::parse(callback)
            .map_err(|_| error("OAuthCallbackInvalid", "invalid authorization callback"))?;
        let mut origin = callback.clone();
        origin.set_query(None);
        if origin.as_str() != self.redirect || callback.fragment().is_some() {
            return Err(error(
                "OAuthCallbackInvalid",
                "authorization callback URI mismatch",
            ));
        }
        let pairs: Vec<_> = callback.query_pairs().collect();
        let read = |name: &str| -> Result<Option<String>> {
            let values: Vec<_> = pairs
                .iter()
                .filter(|(key, _)| key == name)
                .map(|(_, v)| v.to_string())
                .collect();
            if values.len() > 1 {
                return Err(error(
                    "OAuthCallbackInvalid",
                    "duplicate authorization response parameter",
                ));
            }
            Ok(values.into_iter().next())
        };
        let received_state = read("state")?.unwrap_or_default();
        if !constant_time_eq::constant_time_eq(received_state.as_bytes(), self.state.as_bytes()) {
            return Err(error(
                "OAuthCallbackInvalid",
                "authorization state mismatch",
            ));
        }
        let iss = read("iss")?;
        if iss
            .as_ref()
            .is_some_and(|v| v != &self.discovery.server.issuer)
            || iss.is_none()
                && self
                    .discovery
                    .server
                    .authorization_response_iss_parameter_supported
        {
            return Err(error(
                "OAuthCallbackInvalid",
                "authorization response issuer mismatch",
            ));
        }
        if read("error")?.is_some() {
            return Err(error("OAuthConsentDenied", "authorization was not granted"));
        }
        let code = read("code")?
            .filter(|v| !v.is_empty())
            .ok_or_else(|| error("OAuthCallbackInvalid", "authorization code is missing"))?;
        Ok(code)
    }
    pub async fn complete(self, http: &mut impl Http, callback: &str, now: u64) -> Result<Grant> {
        let code = self.validate_callback(callback)?;
        let response = http
            .send(Request {
                url: self.discovery.server.token_endpoint.clone(),
                headers_only: false,
                form: Some(vec![
                    ("grant_type".into(), "authorization_code".into()),
                    ("code".into(), code),
                    ("client_id".into(), self.client_id.clone()),
                    ("redirect_uri".into(), self.redirect),
                    ("code_verifier".into(), self.verifier.secret().clone()),
                    ("resource".into(), self.discovery.resource.clone()),
                ]),
            })
            .await?;
        let tokens = tokens(&response, None, &self.discovery.scopes, now)?;
        Ok(Grant {
            resource: self.discovery.resource,
            server: self.discovery.server,
            client_id: self.client_id,
            context_id: CsrfToken::new_random().secret().clone(),
            tokens,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<u64>,
    pub scopes: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Grant {
    pub resource: String,
    pub server: ServerMetadata,
    pub client_id: String,
    pub context_id: String,
    pub tokens: Tokens,
}
fn tokens(
    response: &Response,
    previous: Option<&Tokens>,
    scopes: &[String],
    now: u64,
) -> Result<Tokens> {
    if response.status != 200 {
        let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap_or_default();
        return Err(error(
            if value["error"] == "invalid_grant" {
                "LoginRequired"
            } else {
                "OAuthTokenFailed"
            },
            "OAuth token request failed; run mcpx auth login again",
        ));
    }
    let value: oauth2::basic::BasicTokenResponse = serde_json::from_slice(&response.body)
        .map_err(|_| error("OAuthTokenFailed", "invalid token response"))?;
    if value.token_type() != &oauth2::basic::BasicTokenType::Bearer {
        return Err(error(
            "OAuthTokenFailed",
            "only bearer tokens are supported",
        ));
    }
    let mut scopes: Vec<_> = value
        .scopes()
        .map(|v| v.iter().map(|s| s.as_str().to_owned()).collect())
        .unwrap_or_else(|| scopes.to_vec());
    scopes.sort();
    scopes.dedup();
    if value.access_token().secret().is_empty()
        || !value
            .access_token()
            .secret()
            .bytes()
            .all(|b| b.is_ascii_graphic())
    {
        return Err(error("OAuthTokenFailed", "invalid bearer token"));
    }
    Ok(Tokens {
        access_token: value.access_token().secret().clone(),
        refresh_token: value
            .refresh_token()
            .map(|s| s.secret().clone())
            .or_else(|| previous.and_then(|s| s.refresh_token.clone())),
        expires_at: value.expires_in().map(|v| now.saturating_add(v.as_secs())),
        scopes,
    })
}
impl Grant {
    pub fn needs_refresh(&self, now: u64) -> bool {
        self.tokens
            .expires_at
            .is_some_and(|t| t <= now.saturating_add(30))
    }
    pub async fn refresh(&mut self, http: &mut impl Http, now: u64) -> Result<()> {
        let refresh = self.tokens.refresh_token.clone().ok_or_else(|| {
            error(
                "LoginRequired",
                "no refresh token; run mcpx auth login again",
            )
        })?;
        let response = http
            .send(Request {
                url: self.server.token_endpoint.clone(),
                headers_only: false,
                form: Some(vec![
                    ("grant_type".into(), "refresh_token".into()),
                    ("refresh_token".into(), refresh),
                    ("client_id".into(), self.client_id.clone()),
                    ("resource".into(), self.resource.clone()),
                ]),
            })
            .await?;
        let next = tokens(&response, Some(&self.tokens), &self.tokens.scopes, now)?;
        if next.scopes != self.tokens.scopes {
            self.context_id = CsrfToken::new_random().secret().clone();
        }
        self.tokens = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    fn ready<T>(f: impl Future<Output = T>) -> T {
        let mut f = std::pin::pin!(f);
        match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("memory HTTP must be ready"),
        }
    }
    struct NoHttp;
    impl Http for NoHttp {
        async fn send(&mut self, _: Request) -> Result<Response> {
            panic!("invalid callback must not reach the token endpoint");
        }
    }
    fn discovery() -> Discovery {
        Discovery {
            resource: "https://mcp.example/mcp".into(),
            scopes: vec!["tools.read".into()],
            server: ServerMetadata {
                issuer: "https://issuer.example".into(),
                authorization_endpoint: "https://issuer.example/authorize".into(),
                token_endpoint: "https://issuer.example/token".into(),
                registration_endpoint: None,
                revocation_endpoint: None,
                code_challenge_methods_supported: vec!["S256".into()],
                client_id_metadata_document_supported: false,
                authorization_response_iss_parameter_supported: true,
            },
        }
    }
    #[test]
    fn challenge_preserves_quoted_values_and_skips_other_schemes() {
        let parsed=challenge(&[r#"Basic realm="example", Bearer resource_metadata="https://example.test/meta?a=1,b=2", scope="read write""#.into()]).unwrap();
        assert_eq!(
            parsed.resource_metadata.as_deref(),
            Some("https://example.test/meta?a=1,b=2")
        );
        assert_eq!(parsed.scope.as_deref(), Some("read write"));
        assert!(validate_url("http://metadata.example.test/").is_err());
        assert!(validate_url("https://user:secret@example.test/").is_err());
        assert!(validate_url("http://127.0.0.1:8000/").is_ok());
    }
    #[test]
    fn authorization_carries_resource_and_rejects_bad_state_issuer_and_redirect() {
        for failure in ["state", "issuer", "missing_issuer", "redirect"] {
            let auth = Authorization::begin(
                discovery(),
                "client".into(),
                "http://127.0.0.1:48721/callback".into(),
                vec![],
            )
            .unwrap();
            let request = Url::parse(&auth.url).unwrap();
            let fields: std::collections::BTreeMap<_, _> = request.query_pairs().collect();
            assert_eq!(fields["resource"], "https://mcp.example/mcp");
            assert_eq!(fields["code_challenge_method"], "S256");
            assert!(!fields.contains_key("code_verifier"));
            let mut callback = Url::parse("http://127.0.0.1:48721/callback").unwrap();
            callback
                .query_pairs_mut()
                .append_pair("code", "never-send")
                .append_pair(
                    "state",
                    if failure == "state" {
                        "wrong"
                    } else {
                        &auth.state
                    },
                );
            if failure != "missing_issuer" {
                callback.query_pairs_mut().append_pair(
                    "iss",
                    if failure == "issuer" {
                        "https://other.example"
                    } else {
                        "https://issuer.example"
                    },
                );
            }
            if failure == "redirect" {
                callback.set_path("/other");
            }
            assert_eq!(
                ready(auth.complete(&mut NoHttp, callback.as_str(), 1))
                    .err()
                    .unwrap()
                    .code,
                "OAuthCallbackInvalid"
            );
        }
    }
    #[test]
    fn discovery_builds_path_aware_oauth_and_oidc_locations() {
        let issuer = Url::parse("https://auth.example/tenant/one").unwrap();
        assert_eq!(
            well_known(&issuer, "oauth-authorization-server", false),
            "https://auth.example/.well-known/oauth-authorization-server/tenant/one"
        );
        assert_eq!(
            well_known(&issuer, "openid-configuration", true),
            "https://auth.example/tenant/one/.well-known/openid-configuration"
        );
    }
}
