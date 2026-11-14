use crate::{Error, Result};
use std::{fmt, future::Future, pin::Pin};

#[derive(Clone)]
pub struct Credential {
    bearer: Option<String>,
    auth_context_id: String,
}

impl Credential {
    pub fn anonymous() -> Self {
        Self {
            bearer: None,
            auth_context_id: "anonymous".into(),
        }
    }
    pub fn bearer(token: String, auth_context_id: String) -> Result<Self> {
        if token.is_empty() || !token.bytes().all(|c| c.is_ascii_graphic()) {
            return Err(Error::local(
                "CredentialUnavailable",
                "bearer credential must be nonempty header-safe ASCII",
            ));
        }
        let mut credential = Self::scoped(auth_context_id)?;
        credential.bearer = Some(token);
        Ok(credential)
    }
    /// An explicit non-secret identity for transports whose authentication is
    /// supplied outside an HTTP bearer header. This does not authenticate a peer.
    pub fn scoped(auth_context_id: String) -> Result<Self> {
        if auth_context_id.is_empty() || auth_context_id.len() > 256 {
            return Err(Error::local(
                "CredentialUnavailable",
                "credential requires a bounded non-secret authorization context ID",
            ));
        }
        Ok(Self {
            bearer: None,
            auth_context_id,
        })
    }
    pub fn token(&self) -> Option<&str> {
        self.bearer.as_deref()
    }
    pub fn auth_context_id(&self) -> &str {
        &self.auth_context_id
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("auth_context_id", &self.auth_context_id)
            .field("bearer", &self.bearer.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

pub struct CredentialRequest<'a> {
    pub endpoint: &'a str,
    pub reference: Option<&'a str>,
}
pub type CredentialFuture<'a> = Pin<Box<dyn Future<Output = Result<Credential>> + 'a>>;

/// Providers must change the non-secret context ID when identity or permissions change.
pub trait CredentialProvider {
    fn resolve<'a>(&'a mut self, request: CredentialRequest<'a>) -> CredentialFuture<'a>;
}
