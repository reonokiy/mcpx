use mcpx_core::{Credential, CredentialFuture, CredentialProvider, CredentialRequest, Error};
use sha2::{Digest, Sha256};

fn context(source: &str, token: &str) -> String {
    format!("{source}:{:x}", Sha256::digest(token.as_bytes()))
}

pub struct EnvCredentialProvider {
    variable: String,
}
impl EnvCredentialProvider {
    pub fn new(variable: impl Into<String>) -> Self {
        Self {
            variable: variable.into(),
        }
    }
}
impl CredentialProvider for EnvCredentialProvider {
    fn resolve<'a>(&'a mut self, _request: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(async move {
            let token = std::env::var(&self.variable).map_err(|_| {
                Error::local(
                    "CredentialUnavailable",
                    "credential environment variable is unavailable",
                )
            })?;
            Credential::bearer(
                token.clone(),
                context(&format!("env:{}", self.variable), &token),
            )
        })
    }
}

pub(crate) struct StaticCredentialProvider {
    pub token: String,
}
impl CredentialProvider for StaticCredentialProvider {
    fn resolve<'a>(&'a mut self, _request: CredentialRequest<'a>) -> CredentialFuture<'a> {
        Box::pin(
            async move { Credential::bearer(self.token.clone(), context("static", &self.token)) },
        )
    }
}
