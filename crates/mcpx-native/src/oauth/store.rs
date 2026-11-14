use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
};

#[derive(Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StoreKind {
    #[default]
    Keyring,
    File,
}
#[derive(Serialize, Deserialize)]
struct Profile {
    store: StoreKind,
}
#[derive(Clone)]
pub struct TokenStore {
    directory: PathBuf,
    key: String,
}
impl TokenStore {
    pub fn new(endpoint: &str) -> Result<Self> {
        Self::for_profile(endpoint, "default")
    }
    pub fn for_profile(endpoint: &str, profile: &str) -> Result<Self> {
        if profile.is_empty() || profile.len() > 256 {
            return Err(error(
                "ConfigurationError",
                "OAuth profile must contain 1..256 bytes",
            ));
        }
        let directory = if let Some(path) = std::env::var_os("MCPX_AUTH_DIR") {
            PathBuf::from(path)
        } else {
            #[cfg(target_os = "windows")]
            let base = std::env::var_os("LOCALAPPDATA").map(PathBuf::from);
            #[cfg(target_os = "macos")]
            let base = std::env::var_os("HOME")
                .map(|p| PathBuf::from(p).join("Library/Application Support"));
            #[cfg(not(any(target_os = "windows", target_os = "macos")))]
            let base = std::env::var_os("XDG_STATE_HOME")
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".local/state"))
                });
            base.ok_or_else(|| {
                error(
                    "TokenStoreUnavailable",
                    "set MCPX_AUTH_DIR to a private authentication directory",
                )
            })?
            .join("mcpx/oauth")
        };
        let resource = mcpx_auth::canonical_resource(endpoint)?;
        Ok(Self {
            directory,
            key: format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&(profile, &resource)).map_err(|_| store_error())?
                )
            ),
        })
    }
    fn path(&self, suffix: &str) -> PathBuf {
        self.directory.join(format!("{}.{suffix}", self.key))
    }
    fn entry(&self) -> Result<keyring::Entry> {
        keyring::Entry::new("mcpx.oauth", &self.key).map_err(|_| store_error())
    }
    pub fn exists(&self) -> bool {
        self.path("profile.json").is_file()
    }
    fn kind(&self) -> Result<StoreKind> {
        let bytes = fs::read(self.path("profile.json")).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                login_required()
            } else {
                store_error()
            }
        })?;
        serde_json::from_slice::<Profile>(&bytes)
            .map(|p| p.store)
            .map_err(|_| store_error())
    }
    fn ensure_directory(&self) -> Result<()> {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&self.directory).map_err(|_| store_error())
    }
    pub async fn lock(&self) -> Result<File> {
        self.ensure_directory()?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(self.path("lock")).map_err(|_| store_error())?;
        loop {
            match fs2::FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(file),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.raw_os_error() == fs2::lock_contended_error().raw_os_error() =>
                {
                    tokio::time::sleep(Duration::from_millis(25)).await
                }
                Err(_) => return Err(store_error()),
            }
        }
    }
    fn write(&self, path: PathBuf, bytes: &[u8]) -> Result<()> {
        self.ensure_directory()?;
        let mut file =
            tempfile::NamedTempFile::new_in(&self.directory).map_err(|_| store_error())?;
        file.write_all(bytes)
            .and_then(|_| file.as_file().sync_all())
            .map_err(|_| store_error())?;
        file.persist(path).map_err(|_| store_error())?;
        Ok(())
    }
    pub fn load(&self) -> Result<Grant> {
        let source = match self.kind()? {
            StoreKind::File => fs::read(self.path("tokens.json")).map_err(|_| store_error())?,
            StoreKind::Keyring => self
                .entry()?
                .get_password()
                .map_err(|e| match e {
                    keyring::Error::NoEntry => login_required(),
                    _ => store_error(),
                })?
                .into_bytes(),
        };
        serde_json::from_slice(&source).map_err(|_| store_error())
    }
    pub fn save(&self, grant: &Grant, kind: Option<StoreKind>) -> Result<()> {
        let previous = self.kind().ok();
        let kind = kind.unwrap_or(previous.unwrap_or_default());
        let value = serde_json::to_string(grant).map_err(|_| store_error())?;
        match kind {
            StoreKind::File => self.write(self.path("tokens.json"), value.as_bytes())?,
            StoreKind::Keyring => self
                .entry()?
                .set_password(&value)
                .map_err(|_| store_error())?,
        }
        self.write(
            self.path("profile.json"),
            &serde_json::to_vec(&Profile { store: kind }).map_err(|_| store_error())?,
        )?;
        // Remove the previous backend only after the new grant is durable.
        if kind == StoreKind::Keyring {
            let _ = fs::remove_file(self.path("tokens.json"));
        } else if previous == Some(StoreKind::Keyring)
            && let Ok(entry) = self.entry()
        {
            let _ = entry.delete_credential();
        }
        Ok(())
    }
    pub fn delete(&self) -> Result<()> {
        match self.kind() {
            Ok(StoreKind::Keyring) => match self.entry()?.delete_credential() {
                Ok(()) | Err(keyring::Error::NoEntry) => {}
                Err(_) => return Err(store_error()),
            },
            Ok(StoreKind::File) => match fs::remove_file(self.path("tokens.json")) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(store_error()),
            },
            Err(e) if e.code == "LoginRequired" => return Ok(()),
            Err(e) => return Err(e),
        }
        fs::remove_file(self.path("profile.json")).map_err(|_| store_error())?;
        Ok(())
    }
}
fn store_error() -> Error {
    error(
        "TokenStoreUnavailable",
        "cannot access credential store; use auth login --store file for explicit private-file storage",
    )
}
