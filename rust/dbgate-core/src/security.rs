//! Credential vault for connection secrets.
//!
//! Secrets (the connection `password` and any `sshPassword`/`sshPassphrase`
//! inside `extra`) are stored in the OS credential store — macOS Keychain,
//! Windows Credential Manager, or the *nix Secret Service — keyed by the
//! connection id. `connections.jsonl` holds a `keyring:<user>` placeholder
//! instead of the secret. When the keyring is unavailable (headless CI, a
//! machine without a backend) callers embed plaintext instead and reading it
//! back is a no-op, so the file stays valid everywhere.

const KEYRING_SERVICE: &str = "dbgate";

/// Prefix of the placeholder written into `connections.jsonl` in place of a
/// vaulted secret. The text after the prefix is the keyring `user` key.
pub const SECRET_PLACEHOLDER_PREFIX: &str = "keyring:";

/// OS credential store, addressed by a fixed `service` and a per-connection
/// `user` key.
pub struct CredentialVault {
    service: &'static str,
}

impl CredentialVault {
    pub fn new() -> Self {
        Self {
            service: KEYRING_SERVICE,
        }
    }

    pub fn set_password(&self, user: &str, password: &str) -> keyring::Result<()> {
        keyring::Entry::new(self.service, user)?.set_password(password)
    }

    pub fn get_password(&self, user: &str) -> keyring::Result<String> {
        keyring::Entry::new(self.service, user)?.get_password()
    }

    pub fn delete_password(&self, user: &str) -> keyring::Result<()> {
        keyring::Entry::new(self.service, user)?.delete_credential()
    }
}

impl Default for CredentialVault {
    fn default() -> Self {
        Self::new()
    }
}

/// Abstraction over the credential store so connection routes can substitute
/// a deterministic stub in unit tests instead of the real OS keyring.
pub trait CredentialStore {
    fn set_password(&self, user: &str, password: &str) -> keyring::Result<()>;
    fn get_password(&self, user: &str) -> keyring::Result<String>;
    fn delete_password(&self, user: &str) -> keyring::Result<()>;
}

impl CredentialStore for CredentialVault {
    fn set_password(&self, user: &str, password: &str) -> keyring::Result<()> {
        Self::set_password(self, user, password)
    }

    fn get_password(&self, user: &str) -> keyring::Result<String> {
        Self::get_password(self, user)
    }

    fn delete_password(&self, user: &str) -> keyring::Result<()> {
        Self::delete_password(self, user)
    }
}

/// True when `s` is a stored `keyring:` placeholder rather than a secret.
pub fn is_placeholder(s: &str) -> bool {
    s.starts_with(SECRET_PLACEHOLDER_PREFIX)
}

/// Resolve a stored secret: a placeholder is looked up in the vault; on any
/// keyring error, and for plaintext input, the stored value is returned
/// unchanged (plaintext fallback).
pub fn resolve_password(vault: &dyn CredentialStore, stored: &str) -> String {
    match stored.strip_prefix(SECRET_PLACEHOLDER_PREFIX) {
        Some(user) => vault
            .get_password(user)
            .unwrap_or_else(|_| stored.to_string()),
        None => stored.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MemVault {
        secrets: Mutex<HashMap<String, String>>,
        fail: bool,
    }

    impl MemVault {
        fn ok() -> Self {
            Self {
                secrets: Mutex::new(HashMap::new()),
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                secrets: Mutex::new(HashMap::new()),
                fail: true,
            }
        }
    }

    impl CredentialStore for MemVault {
        fn set_password(&self, user: &str, password: &str) -> keyring::Result<()> {
            if self.fail {
                return Err(keyring::Error::NoEntry);
            }
            self.secrets
                .lock()
                .unwrap()
                .insert(user.to_string(), password.to_string());
            Ok(())
        }

        fn get_password(&self, user: &str) -> keyring::Result<String> {
            if self.fail {
                return Err(keyring::Error::NoEntry);
            }
            self.secrets
                .lock()
                .unwrap()
                .get(user)
                .cloned()
                .ok_or(keyring::Error::NoEntry)
        }

        fn delete_password(&self, user: &str) -> keyring::Result<()> {
            if self.fail {
                return Err(keyring::Error::NoEntry);
            }
            self.secrets.lock().unwrap().remove(user);
            Ok(())
        }
    }

    #[test]
    fn placeholder_detection() {
        assert!(is_placeholder("keyring:abc"));
        assert!(is_placeholder("keyring:abc:sshPassword"));
        assert!(!is_placeholder("plain-secret"));
        assert!(!is_placeholder("keyringx:abc"));
        assert!(!is_placeholder(""));
    }

    #[test]
    fn resolve_round_trips_placeholder_via_fake_store() {
        let vault = MemVault::ok();
        vault.set_password("conid", "hunter2").unwrap();
        assert_eq!(resolve_password(&vault, "keyring:conid"), "hunter2");
    }

    #[test]
    fn resolve_keeps_plaintext_unchanged() {
        let vault = MemVault::ok();
        assert_eq!(resolve_password(&vault, "plain-secret"), "plain-secret");
    }

    #[test]
    fn resolve_falls_back_to_stored_on_keyring_error() {
        let vault = MemVault::failing();
        assert_eq!(resolve_password(&vault, "keyring:conid"), "keyring:conid");
    }

    #[test]
    fn delete_password_removes_entry() {
        let vault = MemVault::ok();
        vault.set_password("conid", "hunter2").unwrap();
        vault.delete_password("conid").unwrap();
        match vault.get_password("conid") {
            Err(keyring::Error::NoEntry) => {}
            other => panic!("expected NoEntry, got {other:?}"),
        }
    }
}
