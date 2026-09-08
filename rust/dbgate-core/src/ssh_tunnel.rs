//! SSH tunnel support.
//!
//! Rust port of the Node `sshTunnel.js` / `sshForwardProcess.js` utilities on
//! top of the [`openssh`] crate. The crate spawns the system `ssh` binary as a
//! control master (`process-mux` feature), so no SSH protocol implementation
//! lives in tree and the same OpenSSH agent / key handling the Node app
//! relies on is used.
//!
//! A tunnel is a local port on `127.0.0.1` forwarded to the database server
//! through the configured SSH gateway. Drivers open one tunnel per remote
//! (host, port) pair, then connect to the returned local endpoint instead of
//! the real server address. The tunnel must stay alive for the lifetime of the
//! database connection, so each driver connection struct stores the
//! [`SshTunnel`] it opened.

use tokio::runtime::Runtime;

use openssh::{ForwardType, Session, SessionBuilder, Socket};

use crate::connection::ConnectionDefinition;
use crate::error::{DbgmError, DbgmResult};

/// The local bind host for tunnel forwards. Mirrors the Node default
/// `connection.sshBindHost` / `127.0.0.1`.
const LOCAL_HOST: &str = "127.0.0.1";

/// A live SSH tunnel: an SSH session to the gateway with one `-L` style local
/// forward registered on it. Kept inside driver connection structs; dropping
/// it severs the SSH session and the forward.
pub struct SshTunnel {
    #[allow(dead_code)]
    session: Session,
    #[allow(dead_code)]
    runtime: Runtime,
    local_port: u16,
}

impl SshTunnel {
    /// Open a local forward to `connect_host:connect_port` through the SSH
    /// gateway described in `def`, returning `Ok(None)` when the connection
    /// does not request a tunnel (`useSshTunnel` absent/false).
    pub fn open(
        def: &ConnectionDefinition,
        connect_host: &str,
        connect_port: u16,
    ) -> DbgmResult<Option<SshTunnel>> {
        if !def.use_ssh_tunnel.unwrap_or(false) {
            return Ok(None);
        }

        let ssh_host = def
            .ssh_host
            .clone()
            .ok_or_else(|| DbgmError::new("SSH tunnel requires a host (sshHost)"))?;
        let ssh_port = def.ssh_port.unwrap_or(22) as u16;

        let mut builder = SessionBuilder::default();
        builder.port(ssh_port);
        if let Some(login) = &def.ssh_login {
            builder.user(login.clone());
        }
        apply_auth(def, &mut builder)?;
        if let Some(bastion) = &def.ssh_bastion_host {
            builder.jump_hosts([bastion.as_str()]);
        }

        let local_port = find_free_local_port()?;

        let runtime = Runtime::new()
            .map_err(|e| DbgmError::with_source("Cannot start tokio runtime for SSH tunnel", e))?;

        let session = runtime
            .block_on(builder.connect(ssh_host.as_str()))
            .map_err(|e| {
                DbgmError::with_source(format!("Cannot open SSH tunnel to {ssh_host}"), e)
            })?;

        runtime
            .block_on(session.request_port_forward(
                ForwardType::Local,
                Socket::new(LOCAL_HOST, local_port),
                Socket::new(connect_host, connect_port),
            ))
            .map_err(|e| {
                DbgmError::with_source(
                    format!(
                        "Cannot forward local port {local_port} to {connect_host}:{connect_port} via {ssh_host}"
                    ),
                    e,
                )
            })?;

        Ok(Some(SshTunnel {
            session,
            runtime,
            local_port,
        }))
    }

    /// The local endpoint drivers connect to instead of the real server.
    pub fn local_endpoint(&self) -> (String, u16) {
        (LOCAL_HOST.to_string(), self.local_port)
    }
}

/// Configure SSH authentication on the builder according to `sshMode`.
fn apply_auth(def: &ConnectionDefinition, builder: &mut SessionBuilder) -> DbgmResult<()> {
    match def.ssh_mode.as_deref() {
        // `userPassword` is the frontend default; the 0.11 openssh crate has no
        // password auth API, so refusing loudly is better than silent failure.
        None | Some("userPassword") => Err(DbgmError::new(
            "SSH username/password authentication is not yet supported by the Rust backend; use an SSH agent or a private key file",
        )),
        Some("keyFile") => {
            let keyfile = def
                .ssh_keyfile
                .clone()
                .ok_or_else(|| DbgmError::new("SSH keyFile mode requires a private key path (sshKeyfile)"))?;
            if def
                .ssh_keyfile_password
                .as_deref()
                .is_some_and(|p| !p.is_empty())
            {
                return Err(DbgmError::new(
                    "SSH private key passphrase is not yet supported by the Rust backend; use a key without a passphrase or an SSH agent",
                ));
            }
            builder.keyfile(keyfile);
            Ok(())
        }
        Some("agent") => Ok(()),
        Some(other) => Err(DbgmError::new(format!(
            "Unknown SSH authentication mode '{other}'"
        ))),
    }
}

/// Reserve a free local port by binding an ephemeral socket, reading the
/// assigned port, and dropping the listener so OpenSSH (`-L`) can bind it.
fn find_free_local_port() -> DbgmResult<u16> {
    let listener = std::net::TcpListener::bind((LOCAL_HOST, 0))
        .map_err(|e| DbgmError::with_source("Cannot bind a local port for SSH tunnel", e))?;
    let port = listener
        .local_addr()
        .map_err(|e| DbgmError::with_source("Cannot read local port for SSH tunnel", e))?
        .port();
    drop(listener);
    Ok(port)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionDefinition;

    fn ssh_def(use_tunnel: bool) -> ConnectionDefinition {
        ConnectionDefinition {
            use_ssh_tunnel: Some(use_tunnel),
            ..Default::default()
        }
    }

    #[test]
    fn open_without_tunnel_returns_none() {
        assert!(SshTunnel::open(&ssh_def(false), "localhost", 5432)
            .unwrap()
            .is_none());
    }

    #[test]
    fn open_requires_ssh_host() {
        match SshTunnel::open(&ssh_def(true), "localhost", 5432) {
            Err(err) => assert!(err.message.contains("requires a host")),
            Ok(_) => panic!("expected an SSH host error"),
        }
    }

    #[test]
    fn user_password_mode_is_rejected() {
        let mut def = ssh_def(false);
        def.ssh_mode = Some("userPassword".to_string());
        let err = apply_auth(&def, &mut SessionBuilder::default()).unwrap_err();
        assert!(err.message.contains("username/password"));
    }

    #[test]
    fn missing_mode_defaults_to_user_password() {
        let mut def = ssh_def(false);
        def.ssh_mode = None;
        let err = apply_auth(&def, &mut SessionBuilder::default()).unwrap_err();
        assert!(err.message.contains("username/password"));
    }

    #[test]
    fn keyfile_mode_requires_keyfile() {
        let mut def = ssh_def(false);
        def.ssh_mode = Some("keyFile".to_string());
        def.ssh_keyfile = None;
        let err = apply_auth(&def, &mut SessionBuilder::default()).unwrap_err();
        assert!(err.message.contains("requires a private key"));
    }

    #[test]
    fn keyfile_passphrase_is_rejected() {
        let mut def = ssh_def(false);
        def.ssh_mode = Some("keyFile".to_string());
        def.ssh_keyfile = Some("~/.ssh/id_rsa".to_string());
        def.ssh_keyfile_password = Some("s3cret".to_string());
        let err = apply_auth(&def, &mut SessionBuilder::default()).unwrap_err();
        assert!(err.message.contains("passphrase"));
    }

    #[test]
    fn agent_mode_is_accepted() {
        let mut def = ssh_def(false);
        def.ssh_mode = Some("agent".to_string());
        assert!(apply_auth(&def, &mut SessionBuilder::default()).is_ok());
    }

    #[test]
    fn unknown_mode_is_rejected() {
        let mut def = ssh_def(false);
        def.ssh_mode = Some("gssapi".to_string());
        let err = apply_auth(&def, &mut SessionBuilder::default()).unwrap_err();
        assert!(err.message.contains("Unknown SSH authentication mode"));
    }

    #[test]
    fn find_free_local_port_yields_bindable_port() {
        let port = find_free_local_port().unwrap();
        assert!(port > 0);
        let listener = std::net::TcpListener::bind((LOCAL_HOST, port)).unwrap();
        listener.set_nonblocking(true).unwrap();
    }
}