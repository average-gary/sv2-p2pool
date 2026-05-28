//! `JdClientD` — spawn the sv2-apps `jd_client_sv2` binary against a
//! [`Sv2P2poolD`].
//!
//! Mirrors the spawner pattern of [`crate::p2poolv2d::P2poolV2D`] and
//! [`crate::sv2_p2pool_d::Sv2P2poolD`]: three-tier binary discovery,
//! tempdir-based config, OS-allocated free ports, Drop kills the
//! child.
//!
//! ## Discovery
//!
//! 1. `JD_CLIENT_EXE` env var.
//! 2. `vendor/sv2-apps/target/debug/jd_client_sv2` relative to the
//!    workspace root (since sv2-apps is a separate cargo workspace
//!    nested under `vendor/`).
//! 3. `jd_client_sv2` on `$PATH`.
//!
//! ## Config
//!
//! Generates a single `jdc-config.toml` in the spawner's tempdir:
//! - `listening_address` for downstream miners (random free port)
//! - `[[upstreams]]` pointing at the supplied [`Sv2P2poolD`]'s pool
//!   + JDS addresses, with the matching test authority pubkey
//! - `[template_provider_type.BitcoinCoreIpc]` for the
//!   [`crate::BitcoinD`]'s data directory + the chosen network
//!
//! ## Authority keys
//!
//! Reuses [`crate::sv2_p2pool_d::TEST_AUTHORITY_PUBLIC_KEY`] (and the
//! matching secret key) — JDC must use the same authority pubkey it
//! expects the pool/JDS to present in the noise handshake.

use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use thiserror::Error;
use tracing::{debug, info, warn};

use crate::sv2_p2pool_d::{Sv2P2poolD, TEST_AUTHORITY_PUBLIC_KEY, TEST_AUTHORITY_SECRET_KEY};
use crate::{BitcoinD, p2poolv2d};

/// Default readiness timeout: cold start spans config parse + noise
/// handshake against the pool/JDS. 10s is generous for tests.
pub const DEFAULT_JD_CLIENT_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors from JDC spawning.
#[derive(Debug, Error)]
pub enum JdClientDError {
    #[error(
        "jd_client_sv2 binary not found: set JD_CLIENT_EXE env var, build it with `cargo build --manifest-path vendor/sv2-apps/miner-apps/Cargo.toml`, or place it on PATH"
    )]
    BinaryNotFound,
    #[error("failed to allocate a free TCP port: {0}")]
    PortAllocation(String),
    #[error("failed to create tempdir: {0}")]
    Tempdir(String),
    #[error("failed to write JDC config: {0}")]
    WriteConfig(String),
    #[error("failed to spawn jd_client_sv2: {0}")]
    Spawn(String),
    #[error("jd_client_sv2 did not become ready within {0:?}")]
    ReadinessTimeout(Duration),
    #[error("jd_client_sv2 exited during startup")]
    ExitedDuringStartup,
}

/// A running `jd_client_sv2` child process with auto-cleanup on Drop.
pub struct JdClientD {
    child: Child,
    _tempdir: tempfile::TempDir,
    pub config_path: PathBuf,
    /// Downstream listen address — where miners (or the test) connect.
    pub listening_addr: SocketAddr,
}

impl Drop for JdClientD {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(status)) => debug!(?status, "jd_client_sv2 already exited"),
            Ok(None) => {
                if let Err(e) = self.child.kill() {
                    warn!(error = %e, "failed to kill jd_client_sv2 child");
                }
                let _ = self.child.wait();
            }
            Err(e) => warn!(error = %e, "jd_client_sv2 try_wait failed during Drop"),
        }
    }
}

/// Builder for [`JdClientD`].
pub struct JdClientDBuilder<'a> {
    bitcoind: &'a BitcoinD,
    upstream: &'a Sv2P2poolD,
    jd_client_exe: Option<PathBuf>,
    ready_timeout: Duration,
    network: bitcoin::Network,
    bitcoin_data_dir: Option<PathBuf>,
}

impl<'a> JdClientDBuilder<'a> {
    pub fn new(bitcoind: &'a BitcoinD, upstream: &'a Sv2P2poolD) -> Self {
        Self {
            bitcoind,
            upstream,
            jd_client_exe: None,
            ready_timeout: DEFAULT_JD_CLIENT_READY_TIMEOUT,
            network: bitcoin::Network::Testnet4,
            bitcoin_data_dir: None,
        }
    }

    pub fn with_exe(mut self, path: impl Into<PathBuf>) -> Self {
        self.jd_client_exe = Some(path.into());
        self
    }

    pub fn with_ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    pub fn with_network(mut self, network: bitcoin::Network) -> Self {
        self.network = network;
        self
    }

    pub fn with_bitcoin_data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.bitcoin_data_dir = Some(dir.into());
        self
    }

    pub fn build(self) -> Result<JdClientD, JdClientDError> {
        let exe = self
            .jd_client_exe
            .clone()
            .or_else(|| std::env::var_os("JD_CLIENT_EXE").map(PathBuf::from))
            .or_else(find_jd_client_in_sv2_apps_target)
            .or_else(|| p2poolv2d::which_compat_pub("jd_client_sv2"))
            .ok_or(JdClientDError::BinaryNotFound)?;
        info!(exe = %exe.display(), "starting jd_client_sv2");

        let tempdir = tempfile::tempdir().map_err(|e| JdClientDError::Tempdir(e.to_string()))?;

        let listening_port = allocate_free_port()?;
        let monitoring_port = allocate_free_port()?;
        let network_name = network_to_name(self.network);
        let coinbase_addr = address_for_network(self.network);
        let pool_addr = self.upstream.mining_addr;
        let jds_addr = self.upstream.jds_addr;

        let mut toml = format!(
            r#"
listening_address = "127.0.0.1:{listening_port}"

max_supported_version = 2
min_supported_version = 2

authority_public_key = "{TEST_AUTHORITY_PUBLIC_KEY}"
authority_secret_key = "{TEST_AUTHORITY_SECRET_KEY}"
cert_validity_sec = 3600

user_identity = "sv2-p2pool-testenv"

shares_per_minute = 6.0
share_batch_size = 10

mode = "FULLTEMPLATE"

jdc_signature = "sv2-p2pool-test"

coinbase_reward_script = "addr({coinbase_addr})"

supported_extensions = []

monitoring_address = "127.0.0.1:{monitoring_port}"
monitoring_cache_refresh_secs = 15

[[upstreams]]
authority_pubkey = "{TEST_AUTHORITY_PUBLIC_KEY}"
pool_address = "{pool_ip}"
pool_port = {pool_port}
jds_address = "{jds_ip}"
jds_port = {jds_port}

[template_provider_type.BitcoinCoreIpc]
network = "{network_name}"
fee_threshold = 100
min_interval = 5
"#,
            pool_ip = pool_addr.ip(),
            pool_port = pool_addr.port(),
            jds_ip = jds_addr.ip(),
            jds_port = jds_addr.port(),
        );
        if let Some(dir) = self.bitcoin_data_dir.as_ref() {
            toml.push_str(&format!("data_dir = \"{}\"\n", dir.display()));
        }

        let config_path = tempdir.path().join("jdc-config.toml");
        std::fs::write(&config_path, toml.as_bytes())
            .map_err(|e| JdClientDError::WriteConfig(e.to_string()))?;
        debug!(config_path = %config_path.display(), "wrote jdc config");

        let _ = self.bitcoind; // silence unused-field clippy until we
        // wire bitcoind credentials into JDC. JDC currently reads
        // bitcoind via IPC (no rpc creds needed) — the field is kept
        // for symmetry with the other spawners.

        let child = Command::new(&exe)
            .arg("-c")
            .arg(&config_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| JdClientDError::Spawn(e.to_string()))?;
        info!(pid = child.id(), "jd_client_sv2 spawned");

        let listening_addr = SocketAddr::from(([127, 0, 0, 1], listening_port));
        let mut spawner = JdClientD {
            child,
            _tempdir: tempdir,
            config_path,
            listening_addr,
        };

        wait_for_ready(&mut spawner, self.ready_timeout)?;
        Ok(spawner)
    }
}

/// Try to find `jd_client_sv2` under `vendor/sv2-apps/target/`. The
/// sv2-apps subworkspace builds into its own target/ directory; our
/// outer workspace doesn't include it as a member.
fn find_jd_client_in_sv2_apps_target() -> Option<PathBuf> {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")?;
    let mut path = PathBuf::from(manifest_dir);
    // testenv crate is at <ws>/crates/sv2-p2pool-testenv. Walk up to <ws>.
    for _ in 0..3 {
        if !path.pop() {
            return None;
        }
        // sv2-apps has multiple subworkspaces (miner-apps, pool-apps,
        // ...). The jd_client_sv2 binary lives under miner-apps.
        for profile in ["debug", "release"] {
            let candidate = path
                .join("vendor")
                .join("sv2-apps")
                .join("miner-apps")
                .join("target")
                .join(profile)
                .join("jd_client_sv2");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn allocate_free_port() -> Result<u16, JdClientDError> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| JdClientDError::PortAllocation(e.to_string()))?;
    let port = listener
        .local_addr()
        .map_err(|e| JdClientDError::PortAllocation(e.to_string()))?
        .port();
    Ok(port)
}

fn network_to_name(network: bitcoin::Network) -> &'static str {
    match network {
        bitcoin::Network::Bitcoin => "mainnet",
        bitcoin::Network::Testnet => "testnet",
        bitcoin::Network::Testnet4 => "testnet4",
        bitcoin::Network::Signet => "signet",
        bitcoin::Network::Regtest => "regtest",
    }
}

fn address_for_network(network: bitcoin::Network) -> &'static str {
    match network {
        bitcoin::Network::Bitcoin => "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
        bitcoin::Network::Testnet | bitcoin::Network::Testnet4 | bitcoin::Network::Signet => {
            "tb1qpusf5256yxv50qt0pm0tue8k952fsu5lzsphft"
        }
        bitcoin::Network::Regtest => "bcrt1qx2nf3uvxq6h2vksjwgdvnwhvexmkqwqx0vexap",
    }
}

fn wait_for_ready(spawner: &mut JdClientD, timeout: Duration) -> Result<(), JdClientDError> {
    let deadline = Instant::now() + timeout;
    loop {
        match spawner.child.try_wait() {
            Ok(Some(status)) => {
                warn!(?status, "jd_client_sv2 exited before becoming ready");
                return Err(JdClientDError::ExitedDuringStartup);
            }
            Ok(None) => {}
            Err(e) => warn!(error = %e, "try_wait error during readiness poll"),
        }

        // JDC binds its listening_address before completing the
        // upstream handshake, so we use TCP accept as the readiness
        // signal — same approach as the other spawners.
        if TcpStream::connect_timeout(&spawner.listening_addr, Duration::from_millis(100)).is_ok() {
            info!(listening_addr = %spawner.listening_addr, "jd_client_sv2 listening");
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(JdClientDError::ReadinessTimeout(timeout));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_target_lookup_returns_a_pathbuf_or_none() {
        // Sanity: helper either finds the binary or returns None;
        // shouldn't panic regardless of build state.
        let _ = find_jd_client_in_sv2_apps_target();
    }
}
