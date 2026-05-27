//! `P2poolV2D` — spawn a `p2poolv2` binary against a `BitcoinD` for
//! integration tests.
//!
//! Mirrors [`corepc_node::Node`]'s lifecycle pattern: three-tier binary
//! discovery (`P2POOLV2_EXE` env var → PATH → fail), tempdir datadir,
//! OS-allocated free ports for libp2p / Stratum / API / ZMQ stubs,
//! `Drop` kills the child process and cleans up the tempdir.
//!
//! ## Discovery
//!
//! 1. `P2POOLV2_EXE` env var if set (test/CI override).
//! 2. `p2poolv2` binary on `$PATH`.
//! 3. Else: [`P2poolV2DError::BinaryNotFound`] — caller marks the test
//!    `#[ignore]` if the binary isn't expected to be present.
//!
//! ## Config generation
//!
//! Writes a minimal TOML to `tempdir()/p2pool.toml` with:
//! - `[store] path = tempdir()/store.db`
//! - `[stratum] hostname = 127.0.0.1, port = 0` (let the OS pick)
//! - `[bitcoinrpc]` derived from the supplied [`BitcoinD`]
//! - `[network]` listen on `127.0.0.1` random port, no dial peers
//! - `[api] hostname = 127.0.0.1, port = 0` (random)
//!
//! `[stratum].network` is forced to `signet` because p2poolv2's genesis
//! builder doesn't yet support regtest — this matches the Phase 2.5b
//! workaround. Tests should compose the share-chain layer at signet
//! difficulty against the regtest bitcoind.
//!
//! ## Readiness
//!
//! [`P2poolV2DBuilder::build`] blocks until the child process exits its
//! preflight phase OR a timeout elapses. Phase 2.6 keeps the readiness
//! signal simple: poll the API hostname:port until it accepts a TCP
//! connection. Phase 2.7 may upgrade to "API responds 200 OK on a
//! known endpoint" once the API is exercised.

use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use thiserror::Error;
use tracing::{debug, info, warn};

use crate::BitcoinD;

/// Default readiness timeout. Cold-start of `p2poolv2_node` is
/// dominated by rocksdb open + libp2p bind; 5s is generous for tests.
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors from p2poolv2 spawning.
#[derive(Debug, Error)]
pub enum P2poolV2DError {
    #[error("p2poolv2 binary not found: set P2POOLV2_EXE env var or place `p2poolv2` on PATH")]
    BinaryNotFound,
    #[error("failed to allocate a free TCP port: {0}")]
    PortAllocation(String),
    #[error("failed to create tempdir: {0}")]
    Tempdir(String),
    #[error("failed to write p2pool config: {0}")]
    WriteConfig(String),
    #[error("failed to spawn p2poolv2: {0}")]
    Spawn(String),
    #[error("p2poolv2 did not become ready within {0:?}")]
    ReadinessTimeout(Duration),
    #[error("p2poolv2 exited during startup")]
    ExitedDuringStartup,
}

/// A running `p2poolv2` child process with auto-cleanup on `Drop`.
///
/// Hold this across the lifetime of the test; dropping it kills the
/// child process (best-effort SIGKILL) and removes the tempdir.
pub struct P2poolV2D {
    child: Child,
    /// Tempdir for the child's datadir + config. Kept alive for the
    /// lifetime of the spawner so it isn't deleted while the child runs.
    _tempdir: tempfile::TempDir,
    /// Path to the generated config TOML.
    pub config_path: PathBuf,
    /// API listen address (the random `[api]` port the child bound to,
    /// derived from the spawner's allocation).
    pub api_addr: SocketAddr,
    /// Stratum listen address.
    pub stratum_addr: SocketAddr,
    /// libp2p listen multiaddr-ish hint (the TCP port we asked for).
    pub libp2p_port: u16,
}

impl Drop for P2poolV2D {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(status)) => {
                debug!(?status, "p2poolv2 already exited");
            }
            Ok(None) => {
                if let Err(e) = self.child.kill() {
                    warn!(error = %e, "failed to kill p2poolv2 child");
                }
                let _ = self.child.wait();
            }
            Err(e) => warn!(error = %e, "p2poolv2 try_wait failed during Drop"),
        }
    }
}

/// Builder for [`P2poolV2D`].
pub struct P2poolV2DBuilder<'a> {
    bitcoind: &'a BitcoinD,
    p2poolv2_exe: Option<PathBuf>,
    ready_timeout: Duration,
}

impl<'a> P2poolV2DBuilder<'a> {
    /// Start a builder for a `p2poolv2` running against `bitcoind`.
    pub fn new(bitcoind: &'a BitcoinD) -> Self {
        Self {
            bitcoind,
            p2poolv2_exe: None,
            ready_timeout: DEFAULT_READY_TIMEOUT,
        }
    }

    /// Override the binary path.
    pub fn with_exe(mut self, path: impl Into<PathBuf>) -> Self {
        self.p2poolv2_exe = Some(path.into());
        self
    }

    /// Override the readiness timeout (default
    /// [`DEFAULT_READY_TIMEOUT`]).
    pub fn with_ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// Spawn p2poolv2 and wait for readiness.
    pub fn build(self) -> Result<P2poolV2D, P2poolV2DError> {
        let exe = self
            .p2poolv2_exe
            .clone()
            .or_else(|| std::env::var_os("P2POOLV2_EXE").map(PathBuf::from))
            .or_else(find_p2poolv2_on_path)
            .ok_or(P2poolV2DError::BinaryNotFound)?;
        info!(exe = %exe.display(), "starting p2poolv2");

        let tempdir = tempfile::tempdir().map_err(|e| P2poolV2DError::Tempdir(e.to_string()))?;
        let store_path = tempdir.path().join("store.db");
        let stats_dir = tempdir.path().join("stats");
        std::fs::create_dir_all(&stats_dir)
            .map_err(|e| P2poolV2DError::WriteConfig(e.to_string()))?;

        // Allocate four free ports (libp2p, stratum, ZMQ pub, API).
        // Drop the listeners immediately so the child can rebind them;
        // there's a tiny race window but it's tolerable for tests.
        let libp2p_port = allocate_free_port()?;
        let stratum_port = allocate_free_port()?;
        let zmq_port = allocate_free_port()?;
        let api_port = allocate_free_port()?;

        let bitcoinrpc_url = self.bitcoind.rpc_url();
        // corepc-node uses cookie-based auth by default; reset password
        // is not exposed, so we read the cookie file path. For
        // simplicity Phase 2.6 uses the rpcuser/rpcpassword from
        // corepc-node's parsed config — corepc exposes via `params()`.
        let (user, pass) = bitcoind_credentials(self.bitcoind);

        let config_path = tempdir.path().join("p2pool.toml");
        let toml = format!(
            r#"
[network]
listen_address = "/ip4/127.0.0.1/tcp/{libp2p_port}"
dial_peers = []
max_pending_incoming = 10
max_pending_outgoing = 10
max_established_incoming = 50
max_established_outgoing = 50
max_established_per_peer = 1
max_workbase_per_second = 10
max_userworkbase_per_second = 10
max_miningshare_per_second = 100
max_inventory_per_second = 100
max_transaction_per_second = 100
max_requests_per_second = 100
dial_timeout_secs = 30

[store]
path = "{store_path}"
background_task_frequency_hours = 24
pplns_ttl_days = 7

[stratum]
hostname = "127.0.0.1"
port = {stratum_port}
start_difficulty = 10000
minimum_difficulty = 100
solo_address = "tb1qyazxde6558qj6z3d9np5e6msmrspwpf6k0qggk"
bootstrap_address = "tb1qyazxde6558qj6z3d9np5e6msmrspwpf6k0qggk"
zmqpubhashblock = "tcp://127.0.0.1:{zmq_port}"
network = "signet"
version_mask = "1fffe000"
difficulty_multiplier = 1.0
pool_signature = "sv2-p2pool-testenv"

[bitcoinrpc]
url = "{bitcoinrpc_url}"
username = "{user}"
password = "{pass}"

[logging]
console = true
level = "info"
stats_dir = "{stats_dir}"

[api]
hostname = "127.0.0.1"
port = {api_port}
"#,
            store_path = store_path.display(),
            stats_dir = stats_dir.display(),
        );
        std::fs::write(&config_path, toml.as_bytes())
            .map_err(|e| P2poolV2DError::WriteConfig(e.to_string()))?;
        debug!(config_path = %config_path.display(), "wrote p2pool config");

        let child = Command::new(&exe)
            .arg("-c")
            .arg(&config_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| P2poolV2DError::Spawn(e.to_string()))?;
        info!(pid = child.id(), "p2poolv2 spawned");

        let api_addr = SocketAddr::from(([127, 0, 0, 1], api_port));
        let stratum_addr = SocketAddr::from(([127, 0, 0, 1], stratum_port));
        let mut spawner = P2poolV2D {
            child,
            _tempdir: tempdir,
            config_path,
            api_addr,
            stratum_addr,
            libp2p_port,
        };

        wait_for_ready(&mut spawner, self.ready_timeout)?;
        Ok(spawner)
    }
}

/// Locate `p2poolv2` on `$PATH`.
fn find_p2poolv2_on_path() -> Option<PathBuf> {
    static CACHED: OnceLock<Option<PathBuf>> = OnceLock::new();
    CACHED.get_or_init(|| which_compat("p2poolv2")).clone()
}

/// Minimal `which`-style PATH lookup (avoid taking a `which` dep). On
/// Windows, also tries `.exe`.
fn which_compat(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let exe = candidate.with_extension("exe");
            if exe.is_file() {
                return Some(exe);
            }
        }
    }
    None
}

/// Allocate a free TCP port by binding to `127.0.0.1:0`, recording the
/// port, and dropping the listener. Subject to a TOCTOU race but
/// acceptable for tests.
fn allocate_free_port() -> Result<u16, P2poolV2DError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|e| P2poolV2DError::PortAllocation(e.to_string()))?;
    let port = listener
        .local_addr()
        .map_err(|e| P2poolV2DError::PortAllocation(e.to_string()))?
        .port();
    Ok(port)
}

/// Pull bitcoind RPC credentials from a `corepc_node::Node`.
///
/// `corepc-node` exposes credentials via `cookie_values()` which
/// returns `Some((user, pass))` for cookie-auth bitcoinds. If that's
/// unavailable, fall back to a plausible default — tests that need the
/// real creds should set `BITCOIND_EXE` to a binary that supports
/// rpcuser/rpcpassword.
fn bitcoind_credentials(bitcoind: &BitcoinD) -> (String, String) {
    if let Some(cookie) = bitcoind.params.get_cookie_values().ok().flatten() {
        return (cookie.user, cookie.password);
    }
    ("user".to_string(), "pass".to_string())
}

/// Block until the child's API port accepts a TCP connection or
/// `timeout` elapses.
fn wait_for_ready(spawner: &mut P2poolV2D, timeout: Duration) -> Result<(), P2poolV2DError> {
    let deadline = Instant::now() + timeout;
    loop {
        // If the child died, fail fast.
        match spawner.child.try_wait() {
            Ok(Some(status)) => {
                warn!(?status, "p2poolv2 exited before becoming ready");
                return Err(P2poolV2DError::ExitedDuringStartup);
            }
            Ok(None) => {}
            Err(e) => warn!(error = %e, "try_wait error during readiness poll"),
        }

        if TcpStream::connect_timeout(&spawner.api_addr, Duration::from_millis(100)).is_ok() {
            info!(api_addr = %spawner.api_addr, "p2poolv2 API ready");
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(P2poolV2DError::ReadinessTimeout(timeout));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Avoid clippy::write-with-newline by using writeln from `Write`. Kept
/// for symmetry with corepc-node's pattern.
#[allow(dead_code)]
fn _force_writeln_in_scope() {
    let _ = std::io::sink().write_all(b"");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestEnv;

    /// Smoke test: spawn p2poolv2 against a bitcoind regtest, verify
    /// the API port becomes reachable. Requires both `BITCOIND_EXE` and
    /// `P2POOLV2_EXE` env vars (or matching binaries on PATH); CI
    /// shouldn't run by default.
    #[test]
    #[ignore = "requires P2POOLV2_EXE + BITCOIND_EXE — run locally"]
    fn smoke_p2poolv2_boots_against_bitcoind() {
        let env = TestEnv::new().expect("bitcoind starts");
        let _p2pool = P2poolV2DBuilder::new(&env.bitcoind)
            .build()
            .expect("p2poolv2 starts");
    }

    #[test]
    fn which_compat_returns_none_for_missing_binary() {
        // Sanity-check the PATH lookup helper: an obviously-fake binary
        // name returns None.
        let result = which_compat("definitely-not-a-real-binary-12345-xyz-zzz");
        assert!(result.is_none());
    }
}
