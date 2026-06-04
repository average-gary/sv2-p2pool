//! `TranslatorSv2D` — spawn the sv2-apps `translator_sv2` binary
//! against a [`JdClientD`].
//!
//! Mirrors the spawner pattern of [`crate::jdcd::JdClientD`]: three-tier
//! binary discovery, tempdir-based config, OS-allocated free ports,
//! Drop kills the child.
//!
//! ## Topology
//!
//! ```text
//! SV1 miner  ──►  TranslatorSv2D (SV1↔SV2)  ──►  JdClientD  ──►  Sv2P2poolD
//! ```
//!
//! The translator listens on `downstream_port` for SV1 mining clients
//! and forwards SV2 traffic upstream to `JdClientD.listening_addr`.
//! `enable_vardiff = false` is required when running with a JDC
//! (per the upstream README — JDC supplies its own difficulty).
//!
//! ## Discovery
//!
//! 1. `TRANSLATOR_SV2_EXE` env var.
//! 2. `vendor/sv2-apps/target/<profile>/translator_sv2` relative to the
//!    workspace root (sv2-apps's miner-apps subworkspace builds into
//!    its own target/ directory; or the parent vendor/sv2-apps/target
//!    when built at the top sv2-apps workspace level).
//! 3. `translator_sv2` on `$PATH`.
//!
//! ## Config
//!
//! Generates a single `translator-config.toml` in the spawner's
//! tempdir wired to:
//! - `downstream_address = 127.0.0.1`, `downstream_port` = OS-allocated
//!   free port (where SV1 miners connect)
//! - `[[upstreams]]` pointing at the supplied `JdClientD.listening_addr`
//!   with `TEST_AUTHORITY_PUBLIC_KEY`
//! - `aggregate_channels = false` (per-miner channel)
//! - `[downstream_difficulty_config] enable_vardiff = false`
//!   (JDC mode), `min_individual_miner_hashrate = 1_000_000.0`
//!   (1 MH/s — low enough that a CPU miner reaches the channel
//!   target in tests)

use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use thiserror::Error;
use tracing::{debug, info, warn};

use crate::jdcd::JdClientD;
use crate::p2poolv2d;
use crate::sv2_p2pool_d::TEST_AUTHORITY_PUBLIC_KEY;

/// Default readiness timeout: cold start spans config parse + noise
/// handshake against the JDC. 10s is generous for tests.
pub const DEFAULT_TRANSLATOR_SV2_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors from translator_sv2 spawning.
#[derive(Debug, Error)]
pub enum TranslatorSv2DError {
    #[error(
        "translator_sv2 binary not found: set TRANSLATOR_SV2_EXE env var, build it with `cargo build --manifest-path vendor/sv2-apps/miner-apps/Cargo.toml --bin translator_sv2`, or place it on PATH"
    )]
    BinaryNotFound,
    #[error("failed to allocate a free TCP port: {0}")]
    PortAllocation(String),
    #[error("failed to create tempdir: {0}")]
    Tempdir(String),
    #[error("failed to write translator config: {0}")]
    WriteConfig(String),
    #[error("failed to spawn translator_sv2: {0}")]
    Spawn(String),
    #[error("translator_sv2 did not become ready within {0:?}")]
    ReadinessTimeout(Duration),
    #[error("translator_sv2 exited during startup")]
    ExitedDuringStartup,
}

/// A running `translator_sv2` child process with auto-cleanup on Drop.
pub struct TranslatorSv2D {
    child: Child,
    _tempdir: tempfile::TempDir,
    pub config_path: PathBuf,
    /// SV1 downstream listen address — where SV1 miners (e.g. mujina)
    /// connect.
    pub downstream_addr: SocketAddr,
}

impl Drop for TranslatorSv2D {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(status)) => debug!(?status, "translator_sv2 already exited"),
            Ok(None) => {
                if let Err(e) = self.child.kill() {
                    warn!(error = %e, "failed to kill translator_sv2 child");
                }
                let _ = self.child.wait();
            }
            Err(e) => warn!(error = %e, "translator_sv2 try_wait failed during Drop"),
        }
    }
}

/// Builder for [`TranslatorSv2D`].
pub struct TranslatorSv2DBuilder<'a> {
    upstream: &'a JdClientD,
    translator_sv2_exe: Option<PathBuf>,
    ready_timeout: Duration,
    user_identity: String,
    /// Floor on the per-miner difficulty target the translator hands
    /// out. Lower = trivial CPU shares; tests should leave at the
    /// default unless they specifically want to suppress shares.
    min_individual_miner_hashrate: f64,
    shares_per_minute: f64,
}

impl<'a> TranslatorSv2DBuilder<'a> {
    pub fn new(upstream: &'a JdClientD) -> Self {
        Self {
            upstream,
            translator_sv2_exe: None,
            ready_timeout: DEFAULT_TRANSLATOR_SV2_READY_TIMEOUT,
            user_identity: "sv2-p2pool-testenv".to_string(),
            // 1 MH/s floor — well within CPU range so the channel
            // target a CPU miner sees is trivially achievable.
            min_individual_miner_hashrate: 1_000_000.0,
            shares_per_minute: 6.0,
        }
    }

    pub fn with_exe(mut self, path: impl Into<PathBuf>) -> Self {
        self.translator_sv2_exe = Some(path.into());
        self
    }

    pub fn with_ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    pub fn with_user_identity(mut self, identity: impl Into<String>) -> Self {
        self.user_identity = identity.into();
        self
    }

    pub fn with_min_individual_miner_hashrate(mut self, hashrate: f64) -> Self {
        self.min_individual_miner_hashrate = hashrate;
        self
    }

    pub fn with_shares_per_minute(mut self, shares_per_minute: f64) -> Self {
        self.shares_per_minute = shares_per_minute;
        self
    }

    pub fn build(self) -> Result<TranslatorSv2D, TranslatorSv2DError> {
        let exe = self
            .translator_sv2_exe
            .clone()
            .or_else(|| std::env::var_os("TRANSLATOR_SV2_EXE").map(PathBuf::from))
            .or_else(find_translator_in_sv2_apps_target)
            .or_else(|| p2poolv2d::which_compat_pub("translator_sv2"))
            .ok_or(TranslatorSv2DError::BinaryNotFound)?;
        info!(exe = %exe.display(), "starting translator_sv2");

        let tempdir =
            tempfile::tempdir().map_err(|e| TranslatorSv2DError::Tempdir(e.to_string()))?;

        let downstream_port = allocate_free_port()?;
        let monitoring_port = allocate_free_port()?;
        let upstream_addr = self.upstream.listening_addr;

        let user_identity = &self.user_identity;
        let min_hashrate = self.min_individual_miner_hashrate;
        let shares_per_minute = self.shares_per_minute;

        let toml = format!(
            r#"
downstream_address = "127.0.0.1"
downstream_port = {downstream_port}

max_supported_version = 2
min_supported_version = 2

downstream_extranonce2_size = 4

user_identity = "{user_identity}"

verify_payout = false

aggregate_channels = false

supported_extensions = []

monitoring_address = "127.0.0.1:{monitoring_port}"
monitoring_cache_refresh_secs = 15

[downstream_difficulty_config]
min_individual_miner_hashrate = {min_hashrate}
shares_per_minute = {shares_per_minute}
enable_vardiff = false

job_keepalive_interval_secs = 60

[[upstreams]]
address = "{upstream_ip}"
port = {upstream_port}
authority_pubkey = "{TEST_AUTHORITY_PUBLIC_KEY}"
"#,
            upstream_ip = upstream_addr.ip(),
            upstream_port = upstream_addr.port(),
        );

        let config_path = tempdir.path().join("translator-config.toml");
        std::fs::write(&config_path, toml.as_bytes())
            .map_err(|e| TranslatorSv2DError::WriteConfig(e.to_string()))?;
        debug!(config_path = %config_path.display(), "wrote translator config");

        let child = Command::new(&exe)
            .arg("-c")
            .arg(&config_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| TranslatorSv2DError::Spawn(e.to_string()))?;
        info!(pid = child.id(), "translator_sv2 spawned");

        let downstream_addr = SocketAddr::from(([127, 0, 0, 1], downstream_port));
        let mut spawner = TranslatorSv2D {
            child,
            _tempdir: tempdir,
            config_path,
            downstream_addr,
        };

        wait_for_ready(&mut spawner, self.ready_timeout)?;
        Ok(spawner)
    }
}

/// Try to find `translator_sv2` under `vendor/sv2-apps/target/`. The
/// sv2-apps subworkspaces (miner-apps, pool-apps, …) each build into
/// their own `target/` directory; check the miner-apps one first since
/// that's where translator_sv2 lives, then fall back to the top-level
/// sv2-apps target if the workspace was built from there.
fn find_translator_in_sv2_apps_target() -> Option<PathBuf> {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")?;
    let mut path = PathBuf::from(manifest_dir);
    // testenv crate is at <ws>/crates/sv2-p2pool-testenv. Walk up to <ws>.
    for _ in 0..3 {
        if !path.pop() {
            return None;
        }
        for profile in ["debug", "release"] {
            for target_root in [
                path.join("vendor")
                    .join("sv2-apps")
                    .join("miner-apps")
                    .join("target"),
                path.join("vendor").join("sv2-apps").join("target"),
            ] {
                let candidate = target_root.join(profile).join("translator_sv2");
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
    }
    None
}

fn allocate_free_port() -> Result<u16, TranslatorSv2DError> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| TranslatorSv2DError::PortAllocation(e.to_string()))?;
    let port = listener
        .local_addr()
        .map_err(|e| TranslatorSv2DError::PortAllocation(e.to_string()))?
        .port();
    Ok(port)
}

fn wait_for_ready(
    spawner: &mut TranslatorSv2D,
    timeout: Duration,
) -> Result<(), TranslatorSv2DError> {
    let deadline = Instant::now() + timeout;
    loop {
        match spawner.child.try_wait() {
            Ok(Some(status)) => {
                warn!(?status, "translator_sv2 exited before becoming ready");
                return Err(TranslatorSv2DError::ExitedDuringStartup);
            }
            Ok(None) => {}
            Err(e) => warn!(error = %e, "try_wait error during readiness poll"),
        }

        // The translator binds its SV1 downstream listener before
        // completing the upstream handshake, so TCP-accept on
        // downstream_port is the readiness signal — same approach as
        // the other spawners.
        if TcpStream::connect_timeout(&spawner.downstream_addr, Duration::from_millis(100)).is_ok()
        {
            info!(downstream_addr = %spawner.downstream_addr, "translator_sv2 listening");
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(TranslatorSv2DError::ReadinessTimeout(timeout));
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
        let _ = find_translator_in_sv2_apps_target();
    }
}
