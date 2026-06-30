//! `MujinaMinerD` — spawn `mujina-minerd` as an SV1 CPU miner.
//!
//! Mujina ([github.com/256foundation/mujina]) is open-source mining
//! firmware that speaks Stratum V1. We use it as a downstream miner
//! against an SV1↔SV2 translator to drive `SubmitSharesExtended`
//! through the full SV2 stack.
//!
//! ## Discovery
//!
//! 1. `MUJINA_MINERD_EXE` env var.
//! 2. `mujina-minerd` on `$PATH`.
//!
//! Mujina is not vendored in this repo (its build pulls system udev +
//! openssl); operators provide a built binary via env var or PATH.
//!
//! ## Configuration via env vars
//!
//! Mujina reads its runtime configuration from environment variables
//! (per its README; the in-tree `Config` struct is unimplemented). The
//! variables the spawner sets:
//!
//! - `MUJINA_POOL_URL` — `stratum+tcp://<translator_downstream_addr>`
//! - `MUJINA_POOL_USER` — defaults to `"mujina-testing"`; the spawner
//!   writes a deterministic name so logs are easy to match.
//! - `MUJINA_POOL_PASS` — `"x"` (mujina's default).
//! - `MUJINA_CPUMINER_THREADS=1` — single CPU thread suffices for
//!   tests; tighter than the default keeps the host responsive.
//! - `MUJINA_CPUMINER_DUTY=50` — 50 % duty cycle (matches mujina's
//!   own quickstart example).
//! - `MUJINA_USB_DISABLE=1` — skip the udev USB-discovery probe so
//!   the daemon doesn't error on hosts without `/run/udev`.
//! - `MUJINA_API_LISTEN=127.0.0.1:<free port>` — pin the REST API to
//!   an OS-allocated free port (default would be 7785 — would
//!   conflict if multiple tests run in parallel).
//! - `RUST_LOG=info` — visible logging by default. Override with
//!   [`MujinaMinerDBuilder::with_rust_log`].
//!
//! ## Readiness
//!
//! TCP-accept on `MUJINA_API_LISTEN` — the daemon binds the REST API
//! once startup completes. This is not a guarantee that the SV1 client
//! has connected to the pool yet, just that mujina is alive.

use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use thiserror::Error;
use tracing::{debug, info, warn};

use crate::p2poolv2d;

/// Default readiness timeout: cold-start of mujina is dominated by
/// USB-disabled init + API bind; 10s is generous.
pub const DEFAULT_MUJINA_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors from mujina spawning.
#[derive(Debug, Error)]
pub enum MujinaMinerDError {
    #[error(
        "mujina-minerd binary not found: set MUJINA_MINERD_EXE env var or place `mujina-minerd` on PATH"
    )]
    BinaryNotFound,
    #[error("failed to allocate a free TCP port: {0}")]
    PortAllocation(String),
    #[error("failed to spawn mujina-minerd: {0}")]
    Spawn(String),
    #[error("mujina-minerd did not become ready within {0:?}")]
    ReadinessTimeout(Duration),
    #[error("mujina-minerd exited during startup")]
    ExitedDuringStartup,
}

/// A running `mujina-minerd` child process with auto-cleanup on Drop.
pub struct MujinaMinerD {
    child: Child,
    /// Per-instance tempdir that backs the child's `$HOME` and
    /// `$XDG_CONFIG_HOME`. Held so the directory survives at least
    /// as long as the child; cleaned up by `tempfile::TempDir::Drop`
    /// after the child exits.
    _tempdir: tempfile::TempDir,
    /// REST API listen address (the free port we allocated). Useful
    /// for tests that want to scrape mujina's `/api/v0/...` endpoints.
    pub api_addr: SocketAddr,
    /// The pool URL handed to mujina, or `None` if mujina was started
    /// against its built-in dummy job source. Captured for diagnostics.
    pub pool_url: Option<String>,
}

impl Drop for MujinaMinerD {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(status)) => debug!(?status, "mujina-minerd already exited"),
            Ok(None) => {
                if let Err(e) = self.child.kill() {
                    warn!(error = %e, "failed to kill mujina-minerd child");
                }
                let _ = self.child.wait();
            }
            Err(e) => warn!(error = %e, "mujina-minerd try_wait failed during Drop"),
        }
    }
}

/// Builder for [`MujinaMinerD`].
pub struct MujinaMinerDBuilder {
    mujina_exe: Option<PathBuf>,
    pool_url: Option<String>,
    pool_user: String,
    pool_pass: String,
    cpu_threads: u32,
    cpu_duty: u32,
    rust_log: String,
    ready_timeout: Duration,
}

impl Default for MujinaMinerDBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MujinaMinerDBuilder {
    pub fn new() -> Self {
        Self {
            mujina_exe: None,
            pool_url: None,
            pool_user: "sv2-p2pool-testenv-miner".to_string(),
            pool_pass: "x".to_string(),
            cpu_threads: 1,
            cpu_duty: 50,
            rust_log: "info".to_string(),
            ready_timeout: DEFAULT_MUJINA_READY_TIMEOUT,
        }
    }

    pub fn with_exe(mut self, path: impl Into<PathBuf>) -> Self {
        self.mujina_exe = Some(path.into());
        self
    }

    /// Set `MUJINA_POOL_URL`. Format: `stratum+tcp://host:port`. Omit
    /// (leave unset) to let mujina run against its built-in dummy job
    /// source — useful for isolated smoke tests.
    pub fn with_pool_url(mut self, url: impl Into<String>) -> Self {
        self.pool_url = Some(url.into());
        self
    }

    pub fn with_pool_user(mut self, user: impl Into<String>) -> Self {
        self.pool_user = user.into();
        self
    }

    pub fn with_pool_pass(mut self, pass: impl Into<String>) -> Self {
        self.pool_pass = pass.into();
        self
    }

    pub fn with_cpu_threads(mut self, threads: u32) -> Self {
        self.cpu_threads = threads;
        self
    }

    pub fn with_cpu_duty(mut self, duty: u32) -> Self {
        self.cpu_duty = duty;
        self
    }

    pub fn with_rust_log(mut self, directive: impl Into<String>) -> Self {
        self.rust_log = directive.into();
        self
    }

    pub fn with_ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    pub fn build(self) -> Result<MujinaMinerD, MujinaMinerDError> {
        let exe = self
            .mujina_exe
            .clone()
            .or_else(|| std::env::var_os("MUJINA_MINERD_EXE").map(PathBuf::from))
            .or_else(|| p2poolv2d::which_compat_pub("mujina-minerd"))
            .ok_or(MujinaMinerDError::BinaryNotFound)?;
        info!(exe = %exe.display(), "starting mujina-minerd");

        let api_port = allocate_free_port()?;
        let api_addr = SocketAddr::from(([127, 0, 0, 1], api_port));

        // Per-instance tempdir backs HOME/XDG so parallel test runs
        // can't collide via shared user-config paths if mujina ever
        // grows a config-on-disk. Mirrors the tempdir pattern used
        // by every sibling spawner. The XDG subdir is created
        // eagerly so callers don't have to special-case missing dirs.
        let tempdir = tempfile::tempdir()
            .map_err(|e| MujinaMinerDError::Spawn(format!("failed to create tempdir: {e}")))?;
        let xdg_config = tempdir.path().join("config");
        std::fs::create_dir_all(&xdg_config).map_err(|e| {
            MujinaMinerDError::Spawn(format!("failed to create XDG config dir: {e}"))
        })?;

        let mut cmd = Command::new(&exe);
        cmd.env("MUJINA_API_LISTEN", api_addr.to_string())
            .env("MUJINA_USB_DISABLE", "1")
            .env("MUJINA_CPUMINER_THREADS", self.cpu_threads.to_string())
            .env("MUJINA_CPUMINER_DUTY", self.cpu_duty.to_string())
            .env("MUJINA_POOL_USER", &self.pool_user)
            .env("MUJINA_POOL_PASS", &self.pool_pass)
            .env("RUST_LOG", &self.rust_log)
            .env("HOME", tempdir.path())
            .env("XDG_CONFIG_HOME", &xdg_config)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(url) = self.pool_url.as_ref() {
            cmd.env("MUJINA_POOL_URL", url);
        }

        let child = cmd
            .spawn()
            .map_err(|e| MujinaMinerDError::Spawn(e.to_string()))?;
        info!(pid = child.id(), api_addr = %api_addr, "mujina-minerd spawned");

        let mut spawner = MujinaMinerD {
            child,
            _tempdir: tempdir,
            api_addr,
            pool_url: self.pool_url.clone(),
        };
        wait_for_ready(&mut spawner, self.ready_timeout)?;
        Ok(spawner)
    }
}

fn allocate_free_port() -> Result<u16, MujinaMinerDError> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| MujinaMinerDError::PortAllocation(e.to_string()))?;
    let port = listener
        .local_addr()
        .map_err(|e| MujinaMinerDError::PortAllocation(e.to_string()))?
        .port();
    Ok(port)
}

fn wait_for_ready(spawner: &mut MujinaMinerD, timeout: Duration) -> Result<(), MujinaMinerDError> {
    let deadline = Instant::now() + timeout;
    loop {
        match spawner.child.try_wait() {
            Ok(Some(status)) => {
                warn!(?status, "mujina-minerd exited before becoming ready");
                return Err(MujinaMinerDError::ExitedDuringStartup);
            }
            Ok(None) => {}
            Err(e) => warn!(error = %e, "try_wait error during readiness poll"),
        }

        if TcpStream::connect_timeout(&spawner.api_addr, Duration::from_millis(100)).is_ok() {
            info!(api_addr = %spawner.api_addr, "mujina-minerd API ready");
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(MujinaMinerDError::ReadinessTimeout(timeout));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
