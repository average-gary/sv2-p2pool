//! `Sv2P2poolD` — spawn the `sv2-p2pool` binary against a [`BitcoinD`]
//! and a [`P2poolV2D`].
//!
//! Mirrors the spawner pattern of [`crate::p2poolv2d::P2poolV2D`]:
//! three-tier binary discovery (`SV2_P2POOL_EXE` env var → workspace
//! `target/debug/sv2-p2pool` → PATH), tempdir-based config files,
//! OS-allocated free ports, Drop kills the child process.
//!
//! ## Configs
//!
//! Two TOML files are written to the spawner's tempdir:
//! - `pool.toml`: sv2-apps `PoolConfig` (authority keys + listen +
//!   JDS + Bitcoin Core IPC TP type).
//! - `p2pool.toml`: p2poolv2 share-chain config (store path +
//!   bitcoinrpc creds + stratum.network).
//!
//! ## Authority keys
//!
//! Hard-coded test keys taken from sv2-apps's
//! `config-examples/testnet4/pool-jds-config-bitcoin-core-ipc-example.toml`.
//! These are NOT secret; they're used in upstream's test fixtures and
//! must match the keys the test JDC expects.
//!
//! ## Discovery
//!
//! 1. `SV2_P2POOL_EXE` env var.
//! 2. `target/debug/sv2-p2pool` relative to the workspace root (via
//!    `CARGO_MANIFEST_DIR`).
//! 3. `sv2-p2pool` on `$PATH`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use thiserror::Error;
use tracing::{debug, info, warn};

use crate::BitcoinD;

/// Default readiness timeout for sv2-p2pool. Cold start spans
/// rocksdb open + libp2p bind + JDS listen; 10s is generous.
pub const DEFAULT_SV2_P2POOL_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Test-only authority keypair from sv2-apps's testnet4 example
/// configs. Not secret; matches what the test JDC expects.
pub const TEST_AUTHORITY_PUBLIC_KEY: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
pub const TEST_AUTHORITY_SECRET_KEY: &str = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n";

/// Errors from sv2-p2pool spawning.
#[derive(Debug, Error)]
pub enum Sv2P2poolDError {
    #[error(
        "sv2-p2pool binary not found: set SV2_P2POOL_EXE env var, build it with `cargo build`, or place it on PATH"
    )]
    BinaryNotFound,
    #[error("failed to allocate a free TCP port: {0}")]
    PortAllocation(String),
    #[error("failed to create tempdir: {0}")]
    Tempdir(String),
    #[error("failed to write config: {0}")]
    WriteConfig(String),
    #[error("failed to spawn sv2-p2pool: {0}")]
    Spawn(String),
    #[error("sv2-p2pool did not become ready within {0:?}")]
    ReadinessTimeout(Duration),
    #[error("sv2-p2pool exited during startup")]
    ExitedDuringStartup,
}

/// A running `sv2-p2pool` child process with auto-cleanup on Drop.
pub struct Sv2P2poolD {
    child: Child,
    _tempdir: tempfile::TempDir,
    pub pool_config_path: PathBuf,
    pub p2pool_config_path: PathBuf,
    /// JDS listen address (where the JDC connects).
    pub jds_addr: SocketAddr,
    /// Mining-protocol downstream listen address.
    pub mining_addr: SocketAddr,
    /// Address of the built-in `/metrics` + `/healthz` HTTP endpoint.
    pub metrics_addr: SocketAddr,
    /// Address of the upstream sv2-apps `MonitoringServer`. Exposes
    /// `/metrics` (per-channel Prometheus counters including
    /// `sv2_client_shares_accepted_total`) and JSON `/api/v0/clients`.
    /// Driven from the pool's `monitoring_address` config field.
    pub monitoring_addr: SocketAddr,
}

impl Sv2P2poolD {
    /// Scrape `/metrics` and parse the value of an `IntCounter`-shaped
    /// metric line (`<name> <integer>` for unlabeled, `<name>{...} <integer>`
    /// for labeled). Returns `None` if the line is not present (which
    /// for pre-registered counters means: the metric was never bumped
    /// AND no zero-line was emitted — operationally this is "0").
    ///
    /// This is a *test-harness* HTTP/1.1 client — it deliberately
    /// avoids a hyper/reqwest dep just to walk a few hundred bytes of
    /// exposition format.
    pub fn scrape_metric_value(&self, name_with_labels: &str) -> std::io::Result<Option<u64>> {
        let body = self.scrape_metrics_body()?;
        for line in body.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let Some((lhs, rhs)) = line.rsplit_once(' ') else {
                continue;
            };
            if lhs == name_with_labels {
                return Ok(rhs.parse::<u64>().ok());
            }
        }
        Ok(None)
    }

    /// Scrape the raw `/metrics` body. Useful for tests that want to
    /// match against multiple metrics at once.
    pub fn scrape_metrics_body(&self) -> std::io::Result<String> {
        scrape_http_metrics(self.metrics_addr)
    }

    /// Scrape the upstream `MonitoringServer` `/metrics` body
    /// (per-channel `sv2_client_shares_accepted_total` etc.).
    ///
    /// NOTE: the upstream server's per-channel metrics are populated
    /// by a snapshot cache that refreshes at
    /// `monitoring_cache_refresh_secs` (default 15s in our test
    /// configs). A share that lands at time T may not appear in this
    /// scrape until the next cache tick — up to ~15s lag — so callers
    /// polling for "share landed" must allow that headroom.
    pub fn scrape_monitoring_metrics_body(&self) -> std::io::Result<String> {
        scrape_http_metrics(self.monitoring_addr)
    }

    /// Block until `predicate(monitoring_body)` returns true or `timeout` elapses.
    /// See [`Self::scrape_monitoring_metrics_body`] for the snapshot-cache caveat.
    pub fn wait_for_monitoring_metric<F>(
        &self,
        mut predicate: F,
        timeout: Duration,
    ) -> std::io::Result<bool>
    where
        F: FnMut(&str) -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            let body = self.scrape_monitoring_metrics_body()?;
            if predicate(&body) {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Block until `predicate(scraped_body)` returns true or `timeout` elapses.
    pub fn wait_for_metric<F>(&self, mut predicate: F, timeout: Duration) -> std::io::Result<bool>
    where
        F: FnMut(&str) -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            let body = self.scrape_metrics_body()?;
            if predicate(&body) {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

impl Drop for Sv2P2poolD {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(status)) => debug!(?status, "sv2-p2pool already exited"),
            Ok(None) => {
                if let Err(e) = self.child.kill() {
                    warn!(error = %e, "failed to kill sv2-p2pool child");
                }
                let _ = self.child.wait();
            }
            Err(e) => warn!(error = %e, "sv2-p2pool try_wait failed during Drop"),
        }
    }
}

/// Builder for [`Sv2P2poolD`].
pub struct Sv2P2poolDBuilder<'a> {
    bitcoind: &'a BitcoinD,
    sv2_p2pool_exe: Option<PathBuf>,
    ready_timeout: Duration,
    network: bitcoin::Network,
    /// `Some(data_dir)` overrides the bitcoin Core data directory the
    /// pool will use for IPC. When `None`, the default `~/.bitcoin` (or
    /// platform equivalent) applies — for a `corepc-node` BitcoinD the
    /// caller should pass its `params.datadir.path()`.
    bitcoin_data_dir: Option<PathBuf>,
    /// `Some((start, minimum))` overrides the share-chain difficulty
    /// pair written to `[stratum]` in the generated `p2pool.toml`.
    /// When `None`, the upstream-style defaults (10000 / 100) are
    /// kept. Test paths that need a CPU miner to clear share difficulty
    /// in milliseconds (e.g. the regtest block-submission E2E) set this
    /// to `(1, 1)` via [`Self::with_low_difficulty`].
    difficulty_override: Option<(u64, u64)>,
}

impl<'a> Sv2P2poolDBuilder<'a> {
    pub fn new(bitcoind: &'a BitcoinD) -> Self {
        Self {
            bitcoind,
            sv2_p2pool_exe: None,
            ready_timeout: DEFAULT_SV2_P2POOL_READY_TIMEOUT,
            network: bitcoin::Network::Testnet4,
            bitcoin_data_dir: None,
            difficulty_override: None,
        }
    }

    pub fn with_exe(mut self, path: impl Into<PathBuf>) -> Self {
        self.sv2_p2pool_exe = Some(path.into());
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

    /// Drop both `start_difficulty` and `minimum_difficulty` to 1 in
    /// the share-chain `[stratum]` config so a CPU miner clears the
    /// channel target in milliseconds. Required for the regtest
    /// block-submission E2E; leave defaulted for production-shaped
    /// tests on testnet4.
    pub fn with_low_difficulty(mut self) -> Self {
        self.difficulty_override = Some((1, 1));
        self
    }

    /// Override the bitcoin Core data directory the pool uses for IPC.
    /// When unset, the pool tries default platform paths.
    pub fn with_bitcoin_data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.bitcoin_data_dir = Some(dir.into());
        self
    }

    /// Spawn and wait for readiness.
    pub fn build(self) -> Result<Sv2P2poolD, Sv2P2poolDError> {
        let exe = self
            .sv2_p2pool_exe
            .clone()
            .or_else(|| std::env::var_os("SV2_P2POOL_EXE").map(PathBuf::from))
            .or_else(find_sv2_p2pool_in_workspace_target)
            .or_else(|| crate::p2poolv2d::which_compat_pub("sv2-p2pool"))
            .ok_or(Sv2P2poolDError::BinaryNotFound)?;
        info!(exe = %exe.display(), "starting sv2-p2pool");

        let tempdir = tempfile::tempdir().map_err(|e| Sv2P2poolDError::Tempdir(e.to_string()))?;

        let jds_port = allocate_free_port()?;
        let mining_port = allocate_free_port()?;
        let monitoring_port = allocate_free_port()?;
        let metrics_port = allocate_free_port()?;
        let store_path = tempdir.path().join("p2pool-store.db");
        let stats_dir = tempdir.path().join("p2pool-stats");
        std::fs::create_dir_all(&stats_dir)
            .map_err(|e| Sv2P2poolDError::WriteConfig(e.to_string()))?;

        let bitcoinrpc_url = self.bitcoind.rpc_url();
        let (user, pass) = crate::p2poolv2d::bitcoind_credentials_pub(self.bitcoind);
        let network_name = network_to_name(self.network);
        let coinbase_addr = address_for_network(self.network);

        // sv2-apps PoolConfig.
        let mut pool_toml = format!(
            r#"
authority_public_key = "{TEST_AUTHORITY_PUBLIC_KEY}"
authority_secret_key = "{TEST_AUTHORITY_SECRET_KEY}"
cert_validity_sec = 3600
listen_address = "127.0.0.1:{mining_port}"

coinbase_reward_script = "addr({coinbase_addr})"

server_id = 1
pool_signature = "sv2-p2pool-test"
shares_per_minute = 6.0
share_batch_size = 10

supported_extensions = []
required_extensions = []

monitoring_address = "127.0.0.1:{monitoring_port}"
monitoring_cache_refresh_secs = 15

[template_provider_type.BitcoinCoreIpc]
network = "{network_name}"
fee_threshold = 100
min_interval = 5
"#
        );
        if let Some(dir) = self.bitcoin_data_dir.as_ref() {
            pool_toml.push_str(&format!("data_dir = \"{}\"\n", dir.display()));
        }
        pool_toml.push_str(&format!(
            r#"
[jds]
listen_address = "127.0.0.1:{jds_port}"
"#
        ));

        let pool_config_path = tempdir.path().join("pool.toml");
        std::fs::write(&pool_config_path, pool_toml.as_bytes())
            .map_err(|e| Sv2P2poolDError::WriteConfig(e.to_string()))?;

        // p2poolv2 share-chain config.
        let p2pool_libp2p_port = allocate_free_port()?;
        let p2pool_stratum_port = allocate_free_port()?;
        let p2pool_zmq_port = allocate_free_port()?;
        let p2pool_api_port = allocate_free_port()?;
        let (start_difficulty, minimum_difficulty) =
            self.difficulty_override.unwrap_or((10000, 100));
        let p2pool_toml = format!(
            r#"
[network]
listen_address = "/ip4/127.0.0.1/tcp/{p2pool_libp2p_port}"
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
port = {p2pool_stratum_port}
start_difficulty = {start_difficulty}
minimum_difficulty = {minimum_difficulty}
solo_address = "{coinbase_addr}"
bootstrap_address = "{coinbase_addr}"
zmqpubhashblock = "tcp://127.0.0.1:{p2pool_zmq_port}"
network = "{network_name}"
version_mask = "1fffe000"
difficulty_multiplier = 1.0
pool_signature = "sv2-p2pool-test"

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
port = {p2pool_api_port}
"#,
            store_path = store_path.display(),
            stats_dir = stats_dir.display(),
        );
        let p2pool_config_path = tempdir.path().join("p2pool.toml");
        std::fs::write(&p2pool_config_path, p2pool_toml.as_bytes())
            .map_err(|e| Sv2P2poolDError::WriteConfig(e.to_string()))?;
        debug!(
            pool_config = %pool_config_path.display(),
            p2pool_config = %p2pool_config_path.display(),
            "wrote sv2-p2pool configs"
        );

        let metrics_addr = SocketAddr::from(([127, 0, 0, 1], metrics_port));
        let monitoring_addr = SocketAddr::from(([127, 0, 0, 1], monitoring_port));
        let child = Command::new(&exe)
            .arg("--config")
            .arg(&pool_config_path)
            .arg("--p2pool-config")
            .arg(&p2pool_config_path)
            .arg("--metrics-addr")
            .arg(metrics_addr.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Sv2P2poolDError::Spawn(e.to_string()))?;
        info!(pid = child.id(), "sv2-p2pool spawned");

        let jds_addr = SocketAddr::from(([127, 0, 0, 1], jds_port));
        let mining_addr = SocketAddr::from(([127, 0, 0, 1], mining_port));
        let mut spawner = Sv2P2poolD {
            child,
            _tempdir: tempdir,
            pool_config_path,
            p2pool_config_path,
            jds_addr,
            mining_addr,
            metrics_addr,
            monitoring_addr,
        };

        wait_for_ready(&mut spawner, self.ready_timeout)?;
        Ok(spawner)
    }
}

/// Try to find `sv2-p2pool` in the workspace's `target/debug/` or
/// `target/release/`. Discovered via `CARGO_MANIFEST_DIR` walking up
/// to find the workspace root.
fn find_sv2_p2pool_in_workspace_target() -> Option<PathBuf> {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")?;
    let mut path = PathBuf::from(manifest_dir);
    // testenv crate is at <ws>/crates/sv2-p2pool-testenv. Walk up to <ws>.
    for _ in 0..3 {
        if !path.pop() {
            return None;
        }
        for profile in ["debug", "release"] {
            let candidate = path.join("target").join(profile).join("sv2-p2pool");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// Minimal HTTP/1.1 GET for `/metrics`-style endpoints. Returns the
/// response body (everything after the blank line). Test-harness
/// only — avoids a hyper/reqwest dep just to walk a few hundred
/// bytes of Prometheus exposition format.
fn scrape_http_metrics(addr: SocketAddr) -> std::io::Result<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let response = String::from_utf8_lossy(&response).to_string();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Ok(body)
}

fn allocate_free_port() -> Result<u16, Sv2P2poolDError> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| Sv2P2poolDError::PortAllocation(e.to_string()))?;
    let port = listener
        .local_addr()
        .map_err(|e| Sv2P2poolDError::PortAllocation(e.to_string()))?
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
            "tb1qa0sm0hxzj0x25rh8gw5xlzwlsfvvyz8u96w3p8"
        }
        bitcoin::Network::Regtest => "bcrt1qx2nf3uvxq6h2vksjwgdvnwhvexmkqwqx0vexap",
    }
}

fn wait_for_ready(spawner: &mut Sv2P2poolD, timeout: Duration) -> Result<(), Sv2P2poolDError> {
    let deadline = Instant::now() + timeout;
    loop {
        match spawner.child.try_wait() {
            Ok(Some(status)) => {
                warn!(?status, "sv2-p2pool exited before becoming ready");
                return Err(Sv2P2poolDError::ExitedDuringStartup);
            }
            Ok(None) => {}
            Err(e) => warn!(error = %e, "try_wait error during readiness poll"),
        }

        // Ready when BOTH the JDS port AND the /metrics port accept a
        // TCP connection. The metrics endpoint comes up later in
        // Pool::start than the JDS listener, so a JDS-only check would
        // race with tests that scrape /metrics immediately on return.
        let jds_up =
            TcpStream::connect_timeout(&spawner.jds_addr, Duration::from_millis(100)).is_ok();
        let metrics_up =
            TcpStream::connect_timeout(&spawner.metrics_addr, Duration::from_millis(100)).is_ok();
        if jds_up && metrics_up {
            info!(
                jds_addr = %spawner.jds_addr,
                metrics_addr = %spawner.metrics_addr,
                "sv2-p2pool JDS + /metrics ready"
            );
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(Sv2P2poolDError::ReadinessTimeout(timeout));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestEnv;

    /// Smoke test: spawn sv2-p2pool against a bitcoind regtest. Requires
    /// `cargo build --bin sv2-p2pool` to have been run first (so the
    /// `target/debug/sv2-p2pool` binary exists), plus `BITCOIND_EXE`
    /// (or auto-download) for bitcoind. CI shouldn't run by default.
    #[test]
    #[ignore = "requires SV2_P2POOL_EXE or `cargo build` + BITCOIND_EXE"]
    fn smoke_sv2_p2pool_boots_against_bitcoind() {
        let env = TestEnv::new().expect("bitcoind starts");
        let _sv2 = Sv2P2poolDBuilder::new(&env.bitcoind)
            .with_network(bitcoin::Network::Regtest)
            .with_bitcoin_data_dir(env.bitcoind.workdir())
            .build()
            .expect("sv2-p2pool starts");
    }

    #[test]
    fn workspace_target_lookup_returns_a_pathbuf_or_none() {
        // Sanity: the helper either finds the binary or returns None;
        // shouldn't panic regardless of whether the binary is built.
        let _ = find_sv2_p2pool_in_workspace_target();
    }

    #[test]
    fn scrape_metric_value_parses_unlabeled_counter_lines() {
        // Spin up a minimal HTTP server that replies with a fixed
        // /metrics body and verify the parsing logic against it.
        // Doesn't exercise the spawn path — just the parser.
        use std::io::Write as _;
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let body = "\
# HELP sv2_p2pool_engine_declare_mining_job_accepted_total Successful DeclareMiningJob exchanges
# TYPE sv2_p2pool_engine_declare_mining_job_accepted_total counter
sv2_p2pool_engine_declare_mining_job_accepted_total 7
sv2_p2pool_engine_blocks_submitted_total 0
";
        let server_thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = [0u8; 4096];
            // Drain the request line + headers; we don't care.
            use std::io::Read as _;
            let _ = stream.read(&mut buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body,
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.shutdown(std::net::Shutdown::Both);
        });

        // Build a fake Sv2P2poolD pointing at our stub server. Drop
        // safety: the child process is never spawned for real here, so
        // child kill in Drop is a no-op and we provide a placeholder.
        // To avoid that, just call scrape_metric_value via the same
        // socket-and-parse logic by inlining what the method does.
        let body_response = {
            let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut buf = Vec::new();
            use std::io::Read as _;
            stream.read_to_end(&mut buf).unwrap();
            String::from_utf8_lossy(&buf).to_string()
        };
        server_thread.join().expect("server thread");
        let body_only = body_response.split("\r\n\r\n").nth(1).unwrap_or("");

        // Run the same scan as scrape_metric_value.
        let lookup = |name: &str| -> Option<u64> {
            for line in body_only.lines() {
                if line.starts_with('#') || line.is_empty() {
                    continue;
                }
                let Some((lhs, rhs)) = line.rsplit_once(' ') else {
                    continue;
                };
                if lhs == name {
                    return rhs.parse::<u64>().ok();
                }
            }
            None
        };
        assert_eq!(
            lookup("sv2_p2pool_engine_declare_mining_job_accepted_total"),
            Some(7)
        );
        assert_eq!(lookup("sv2_p2pool_engine_blocks_submitted_total"), Some(0));
        assert_eq!(lookup("sv2_p2pool_engine_does_not_exist"), None);
    }
}
