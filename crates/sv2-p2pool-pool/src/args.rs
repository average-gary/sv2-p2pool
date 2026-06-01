//! CLI argument parsing for the sv2-p2pool binary.
//!
//! Mirrors `vendor/sv2-apps/pool-apps/pool/src/args.rs` in shape so
//! operators familiar with the upstream pool find the same flags here.
//! Phase 2.5b adds a second `--p2pool-config` flag that loads the
//! p2poolv2 share-chain config (store path + bitcoinrpc creds +
//! stratum network/multiplier) — kept separate from `PoolConfig` so
//! the upstream type stays unmodified.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use ext_config::{Config, File, FileFormat};
use p2poolv2_lib::config::Config as P2poolConfig;
use pool_sv2::config::PoolConfig;

/// CLI shape.
#[derive(Parser, Debug)]
#[command(author, version, about = "sv2-p2pool: SV2 mining pool with p2poolv2 share-chain backend", long_about = None)]
pub struct Args {
    /// Path to the TOML configuration file (sv2-apps PoolConfig).
    #[arg(
        short = 'c',
        long = "config",
        help = "Path to the sv2-apps PoolConfig TOML",
        default_value = "sv2-p2pool.toml"
    )]
    pub config_path: PathBuf,
    /// Path to the p2poolv2 share-chain config TOML (store, bitcoinrpc,
    /// stratum.network).
    #[arg(
        long = "p2pool-config",
        help = "Path to the p2poolv2 share-chain config TOML",
        default_value = "p2poolv2.toml"
    )]
    pub p2pool_config_path: PathBuf,
    /// Optional log file path.
    #[arg(
        short = 'f',
        long = "log-file",
        help = "Path to the log file. If not set, logs go to stdout only."
    )]
    pub log_file: Option<PathBuf>,
    /// Optional listen address for the built-in `/metrics` endpoint.
    /// When set, the pool serves Prometheus metrics on this addr;
    /// when unset, no metrics endpoint is started.
    #[arg(
        long = "metrics-addr",
        help = "Listen address for /metrics (e.g. 127.0.0.1:9000). Omit to disable."
    )]
    pub metrics_addr: Option<SocketAddr>,
}

/// Loaded configs for the sv2-p2pool binary.
#[derive(Debug)]
pub struct LoadedConfigs {
    pub pool: PoolConfig,
    pub p2pool: P2poolConfig,
    pub metrics_addr: Option<SocketAddr>,
}

/// Parse CLI arguments and load both TOML configs.
pub fn process_cli_args() -> anyhow::Result<LoadedConfigs> {
    let args = Args::parse();
    let config_path = args
        .config_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid config path: {:?}", args.config_path))?;
    let mut pool: PoolConfig = Config::builder()
        .add_source(File::new(config_path, FileFormat::Toml))
        .build()?
        .try_deserialize::<PoolConfig>()?;
    pool.set_log_dir(args.log_file);

    let p2pool_path = args.p2pool_config_path.to_str().ok_or_else(|| {
        anyhow::anyhow!("invalid p2pool config path: {:?}", args.p2pool_config_path)
    })?;
    let p2pool = P2poolConfig::load(p2pool_path)
        .map_err(|e| anyhow::anyhow!("failed to load p2poolv2 config from {p2pool_path}: {e}"))?;

    Ok(LoadedConfigs {
        pool,
        p2pool,
        metrics_addr: args.metrics_addr,
    })
}
