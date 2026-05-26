//! CLI argument parsing for the sv2-p2pool binary.
//!
//! Mirrors `vendor/sv2-apps/pool-apps/pool/src/args.rs` in shape so
//! operators familiar with the upstream pool find the same flags here.

use std::path::PathBuf;

use clap::Parser;
use ext_config::{Config, File, FileFormat};
use pool_sv2::config::PoolConfig;

/// CLI shape.
#[derive(Parser, Debug)]
#[command(author, version, about = "sv2-p2pool: SV2 mining pool with p2poolv2 share-chain backend", long_about = None)]
pub struct Args {
    /// Path to the TOML configuration file.
    #[arg(
        short = 'c',
        long = "config",
        help = "Path to the TOML configuration file",
        default_value = "sv2-p2pool.toml"
    )]
    pub config_path: PathBuf,
    /// Optional log file path.
    #[arg(
        short = 'f',
        long = "log-file",
        help = "Path to the log file. If not set, logs go to stdout only."
    )]
    pub log_file: Option<PathBuf>,
}

/// Parse CLI arguments and load the [`PoolConfig`] from the named TOML.
pub fn process_cli_args() -> anyhow::Result<PoolConfig> {
    let args = Args::parse();
    let config_path = args
        .config_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid config path: {:?}", args.config_path))?;
    let mut config: PoolConfig = Config::builder()
        .add_source(File::new(config_path, FileFormat::Toml))
        .build()?
        .try_deserialize::<PoolConfig>()?;
    config.set_log_dir(args.log_file);
    Ok(config)
}
