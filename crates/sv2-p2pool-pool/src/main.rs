//! sv2-p2pool — SV2 mining pool binary using p2poolv2 as share-chain backend.
//!
//! Phase 2.5b entry point. Loads both the sv2-apps `PoolConfig` and the
//! p2poolv2 share-chain `Config` from TOML files, builds a [`Pool`]
//! with both attached, and runs it until `Ctrl+C` or external
//! cancellation.

use sv2_p2pool::{PoolBuilder, process_cli_args};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("sv2-p2pool: boot");

    let configs = process_cli_args()?;
    tracing::info!(
        listen = %configs.pool.listen_address(),
        signature = %configs.pool.pool_signature(),
        store_path = %configs.p2pool.store.path,
        bitcoinrpc_url = %configs.p2pool.bitcoinrpc.url,
        network = %configs.p2pool.stratum.network,
        "loaded configs"
    );

    let builder = PoolBuilder::new(bitcoin::Network::Regtest);
    let mut pool = builder.build_pool_with_p2pool_config(configs.pool, configs.p2pool);
    if let Some(addr) = configs.metrics_addr {
        pool = pool.with_metrics_addr(addr);
        tracing::info!(metrics_addr = %addr, "metrics endpoint enabled");
    }

    pool.start()
        .await
        .map_err(|e| anyhow::anyhow!("pool runtime error: {e:?}"))?;
    Ok(())
}
