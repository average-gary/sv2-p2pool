//! sv2-p2pool — SV2 mining pool binary using p2poolv2 as share-chain backend.
//!
//! Phase 1.7 entry point. Loads `PoolConfig` from a TOML file, builds a
//! [`Pool`] via [`PoolBuilder`], and runs it until `Ctrl+C` or external
//! cancellation.
//!
//! See [the Phase 1 wiring plan][1] for the full execution roadmap.
//!
//! [1]: ~/wiki/topics/sv2-p2pool-integration/output/plan-phase-1-wiring-2026-05-26.md

use sv2_p2pool::{PoolBuilder, process_cli_args};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("sv2-p2pool: Phase 1 boot");

    let config = process_cli_args()?;
    tracing::info!(
        listen = %config.listen_address(),
        signature = %config.pool_signature(),
        "loaded config"
    );

    // Phase 1.7: full binary entry. Engine network is derived from the
    // template_provider_type inside Pool::start.
    let builder = PoolBuilder::new(bitcoin::Network::Regtest);
    let pool = builder.build_pool(config);

    pool.start()
        .await
        .map_err(|e| anyhow::anyhow!("pool runtime error: {e:?}"))?;
    Ok(())
}
