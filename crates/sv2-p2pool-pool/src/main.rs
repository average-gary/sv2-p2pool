//! sv2-p2pool — SV2 mining pool binary using p2poolv2 as share-chain backend.
//!
//! Phase 1 assembles `JobDeclarator` + `ChannelManager` directly via
//! [`sv2_p2pool::PoolBuilder`], bypassing `PoolSv2::start`. The engine
//! ([`sv2_p2pool_engine::P2poolV2Engine`]) is the `JobValidationEngine`
//! implementation.
//!
//! See [the Phase 1 wiring plan][1] for the full execution roadmap.
//!
//! [1]: ~/wiki/topics/sv2-p2pool-integration/output/plan-phase-1-wiring-2026-05-26.md

use sv2_p2pool::PoolBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("sv2-p2pool: Phase 1 boot");

    // Phase 1.5: PoolBuilder produces an Arc<dyn JobValidationEngine>
    // that JobDeclarator::new will accept. Phase 1.6 wires
    // ChannelManager + downstream listener; Phase 1.7 lands the full
    // entry point including config loading and graceful shutdown.
    let builder = PoolBuilder::new(bitcoin::Network::Regtest);
    let engine = builder.build_engine_arc();
    tracing::info!(
        engine_type = std::any::type_name_of_val(&*engine),
        "engine constructed"
    );
    Ok(())
}
