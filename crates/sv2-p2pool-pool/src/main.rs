//! sv2-p2pool — SV2 mining pool binary using p2poolv2 as share-chain backend.
//!
//! Phase 0 stub. Phase 1 will assemble JobDeclarator + ChannelManager directly,
//! bypassing PoolSv2::start, with sv2_p2pool_engine::P2poolV2Engine as the
//! JobValidationEngine implementation.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("sv2-p2pool: Phase 0 stub. Not yet operational.");
    let _engine = sv2_p2pool_engine::P2poolV2Engine::new();
    Ok(())
}
