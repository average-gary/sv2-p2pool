//! Pool assembly entry point.
//!
//! `PoolBuilder` constructs the engine + `JobDeclarator` with the right
//! Arc-sharing semantics, bypassing `PoolSv2::start()`'s hardcoded engine
//! selection.
//!
//! # ADR 0002 — token → payout script
//!
//! The upstream `JobDeclarator::new` takes a single pool-wide
//! `coinbase_reward_script` ([reference][1]). For Phase 1, every miner
//! gets that script as their payout — matching upstream behavior. The
//! engine's `TokenPayoutMap` is exposed via [`PoolBuilder::engine`] so
//! a future interceptor (Phase 2 work, requires either a JDC TLV
//! extension or an upstream sv2-apps trait change) can write per-miner
//! overrides.
//!
//! `P2poolV2Engine::lookup_payout_script` returns `None` for unknown
//! tokens; callers should fall back to the binary's configured
//! `coinbase_reward_script`.
//!
//! [1]: ../../vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/mod.rs:111-148

use std::sync::Arc;

use jd_server_sv2::job_declarator::job_validation::JobValidationEngine;
use pool_sv2::config::PoolConfig;
use sv2_p2pool_engine::{EngineHandles, P2poolV2Engine};

use crate::Pool;

/// Composes a [`P2poolV2Engine`] with the bits sv2-apps needs to start
/// a `JobDeclarator`.
///
/// Phase 1.5/1.6 scope: enough to construct the engine, expose it as
/// `Arc<dyn JobValidationEngine>` for `JobDeclarator::new`, and produce
/// a [`Pool`] given a loaded [`PoolConfig`].
pub struct PoolBuilder {
    network: bitcoin::Network,
}

impl PoolBuilder {
    /// Start a new builder targeting the given Bitcoin network.
    ///
    /// Most setups will use `bitcoin::Network::Regtest` for tests,
    /// `Signet` or `Mainnet` for deployment.
    pub fn new(network: bitcoin::Network) -> Self {
        Self { network }
    }

    /// Build a fresh [`P2poolV2Engine`] without backend handles
    /// (structural-only mode).
    pub fn build_engine(&self) -> P2poolV2Engine {
        P2poolV2Engine::new(self.network)
    }

    /// Build a [`P2poolV2Engine`] with real backend handles (chain +
    /// validator + bitcoind). Phase 2.5b uses this when a p2poolv2
    /// share-chain config is attached to the binary.
    pub fn build_engine_with_handles(&self, handles: EngineHandles) -> P2poolV2Engine {
        P2poolV2Engine::with_handles(self.network, handles)
    }

    /// Build the engine wrapped in `Arc<dyn JobValidationEngine>` so it
    /// can be passed to `jd_server_sv2::JobDeclarator::new`.
    pub fn build_engine_arc(&self) -> Arc<dyn JobValidationEngine> {
        Arc::new(self.build_engine())
    }

    /// Build a [`Pool`] from a loaded [`PoolConfig`]. The pool isn't
    /// started until [`Pool::start`] is called.
    pub fn build_pool(self, config: PoolConfig) -> Pool {
        Pool::new(config)
    }

    /// Build a [`Pool`] with both the sv2-apps [`PoolConfig`] and the
    /// p2poolv2 share-chain config attached. `Pool::start` will then
    /// bootstrap real `EngineHandles`.
    pub fn build_pool_with_p2pool_config(
        self,
        config: PoolConfig,
        p2pool_config: p2poolv2_lib::config::Config,
    ) -> Pool {
        Pool::new(config).with_p2pool_config(p2pool_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_engine_arc_returns_dyn_trait() {
        let builder = PoolBuilder::new(bitcoin::Network::Regtest);
        let engine: Arc<dyn JobValidationEngine> = builder.build_engine_arc();
        // Compile-time check; dropped immediately.
        drop(engine);
    }

    #[tokio::test]
    async fn engine_arc_satisfies_jve_methods() {
        // Smoke test: the Arc<dyn JobValidationEngine> we hand to
        // JobDeclarator::new can actually be invoked. Use the no-op
        // shutdown path because the others need wire-format inputs.
        let engine = PoolBuilder::new(bitcoin::Network::Regtest).build_engine_arc();
        engine.shutdown(); // no-op; just verifies dispatch works
    }

    #[test]
    fn pool_exposes_metrics_registry() {
        // The registry is constructed empty; engine counters are
        // registered inside Pool::start. The accessor is available
        // pre-start so a binary can mount it on an HTTP endpoint
        // before the engine is wired.
        let pool = PoolBuilder::new(bitcoin::Network::Regtest).build_pool(make_test_pool_config());
        let registry = pool.metrics_registry();
        // No counters yet (Pool::start hasn't run).
        assert_eq!(registry.gather().len(), 0);
    }

    /// Hand-rolled minimal `PoolConfig` for tests that need a `Pool`
    /// without touching disk. Mirrors the upstream test fixtures' shape.
    fn make_test_pool_config() -> pool_sv2::config::PoolConfig {
        // Use upstream's serde-roundtrip — every public ctor needs more
        // fields than we want to inline. Build the simplest TOML that
        // PoolConfig::deserialize accepts.
        let toml = r#"
authority_public_key = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72"
authority_secret_key = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n"
cert_validity_sec = 3600
listen_address = "127.0.0.1:0"
coinbase_reward_script = "addr(tb1qa0sm0hxzj0x25rh8gw5xlzwlsfvvyz8u96w3p8)"
server_id = 1
pool_signature = "test"
shares_per_minute = 6.0
share_batch_size = 10
supported_extensions = []
required_extensions = []
monitoring_address = "127.0.0.1:0"
monitoring_cache_refresh_secs = 15

[template_provider_type.BitcoinCoreIpc]
network = "testnet4"
fee_threshold = 100
min_interval = 5

[jds]
listen_address = "127.0.0.1:0"
"#;
        toml::from_str(toml).expect("PoolConfig deserialize")
    }
}
