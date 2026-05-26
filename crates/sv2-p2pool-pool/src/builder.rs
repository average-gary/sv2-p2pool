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
use sv2_p2pool_engine::P2poolV2Engine;

/// Composes a [`P2poolV2Engine`] with the bits sv2-apps needs to start
/// a `JobDeclarator`.
///
/// Phase 1.5 scope: enough to construct the engine + expose it as
/// `Arc<dyn JobValidationEngine>` for `JobDeclarator::new`. Phase 1.7
/// will compose this into a runnable binary; Phase 1.6 wires
/// ChannelManager.
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

    /// Build a fresh [`P2poolV2Engine`].
    ///
    /// Phase 1.7+ will widen this to a full builder accepting
    /// `ChainStoreHandle` / `Arc<dyn BitcoindLike>` / `Arc<dyn ShareValidator>`.
    /// For now the engine constructs an empty in-memory state.
    pub fn build_engine(&self) -> P2poolV2Engine {
        P2poolV2Engine::new(self.network)
    }

    /// Build the engine wrapped in `Arc<dyn JobValidationEngine>` so it
    /// can be passed to `jd_server_sv2::JobDeclarator::new`.
    pub fn build_engine_arc(&self) -> Arc<dyn JobValidationEngine> {
        Arc::new(self.build_engine())
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
}
