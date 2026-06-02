//! Prometheus metrics surface for [`crate::P2poolV2Engine`].
//!
//! All counters are `IntCounter`s registered on a `prometheus::Registry`
//! the binary owns. The engine's own per-event log lines stay; metrics
//! are a parallel observability channel intended for operator dashboards
//! (Grafana / Prometheus).
//!
//! ## Wiring
//!
//! ```no_run
//! use prometheus::Registry;
//! use sv2_p2pool_engine::{EngineMetrics, P2poolV2Engine};
//!
//! let registry = Registry::new();
//! let metrics = EngineMetrics::register(&registry).expect("register");
//! let engine = P2poolV2Engine::new(bitcoin::Network::Regtest)
//!     .with_metrics(metrics);
//! ```
//!
//! `Pool::start` in the binary plumbs a registry from the sv2-apps
//! monitoring server when one is configured; otherwise the engine
//! constructs without metrics and `record_*` calls are no-ops.

use prometheus::{IntCounter, IntGauge, Registry};

/// Counter set tracked by the engine. All counters are monotonic.
///
/// Cheap to clone — every field is `IntCounter`, which is internally
/// `Arc`-wrapped.
#[derive(Clone)]
pub struct EngineMetrics {
    /// `DeclareMiningJob` calls that returned `Success`.
    pub declare_mining_job_accepted: IntCounter,
    /// `DeclareMiningJob` calls that returned `Error(_)` (any error code).
    pub declare_mining_job_rejected: IntCounter,
    /// `DeclareMiningJob` calls that returned `MissingTransactions(_)`.
    pub declare_mining_job_missing_txns: IntCounter,
    /// `SetCustomMiningJob` calls that returned `Success`.
    pub set_custom_mining_job_accepted: IntCounter,
    /// `SetCustomMiningJob` calls that returned `Error(_)`.
    pub set_custom_mining_job_rejected: IntCounter,
    /// `PushSolution` calls handled (any outcome — see other counters
    /// for the breakdown).
    pub push_solution_received: IntCounter,
    /// Blocks fully reconstructed and handed to `bitcoind.submit_block`.
    /// Excludes the structural-only / no-handles fallback path.
    pub blocks_submitted: IntCounter,
    /// `notify_share_chain_reorg` invocations (any path: selective +
    /// fallback).
    pub reorg_notifications: IntCounter,
    /// `DeclaredJob`s dropped on share-chain reorg, summed across all
    /// `notify_share_chain_reorg` invocations.
    pub jobs_invalidated_total: IntCounter,

    /// Current size of the declared-jobs cache. Updated periodically
    /// by the engine's stats sweeper task (same cadence as
    /// [`crate::DEFAULT_RECENT_SOLUTIONS_SWEEP_INTERVAL`]).
    pub declared_jobs_cache_size: IntGauge,
    /// Current size of the recent-solutions buffer. Same update path
    /// as `declared_jobs_cache_size`.
    pub recent_solutions_buffer_size: IntGauge,
}

impl EngineMetrics {
    /// Register all counters and gauges on `registry`. Returns the
    /// populated struct or the first registration error.
    pub fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        let metrics = Self {
            declare_mining_job_accepted: int_counter(
                "sv2_p2pool_engine_declare_mining_job_accepted_total",
                "Successful DeclareMiningJob exchanges",
            )?,
            declare_mining_job_rejected: int_counter(
                "sv2_p2pool_engine_declare_mining_job_rejected_total",
                "DeclareMiningJob calls returning an Error code",
            )?,
            declare_mining_job_missing_txns: int_counter(
                "sv2_p2pool_engine_declare_mining_job_missing_txns_total",
                "DeclareMiningJob calls returning MissingTransactions",
            )?,
            set_custom_mining_job_accepted: int_counter(
                "sv2_p2pool_engine_set_custom_mining_job_accepted_total",
                "Successful SetCustomMiningJob cross-checks",
            )?,
            set_custom_mining_job_rejected: int_counter(
                "sv2_p2pool_engine_set_custom_mining_job_rejected_total",
                "SetCustomMiningJob calls returning an Error code",
            )?,
            push_solution_received: int_counter(
                "sv2_p2pool_engine_push_solution_received_total",
                "PushSolution messages handled",
            )?,
            blocks_submitted: int_counter(
                "sv2_p2pool_engine_blocks_submitted_total",
                "Blocks reconstructed and forwarded to bitcoind.submit_block",
            )?,
            reorg_notifications: int_counter(
                "sv2_p2pool_engine_reorg_notifications_total",
                "notify_share_chain_reorg invocations",
            )?,
            jobs_invalidated_total: int_counter(
                "sv2_p2pool_engine_jobs_invalidated_total",
                "DeclaredJobs dropped on share-chain reorg",
            )?,
            declared_jobs_cache_size: int_gauge(
                "sv2_p2pool_engine_declared_jobs_cache_size",
                "Current count of cached DeclaredJobs",
            )?,
            recent_solutions_buffer_size: int_gauge(
                "sv2_p2pool_engine_recent_solutions_buffer_size",
                "Current count of buffered share-finder credits",
            )?,
        };

        for c in metrics.all_counters() {
            registry.register(Box::new(c.clone()))?;
        }
        for g in metrics.all_gauges() {
            registry.register(Box::new(g.clone()))?;
        }
        Ok(metrics)
    }

    fn all_counters(&self) -> [&IntCounter; 9] {
        [
            &self.declare_mining_job_accepted,
            &self.declare_mining_job_rejected,
            &self.declare_mining_job_missing_txns,
            &self.set_custom_mining_job_accepted,
            &self.set_custom_mining_job_rejected,
            &self.push_solution_received,
            &self.blocks_submitted,
            &self.reorg_notifications,
            &self.jobs_invalidated_total,
        ]
    }

    fn all_gauges(&self) -> [&IntGauge; 2] {
        [
            &self.declared_jobs_cache_size,
            &self.recent_solutions_buffer_size,
        ]
    }
}

fn int_counter(name: &str, help: &str) -> Result<IntCounter, prometheus::Error> {
    IntCounter::new(name, help)
}

fn int_gauge(name: &str, help: &str) -> Result<IntGauge, prometheus::Error> {
    IntGauge::new(name, help)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_creates_all_collectors_with_zero_values() {
        let registry = Registry::new();
        let metrics = EngineMetrics::register(&registry).expect("register");
        for c in metrics.all_counters() {
            assert_eq!(c.get(), 0);
        }
        for g in metrics.all_gauges() {
            assert_eq!(g.get(), 0);
        }
        let names: Vec<String> = registry
            .gather()
            .iter()
            .map(|mf| mf.get_name().to_string())
            .collect();
        assert!(names.contains(&"sv2_p2pool_engine_declared_jobs_cache_size".to_string()));
        assert!(names.contains(&"sv2_p2pool_engine_recent_solutions_buffer_size".to_string()));
    }

    #[test]
    fn register_twice_on_same_registry_errors() {
        let registry = Registry::new();
        let _first = EngineMetrics::register(&registry).expect("first register");
        let second = EngineMetrics::register(&registry);
        assert!(second.is_err(), "duplicate registration must error");
    }

    #[test]
    fn counters_are_independently_incrementable() {
        let registry = Registry::new();
        let metrics = EngineMetrics::register(&registry).expect("register");
        metrics.declare_mining_job_accepted.inc();
        metrics.declare_mining_job_accepted.inc();
        metrics.blocks_submitted.inc();
        assert_eq!(metrics.declare_mining_job_accepted.get(), 2);
        assert_eq!(metrics.blocks_submitted.get(), 1);
        assert_eq!(metrics.declare_mining_job_rejected.get(), 0);
    }
}
