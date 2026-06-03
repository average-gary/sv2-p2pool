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

use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts, Registry};

/// Stable `reason` label values for [`EngineMetrics::push_solution_dropped`].
///
/// Each variant maps to one early-exit path in
/// [`crate::P2poolV2Engine::handle_push_solution`]. The strings are
/// part of the public metrics surface — operators write Prometheus
/// queries against them — so they are versioned with the same care as
/// metric names.
#[derive(Debug, Clone, Copy)]
pub enum PushSolutionDropReason {
    /// `find_by_solution` did not match any cached `DeclaredJob`.
    /// Either a stale share or a job that was reorg-invalidated
    /// before the solution arrived.
    NoMatchingJob,
    /// `find_by_solution` matched a request_id but `get` returned
    /// `None` — the cache was mutated between the two calls.
    CacheRace,
    /// Cached `DeclaredJob` was declared before TDP populated a
    /// `template_id`. Without it the engine cannot fetch tx bodies.
    NoTemplateId,
    /// `TdpHandle::request_tx_bodies` returned an error (TP timeout,
    /// `RequestTransactionDataError`, etc.).
    TdpFetchFailed,
    /// `block::reconstruct_block` returned an error (merkle mismatch,
    /// coinbase reconstruction failure).
    ReconstructFailed,
    /// `EngineHandles` not wired (Phase 2.5a / unit tests). Solution
    /// is recorded as synthetic credit only.
    NoHandles,
}

impl PushSolutionDropReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoMatchingJob => "no_matching_job",
            Self::CacheRace => "cache_race",
            Self::NoTemplateId => "no_template_id",
            Self::TdpFetchFailed => "tdp_fetch_failed",
            Self::ReconstructFailed => "reconstruct_failed",
            Self::NoHandles => "no_handles",
        }
    }
}

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
    /// Includes both successful and failed submissions — see
    /// `blocks_submit_failed` for the failure breakdown.
    pub blocks_submitted: IntCounter,
    /// `submit_block` calls bitcoind did not accept. Increments on
    /// transport errors (`Err(_)` from the RPC client) AND on consensus
    /// rejections (bitcoind returns `Ok(<reason-string>)` for these —
    /// e.g. `"high-hash"`, `"bad-prevblk"`). A non-zero value indicates
    /// lost block credit and warrants an operator alert.
    pub blocks_submit_failed: IntCounter,
    /// `notify_share_chain_reorg` invocations (any path: selective +
    /// fallback).
    pub reorg_notifications: IntCounter,
    /// `DeclaredJob`s dropped on share-chain reorg, summed across all
    /// `notify_share_chain_reorg` invocations.
    pub jobs_invalidated_total: IntCounter,

    /// `PushSolution` messages that did not result in a `submit_block`
    /// attempt, broken down by `reason`. The label values are stable
    /// strings (see [`PushSolutionDropReason`]) so dashboards can sum
    /// across them or break out individual failure modes.
    pub push_solution_dropped: IntCounterVec,

    /// Current size of the declared-jobs cache. Updated periodically
    /// by the engine's stats sweeper task (same cadence as
    /// [`crate::DEFAULT_RECENT_SOLUTIONS_SWEEP_INTERVAL`]).
    pub declared_jobs_cache_size: IntGauge,
    /// Current size of the recent-solutions buffer. Same update path
    /// as `declared_jobs_cache_size`.
    pub recent_solutions_buffer_size: IntGauge,
    /// Unix timestamp (seconds since epoch) of the most recent
    /// recent-solutions sweeper tick. Stays at 0 until the first tick
    /// runs. Operators detect a wedged sweeper task by alerting on
    /// `(time() - sweeper_last_run_timestamp_seconds) > N * scrape_interval`.
    pub sweeper_last_run_timestamp_seconds: IntGauge,
    /// Most-recently-observed share-chain tip height. `-1` until the
    /// first poll succeeds; `-1` again if a poll fails. Lets operator
    /// dashboards correlate reorg counts with the heights at which
    /// they occurred (e.g. plot `rate(reorg_notifications_total[5m])`
    /// alongside this gauge to see whether reorgs cluster at specific
    /// heights).
    pub share_chain_tip_height: IntGauge,
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
            blocks_submit_failed: int_counter(
                "sv2_p2pool_engine_blocks_submit_failed_total",
                "submit_block calls bitcoind did not accept (transport error or consensus rejection)",
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
            sweeper_last_run_timestamp_seconds: int_gauge(
                "sv2_p2pool_engine_sweeper_last_run_timestamp_seconds",
                "Unix epoch seconds of the most recent recent-solutions sweeper tick (0 = never)",
            )?,
            share_chain_tip_height: int_gauge(
                "sv2_p2pool_engine_share_chain_tip_height",
                "Most-recently-observed share-chain tip height (-1 = unknown / poll failed)",
            )?,
            push_solution_dropped: IntCounterVec::new(
                Opts::new(
                    "sv2_p2pool_engine_push_solution_dropped_total",
                    "PushSolution messages that did not result in a submit_block attempt",
                ),
                &["reason"],
            )?,
        };

        for c in metrics.all_counters() {
            registry.register(Box::new(c.clone()))?;
        }
        for g in metrics.all_gauges() {
            registry.register(Box::new(g.clone()))?;
        }
        registry.register(Box::new(metrics.push_solution_dropped.clone()))?;

        // Seed the tip-height gauge to -1 ("unknown") so dashboards
        // can distinguish "never polled" from "tip at height 0".
        metrics.share_chain_tip_height.set(-1);

        // Pre-create one child per known reason so the labels show at
        // zero in /metrics from boot — operator dashboards don't have
        // to special-case "label not yet present".
        for reason in [
            PushSolutionDropReason::NoMatchingJob,
            PushSolutionDropReason::CacheRace,
            PushSolutionDropReason::NoTemplateId,
            PushSolutionDropReason::TdpFetchFailed,
            PushSolutionDropReason::ReconstructFailed,
            PushSolutionDropReason::NoHandles,
        ] {
            metrics
                .push_solution_dropped
                .with_label_values(&[reason.as_str()]);
        }

        Ok(metrics)
    }

    /// Increment the `push_solution_dropped_total{reason}` counter for
    /// the given reason.
    pub fn record_push_solution_drop(&self, reason: PushSolutionDropReason) {
        self.push_solution_dropped
            .with_label_values(&[reason.as_str()])
            .inc();
    }

    fn all_counters(&self) -> [&IntCounter; 10] {
        [
            &self.declare_mining_job_accepted,
            &self.declare_mining_job_rejected,
            &self.declare_mining_job_missing_txns,
            &self.set_custom_mining_job_accepted,
            &self.set_custom_mining_job_rejected,
            &self.push_solution_received,
            &self.blocks_submitted,
            &self.blocks_submit_failed,
            &self.reorg_notifications,
            &self.jobs_invalidated_total,
        ]
    }

    fn all_gauges(&self) -> [&IntGauge; 4] {
        [
            &self.declared_jobs_cache_size,
            &self.recent_solutions_buffer_size,
            &self.sweeper_last_run_timestamp_seconds,
            &self.share_chain_tip_height,
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
        // share_chain_tip_height is seeded to -1 ("unknown") at register
        // time; every other gauge is 0.
        assert_eq!(metrics.declared_jobs_cache_size.get(), 0);
        assert_eq!(metrics.recent_solutions_buffer_size.get(), 0);
        assert_eq!(metrics.sweeper_last_run_timestamp_seconds.get(), 0);
        assert_eq!(metrics.share_chain_tip_height.get(), -1);
        let names: Vec<String> = registry
            .gather()
            .iter()
            .map(|mf| mf.get_name().to_string())
            .collect();
        assert!(names.contains(&"sv2_p2pool_engine_declared_jobs_cache_size".to_string()));
        assert!(names.contains(&"sv2_p2pool_engine_recent_solutions_buffer_size".to_string()));
        assert!(names.contains(&"sv2_p2pool_engine_blocks_submit_failed_total".to_string()));
        assert!(names.contains(&"sv2_p2pool_engine_share_chain_tip_height".to_string()));
        assert!(
            names.contains(&"sv2_p2pool_engine_sweeper_last_run_timestamp_seconds".to_string())
        );
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
