//! `JobValidationEngine` implementation backed by p2poolv2's share chain.
//!
//! See the project's design doc for the SV2 ↔ p2poolv2 message-by-message mapping.
//! This crate is a Phase 1 stub — only individual subsystems are implemented; the
//! full `JobValidationEngine` impl will land alongside the JDS wiring.
//!
//! # Phase 1 reorg-revocation hook
//!
//! [`reorg_detector`] is a poll-based fallback for detecting share-chain tip
//! changes. The eventual design (see ADR
//! [`0002-jdtoken-payout-script`](../../../docs/adr/0002-jdtoken-payout-script.md)
//! §"Follow-ups") is a direct call from the share-chain's organize-block path
//! into [`JobValidationEngine::notify_share_chain_reorg`] (extension added on
//! the `feat/jve-reorg-notify` branch of `vendor/sv2-apps`,
//! `pool-apps/jd-server/src/lib/job_declarator/job_validation/mod.rs`). Until
//! that ships, the engine self-detects tip changes via [`ReorgDetector`] and
//! invalidates its in-memory `declared_jobs` cache.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bitcoin::{BlockHash, ScriptBuf, Txid};
use bitcoindrpc::BitcoindLike;
use dashmap::DashMap;
use stratum_apps::utils::types::JdToken;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

pub mod block;
pub mod coinbase;
mod engine_impl;
pub mod metrics;
pub mod recent_solutions;
pub mod reorg_detector;
pub mod share_chain_reader;
pub mod tdp;

pub use block::{BlockReconstructError, reconstruct_block, reconstruct_header};
pub use coinbase::{
    CoinbaseReconstructError, merkle_path, reconstruct_coinbase,
    reconstruct_coinbase_with_extranonce,
};
pub use metrics::{EngineMetrics, PushSolutionDropReason};
pub use recent_solutions::RecentSolutions;
pub use reorg_detector::{DEFAULT_POLL_PERIOD, ReorgDetector};
// Re-exports so consumers don't need to depend on `sv2-p2pool-ipc` for
// the trait's data types (ADR 0011 § Decision § "Trait surface").
// `InProcessChain` and `IpcChain` live in the pool crate now — see
// `crates/sv2-p2pool-pool/src/share_chain.rs`. The engine crate is
// AGPL-clean: no `p2poolv2_lib` link in this dependency graph.
pub use share_chain_reader::{BoxFuture, ShareChainReader, ShareHeaderLookup, ShareHeaderRead};
pub use tdp::{TdpError, TdpHandle, TxDataResult};

/// Opaque request-id used to key declared-job cache entries. Mirrors
/// `BitcoinCoreIPCEngine::declared_custom_jobs`'s
/// `Arc<DashMap<RequestId, _>>` shape (see
/// `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:214`).
/// Aliased here to keep this crate independent of the upstream type while we
/// wait for the JDS wiring to land.
pub type RequestId = u32;

/// Snapshot of a declared mining job, cached after `handle_declare_mining_job`
/// returns `Success` (or `MissingTransactions`, with `validated = false`).
///
/// Mirrors `BitcoinCoreIPCEngine`'s `DeclaredCustomJob` shape (see
/// `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:62-68`)
/// without taking a hard dep on the upstream type.
#[derive(Clone, Debug)]
pub struct DeclaredJob {
    /// Block version from the original `DeclareMiningJob`.
    pub version: u32,
    /// Coinbase prefix bytes (first part of the coinbase, before the extranonce).
    pub coinbase_tx_prefix: Vec<u8>,
    /// Coinbase suffix bytes (after the extranonce).
    pub coinbase_tx_suffix: Vec<u8>,
    /// Full wtxid list (including coinbase wtxid at index 0).
    pub wtxid_list: Vec<bitcoin::Wtxid>,
    /// Txid list, computed from full transaction bodies after `Success`.
    /// `None` while waiting for `ProvideMissingTransactions`.
    pub txid_list: Option<Vec<Txid>>,
    /// Bitcoin tip metadata captured at declare time. Phase 2.4 populates
    /// these from the TDP `SetNewPrevHash` snapshot exposed by
    /// [`TdpHandle::current_tip`]; without handles they remain at
    /// `Default` values (all-zeros `prev_hash`, zero `nbits`/`min_ntime`)
    /// and the trait's structural-only mode tolerates the placeholders.
    pub tip: TipMetadata,
    /// TDP `template_id` captured from the most recent `NewTemplate` at
    /// declare time. Phase 2.4 uses this to fetch the matching
    /// transaction bodies via `RequestTransactionData(template_id)` when
    /// `handle_push_solution` reconstructs the full block. `None` when
    /// handles aren't wired (structural-only mode).
    pub template_id: Option<u64>,
    /// Share-chain tip blockhash at declare time. Populated from
    /// `EngineHandles::chain.get_chain_tip()` when handles are wired;
    /// `None` in structural-only mode.
    ///
    /// Stored so a future selective invalidation rule can drop just the
    /// jobs whose ancestor is no longer on the share chain after a tip
    /// swap. Until that lands, `notify_share_chain_reorg` still flushes
    /// the whole cache (see ADR 0001 + `DeclaredJobCache::invalidate_all`).
    pub share_chain_tip: Option<BlockHash>,
    /// Whether the job has been fully validated.
    /// Set to `true` once `handle_declare_mining_job` returns `Success`.
    pub validated: bool,
}

/// Bitcoin tip metadata captured at `DeclareMiningJob` time.
///
/// Used by `handle_set_custom_mining_job` to detect a stale chain tip:
/// if the JDC's `SetCustomMiningJob.prev_hash` doesn't match what we
/// captured from bitcoind's GBT response, the candidate is stale.
///
/// Mirrors the role of `BitcoinCoreIPCEngine`'s `ValidationContext`
/// (`prev_hash`, `nbits`, `min_ntime`) at
/// `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:65`.
#[derive(Clone, Copy, Debug)]
pub struct TipMetadata {
    /// Bitcoin tip's previous-block hash.
    pub prev_hash: BlockHash,
    /// nbits (compact difficulty target) of the tip.
    pub nbits: u32,
    /// Minimum acceptable `ntime` for blocks built on this tip
    /// (typically the tip's median time + 1).
    pub min_ntime: u32,
}

impl Default for TipMetadata {
    /// All-zeros fallback used in structural-only mode (no handles).
    /// `bitcoin::BlockHash` doesn't impl `Default`, so we provide one
    /// with `from_byte_array([0; 32])`.
    fn default() -> Self {
        use bitcoin::hashes::Hash as _;
        Self {
            prev_hash: BlockHash::from_byte_array([0u8; 32]),
            nbits: 0,
            min_ntime: 0,
        }
    }
}

/// In-memory cache of declared jobs, keyed by `RequestId`.
///
/// Mirrors the role of `BitcoinCoreIPCEngine::declared_custom_jobs` for our
/// p2pool-flavoured engine. Cheap to clone via `Arc`.
#[derive(Clone, Debug, Default)]
pub struct DeclaredJobCache {
    inner: Arc<DashMap<RequestId, DeclaredJob>>,
}

impl DeclaredJobCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a declared job. Returns the previous value if any.
    pub fn insert(&self, request_id: RequestId, job: DeclaredJob) -> Option<DeclaredJob> {
        self.inner.insert(request_id, job)
    }

    /// Remove a single declared job.
    pub fn remove(&self, request_id: &RequestId) -> Option<DeclaredJob> {
        self.inner.remove(request_id).map(|(_, v)| v)
    }

    /// Number of cached jobs.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Find the request_id of the cached declared job whose
    /// `(tip.prev_hash, tip.nbits, version)` matches the given triple.
    /// Used by `handle_push_solution` to recover the cached job that
    /// produced a given `PushSolution` (PushSolution itself doesn't
    /// carry a token or request_id).
    ///
    /// Returns `None` if no cached job matches. If multiple jobs match
    /// (rare; same template declared multiple times), returns the first
    /// one encountered.
    pub fn find_by_solution(
        &self,
        prev_hash: BlockHash,
        nbits: u32,
        version: u32,
    ) -> Option<RequestId> {
        for entry in self.inner.iter() {
            let job = entry.value();
            if job.tip.prev_hash == prev_hash && job.tip.nbits == nbits && job.version == version {
                return Some(*entry.key());
            }
        }
        None
    }

    /// Get a clone of the cached declared job for the given request_id.
    pub fn get(&self, request_id: &RequestId) -> Option<DeclaredJob> {
        self.inner.get(request_id).map(|e| e.value().clone())
    }

    /// Phase 1 invalidation rule: drop every cached entry.
    ///
    /// "Dead ancestry" is hard to evaluate without a formal share-chain tip
    /// height tracked alongside each cached job (the upstream
    /// `BitcoinCoreIPCEngine` carries `min_ntime` in `validation_context`,
    /// but for p2pool we'd want a `share_chain_tip_at_declare_time` field
    /// which doesn't exist yet). Until that lands, we conservatively
    /// invalidate the whole cache on every tip change. ADR
    /// [`0001-uncle-weighting`](../../../docs/adr/0001-uncle-weighting.md)
    /// (α=1, uncles not stale) does not change this: a *new uncle* is not a
    /// tip change, so the detector won't fire; only an actual tip swap
    /// reaches `invalidate_all`.
    pub fn invalidate_all(&self) -> usize {
        let dropped = self.inner.len();
        self.inner.clear();
        dropped
    }

    /// Selective invalidation: drop every cached entry where
    /// `predicate(&job)` returns `false`. Returns the number of
    /// entries dropped.
    ///
    /// Used by `notify_share_chain_reorg` (Phase 2-A) to keep jobs
    /// whose captured `share_chain_tip` is still an ancestor of the
    /// new tip. The predicate is invoked under the cache's internal
    /// lock; keep it cheap.
    pub fn retain<F>(&self, predicate: F) -> usize
    where
        F: Fn(&DeclaredJob) -> bool,
    {
        let mut dropped = 0;
        self.inner.retain(|_request_id, job| {
            let keep = predicate(job);
            if !keep {
                dropped += 1;
            }
            keep
        });
        dropped
    }
}

/// Token-payout binding. Per ADR 0002 § Decision § 1.
///
/// Maps each `JdToken` to the miner's coinbase payout script chosen by
/// the JDC at allocation time. Populated by the binary's token-allocation
/// interceptor (Phase 1.5) BEFORE the JDS sees the message. The engine
/// reads from it inside `handle_push_solution` to credit the right
/// p2pool miner.
pub type TokenPayoutMap = Arc<DashMap<JdToken, ScriptBuf>>;

/// Token → request_id binding. Mirrors
/// `BitcoinCoreIPCEngine::allocated_token_entries`'s lookup role at
/// `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:213`.
///
/// `handle_declare_mining_job` writes (token, request_id) on Success;
/// `handle_set_custom_mining_job` reads + removes the entry to find the
/// matching declared-job snapshot. Decoupled from `TokenPayoutMap`
/// because the latter is owned by the binary's interceptor and lives
/// across the trait boundary, while this map is engine-internal state.
pub type AllocatedTokenMap = Arc<DashMap<JdToken, RequestId>>;

/// Backend handles for the engine.
///
/// When present, the trait methods perform real share-chain validation
/// (Phase 2.3+ wires this through). When absent (Phase 1 default), the
/// trait methods do structural validation only with stub-zero values for
/// `prev_hash`/`nbits`.
///
/// Both handles are cloneable / `Arc`-shareable, matching how
/// `p2poolv2_node` constructs them at startup
/// (`vendor/p2poolv2/p2poolv2_node/src/main.rs`).
///
/// **Note**: an earlier revision carried an `Arc<dyn ShareValidator>`
/// here. The trait validates `ShareHeader` / `ShareBlock` — types the
/// JDP-side engine never sees, since it works with bitcoin coinbase /
/// `PushSolution` / reconstructed `bitcoin::Block`. The validator
/// belongs on the share-chain node side, not in our engine. Removed
/// in PR #50.
#[derive(Clone)]
pub struct EngineHandles {
    /// Read access to the share chain. Used to look up the current
    /// share-chain tip + validate share-block ancestry on reorg.
    ///
    /// Phase 2-B Track A (ADR 0011) abstracts this behind the
    /// [`ShareChainReader`] trait so the engine no longer depends on
    /// the AGPL-licensed `p2poolv2_lib::ChainStoreHandle` directly.
    /// The pool crate provides two `Arc<dyn ShareChainReader>`
    /// backends: an `InProcessChain` adapter that wraps a real
    /// `ChainStoreHandle` (single-process / tests) and an `IpcChain`
    /// actor that talks to a separate p2poolv2 daemon over capnp-on-UDS
    /// (production). Both live in
    /// `crates/sv2-p2pool-pool/src/share_chain.rs`.
    pub chain: Arc<dyn ShareChainReader>,
    /// Bitcoin RPC backend. Phase 2.4 uses this only for `submit_block`
    /// (forward found blocks); tip metadata + tx bodies come from the
    /// SV2 Template Distribution Protocol via [`TdpHandle`]. The trait
    /// abstraction (vendored fork `feat/bitcoind-trait`) lets us mock
    /// bitcoind in tests.
    pub bitcoind: Arc<dyn BitcoindLike>,
}

impl std::fmt::Debug for EngineHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineHandles")
            .field("chain", &"<dyn ShareChainReader>")
            .field("bitcoind", &"<dyn BitcoindLike>")
            .finish()
    }
}

/// Phase 1 engine surface.
///
/// Holds:
/// - The cached declared-jobs map (`DeclaredJobCache`)
/// - A handle to the optional reorg-watcher background task
/// - Token → payout-script binding (`TokenPayoutMap`, populated by the
///   binary's allocation interceptor — see ADR 0002 § Decision § 1)
/// - The most-recent-solutions buffer for the PushSolution race (PR #17)
/// - Backend handles: bitcoind RPC trait, network, and a tip-source closure
///   stored as the watcher's input (set during `start_reorg_watcher`)
///
/// The trait impl methods land in Phase 1.2 — 1.4 below.
pub struct P2poolV2Engine {
    declared_jobs: DeclaredJobCache,
    allocated_tokens: AllocatedTokenMap,
    token_payout: TokenPayoutMap,
    recent_solutions: Arc<RecentSolutions>,
    /// Bitcoin network this engine targets. Used by accounting + payout.
    network: bitcoin::Network,
    /// Active watcher task; aborted on drop.
    reorg_watcher: Option<tokio::task::JoinHandle<()>>,
    /// Periodic [`RecentSolutions::sweep`] task; aborted on drop.
    /// Without this the buffer grows unbounded under sustained
    /// PushSolution traffic — entries are only proactively evicted by
    /// `take`, and shares for which a matching SubmitSharesExtended
    /// never arrives would otherwise leak forever.
    recent_solutions_sweeper: Option<tokio::task::JoinHandle<()>>,
    /// Backend handles (chain + validator + bitcoind). `None` for
    /// Phase-1-style structural-only mode; `Some` for Phase 2.5b+ when
    /// the binary plumbs in p2poolv2 Node + bitcoind RPC.
    handles: Option<EngineHandles>,
    /// SV2 Template Distribution Protocol bridge. Independent of
    /// `handles` because Phase 2.5a wires the TDP demux ahead of the
    /// full Node bring-up. When `Some`, the trait impl reads tip
    /// metadata + fetches tx bodies via TDP. When `None`, falls back to
    /// `TipMetadata::default()` + skips block submission.
    tdp: Option<tdp::TdpHandle>,
    /// Prometheus counter set. `None` skips all metrics increments;
    /// `Some` is wired by [`P2poolV2Engine::with_metrics`] from a
    /// registry the binary owns.
    metrics: Option<EngineMetrics>,
}

/// Default TTL for the `RecentSolutions` buffer — long enough to cover
/// reasonable share-submission latency after a `PushSolution` arrives.
pub const DEFAULT_RECENT_SOLUTIONS_TTL: Duration = Duration::from_secs(30);

/// Default sweep interval for the [`RecentSolutions`] periodic
/// housekeeping task. Half the TTL ensures every entry is observed at
/// least once before its TTL elapses, bounding the worst-case memory
/// envelope to ~1.5× the share-submission rate × TTL.
pub const DEFAULT_RECENT_SOLUTIONS_SWEEP_INTERVAL: Duration = Duration::from_secs(15);

impl P2poolV2Engine {
    /// Construct an engine for the given network. The binary is responsible
    /// for wiring in the bitcoind backend and tip source via
    /// [`P2poolV2Engine::start_reorg_watcher`] (Phase 1.2 — 1.4 will widen
    /// this to take an `Arc<dyn BitcoindLike>` and `ChainStoreHandle` once
    /// the trait impl lands).
    pub fn new(network: bitcoin::Network) -> Self {
        Self {
            declared_jobs: DeclaredJobCache::new(),
            allocated_tokens: Arc::new(DashMap::new()),
            token_payout: Arc::new(DashMap::new()),
            recent_solutions: Arc::new(RecentSolutions::new(DEFAULT_RECENT_SOLUTIONS_TTL)),
            network,
            reorg_watcher: None,
            recent_solutions_sweeper: None,
            handles: None,
            tdp: None,
            metrics: None,
        }
    }

    /// Construct an engine with real backend handles. The trait methods
    /// will perform real share-chain validation (Phase 2.5b+ wires this
    /// through). Use [`P2poolV2Engine::new`] for structural-only tests.
    pub fn with_handles(network: bitcoin::Network, handles: EngineHandles) -> Self {
        let mut engine = Self::new(network);
        engine.handles = Some(handles);
        engine
    }

    /// Set the TDP bridge. Phase 2.5a wires this from `Pool::start` so
    /// the engine receives `SetNewPrevHash`/`NewTemplate` snapshots
    /// from the demux task and can issue `RequestTransactionData` on
    /// the merged Pool→TP channel. Independent of `with_handles`
    /// because the Node bring-up (Phase 2.5b) lands separately.
    pub fn with_tdp(mut self, tdp: tdp::TdpHandle) -> Self {
        self.tdp = Some(tdp);
        self
    }

    /// Attach a Prometheus counter set. The binary registers the set
    /// on its monitoring registry and threads it here. Without this,
    /// every `metrics()`-gated increment is a no-op.
    pub fn with_metrics(mut self, metrics: EngineMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Borrow the metrics counters if wired. Trait-impl event paths
    /// use this to increment counters without needing to thread the
    /// set through every helper.
    pub fn metrics(&self) -> Option<&EngineMetrics> {
        self.metrics.as_ref()
    }

    /// Whether the engine has real backend handles wired in.
    ///
    /// `false` = Phase-1-style structural-only mode (trait methods stub
    /// share-chain validation). `true` = Phase 2+ mode.
    pub fn has_handles(&self) -> bool {
        self.handles.is_some()
    }

    /// Whether a TDP handle is wired in. Independent of `has_handles`.
    pub fn has_tdp(&self) -> bool {
        self.tdp.is_some()
    }

    /// Borrow the TDP bridge if wired. Used by the binary's demux task
    /// to push `SetNewPrevHash`/`NewTemplate` snapshots and deliver
    /// `RequestTransactionData` responses.
    pub fn tdp(&self) -> Option<&tdp::TdpHandle> {
        self.tdp.as_ref()
    }

    /// Access the backend handles, if present.
    ///
    /// Phase 2.1 introduces this accessor; Phase 2.3+ trait methods
    /// consume it to perform real share-chain validation.
    #[allow(dead_code, reason = "consumer is Phase 2.3+ trait methods")]
    pub(crate) fn handles(&self) -> Option<&EngineHandles> {
        self.handles.as_ref()
    }

    /// Internal access to the allocated-tokens map for the trait impl.
    pub(crate) fn allocated_tokens(&self) -> &AllocatedTokenMap {
        &self.allocated_tokens
    }

    /// Access the declared-jobs cache.
    pub fn declared_jobs(&self) -> &DeclaredJobCache {
        &self.declared_jobs
    }

    /// Access the token-payout map (cloneable `Arc`).
    ///
    /// The binary's token-allocation interceptor (Phase 1.5) writes into
    /// this map at `AllocateMiningJobToken` time; the engine reads from it
    /// in `handle_push_solution`.
    pub fn token_payout(&self) -> TokenPayoutMap {
        Arc::clone(&self.token_payout)
    }

    /// Look up the payout script for a token. Returns `None` if the
    /// token has no specific binding — caller should fall back to the
    /// pool-wide `coinbase_reward_script`.
    ///
    /// Per ADR 0002, per-miner payout scripts are populated by the
    /// binary's interceptor; until Phase 2 lands a JDC TLV extension
    /// for sending the per-miner script in `AllocateMiningJobToken`,
    /// this map will be empty in production and every miner gets the
    /// pool-wide fallback.
    pub fn lookup_payout_script(&self, token: JdToken) -> Option<ScriptBuf> {
        self.token_payout
            .get(&token)
            .map(|entry| entry.value().clone())
    }

    /// Access the recent-solutions buffer (cloneable `Arc`).
    ///
    /// Used by `handle_push_solution` to record block-finder credit
    /// before the matching `SubmitSharesExtended` arrives. The
    /// `ChannelManager`'s share-submission path (Phase 1.6) drains it.
    pub fn recent_solutions(&self) -> Arc<RecentSolutions> {
        Arc::clone(&self.recent_solutions)
    }

    /// The Bitcoin network this engine targets.
    pub fn network(&self) -> bitcoin::Network {
        self.network
    }

    /// Spawn the reorg watcher.
    ///
    /// `tip_source` is a closure that returns the current confirmed
    /// share-chain tip (`None` during initial sync). In production this
    /// closes over an `Arc<ChainStoreHandle>` and calls
    /// `chain.get_chain_tip().ok()`
    /// (`vendor/p2poolv2/p2poolv2_lib/src/shares/chain/chain_store_handle.rs:260`).
    ///
    /// The watcher polls at `period` (use [`DEFAULT_POLL_PERIOD`] in
    /// production) and calls [`DeclaredJobCache::invalidate_all`] on every
    /// detected tip change.
    ///
    /// Returns a [`broadcast::Receiver`] for callers (e.g. tests, metrics)
    /// that want to observe tip changes alongside the cache invalidation.
    /// Calling `start_reorg_watcher` more than once aborts the previous
    /// watcher first.
    pub fn start_reorg_watcher<F>(
        &mut self,
        tip_source: F,
        period: Duration,
    ) -> broadcast::Receiver<BlockHash>
    where
        F: Fn() -> Option<BlockHash> + Send + 'static,
    {
        if let Some(prev) = self.reorg_watcher.take() {
            prev.abort();
        }

        let detector = ReorgDetector::new(tip_source, period);
        let observer = detector.subscribe();
        let invalidator = detector.subscribe();

        let cache = self.declared_jobs.clone();
        let metrics_for_invalidator = self.metrics.clone();
        let invalidator_handle = tokio::spawn(reorg_invalidator_loop(
            invalidator,
            cache,
            metrics_for_invalidator,
        ));
        let detector_handle = tokio::spawn(detector.run());

        // Wrap both handles into a single supervisor: aborting the supervisor
        // aborts both children. Using `tokio::spawn` rather than a `JoinSet`
        // keeps the dependency surface minimal.
        let supervisor = tokio::spawn(async move {
            tokio::select! {
                _ = detector_handle => {
                    warn!("ReorgDetector run loop exited unexpectedly");
                }
                _ = invalidator_handle => {
                    warn!("Reorg invalidator loop exited unexpectedly");
                }
            }
        });

        self.reorg_watcher = Some(supervisor);
        observer
    }

    /// Stop the watcher, if running.
    pub fn stop_reorg_watcher(&mut self) {
        if let Some(handle) = self.reorg_watcher.take() {
            handle.abort();
        }
    }

    /// Spawn the periodic [`RecentSolutions::sweep`] task at the given
    /// `interval`. Use [`DEFAULT_RECENT_SOLUTIONS_SWEEP_INTERVAL`] in
    /// production. Calling more than once aborts the previous sweeper
    /// first.
    ///
    /// `RecentSolutions` evicts entries opportunistically inside
    /// [`RecentSolutions::take`]; this background task bounds memory
    /// for shares whose matching `SubmitSharesExtended` never arrives.
    /// When [`EngineMetrics`] is wired, the same task also updates the
    /// `declared_jobs_cache_size` and `recent_solutions_buffer_size`
    /// gauges on each tick — gauges and the sweep run on the same
    /// cadence so dashboards see fresh state right after each sweep
    /// evicts.
    pub fn start_recent_solutions_sweeper(&mut self, interval: Duration) {
        if let Some(prev) = self.recent_solutions_sweeper.take() {
            prev.abort();
        }
        let buf = Arc::clone(&self.recent_solutions);
        let cache = self.declared_jobs.clone();
        let metrics = self.metrics.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip the immediate fire — the buffer just initialised.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                buf.sweep();
                if let Some(m) = metrics.as_ref() {
                    m.declared_jobs_cache_size.set(cache.len() as i64);
                    m.recent_solutions_buffer_size.set(buf.len() as i64);
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    m.sweeper_last_run_timestamp_seconds.set(now);
                }
            }
        });
        self.recent_solutions_sweeper = Some(handle);
    }

    /// Stop the sweeper, if running.
    pub fn stop_recent_solutions_sweeper(&mut self) {
        if let Some(handle) = self.recent_solutions_sweeper.take() {
            handle.abort();
        }
    }
}

impl Drop for P2poolV2Engine {
    fn drop(&mut self) {
        self.stop_reorg_watcher();
        self.stop_recent_solutions_sweeper();
    }
}

impl Default for P2poolV2Engine {
    /// Default-constructs an engine on regtest. Use [`P2poolV2Engine::new`]
    /// to target a different network.
    fn default() -> Self {
        Self::new(bitcoin::Network::Regtest)
    }
}

/// Drains tip-change broadcasts and invalidates the cache for each.
///
/// Lives outside `P2poolV2Engine` so the spawned task doesn't borrow `self`.
/// Bumps `reorg_notifications_total` and `jobs_invalidated_total` on every
/// real reorg event AND on the lagged-broadcast defensive-flush path —
/// both reflect "operator-visible reorg activity that dropped jobs."
async fn reorg_invalidator_loop(
    mut rx: broadcast::Receiver<BlockHash>,
    cache: DeclaredJobCache,
    metrics: Option<EngineMetrics>,
) {
    loop {
        match rx.recv().await {
            Ok(new_tip) => {
                let dropped = cache.invalidate_all();
                if let Some(m) = metrics.as_ref() {
                    m.reorg_notifications.inc();
                    m.jobs_invalidated_total.inc_by(dropped as u64);
                }
                info!(
                    new_tip = %new_tip,
                    dropped,
                    "share-chain reorg detected; invalidated declared-jobs cache"
                );
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // We missed `n` events but the bounded broadcast caught up.
                // Conservatively flush — we don't know which tip we're at.
                let dropped = cache.invalidate_all();
                if let Some(m) = metrics.as_ref() {
                    m.reorg_notifications.inc();
                    m.jobs_invalidated_total.inc_by(dropped as u64);
                }
                warn!(
                    missed = n,
                    dropped,
                    "lagged behind reorg broadcast; flushed declared-jobs cache defensively"
                );
            }
            Err(broadcast::error::RecvError::Closed) => {
                debug!("reorg broadcast channel closed; invalidator exiting");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use bitcoin::hashes::Hash;

    use super::*;

    fn hash_from_u64(seed: u64) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        BlockHash::from_byte_array(bytes)
    }

    fn dummy_job(version: u32) -> DeclaredJob {
        DeclaredJob {
            version,
            coinbase_tx_prefix: vec![],
            coinbase_tx_suffix: vec![],
            wtxid_list: vec![],
            txid_list: None,
            tip: TipMetadata::default(),
            template_id: None,
            share_chain_tip: None,
            validated: false,
        }
    }

    fn job_with_tip(version: u32, prev_hash: BlockHash, nbits: u32) -> DeclaredJob {
        DeclaredJob {
            version,
            coinbase_tx_prefix: vec![],
            coinbase_tx_suffix: vec![],
            wtxid_list: vec![],
            txid_list: None,
            tip: TipMetadata {
                prev_hash,
                nbits,
                min_ntime: 0,
            },
            template_id: None,
            share_chain_tip: None,
            validated: false,
        }
    }

    #[test]
    fn find_by_solution_empty_cache_returns_none() {
        let cache = DeclaredJobCache::new();
        assert_eq!(cache.find_by_solution(BlockHash::all_zeros(), 0, 0), None);
    }

    #[test]
    fn find_by_solution_returns_request_id_on_match() {
        let cache = DeclaredJobCache::new();
        let prev_a = hash_from_u64(1);
        let prev_b = hash_from_u64(2);
        cache.insert(10, job_with_tip(1, prev_a, 0x100));
        cache.insert(20, job_with_tip(2, prev_b, 0x200));

        assert_eq!(cache.find_by_solution(prev_a, 0x100, 1), Some(10));
        assert_eq!(cache.find_by_solution(prev_b, 0x200, 2), Some(20));
    }

    #[test]
    fn find_by_solution_returns_none_when_no_field_matches() {
        let cache = DeclaredJobCache::new();
        let prev_a = hash_from_u64(1);
        cache.insert(10, job_with_tip(1, prev_a, 0x100));

        // version mismatch
        assert_eq!(cache.find_by_solution(prev_a, 0x100, 999), None);
        // nbits mismatch
        assert_eq!(cache.find_by_solution(prev_a, 0x999, 1), None);
        // prev_hash mismatch
        assert_eq!(cache.find_by_solution(hash_from_u64(99), 0x100, 1), None);
    }

    #[test]
    fn retain_keeps_matching_drops_others() {
        let cache = DeclaredJobCache::new();
        cache.insert(1, dummy_job(1));
        cache.insert(2, dummy_job(2));
        cache.insert(3, dummy_job(3));
        // Drop everything with even version.
        let dropped = cache.retain(|job| job.version % 2 == 1);
        assert_eq!(dropped, 1);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&1).is_some());
        assert!(cache.get(&2).is_none());
        assert!(cache.get(&3).is_some());
    }

    #[test]
    fn retain_drop_all_returns_full_count() {
        let cache = DeclaredJobCache::new();
        cache.insert(1, dummy_job(1));
        cache.insert(2, dummy_job(2));
        let dropped = cache.retain(|_| false);
        assert_eq!(dropped, 2);
        assert!(cache.is_empty());
    }

    #[test]
    fn retain_keep_all_returns_zero() {
        let cache = DeclaredJobCache::new();
        cache.insert(1, dummy_job(1));
        cache.insert(2, dummy_job(2));
        let dropped = cache.retain(|_| true);
        assert_eq!(dropped, 0);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn declared_job_cache_invalidate_all_drops_everything() {
        let cache = DeclaredJobCache::new();
        cache.insert(1, dummy_job(1));
        cache.insert(2, dummy_job(2));
        cache.insert(3, dummy_job(3));
        assert_eq!(cache.len(), 3);

        let dropped = cache.invalidate_all();
        assert_eq!(dropped, 3);
        assert!(cache.is_empty());

        // Idempotent — second invalidate is a no-op.
        let dropped = cache.invalidate_all();
        assert_eq!(dropped, 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn engine_default_constructs_with_empty_state() {
        let engine = P2poolV2Engine::default();
        assert_eq!(engine.network(), bitcoin::Network::Regtest);
        assert!(engine.declared_jobs().is_empty());
        assert_eq!(engine.token_payout().len(), 0);
        assert!(
            !engine.has_handles(),
            "::default() / ::new() construct without handles"
        );
    }

    #[tokio::test]
    async fn engine_with_handles_reports_handles_present() {
        use bitcoindrpc::mock::MockBitcoind;

        use crate::share_chain_reader::mock::MockShareChain;

        // Build production handles via the in-memory mock chain
        // backend. ADR 0011 § Decision § "MockShareChain" replaces
        // the on-disk `setup_test_chain_store_handle` fixture so
        // these tests don't require the AGPL `p2poolv2_lib`
        // path-dep at runtime.
        let chain: Arc<dyn ShareChainReader> = Arc::new(MockShareChain::new());
        let bitcoind: Arc<dyn BitcoindLike> = Arc::new(MockBitcoind::default());
        let (tx_sender, _tx_receiver) = async_channel::unbounded();
        let tdp = TdpHandle::new(tx_sender);

        let handles = EngineHandles { chain, bitcoind };
        let engine = P2poolV2Engine::with_handles(bitcoin::Network::Regtest, handles).with_tdp(tdp);
        assert!(engine.has_handles());
        assert!(engine.has_tdp());
        assert_eq!(engine.network(), bitcoin::Network::Regtest);
        // Cache + token map start empty even with handles.
        assert!(engine.declared_jobs().is_empty());
        // The internal accessor returns Some.
        assert!(engine.handles().is_some());
    }

    #[test]
    fn engine_token_payout_is_shared_arc() {
        let engine = P2poolV2Engine::new(bitcoin::Network::Regtest);
        let map_a = engine.token_payout();
        let map_b = engine.token_payout();
        // Both clones point at the same underlying DashMap.
        let script = ScriptBuf::new();
        map_a.insert(42, script.clone());
        assert_eq!(map_b.len(), 1);
        assert_eq!(map_b.get(&42).map(|r| r.value().clone()), Some(script));
    }

    #[tokio::test]
    async fn engine_recent_solutions_sweeper_evicts_expired_entries() {
        use bitcoin::hashes::Hash as _;
        // RecentSolutions::sweep tests wall-clock `Instant::elapsed()`,
        // so we use real time here rather than tokio's start_paused
        // virtual clock (which only fast-forwards Sleep, not Instant).
        let mut engine = P2poolV2Engine::new(bitcoin::Network::Regtest);
        let buf = Arc::new(RecentSolutions::new(Duration::from_millis(50)));
        engine.recent_solutions = buf.clone();

        let share = BlockHash::from_byte_array([1u8; 32]);
        let block = BlockHash::from_byte_array([2u8; 32]);
        buf.record(share, block);
        assert_eq!(buf.len(), 1);

        engine.start_recent_solutions_sweeper(Duration::from_millis(20));
        // Wait past the TTL plus a couple of sweep intervals so the
        // sweeper definitely ticks after the entry expired.
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert_eq!(
            buf.len(),
            0,
            "sweeper should have evicted the expired entry"
        );

        engine.stop_recent_solutions_sweeper();
    }

    #[tokio::test(start_paused = true)]
    async fn engine_sweeper_updates_cache_size_gauges() {
        use bitcoin::hashes::Hash as _;
        use prometheus::Registry;

        let registry = Registry::new();
        let metrics = EngineMetrics::register(&registry).expect("register");
        let mut engine =
            P2poolV2Engine::new(bitcoin::Network::Regtest).with_metrics(metrics.clone());
        // Long TTL so the buffer entry doesn't expire mid-test.
        let buf = Arc::new(RecentSolutions::new(Duration::from_secs(10)));
        engine.recent_solutions = buf.clone();

        // Gauges start at zero before the sweeper has ticked.
        assert_eq!(metrics.declared_jobs_cache_size.get(), 0);
        assert_eq!(metrics.recent_solutions_buffer_size.get(), 0);
        assert_eq!(metrics.sweeper_last_run_timestamp_seconds.get(), 0);

        // Pre-populate both caches BEFORE the sweeper starts so the
        // first tick reports the populated state.
        engine.declared_jobs().insert(1, dummy_job(1));
        engine.declared_jobs().insert(2, dummy_job(2));
        buf.record(
            BlockHash::from_byte_array([1u8; 32]),
            BlockHash::from_byte_array([2u8; 32]),
        );

        engine.start_recent_solutions_sweeper(Duration::from_millis(20));
        // Drive the paused clock past one tick; yield twice so the
        // spawned sweeper task gets to run before we observe.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(25)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(metrics.declared_jobs_cache_size.get(), 2);
        assert_eq!(metrics.recent_solutions_buffer_size.get(), 1);
        // Liveness gauge moved off zero — proving the sweeper actually
        // ran (and not just that the cache was inspected by something
        // else). Don't assert an exact value: paused tokio time doesn't
        // pause SystemTime, but the relative check is enough.
        assert!(
            metrics.sweeper_last_run_timestamp_seconds.get() > 0,
            "sweeper liveness gauge should have moved off zero"
        );

        // Shrink the declared-jobs cache and verify the gauge decrements
        // on the next tick — this is the whole point of using IntGauge
        // over IntCounter.
        engine.declared_jobs().remove(&1);
        tokio::time::advance(Duration::from_millis(25)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(metrics.declared_jobs_cache_size.get(), 1);

        engine.stop_recent_solutions_sweeper();
    }

    #[tokio::test(start_paused = true)]
    async fn engine_invalidates_cache_on_tip_change() {
        let mut engine = P2poolV2Engine::new(bitcoin::Network::Regtest);
        engine.declared_jobs().insert(1, dummy_job(1));
        engine.declared_jobs().insert(2, dummy_job(2));
        assert_eq!(engine.declared_jobs().len(), 2);

        // Scripted tip source: tip_a, tip_a, tip_b.
        let cursor = Arc::new(AtomicUsize::new(0));
        let tip_source = move || {
            let i = cursor.fetch_add(1, Ordering::SeqCst);
            match i {
                0 => Some(hash_from_u64(1)),
                1 => Some(hash_from_u64(1)),
                _ => Some(hash_from_u64(2)),
            }
        };

        let mut observer = engine.start_reorg_watcher(tip_source, Duration::from_millis(50));

        // Drive enough ticks to hit the third poll.
        for _ in 0..6 {
            tokio::time::advance(Duration::from_millis(60)).await;
            tokio::task::yield_now().await;
        }

        // Observer should have seen tip_b (the change), and cache should be empty.
        let received = tokio::time::timeout(Duration::from_millis(500), observer.recv()).await;
        assert_eq!(
            received.expect("recv timed out").ok(),
            Some(hash_from_u64(2))
        );

        // Give the invalidator task a chance to run.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;

        assert!(
            engine.declared_jobs().is_empty(),
            "cache should be flushed after tip change"
        );

        engine.stop_reorg_watcher();
    }

    /// Regression test: the reorg watcher's invalidator loop must bump
    /// `reorg_notifications_total` and `jobs_invalidated_total` on every
    /// detected tip change. Earlier code only bumped these inside
    /// `JobValidationEngine::notify_share_chain_reorg`, which the
    /// production reorg watcher never calls — so both counters were
    /// permanently stuck at zero in production.
    #[tokio::test(start_paused = true)]
    async fn engine_reorg_watcher_bumps_metrics_on_tip_change() {
        use prometheus::Registry;

        let registry = Registry::new();
        let metrics = EngineMetrics::register(&registry).expect("register");
        let mut engine =
            P2poolV2Engine::new(bitcoin::Network::Regtest).with_metrics(metrics.clone());
        engine.declared_jobs().insert(1, dummy_job(1));
        engine.declared_jobs().insert(2, dummy_job(2));
        engine.declared_jobs().insert(3, dummy_job(3));

        assert_eq!(metrics.reorg_notifications.get(), 0);
        assert_eq!(metrics.jobs_invalidated_total.get(), 0);

        // Scripted tip source: tip_a (seen first poll), tip_b (forces a reorg).
        let cursor = Arc::new(AtomicUsize::new(0));
        let tip_source = move || {
            let i = cursor.fetch_add(1, Ordering::SeqCst);
            match i {
                0 => Some(hash_from_u64(1)),
                _ => Some(hash_from_u64(2)),
            }
        };

        let _observer = engine.start_reorg_watcher(tip_source, Duration::from_millis(50));

        for _ in 0..6 {
            tokio::time::advance(Duration::from_millis(60)).await;
            tokio::task::yield_now().await;
        }

        // Give the invalidator task a chance to drain.
        for _ in 0..3 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
        }

        assert!(
            engine.declared_jobs().is_empty(),
            "cache should be flushed after tip change"
        );
        assert_eq!(
            metrics.reorg_notifications.get(),
            1,
            "reorg_notifications must increment when the watcher detects a tip change"
        );
        assert_eq!(
            metrics.jobs_invalidated_total.get(),
            3,
            "jobs_invalidated_total must reflect the count of jobs flushed by the invalidator"
        );

        engine.stop_reorg_watcher();
    }
}
