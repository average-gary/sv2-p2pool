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
use p2poolv2_lib::shares::{
    chain::chain_store_handle::ChainStoreHandle, validation::ShareValidator,
};
use stratum_apps::utils::types::JdToken;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

pub mod coinbase;
mod engine_impl;
pub mod recent_solutions;
pub mod reorg_detector;

pub use coinbase::{CoinbaseReconstructError, merkle_path, reconstruct_coinbase};
pub use recent_solutions::RecentSolutions;
pub use reorg_detector::{DEFAULT_POLL_PERIOD, ReorgDetector};

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
///
/// Phase 1.1: shape is defined; field population happens in Phase 1.2.
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
    /// Whether the job has been fully validated.
    /// Set to `true` once `handle_declare_mining_job` returns `Success`.
    pub validated: bool,
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
/// All three handles are cloneable / `Arc`-shareable, matching how
/// `p2poolv2_node` constructs them at startup
/// (`vendor/p2poolv2/p2poolv2_node/src/main.rs`).
#[derive(Clone)]
pub struct EngineHandles {
    /// Read access to the share chain. Used to look up the current
    /// share-chain tip + validate share-block ancestry.
    pub chain: ChainStoreHandle,
    /// Production share validator. Constructed via
    /// `p2poolv2_lib::shares::validation::DefaultShareValidator::new(...)`
    /// in the binary; passed here as `Arc<dyn ShareValidator>` so tests
    /// can substitute a mock.
    pub validator: Arc<dyn ShareValidator + Send + Sync>,
    /// Bitcoin RPC backend, used for `getblocktemplate` (capture
    /// `prev_hash`/`nbits` for declared jobs) and `submit_block`
    /// (forward found blocks). The trait abstraction (vendored fork
    /// `feat/bitcoind-trait`) lets us mock bitcoind in tests.
    pub bitcoind: Arc<dyn BitcoindLike>,
}

impl std::fmt::Debug for EngineHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineHandles")
            .field("chain", &"<ChainStoreHandle>")
            .field("validator", &"<dyn ShareValidator>")
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
    /// Backend handles. `None` for Phase-1-style structural-only mode
    /// (existing tests); `Some` for Phase 2+ when the binary plumbs in
    /// real share-chain + bitcoind handles.
    handles: Option<EngineHandles>,
}

/// Default TTL for the `RecentSolutions` buffer — long enough to cover
/// reasonable share-submission latency after a `PushSolution` arrives.
pub const DEFAULT_RECENT_SOLUTIONS_TTL: Duration = Duration::from_secs(30);

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
            handles: None,
        }
    }

    /// Construct an engine with real backend handles. The trait methods
    /// will perform real share-chain validation (Phase 2.3+ wires this
    /// through). Use [`P2poolV2Engine::new`] for structural-only tests.
    pub fn with_handles(network: bitcoin::Network, handles: EngineHandles) -> Self {
        let mut engine = Self::new(network);
        engine.handles = Some(handles);
        engine
    }

    /// Whether the engine has real backend handles wired in.
    ///
    /// `false` = Phase-1-style structural-only mode (trait methods stub
    /// share-chain validation). `true` = Phase 2+ mode.
    pub fn has_handles(&self) -> bool {
        self.handles.is_some()
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
        let invalidator_handle = tokio::spawn(reorg_invalidator_loop(invalidator, cache));
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
}

impl Drop for P2poolV2Engine {
    fn drop(&mut self) {
        self.stop_reorg_watcher();
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
async fn reorg_invalidator_loop(mut rx: broadcast::Receiver<BlockHash>, cache: DeclaredJobCache) {
    loop {
        match rx.recv().await {
            Ok(new_tip) => {
                let dropped = cache.invalidate_all();
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
            validated: false,
        }
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
        use bitcoin::CompactTarget;
        use bitcoindrpc::mock::MockBitcoind;
        use p2poolv2_lib::pool_difficulty::PoolDifficulty;
        use p2poolv2_lib::shares::validation::DefaultShareValidator;
        use p2poolv2_lib::test_utils::setup_test_chain_store_handle;

        // Build the three production handles via test fixtures.
        let (chain, _tmpdir) = setup_test_chain_store_handle(false).await;
        // Anchor at regtest genesis difficulty (max-easy target ~ 1d00ffff
        // works for regtest).
        let pool_difficulty = PoolDifficulty::new(CompactTarget::from_consensus(0x207fffff), 0, 0);
        let validator: Arc<dyn ShareValidator + Send + Sync> =
            Arc::new(DefaultShareValidator::new(pool_difficulty, 1, Vec::new()));
        let bitcoind: Arc<dyn BitcoindLike> = Arc::new(MockBitcoind::default());

        let handles = EngineHandles {
            chain,
            validator,
            bitcoind,
        };
        let engine = P2poolV2Engine::with_handles(bitcoin::Network::Regtest, handles);
        assert!(engine.has_handles());
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
}
