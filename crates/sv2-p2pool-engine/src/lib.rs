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

use bitcoin::BlockHash;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

pub mod recent_solutions;
pub mod reorg_detector;

pub use reorg_detector::{DEFAULT_POLL_PERIOD, ReorgDetector};

/// Opaque request-id used to key declared-job cache entries. Mirrors
/// `BitcoinCoreIPCEngine::declared_custom_jobs`'s
/// `Arc<DashMap<RequestId, _>>` shape (see
/// `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:214`).
/// Aliased here to keep this crate independent of the upstream type while we
/// wait for the JDS wiring to land.
pub type RequestId = u32;

/// Minimal stand-in for a cached declared job. The Phase 1 invalidation
/// rule is "drop everything on any tip change" so the value type is opaque
/// to the cache machinery (`()`) — the real `DeclaredCustomJob` shape lives
/// upstream in `BitcoinCoreIPCEngine` and will be mirrored here once the
/// full engine impl lands.
pub type DeclaredJob = ();

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

/// Phase 1 engine surface.
///
/// Holds the cached declared-jobs map and a handle to the (optional)
/// reorg-watcher background task. The full `JobValidationEngine` impl will
/// be added once the JDS wiring lands.
pub struct P2poolV2Engine {
    declared_jobs: DeclaredJobCache,
    /// Active watcher task; aborted on drop.
    reorg_watcher: Option<tokio::task::JoinHandle<()>>,
}

impl P2poolV2Engine {
    pub fn new() -> Self {
        Self {
            declared_jobs: DeclaredJobCache::new(),
            reorg_watcher: None,
        }
    }

    /// Access the declared-jobs cache.
    pub fn declared_jobs(&self) -> &DeclaredJobCache {
        &self.declared_jobs
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
    fn default() -> Self {
        Self::new()
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

    #[test]
    fn declared_job_cache_invalidate_all_drops_everything() {
        let cache = DeclaredJobCache::new();
        cache.insert(1, ());
        cache.insert(2, ());
        cache.insert(3, ());
        assert_eq!(cache.len(), 3);

        let dropped = cache.invalidate_all();
        assert_eq!(dropped, 3);
        assert!(cache.is_empty());

        // Idempotent — second invalidate is a no-op.
        let dropped = cache.invalidate_all();
        assert_eq!(dropped, 0);
        assert!(cache.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn engine_invalidates_cache_on_tip_change() {
        let mut engine = P2poolV2Engine::new();
        engine.declared_jobs().insert(1, ());
        engine.declared_jobs().insert(2, ());
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
