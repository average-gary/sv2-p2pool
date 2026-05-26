//! TTL-buffered map of share-hash -> block-finder credit.
//!
//! # Why this exists
//!
//! The reference SV2 JDS impl handles `PushSolution` fire-and-forget
//! (see `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:639-653`).
//! In a p2pool world, `PushSolution` (JDP) and the corresponding
//! `SubmitSharesExtended` (Mining Protocol) travel over **separate** SV2
//! channels and may arrive in either order. Whichever message arrives second
//! is responsible for crediting the share with block-finder status. If we
//! drop credit when `PushSolution` lands first, the block-finder bonus is
//! lost.
//!
//! See issue [#5](https://github.com/average-gary/sv2-p2pool/issues/5) and
//! the wiki "share-accounting-mapping" topic, **Open Question 7
//! (PushSolution ordering)**:
//! `~/wiki/topics/sv2-p2pool-integration/wiki/topics/share-accounting-mapping.md`.
//!
//! # Design
//!
//! A small, lock-free, TTL-bounded map keyed by share-hash. Two operations:
//!
//! - [`RecentSolutions::record`]: called from `handle_push_solution` to
//!   stash the (share_hash -> block_hash) edge so a later
//!   `SubmitSharesExtended` can pick it up.
//! - [`RecentSolutions::take`]: called from the share-submission path; if
//!   the share-hash matches a recently-recorded solution, the block hash
//!   is returned **and removed** so the block-finder credit can only be
//!   claimed once.
//!
//! Entries older than `ttl` are evicted by [`RecentSolutions::sweep`], which
//! the engine's housekeeping task should call periodically.
//!
//! The reverse race — `SubmitSharesExtended` arriving first — is handled
//! one level up by the share-chain submission path, which records the
//! share unconditionally and consults `RecentSolutions::take` for any
//! pending block-finder credit.
//!
//! # Type choices
//!
//! Both share-hash and block-hash are 32-byte SHA-256d values; in
//! `p2poolv2_lib` shares are keyed by `bitcoin::BlockHash` (verified at
//! `vendor/p2poolv2/p2poolv2_lib/src/shares/chain/chain_store_handle.rs:173`).
//! We re-export those names here so call-sites read naturally even though
//! the Rust types are identical.

use std::time::{Duration, Instant};

use bitcoin::BlockHash;
use dashmap::DashMap;

/// p2poolv2 keys shares by `BlockHash` (32-byte double-SHA-256). We alias
/// to keep call-sites readable; the underlying type is the same so passing
/// a `ShareHash` where a `BlockHash` is expected is a no-op.
pub type ShareHash = BlockHash;

/// TTL-buffered map of share-hash -> (recorded_at, block_hash).
///
/// Cheap to clone via `Arc` because the inner `DashMap` is already shared.
/// All operations are lock-free / per-bucket-locked.
#[derive(Debug)]
pub struct RecentSolutions {
    inner: DashMap<ShareHash, (Instant, BlockHash)>,
    ttl: Duration,
}

impl RecentSolutions {
    /// Create a new buffer that retains entries for `ttl`.
    ///
    /// A reasonable default for the p2pool use-case is 30s — long enough to
    /// cover any plausible clock-skew and queue delay between `PushSolution`
    /// (JDP) and the matching `SubmitSharesExtended` (Mining Protocol),
    /// short enough that a bounded-memory invariant holds even under
    /// adversarial input.
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: DashMap::new(),
            ttl,
        }
    }

    /// Record that `share_hash` is the share-hash of a found block whose
    /// Bitcoin block-hash is `block_hash`.
    ///
    /// If `share_hash` is already present (duplicate `PushSolution`), the
    /// recording timestamp is refreshed and the most recent `block_hash` wins.
    /// Duplicates with the same `block_hash` are idempotent.
    pub fn record(&self, share_hash: ShareHash, block_hash: BlockHash) {
        self.inner.insert(share_hash, (Instant::now(), block_hash));
    }

    /// If a non-expired entry exists for `share_hash`, **remove and return**
    /// the recorded block-hash. Otherwise return `None`.
    ///
    /// "Take" semantics ensure block-finder credit is claimed at most once
    /// per share. Expired entries are treated as absent and proactively
    /// evicted.
    pub fn take(&self, share_hash: &ShareHash) -> Option<BlockHash> {
        let removed = self.inner.remove(share_hash)?;
        let (_, (recorded_at, block_hash)) = removed;
        if recorded_at.elapsed() > self.ttl {
            None
        } else {
            Some(block_hash)
        }
    }

    /// Drop entries whose `recorded_at` is older than `ttl`.
    ///
    /// Intended to be called periodically (e.g. once per second) from the
    /// engine's housekeeping task. Independent of `take` — `take` already
    /// treats expired entries as absent — but `sweep` is needed to bound
    /// memory when shares are recorded but never queried.
    pub fn sweep(&self) {
        let ttl = self.ttl;
        self.inner
            .retain(|_, (recorded_at, _)| recorded_at.elapsed() <= ttl);
    }

    /// Number of currently-buffered entries (including any not-yet-swept
    /// expired ones). Mostly useful for tests and metrics.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::thread::sleep;

    use bitcoin::hashes::Hash;
    use proptest::collection::vec;
    use proptest::prelude::*;

    use super::*;

    /// Build a deterministic `BlockHash` from a u64 seed. Test-only helper.
    fn hash_from_u64(seed: u64) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        BlockHash::from_byte_array(bytes)
    }

    #[test]
    fn record_then_take_returns_block_hash_once() {
        let buf = RecentSolutions::new(Duration::from_secs(30));
        let share = hash_from_u64(1);
        let block = hash_from_u64(101);

        buf.record(share, block);
        assert_eq!(buf.take(&share), Some(block));
        // Second take must not double-credit.
        assert_eq!(buf.take(&share), None);
    }

    #[test]
    fn take_without_record_is_none() {
        let buf = RecentSolutions::new(Duration::from_secs(30));
        assert_eq!(buf.take(&hash_from_u64(42)), None);
    }

    #[test]
    fn sweep_drops_expired_entries() {
        let buf = RecentSolutions::new(Duration::from_millis(20));
        buf.record(hash_from_u64(1), hash_from_u64(101));
        buf.record(hash_from_u64(2), hash_from_u64(102));
        assert_eq!(buf.len(), 2);

        sleep(Duration::from_millis(40));
        buf.sweep();
        assert!(buf.is_empty());
    }

    #[test]
    fn take_after_ttl_is_none() {
        let buf = RecentSolutions::new(Duration::from_millis(20));
        let share = hash_from_u64(1);
        buf.record(share, hash_from_u64(101));
        sleep(Duration::from_millis(40));
        assert_eq!(buf.take(&share), None);
    }

    #[test]
    fn duplicate_record_keeps_latest_block_hash() {
        let buf = RecentSolutions::new(Duration::from_secs(30));
        let share = hash_from_u64(1);
        buf.record(share, hash_from_u64(101));
        buf.record(share, hash_from_u64(202));
        assert_eq!(buf.take(&share), Some(hash_from_u64(202)));
    }

    #[derive(Clone, Debug)]
    enum Op {
        Record { share_id: u32, block_id: u32 },
        Take { share_id: u32 },
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        // Constrain the share-id space to a small range so collisions and
        // taking-before-recording happen often enough to exercise edge cases.
        prop_oneof![
            (0u32..16, 0u32..1024)
                .prop_map(|(share_id, block_id)| Op::Record { share_id, block_id }),
            (0u32..16).prop_map(|share_id| Op::Take { share_id }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1024,
            ..ProptestConfig::default()
        })]

        /// Random ordering of record/take never loses block-finder credit.
        ///
        /// Invariants checked:
        /// 1. `take` only returns block-hashes that were `record`ed for the
        ///    same share-hash. (No fabricated credit.)
        /// 2. Each `record(share, block)` can be claimed by **at most one**
        ///    successful `take(share)` (no double-credit) — modulo overwrites,
        ///    where a later record for the same share replaces the earlier one.
        /// 3. After replaying the trace, calling `take` on every share that
        ///    was ever recorded but never successfully taken yields exactly
        ///    the latest still-buffered block-hash for that share. Together
        ///    with (1) and (2) this proves no credit is lost.
        #[test]
        fn random_interleaving_preserves_block_finder_credit(
            ops in vec(op_strategy(), 1..256)
        ) {
            // TTL well above any wall-clock spent in this test so expiration
            // never fires. We're testing ordering semantics, not GC.
            let buf = RecentSolutions::new(Duration::from_secs(3600));

            // Model state: for each share, the currently-buffered block_id
            // (None if no live record exists). Tracks what the buffer should
            // hold under the same operation sequence.
            let mut model: HashMap<u32, u32> = HashMap::new();
            // All (share_id, block_id) pairs that were credited via `take`.
            let mut credited: Vec<(u32, u32)> = Vec::new();
            // All (share_id, block_id) pairs that were ever recorded.
            let mut recorded: Vec<(u32, u32)> = Vec::new();

            for op in &ops {
                match *op {
                    Op::Record { share_id, block_id } => {
                        recorded.push((share_id, block_id));
                        buf.record(hash_from_u64(share_id as u64),
                                   hash_from_u64(block_id as u64));
                        // Latest record wins per `record` doc.
                        model.insert(share_id, block_id);
                    }
                    Op::Take { share_id } => {
                        let got = buf.take(&hash_from_u64(share_id as u64));
                        match (got, model.remove(&share_id)) {
                            (Some(got_block), Some(expected_block)) => {
                                prop_assert_eq!(
                                    got_block,
                                    hash_from_u64(expected_block as u64),
                                    "take returned wrong block_hash for share {}",
                                    share_id
                                );
                                credited.push((share_id, expected_block));
                            }
                            (None, None) => {
                                // No live record, nothing to credit. OK.
                            }
                            (Some(got_block), None) => {
                                prop_assert!(
                                    false,
                                    "take returned {:?} for share {} which had no live record",
                                    got_block, share_id
                                );
                            }
                            (None, Some(expected_block)) => {
                                prop_assert!(
                                    false,
                                    "take returned None for share {} which should have credited block {}",
                                    share_id, expected_block
                                );
                            }
                        }
                    }
                }
            }

            // Invariant 1: every credited (share, block) pair was previously recorded.
            let recorded_set: HashSet<(u32, u32)> = recorded.iter().copied().collect();
            for pair in &credited {
                prop_assert!(
                    recorded_set.contains(pair),
                    "credited pair {:?} was never recorded",
                    pair
                );
            }

            // Invariant 2: no double-credit (no two takes claim the same record).
            // A record can be overwritten by a later record before being taken,
            // so we deduplicate credited pairs and check that no credited pair
            // appears more times than it was recorded.
            let mut recorded_count: HashMap<(u32, u32), usize> = HashMap::new();
            for pair in &recorded {
                *recorded_count.entry(*pair).or_default() += 1;
            }
            let mut credited_count: HashMap<(u32, u32), usize> = HashMap::new();
            for pair in &credited {
                *credited_count.entry(*pair).or_default() += 1;
            }
            for (pair, c) in &credited_count {
                prop_assert!(
                    recorded_count.get(pair).copied().unwrap_or(0) >= *c,
                    "pair {:?} credited {} times but only recorded {} times",
                    pair,
                    c,
                    recorded_count.get(pair).copied().unwrap_or(0)
                );
            }

            // Invariant 3 (no-loss): drain every share still in the model and
            // confirm the buffer hands back the expected block. Combined with
            // (1) and (2) this proves credit survives any interleaving.
            for (share_id, expected_block) in model {
                let got = buf.take(&hash_from_u64(share_id as u64));
                prop_assert_eq!(
                    got,
                    Some(hash_from_u64(expected_block as u64)),
                    "drain phase: lost block-finder credit for share {} (expected block {})",
                    share_id,
                    expected_block
                );
            }
        }
    }
}
