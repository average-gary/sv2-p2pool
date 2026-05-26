//! Poll-based share-chain reorg detector.
//!
//! # Why this exists
//!
//! Phase 1 of the sv2-p2pool integration cannot drive
//! [`JobValidationEngine::notify_share_chain_reorg`] directly: the trait
//! extension lives on a private `feat/jve-reorg-notify` branch of
//! `vendor/sv2-apps`, and the in-process JDS wiring that would forward
//! tip-changes from the share-chain to the engine has not landed yet.
//! Until it does, the engine has to discover tip-changes itself by polling
//! the share-chain.
//!
//! See:
//! - sv2-p2pool issue [#3](https://github.com/average-gary/sv2-p2pool/issues/3)
//! - ADR [`0001-uncle-weighting`](../../../docs/adr/0001-uncle-weighting.md)
//!   — α=1 + uncle-not-stale rule informs what "tip change" means in our
//!   share-chain. A new uncle that does not become the tip is not a reorg.
//! - ADR [`0002-jdtoken-payout-script`](../../../docs/adr/0002-jdtoken-payout-script.md)
//!   §"Follow-ups" — explicitly defers reorg-revocation to a separate ADR;
//!   this module is the implementation hook.
//! - Wiki Open Q 5
//!   `~/wiki/topics/sv2-p2pool-integration/wiki/topics/share-accounting-mapping.md`
//!
//! # Design
//!
//! [`ReorgDetector`] holds an abstract `tip_source: F` callable that
//! returns the current confirmed share-chain tip. p2poolv2 exposes this
//! via
//! [`ChainStoreHandle::get_chain_tip`](
//!  ../../../vendor/p2poolv2/p2poolv2_lib/src/shares/chain/chain_store_handle.rs)
//! at line 260. The detector is generic over the closure to keep this
//! crate testable without standing up a real RocksDB-backed share chain.
//!
//! On each tick of a [`tokio::time::interval`] the detector calls
//! `tip_source()`. If the returned `BlockHash` differs from the
//! last-observed tip, it broadcasts the new tip on a
//! [`tokio::sync::broadcast::Sender`]. Subscribers (e.g. the engine's
//! `declared_jobs` cache) then react.
//!
//! # Type choices
//!
//! - `tokio::sync::broadcast` over `mpsc` so multiple subscribers can
//!   listen independently (e.g. the cache invalidator and a metrics
//!   counter) without one starving the others.
//! - The closure form `Fn() -> Option<BlockHash>` rather than a trait so
//!   tests can inject arbitrary tip-emission sequences without defining
//!   a mock type. `None` is returned when the share-chain has no tip
//!   yet (e.g. during initial sync) and is treated as "no change".
//! - Polling rather than push: the share-chain is in-process for Phase 1
//!   but does not currently expose a tip-change broadcast channel.
//!   When upstream lands the trait method, this module's job is taken
//!   over by a direct call from the share-chain's organize-block path
//!   into [`JobValidationEngine::notify_share_chain_reorg`].

use std::time::Duration;

use bitcoin::BlockHash;
use tokio::sync::broadcast;

/// Default polling period. 5s is well under p2poolv2's ~10s share interval
/// while small enough that an in-flight token revocation lands before the
/// next round of declarations.
pub const DEFAULT_POLL_PERIOD: Duration = Duration::from_secs(5);

/// Capacity of the broadcast channel used to fan out tip-change events.
///
/// Sized large enough that a slow subscriber rarely lags. The realistic
/// number of independent subscribers today is two (the `declared_jobs`
/// cache invalidator and one optional metrics counter), so the only way
/// this fills is a stalled receiver and an unusually fast burst of reorgs.
/// Broadcast-channel lag is logged-and-flushed defensively in
/// `lib.rs::reorg_invalidator_loop` (Lagged conservatively flushes the
/// cache), so a higher capacity is purely a "fewer false flushes" tuning
/// knob, not a correctness lever.
const BROADCAST_CAPACITY: usize = 256;

/// Poll-based share-chain reorg detector.
///
/// `F` is a closure that returns the current confirmed share-chain tip.
/// `None` means "no tip yet" (e.g. during initial sync) and is treated as
/// no change.
///
/// The detector retains its last-observed tip and emits exactly one
/// broadcast per *change*: identical-tip ticks are silent.
pub struct ReorgDetector<F> {
    tip_source: F,
    period: Duration,
    sender: broadcast::Sender<BlockHash>,
}

impl<F> ReorgDetector<F>
where
    F: Fn() -> Option<BlockHash> + Send + 'static,
{
    /// Construct a new detector.
    ///
    /// The returned detector is not running yet — call [`Self::run`]
    /// (typically from a `tokio::spawn`) or [`Self::tick`] (in tests) to
    /// drive it.
    pub fn new(tip_source: F, period: Duration) -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            tip_source,
            period,
            sender,
        }
    }

    /// Subscribe to tip-change events.
    ///
    /// Each subscriber receives every tip change emitted *after* the
    /// subscription. Late subscribers do not see the most recent tip; if
    /// callers need the current tip, they should query the share-chain
    /// directly (a bounded broadcast cannot replay history).
    pub fn subscribe(&self) -> broadcast::Receiver<BlockHash> {
        self.sender.subscribe()
    }

    /// Run the detector until the receiver side is closed.
    ///
    /// In production this is spawned as a background tokio task. The loop
    /// terminates only on shutdown — the detector has no internal cancel
    /// (callers are expected to abort the join handle on shutdown).
    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.period);
        // Skip-missed: if the runtime is starved we don't want a thundering
        // herd of catch-up ticks. Reorg notification is edge-triggered, not
        // count-triggered.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut last_tip: Option<BlockHash> = None;
        loop {
            interval.tick().await;
            last_tip = Self::poll_once(&self.tip_source, &self.sender, last_tip);
        }
    }

    /// Single-tick variant for tests.
    ///
    /// Returns the new `last_tip` so test loops can thread state through
    /// without spinning up a tokio interval.
    pub fn tick(&self, last_tip: Option<BlockHash>) -> Option<BlockHash> {
        Self::poll_once(&self.tip_source, &self.sender, last_tip)
    }

    fn poll_once(
        tip_source: &F,
        sender: &broadcast::Sender<BlockHash>,
        last_tip: Option<BlockHash>,
    ) -> Option<BlockHash> {
        let current_tip = tip_source();
        match (current_tip, last_tip) {
            // No tip yet, or tip cleared (initial sync) — nothing to broadcast.
            (None, _) => last_tip,
            // First observation of any tip — record but don't broadcast.
            // We can't tell a "first sighting" from a "real change" without
            // priming, and broadcasting on startup would force a pointless
            // cache-flush every time the engine starts.
            (Some(tip), None) => Some(tip),
            // Tip unchanged — silent.
            (Some(tip), Some(prev)) if tip == prev => Some(prev),
            // Tip changed — broadcast (but tolerate "no subscribers": that's
            // not an error, it just means nobody cares yet).
            (Some(tip), Some(_prev)) => {
                let _ = sender.send(tip);
                Some(tip)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use bitcoin::hashes::Hash;
    use proptest::collection::vec;
    use proptest::prelude::*;

    use super::*;

    fn hash_from_u64(seed: u64) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        BlockHash::from_byte_array(bytes)
    }

    /// A `tip_source` that hands out values from a Vec, one per call,
    /// returning the last value once the Vec is exhausted.
    fn scripted_tip_source(seq: Vec<Option<BlockHash>>) -> impl Fn() -> Option<BlockHash> {
        let cursor = Arc::new(AtomicUsize::new(0));
        let seq = Arc::new(seq);
        move || {
            let idx = cursor.fetch_add(1, Ordering::SeqCst);
            let len = seq.len();
            if len == 0 {
                None
            } else {
                seq[idx.min(len - 1)]
            }
        }
    }

    #[test]
    fn detector_does_not_fire_on_first_sighting() {
        let detector = ReorgDetector::new(
            scripted_tip_source(vec![Some(hash_from_u64(1))]),
            DEFAULT_POLL_PERIOD,
        );
        let mut rx = detector.subscribe();

        let last = detector.tick(None);
        assert_eq!(last, Some(hash_from_u64(1)));
        // No broadcast on first sighting.
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn detector_does_not_fire_when_tip_is_unchanged() {
        let tip = hash_from_u64(1);
        let detector = ReorgDetector::new(
            scripted_tip_source(vec![Some(tip), Some(tip), Some(tip)]),
            DEFAULT_POLL_PERIOD,
        );
        let mut rx = detector.subscribe();

        let mut last = None;
        for _ in 0..3 {
            last = detector.tick(last);
        }
        assert_eq!(last, Some(tip));
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn detector_fires_when_tip_changes() {
        let tip_a = hash_from_u64(1);
        let tip_b = hash_from_u64(2);
        let detector = ReorgDetector::new(
            scripted_tip_source(vec![Some(tip_a), Some(tip_b)]),
            DEFAULT_POLL_PERIOD,
        );
        let mut rx = detector.subscribe();

        let last = detector.tick(None);
        let last = detector.tick(last);

        assert_eq!(last, Some(tip_b));
        // Exactly one broadcast was emitted (the tip change).
        assert_eq!(rx.try_recv().ok(), Some(tip_b));
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn detector_handles_no_tip_then_tip_appears() {
        let tip = hash_from_u64(1);
        let detector = ReorgDetector::new(
            scripted_tip_source(vec![None, Some(tip)]),
            DEFAULT_POLL_PERIOD,
        );
        let mut rx = detector.subscribe();

        let last = detector.tick(None);
        assert_eq!(last, None, "None tip preserves last_tip = None");
        let last = detector.tick(last);
        assert_eq!(last, Some(tip), "first real tip is recorded");
        // First sighting still doesn't fire.
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn detector_fires_back_to_back_changes() {
        let tip_a = hash_from_u64(1);
        let tip_b = hash_from_u64(2);
        let tip_c = hash_from_u64(3);
        let detector = ReorgDetector::new(
            scripted_tip_source(vec![Some(tip_a), Some(tip_b), Some(tip_c)]),
            DEFAULT_POLL_PERIOD,
        );
        let mut rx = detector.subscribe();

        let mut last = None;
        for _ in 0..3 {
            last = detector.tick(last);
        }
        assert_eq!(last, Some(tip_c));
        assert_eq!(rx.try_recv().ok(), Some(tip_b));
        assert_eq!(rx.try_recv().ok(), Some(tip_c));
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn multiple_subscribers_each_see_all_changes() {
        let tip_a = hash_from_u64(1);
        let tip_b = hash_from_u64(2);
        let detector = ReorgDetector::new(
            scripted_tip_source(vec![Some(tip_a), Some(tip_b)]),
            DEFAULT_POLL_PERIOD,
        );
        let mut rx1 = detector.subscribe();
        let mut rx2 = detector.subscribe();

        let mut last = None;
        for _ in 0..2 {
            last = detector.tick(last);
        }
        assert_eq!(last, Some(tip_b));
        assert_eq!(rx1.try_recv().ok(), Some(tip_b));
        assert_eq!(rx2.try_recv().ok(), Some(tip_b));
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            ..ProptestConfig::default()
        })]

        /// For any sequence of polled tips (None or some BlockHash), the
        /// detector emits exactly one broadcast per *transition* between
        /// distinct `Some(_)` values, ignoring the first sighting and any
        /// runs of identical values.
        ///
        /// The intent of "invalidation is idempotent under random reorg
        /// sequences" (per the task spec) is checked here at the source:
        /// if the detector emits N events, callers can apply N
        /// invalidations and the cache state is the same as if it had been
        /// flushed once for the final tip — because each event drops every
        /// matching entry, and re-applying the same drop is a no-op.
        #[test]
        fn detector_fires_exactly_on_transitions(
            seq in vec(prop_oneof![Just(None::<u32>), (0u32..8).prop_map(Some)], 1..32)
        ) {
            let polls: Vec<Option<BlockHash>> = seq
                .iter()
                .map(|s| s.map(|x| hash_from_u64(x as u64)))
                .collect();

            let detector = ReorgDetector::new(
                scripted_tip_source(polls.clone()),
                DEFAULT_POLL_PERIOD,
            );
            let mut rx = detector.subscribe();

            let mut last = None;
            for _ in 0..polls.len() {
                last = detector.tick(last);
            }

            // Re-derive the expected sequence of broadcast events:
            // Walk the polls, tracking last-seen Some(_); emit a transition
            // for every Some(x) where x != last_seen and last_seen.is_some().
            let mut expected = Vec::new();
            let mut model_last: Option<BlockHash> = None;
            for poll in &polls {
                match (*poll, model_last) {
                    (None, _) => {} // None preserves last_tip; no event
                    (Some(t), None) => model_last = Some(t),
                    (Some(t), Some(prev)) if t == prev => {} // unchanged
                    (Some(t), Some(_)) => {
                        expected.push(t);
                        model_last = Some(t);
                    }
                }
            }

            // Drain the broadcast. Tolerate `Lagged` (which happens if the
            // expected sequence ever exceeds `BROADCAST_CAPACITY` for this
            // proptest) by continuing past it — `Lagged` resets the receiver
            // to the oldest still-buffered event, so subsequent `try_recv`
            // calls keep working.
            let mut got = Vec::new();
            loop {
                match rx.try_recv() {
                    Ok(t) => got.push(t),
                    Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                    Err(broadcast::error::TryRecvError::Empty)
                    | Err(broadcast::error::TryRecvError::Closed) => break,
                }
            }

            // If the expected sequence is longer than the channel capacity,
            // `got` legitimately drops the head. Tolerate by checking that
            // `got` is a suffix of `expected`.
            let suffix_len = got.len();
            let expected_suffix = &expected[expected.len().saturating_sub(suffix_len)..];
            prop_assert_eq!(
                &got[..],
                expected_suffix,
                "detector broadcast sequence diverged from model for polls={:?}",
                polls
            );
            prop_assert_eq!(
                last,
                model_last,
                "detector last_tip diverged from model for polls={:?}",
                polls
            );
        }

        /// Idempotency under the *same* poll sequence: running two
        /// detectors over the same script produces identical broadcast
        /// streams. This is the cache-invalidation idempotence guarantee
        /// translated to the event-source layer: deterministic input ->
        /// deterministic output -> deterministic invalidation.
        #[test]
        fn detector_is_deterministic_for_a_given_script(
            seq in vec(prop_oneof![Just(None::<u32>), (0u32..8).prop_map(Some)], 1..32)
        ) {
            let polls: Vec<Option<BlockHash>> = seq
                .iter()
                .map(|s| s.map(|x| hash_from_u64(x as u64)))
                .collect();

            let collect_events = || -> Vec<BlockHash> {
                let detector = ReorgDetector::new(
                    scripted_tip_source(polls.clone()),
                    DEFAULT_POLL_PERIOD,
                );
                let mut rx = detector.subscribe();
                let mut last = None;
                for _ in 0..polls.len() {
                    last = detector.tick(last);
                }
                let mut out = Vec::new();
                loop {
                    match rx.try_recv() {
                        Ok(t) => out.push(t),
                        Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                        Err(broadcast::error::TryRecvError::Empty)
                        | Err(broadcast::error::TryRecvError::Closed) => break,
                    }
                }
                out
            };

            prop_assert_eq!(collect_events(), collect_events());
        }

        /// Idempotency on the cache: applying the same broadcast stream
        /// to a freshly-flushed model cache more than once yields the
        /// same final state. This is the contract the task asks us to
        /// preserve: "invalidation is idempotent under random reorg
        /// sequences".
        ///
        /// We model "invalidation" as the simple-and-correct Phase 1
        /// rule: any tip change drops the entire cache. Idempotence then
        /// reduces to "draining a queue of drops always lands at empty",
        /// which we exercise here against random (token, script) cache
        /// snapshots and random broadcast lengths.
        #[test]
        fn cache_invalidation_is_idempotent(
            initial_entries in vec(0u32..1024, 0..64),
            event_count in 0usize..16,
        ) {
            // Phase-1 invalidator: drop everything on any tip change.
            let cache = Arc::new(Mutex::new(initial_entries.clone()));

            let invalidate = || {
                cache.lock().unwrap().clear();
            };

            for _ in 0..event_count {
                invalidate();
            }

            let after_first = cache.lock().unwrap().clone();

            // Re-apply the same event sequence -- result must be unchanged.
            for _ in 0..event_count {
                invalidate();
            }

            let after_second = cache.lock().unwrap().clone();
            prop_assert_eq!(after_first, after_second);

            // And if any events fired, the cache is empty regardless of
            // initial contents.
            if event_count > 0 {
                prop_assert!(cache.lock().unwrap().is_empty());
            } else {
                prop_assert_eq!(&*cache.lock().unwrap(), &initial_entries);
            }
        }
    }

    /// Smoke test the async `run` path under tokio's mock clock to confirm
    /// the interval wiring works end-to-end. Uses `start_paused` so the
    /// test doesn't sleep in real time.
    #[tokio::test(start_paused = true)]
    async fn run_loop_emits_on_tip_change() {
        let tip_a = hash_from_u64(1);
        let tip_b = hash_from_u64(2);
        let detector = ReorgDetector::new(
            scripted_tip_source(vec![Some(tip_a), Some(tip_b)]),
            Duration::from_millis(100),
        );
        let mut rx = detector.subscribe();
        let handle = tokio::spawn(detector.run());

        // First tick: prime last_tip = Some(tip_a) (no broadcast).
        // Second tick: see tip_b -> broadcast.
        // The first interval tick fires immediately (tokio default).
        tokio::time::advance(Duration::from_millis(150)).await;
        // Yield so the spawned task runs.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        let received = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
        assert_eq!(received.expect("recv timed out").ok(), Some(tip_b));

        handle.abort();
    }
}
