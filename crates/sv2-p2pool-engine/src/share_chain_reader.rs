//! `ShareChainReader` — the engine-facing trait for share-chain reads.
//!
//! Phase 2-B Track A introduces this trait as the seam between the
//! [`P2poolV2Engine`](crate::P2poolV2Engine) and whatever backend
//! actually owns the share chain. ADR 0011 documents the design.
//!
//! Three implementations exist:
//! - [`mock::MockShareChain`] — `#[cfg(test)]` only. Configurable
//!   in-memory fake used by the engine's unit tests.
//! - [`InProcessChain`] — gated on the `in-process-chain` Cargo feature.
//!   Wraps a real `p2poolv2_lib::ChainStoreHandle`. This is the only
//!   path that links the AGPL-licensed `p2poolv2_lib` into the engine
//!   crate; ADR 0010 / 0011 plan to replace it with an `IpcChain`
//!   actor over capnp IPC in the next stage. The feature exists so a
//!   downstream binary can opt out by building with
//!   `--no-default-features` once the IPC backend lands.
//! - `IpcChain` — not built in this stage. ADR 0011 step 6 lands the
//!   `Send`-safe actor wrapping the `!Send` capnp client.
//!
//! ## Async signature, no `async-trait` macro
//!
//! ADR 0011 mandates **no** `async-trait` crate. Native async-fn-in-trait
//! (AFIT) with `-> impl Future + Send` returns is not dyn-compatible in
//! stable Rust today, but [`EngineHandles`](crate::EngineHandles) needs
//! `Arc<dyn ShareChainReader>` for the engine to be backend-agnostic at
//! runtime. We square that circle by writing the trait's async methods
//! explicitly as `Pin<Box<dyn Future + Send + '_>>`-returning fns. This
//! is the same desugaring `#[async_trait]` would produce, just done by
//! hand at the trait surface so we control the dependency footprint
//! (no extra crate, no proc-macro on the build graph, no behavior
//! change). Each impl writes a vanilla `async fn` body and wraps it in
//! `Box::pin(async move { ... })`. The reorg ancestry walk still hits
//! 100 sequential `Box::pin`s in the worst case; ADR 0011 § Negative
//! covers the latency budget (10-50 ms p99 over UDS, accepted).
//!
//! ## Error model
//!
//! All transport-level failures funnel through
//! [`sv2_p2pool_ipc::IpcClientError`]. ADR 0011 explicitly keeps a
//! single error type across the seam — no `ShareChainError` is
//! introduced. The `MockShareChain` and `InProcessChain` impls map
//! their internal failure modes onto the same enum.
//!
//! ## Sync vs async
//!
//! [`ShareChainReader::network`] is sync because the value is captured
//! at construction (the IPC client receives it once via
//! `getNetwork @6`; `InProcessChain` reads it once from
//! `ChainStoreHandle::network()`). [`ShareChainReader::subscribe_tip`]
//! is sync because it just hands back a fresh
//! [`tokio::sync::broadcast::Receiver`] cloned from a `Sender`
//! retained on the impl. The other three methods are async because
//! the `IpcChain` impl will await UDS round-trips.

use std::future::Future;
use std::pin::Pin;

use bitcoin::BlockHash;
use sv2_p2pool_ipc::IpcClientError;
use tokio::sync::broadcast;

pub use sv2_p2pool_ipc::{ShareHeaderLookup, ShareHeaderRead};

/// Boxed future alias for the trait's async methods. Mirrors what
/// `#[async_trait]` would produce but keeps the type visible at the
/// trait declaration site (so consumers can `match` on the
/// signature without macro expansion noise). `'a` borrows from
/// `&'a self`.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Read-side trait the engine consumes for share-chain access.
///
/// See the module-level docs for the design rationale (ADR 0011).
///
/// # `Send + Sync`
///
/// The bound is required because the engine stores
/// `Arc<dyn ShareChainReader>` in [`EngineHandles`](crate::EngineHandles)
/// and the engine itself crosses await points on a multi-threaded
/// runtime. The `IpcChain` actor in the next stage achieves this by
/// owning the `!Send` capnp client on a dedicated `LocalSet` thread and
/// exposing only an mpsc-backed handle.
pub trait ShareChainReader: Send + Sync {
    /// Read the confirmed share-chain tip.
    ///
    /// `Ok(None)` means "daemon has not yet completed genesis setup"
    /// (the IPC `ChainTipResult::Uninitialised` variant; in-process
    /// it's the `StoreError::NotFound` arm). Real errors are
    /// transport failures.
    fn get_chain_tip(&self) -> BoxFuture<'_, Result<Option<BlockHash>, IpcClientError>>;

    /// Look up a share header by its hash.
    ///
    /// Returns the discriminated [`ShareHeaderLookup`] enum so the
    /// engine can distinguish "found", "not found", and "genesis
    /// sentinel reached" without overloading `IpcClientError`. ADR
    /// 0011 § Decision § "Schema additions" documents the wire
    /// shape.
    ///
    /// Note: `share_hash` is dereferenced into the future at call
    /// time so the future does not borrow the hash. This matches
    /// upstream `ChainStoreHandle::get_share_header(&BlockHash)` and
    /// keeps the reorg-walk loop free of borrow gymnastics.
    fn get_share_header(
        &self,
        share_hash: &BlockHash,
    ) -> BoxFuture<'_, Result<ShareHeaderLookup, IpcClientError>>;

    /// Read the confirmed share-chain tip height.
    ///
    /// `Ok(None)` means "daemon has not yet completed genesis setup"
    /// (mirrors [`Self::get_chain_tip`] semantics).
    fn get_tip_height(&self) -> BoxFuture<'_, Result<Option<u32>, IpcClientError>>;

    /// Bitcoin network the daemon was configured with.
    ///
    /// **Sync**: the value is captured once at construction (via
    /// `getNetwork @6` in the IPC case; via `ChainStoreHandle::network()`
    /// in the in-process case). If upstream daemon ever supports
    /// network hot-swap (unlikely), this would lie until reconnect —
    /// not relevant to the current shape per ADR 0011.
    fn network(&self) -> bitcoin::Network;

    /// Subscribe to push-driven tip-change notifications.
    ///
    /// Returns a fresh [`broadcast::Receiver`]. The `IpcChain` actor
    /// in the next stage will fan out the existing capnp
    /// `subscribeChainTip @2` callback into a broadcast channel; the
    /// in-process and mock impls drive the channel from their own
    /// state. ADR 0011 § Decision § "Reorg watcher migration"
    /// documents the rewire.
    fn subscribe_tip(&self) -> broadcast::Receiver<BlockHash>;
}

// -----------------------------------------------------------------------
// In-process backend (transitional; AGPL-tainted via p2poolv2_lib).
// -----------------------------------------------------------------------

#[cfg(feature = "in-process-chain")]
mod in_process {
    use super::*;
    use bitcoin::hashes::Hash as _;
    use p2poolv2_lib::shares::chain::chain_store_handle::ChainStoreHandle;
    use p2poolv2_lib::store::writer::StoreError;

    /// `ShareChainReader` impl backed by an in-process
    /// `ChainStoreHandle`. **Transitional**: this is the only place the
    /// engine still links the AGPL-licensed `p2poolv2_lib`. The next
    /// stage (ADR 0011 step 6) replaces this with an IPC-backed
    /// actor; building with `--no-default-features` will then hide
    /// this entirely.
    ///
    /// Mirrors the wire-level error semantics that the daemon's
    /// `ChainReadAdapter` uses (see
    /// `vendor/p2poolv2/p2poolv2_node/src/ipc_chain.rs`):
    /// - `ChainStoreHandle::get_chain_tip` returning `NotFound` →
    ///   `Ok(None)` (genesis not initialised).
    /// - Other `StoreError`s → `IpcClientError::Capnp(..)` carrying
    ///   the formatted message. Reusing the `Capnp` variant keeps
    ///   the error type uniform across the seam without requiring
    ///   a new variant — see ADR 0011 § Decision § "error model".
    /// - `get_share_header` returning `NotFound` →
    ///   [`ShareHeaderLookup::NotFound`].
    /// - All-zero share-hash query → [`ShareHeaderLookup::Genesis`]
    ///   (sentinel mirrored from the daemon's adapter).
    /// - `prev_share_blockhash == all_zeros` →
    ///   [`ShareHeaderLookup::Found`] with `prev_share_blockhash =
    ///   None` so the engine's "stop at genesis" path keys off the
    ///   explicit `None` rather than an in-band sentinel.
    pub struct InProcessChain {
        chain: ChainStoreHandle,
        network: bitcoin::Network,
        // Retained so `subscribe_tip()` can hand out fresh receivers.
        // The in-process backend doesn't itself drive this channel —
        // production wiring at `pool.rs` constructs the
        // `start_reorg_watcher` over `chain.get_chain_tip()` and
        // observes via the engine's existing detector. Tests that
        // need a synthetic tip event reach for `MockShareChain`.
        tip_tx: broadcast::Sender<BlockHash>,
    }

    impl InProcessChain {
        /// Wrap a live `ChainStoreHandle`. The network is captured
        /// from the handle so [`ShareChainReader::network`] can serve
        /// it synchronously.
        pub fn new(chain: ChainStoreHandle) -> Self {
            let network = chain.network();
            // 16-element ring buffer matches the tip broadcast capacity
            // used elsewhere in the workspace (small & cheap).
            let (tip_tx, _) = broadcast::channel(16);
            Self {
                chain,
                network,
                tip_tx,
            }
        }

        /// Borrow the underlying `ChainStoreHandle`. Pool-side code
        /// still needs it to drive
        /// [`P2poolV2Engine::start_reorg_watcher`] over a sync
        /// closure during the transition; once `IpcChain` lands the
        /// closure consumes [`ShareChainReader::subscribe_tip`]
        /// directly and this accessor becomes unnecessary.
        pub fn chain_store_handle(&self) -> &ChainStoreHandle {
            &self.chain
        }

        /// Sender end of the tip broadcast. Production wiring (the
        /// reorg watcher in `pool.rs`) can keep this sender and push
        /// every detected tip onto it so other subscribers
        /// (`subscribe_tip`) wake up. Until that rewire lands the
        /// channel just stays empty — `MockShareChain` is what the
        /// tests use to drive synthetic tips.
        pub fn tip_sender(&self) -> broadcast::Sender<BlockHash> {
            self.tip_tx.clone()
        }
    }

    impl ShareChainReader for InProcessChain {
        fn get_chain_tip(&self) -> BoxFuture<'_, Result<Option<BlockHash>, IpcClientError>> {
            // ChainStoreHandle's read paths are sync (rocksdb
            // DashMap-backed reads); wrap in an immediate-ready
            // future so the trait surface stays uniformly async.
            let result = match self.chain.get_chain_tip() {
                Ok(tip) => Ok(Some(tip)),
                Err(StoreError::NotFound(_)) => Ok(None),
                Err(e) => Err(IpcClientError::Capnp(capnp::Error::failed(format!(
                    "get_chain_tip: {e}"
                )))),
            };
            Box::pin(async move { result })
        }

        fn get_share_header(
            &self,
            share_hash: &BlockHash,
        ) -> BoxFuture<'_, Result<ShareHeaderLookup, IpcClientError>> {
            // Snapshot the input by-value so the returned future
            // doesn't borrow `share_hash` past the call.
            let share_hash = *share_hash;
            // All-zeros sentinel → engine-side "stop walking ancestors".
            // Mirrors `ChainReadAdapter::get_share_header` so the
            // in-process and IPC paths behave identically.
            let result = if share_hash
                .as_raw_hash()
                .as_byte_array()
                .iter()
                .all(|b| *b == 0)
            {
                Ok(ShareHeaderLookup::Genesis)
            } else {
                match self.chain.get_share_header(&share_hash) {
                    Ok(header) => {
                        let prev = header.prev_share_blockhash;
                        let prev_opt = if prev
                            .as_raw_hash()
                            .as_byte_array()
                            .iter()
                            .all(|b| *b == 0)
                        {
                            None
                        } else {
                            Some(prev)
                        };
                        Ok(ShareHeaderLookup::Found(ShareHeaderRead {
                            prev_share_blockhash: prev_opt,
                        }))
                    }
                    Err(StoreError::NotFound(_)) => Ok(ShareHeaderLookup::NotFound),
                    Err(e) => Err(IpcClientError::Capnp(capnp::Error::failed(format!(
                        "get_share_header: {e}"
                    )))),
                }
            };
            Box::pin(async move { result })
        }

        fn get_tip_height(&self) -> BoxFuture<'_, Result<Option<u32>, IpcClientError>> {
            let result = self.chain.get_tip_height().map_err(|e| {
                IpcClientError::Capnp(capnp::Error::failed(format!("get_tip_height: {e}")))
            });
            Box::pin(async move { result })
        }

        fn network(&self) -> bitcoin::Network {
            self.network
        }

        fn subscribe_tip(&self) -> broadcast::Receiver<BlockHash> {
            self.tip_tx.subscribe()
        }
    }
}

#[cfg(feature = "in-process-chain")]
pub use in_process::InProcessChain;

// -----------------------------------------------------------------------
// Mock backend (test-only).
// -----------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod mock {
    use super::*;
    use std::sync::Mutex;

    /// In-memory `ShareChainReader` for unit tests.
    ///
    /// Defaults to a populated regtest chain at a synthetic genesis
    /// tip. Use [`MockShareChain::with_no_genesis`] to model the
    /// pre-genesis path that the original
    /// `setup_test_chain_store_handle(false)` fixture exercised
    /// (`Ok(None)` from tip / height accessors).
    pub struct MockShareChain {
        state: Mutex<MockState>,
        network: bitcoin::Network,
        tip_tx: broadcast::Sender<BlockHash>,
    }

    struct MockState {
        // None ⇒ genesis not initialised (Ok(None) responses).
        chain_tip: Option<BlockHash>,
        tip_height: Option<u32>,
        // share_hash → ShareHeaderRead mapping. Use `with_header`
        // to seed entries.
        headers: std::collections::HashMap<BlockHash, ShareHeaderRead>,
    }

    impl MockShareChain {
        /// Default mock: regtest network, no genesis, no headers.
        /// Returns `Ok(None)` from the tip + height accessors and
        /// `ShareHeaderLookup::NotFound` from `get_share_header`
        /// for any non-genesis hash.
        pub fn new() -> Self {
            Self::with_network(bitcoin::Network::Regtest)
        }

        /// Mock targeting an explicit network.
        pub fn with_network(network: bitcoin::Network) -> Self {
            let (tip_tx, _) = broadcast::channel(16);
            Self {
                state: Mutex::new(MockState {
                    chain_tip: None,
                    tip_height: None,
                    headers: std::collections::HashMap::new(),
                }),
                network,
                tip_tx,
            }
        }

        /// Genesis-uninitialised constructor preserving the test
        /// intent at the original `setup_test_chain_store_handle(false)`
        /// call sites. Equivalent to [`Self::new`] today; kept as a
        /// distinct entry point so the test name documents the
        /// expected `Ok(None)` semantics.
        pub fn with_no_genesis() -> Self {
            Self::new()
        }

        /// Set the chain tip + height in one call. Used by tests
        /// that want a populated chain.
        #[allow(
            dead_code,
            reason = "exercised by upcoming Phase 2-B reorg-walk unit tests"
        )]
        pub fn with_tip(self, tip: BlockHash, height: u32) -> Self {
            {
                let mut s = self.state.lock().expect("lock");
                s.chain_tip = Some(tip);
                s.tip_height = Some(height);
            }
            self
        }

        /// Seed a single header in the chain. The engine's reorg
        /// ancestry walk reads `prev_share_blockhash` only.
        #[allow(
            dead_code,
            reason = "exercised by upcoming Phase 2-B reorg-walk unit tests"
        )]
        pub fn with_header(self, hash: BlockHash, prev: Option<BlockHash>) -> Self {
            {
                let mut s = self.state.lock().expect("lock");
                s.headers.insert(
                    hash,
                    ShareHeaderRead {
                        prev_share_blockhash: prev,
                    },
                );
            }
            self
        }

        /// Push a synthetic tip onto the broadcast channel. Tests
        /// that exercise `subscribe_tip`-driven invalidation use
        /// this; the receiver hand-out logic is otherwise inert.
        #[allow(
            dead_code,
            reason = "exercised by upcoming Phase 2-B integration tests"
        )]
        pub fn push_tip(&self, tip: BlockHash) {
            let _ = self.tip_tx.send(tip);
        }
    }

    impl Default for MockShareChain {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ShareChainReader for MockShareChain {
        fn get_chain_tip(&self) -> BoxFuture<'_, Result<Option<BlockHash>, IpcClientError>> {
            let result = Ok(self.state.lock().expect("lock").chain_tip);
            Box::pin(async move { result })
        }

        fn get_share_header(
            &self,
            share_hash: &BlockHash,
        ) -> BoxFuture<'_, Result<ShareHeaderLookup, IpcClientError>> {
            use bitcoin::hashes::Hash as _;
            let share_hash = *share_hash;
            let result = if share_hash
                .as_raw_hash()
                .as_byte_array()
                .iter()
                .all(|b| *b == 0)
            {
                Ok(ShareHeaderLookup::Genesis)
            } else {
                let s = self.state.lock().expect("lock");
                match s.headers.get(&share_hash) {
                    Some(h) => Ok(ShareHeaderLookup::Found(*h)),
                    None => Ok(ShareHeaderLookup::NotFound),
                }
            };
            Box::pin(async move { result })
        }

        fn get_tip_height(&self) -> BoxFuture<'_, Result<Option<u32>, IpcClientError>> {
            let result = Ok(self.state.lock().expect("lock").tip_height);
            Box::pin(async move { result })
        }

        fn network(&self) -> bitcoin::Network {
            self.network
        }

        fn subscribe_tip(&self) -> broadcast::Receiver<BlockHash> {
            self.tip_tx.subscribe()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::mock::MockShareChain;
    use super::*;
    use bitcoin::hashes::Hash as _;

    fn h(seed: u8) -> BlockHash {
        BlockHash::from_byte_array([seed; 32])
    }

    #[tokio::test]
    async fn mock_default_returns_none_tip() {
        let chain = MockShareChain::new();
        assert_eq!(chain.get_chain_tip().await.unwrap(), None);
        assert_eq!(chain.get_tip_height().await.unwrap(), None);
    }

    #[tokio::test]
    async fn mock_with_tip_returns_some() {
        let tip = h(1);
        let chain = MockShareChain::new().with_tip(tip, 42);
        assert_eq!(chain.get_chain_tip().await.unwrap(), Some(tip));
        assert_eq!(chain.get_tip_height().await.unwrap(), Some(42));
    }

    #[tokio::test]
    async fn mock_get_share_header_unknown_is_not_found() {
        let chain = MockShareChain::new();
        let res = chain.get_share_header(&h(7)).await.unwrap();
        assert!(matches!(res, ShareHeaderLookup::NotFound));
    }

    #[tokio::test]
    async fn mock_get_share_header_all_zeros_is_genesis() {
        let chain = MockShareChain::new();
        let zeros = BlockHash::all_zeros();
        let res = chain.get_share_header(&zeros).await.unwrap();
        assert!(matches!(res, ShareHeaderLookup::Genesis));
    }

    #[tokio::test]
    async fn mock_with_header_returns_found_with_prev() {
        let chain = MockShareChain::new().with_header(h(1), Some(h(2)));
        let res = chain.get_share_header(&h(1)).await.unwrap();
        match res {
            ShareHeaderLookup::Found(read) => {
                assert_eq!(read.prev_share_blockhash, Some(h(2)));
            }
            other => panic!("expected Found, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mock_with_no_genesis_constructor() {
        let chain = MockShareChain::with_no_genesis();
        // Same observable semantics as ::new() but the constructor
        // documents the intent at call sites.
        assert_eq!(chain.get_chain_tip().await.unwrap(), None);
        assert_eq!(chain.get_tip_height().await.unwrap(), None);
    }

    #[tokio::test]
    async fn mock_subscribe_tip_observes_pushed_tip() {
        let chain = MockShareChain::new();
        let mut rx = chain.subscribe_tip();
        chain.push_tip(h(9));
        let got = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .expect("recv timed out")
            .expect("recv");
        assert_eq!(got, h(9));
    }

    #[tokio::test]
    async fn mock_network_is_synchronous_capture() {
        let chain = MockShareChain::with_network(bitcoin::Network::Signet);
        assert_eq!(chain.network(), bitcoin::Network::Signet);
    }

    #[tokio::test]
    async fn mock_is_dyn_compatible() {
        // Critical: EngineHandles holds Arc<dyn ShareChainReader>.
        // Ensure the trait is dyn-compatible end-to-end.
        let chain: std::sync::Arc<dyn ShareChainReader> =
            std::sync::Arc::new(MockShareChain::new());
        assert_eq!(chain.network(), bitcoin::Network::Regtest);
        assert_eq!(chain.get_chain_tip().await.unwrap(), None);
    }
}
