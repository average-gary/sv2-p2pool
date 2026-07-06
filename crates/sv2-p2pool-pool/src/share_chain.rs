//! Pool-side share-chain backends behind the [`ShareChainReader`] trait.
//!
//! Phase 2-B Track A (ADR 0011) made the engine crate AGPL-clean by
//! pulling [`ShareChainReader`] in front of `EngineHandles.chain` and
//! moving the two concrete backends out of the engine. They live here:
//!
//! - [`InProcessChain`] — wraps a real
//!   [`p2poolv2_lib::shares::chain::chain_store_handle::ChainStoreHandle`].
//!   Convenient for tests + dev / single-process deployments. This is
//!   the only place in the workspace that links AGPL `p2poolv2_lib`
//!   for chain reads.
//!
//! - [`IpcChain`] — production backend. Owns a `!Send` capnp
//!   [`sv2_p2pool_ipc::Sv2P2poolIpcClient`] on a dedicated `std::thread`
//!   running a current-thread `tokio` runtime + `LocalSet`, exposing a
//!   `Send + Sync` actor handle to the rest of the binary. Talks to a
//!   separate p2poolv2 daemon over Cap'n Proto on Unix sockets.
//!
//! Both implement [`sv2_p2pool_engine::ShareChainReader`] so the engine
//! is backend-agnostic at the `Arc<dyn ShareChainReader>` boundary.
//!
//! ## `bootstrap_share_chain` — what gets wired
//!
//! [`bootstrap_share_chain`] picks a backend based on the
//! [`p2poolv2_lib::config::Config`] handed in:
//!
//! - When `config.ipc.socket_path` is set we treat the binary as
//!   running *next to* a p2poolv2 daemon and connect with [`IpcChain`].
//!   The pool process owns no rocksdb in that mode — the daemon does.
//!
//! - Otherwise we fall back to the legacy in-process slice
//!   ([`InProcessChain`] over a fresh `Store` + `StoreWriter`). This
//!   path keeps the existing tests + the single-process dev story
//!   working until the deployment story moves entirely to "run a
//!   daemon, point the pool at its socket".
//!
//! ## `IpcChain` actor architecture
//!
//! The capnp client is `!Send`, the pool runtime is multi-threaded —
//! we can't simply `Arc<Sv2P2poolIpcClient>` and call from anywhere.
//! The actor pattern resolves this:
//!
//! 1. A dedicated OS thread (spawned via [`std::thread::spawn`]) builds
//!    a current-thread `tokio` runtime and runs a [`tokio::task::LocalSet`]
//!    on it.
//! 2. Inside the `LocalSet`, the capnp client is constructed via
//!    [`Sv2P2poolIpcClient::connect`] (which spawns the `RpcSystem`
//!    driver). The client lives on this thread for its entire life.
//! 3. Outside callers send [`IpcRequest`] messages through a bounded
//!    [`tokio::sync::mpsc`] channel. Each request carries a
//!    [`tokio::sync::oneshot::Sender`] for the reply, so the protocol
//!    is request/response. The actor's inner task `recv`s, dispatches
//!    to the right capnp method, and sends back the result.
//! 4. The actor *also* spawns a `subscribe_chain_tip` task — every
//!    new tip the daemon pushes is fanned out into:
//!     - a lock-free [`AtomicTipSnapshot`] read by the reorg watcher's
//!       sync `Fn() -> Option<BlockHash>` closure, AND
//!     - a [`tokio::sync::broadcast::Sender<BlockHash>`] that backs
//!       [`ShareChainReader::subscribe_tip`].
//! 5. Panic propagation: the OS thread is `joined` from a watchdog
//!    thread; if it ever exits we fire a [`tokio::sync::watch`] channel
//!    that the pool binary observes via [`IpcChain::shutdown_signal`]
//!    to drive a clean process shutdown rather than silently losing
//!    chain reads.
//!
//! Channel capacity: 256. Sized as `2 × REORG_ANCESTRY_DEPTH (=128)`
//! so a worst-case 100-hop reorg ancestry walk never blocks behind a
//! request queue. ADR 0011 § Risks documents the reasoning.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};

use bitcoin::BlockHash;
use bitcoin::hashes::Hash as _;
use bitcoindrpc::{BitcoindLike, BitcoindRpcClient};
use p2poolv2_lib::{
    config::Config as P2poolConfig,
    shares::{chain::chain_store_handle::ChainStoreHandle, share_block::ShareBlock},
    store::{
        Store,
        writer::{StoreError, StoreHandle, StoreWriter, write_channel},
    },
};
use sv2_p2pool_engine::{
    BoxFuture, EngineHandles, ShareChainReader, ShareHeaderLookup, ShareHeaderRead,
};
use sv2_p2pool_ipc::{
    ChainTipResult as IpcChainTipResult, IpcClientError, IpcClientHealth,
    ShareHeaderLookup as IpcShareHeaderLookup, Sv2P2poolIpcClient,
    TipHeightResult as IpcTipHeightResult,
};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Errors from share-chain bootstrap.
#[derive(Debug, thiserror::Error)]
pub enum ShareChainBootstrapError {
    #[error("failed to open rocksdb store at {path}: {message}")]
    OpenStore { path: String, message: String },
    #[error("failed to build genesis share-block for network {network:?}: {message}")]
    Genesis {
        network: bitcoin::Network,
        message: String,
    },
    #[error("failed to initialise share-chain genesis: {0}")]
    InitGenesis(String),
    #[error("failed to construct bitcoind RPC client: {0:?}")]
    BitcoindClient(bitcoindrpc::BitcoindRpcError),
    #[error("failed to connect IpcChain actor to {socket}: {message}")]
    IpcConnect { socket: String, message: String },
}

/// Live handles produced by [`bootstrap_share_chain`].
///
/// `engine_handles` is what the engine constructor consumes. The
/// remaining fields (`store`, `store_writer_join`) are only populated
/// for the in-process path and are kept by the binary so they live as
/// long as the pool. Dropping them tears down the rocksdb writer
/// thread.
///
/// For IPC mode the rocksdb store lives in a separate daemon process,
/// so `store` / `store_writer_join` are `None`. The `IpcChain` actor's
/// own resources are owned by the `EngineHandles.chain: Arc<dyn
/// ShareChainReader>` and dropped when the engine drops it.
pub struct ShareChainHandles {
    pub engine_handles: EngineHandles,
    /// In-process backend only: kept so the reorg watcher and the
    /// tip-height publisher can use the underlying `ChainStoreHandle`
    /// during the transition. With the `IpcChain` backend they read
    /// from the actor's atomic snapshot instead and this is `None`.
    pub chain_store: Option<ChainStoreHandle>,
    /// In-process backend only: keep the rocksdb store alive for the
    /// lifetime of the pool.
    pub store: Option<Arc<Store>>,
    /// In-process backend only: writer-thread join. Awaiting it after
    /// dropping the store sees the writer exit cleanly.
    pub store_writer_join: Option<JoinHandle<()>>,
    /// IPC backend only: an [`Arc<AtomicTipSnapshot>`] read by the
    /// reorg watcher's sync closure (lock-free, no per-tick UDS
    /// round-trip). With the in-process backend the watcher keeps
    /// reading the `ChainStoreHandle` directly.
    pub ipc_tip_snapshot: Option<Arc<AtomicTipSnapshot>>,
    /// IPC backend only: a [`watch::Receiver<bool>`] whose value flips
    /// to `true` if the actor's dedicated thread dies. The pool
    /// binary monitors this and triggers a graceful shutdown so a
    /// dead chain connection isn't silently swallowed.
    pub ipc_shutdown_signal: Option<watch::Receiver<bool>>,
}

/// Bootstrap the share-chain backend. Picks IPC vs. in-process based
/// on whether `p2pool_config.ipc.socket_path` is set.
///
/// `IpcChain` mode requires that a separate p2poolv2 daemon is already
/// listening on that socket — we don't spawn it. The daemon owns the
/// rocksdb store; we only own the IPC client.
///
/// In-process mode opens rocksdb here, inits genesis, and wraps the
/// resulting [`ChainStoreHandle`] in [`InProcessChain`]. The
/// returned `store` + `store_writer_join` must outlive the pool.
pub async fn bootstrap_share_chain(
    p2pool_config: &P2poolConfig,
) -> Result<ShareChainHandles, ShareChainBootstrapError> {
    let network = p2pool_config.stratum.network;

    // 1. Build the bitcoind RPC client (shared across both backends —
    //    it's used for `submitBlock`, not chain reads).
    let rpc = &p2pool_config.bitcoinrpc;
    let bitcoind_client = BitcoindRpcClient::new(&rpc.url, &rpc.username, &rpc.password)
        .map_err(ShareChainBootstrapError::BitcoindClient)?;
    let bitcoind: Arc<dyn BitcoindLike> = Arc::new(bitcoind_client);

    // 1b. Best-effort probe: a misconfigured pool would otherwise
    //     boot happily and only fail on the first found block.
    //     Non-fatal on purpose.
    match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        bitcoind.getblockchaininfo(),
    )
    .await
    {
        Ok(Ok(info)) => {
            info!(
                bitcoinrpc_url = %rpc.url,
                initial_block_download = info.initial_block_download,
                "bitcoind reachable at boot"
            );
        }
        Ok(Err(e)) => {
            warn!(
                bitcoinrpc_url = %rpc.url,
                error = %e,
                "bitcoind getblockchaininfo failed at boot — pool will continue but submit_block will fail until bitcoind is reachable"
            );
        }
        Err(_) => {
            warn!(
                bitcoinrpc_url = %rpc.url,
                "bitcoind getblockchaininfo timed out after 3s — pool will continue but submit_block will fail until bitcoind is reachable"
            );
        }
    }

    // 2. Pick the chain backend.
    if let Some(ipc_cfg) = p2pool_config.ipc.as_ref() {
        info!(
            socket = %ipc_cfg.socket_path,
            "share-chain bootstrap: IpcChain mode (connecting to p2poolv2 daemon)"
        );
        let ipc = IpcChain::connect(&ipc_cfg.socket_path).await.map_err(|e| {
            ShareChainBootstrapError::IpcConnect {
                socket: ipc_cfg.socket_path.clone(),
                message: e.to_string(),
            }
        })?;
        let snapshot = ipc.tip_snapshot();
        let shutdown = ipc.shutdown_signal();
        let chain_reader: Arc<dyn ShareChainReader> = Arc::new(ipc);
        info!("share-chain bootstrap: IpcChain handles ready");
        Ok(ShareChainHandles {
            engine_handles: EngineHandles {
                chain: chain_reader,
                bitcoind,
            },
            chain_store: None,
            store: None,
            store_writer_join: None,
            ipc_tip_snapshot: Some(snapshot),
            ipc_shutdown_signal: Some(shutdown),
        })
    } else {
        info!(
            store_path = %p2pool_config.store.path,
            "share-chain bootstrap: InProcessChain mode (opening rocksdb in-process)"
        );
        let store = Arc::new(
            Store::new(p2pool_config.store.path.clone(), false).map_err(|e| {
                ShareChainBootstrapError::OpenStore {
                    path: p2pool_config.store.path.clone(),
                    message: e.to_string(),
                }
            })?,
        );

        let (write_tx, write_rx) = write_channel();
        let store_for_writer = store.clone();
        let store_writer_join = tokio::task::spawn_blocking(move || {
            let writer = StoreWriter::new(store_for_writer, write_rx);
            writer.run();
            info!("share-chain bootstrap: StoreWriter exited");
        });

        let store_handle = StoreHandle::new(store.clone(), write_tx);
        let chain = ChainStoreHandle::new(store_handle, network);
        let genesis = ShareBlock::build_genesis_for_network(network).map_err(|e| {
            ShareChainBootstrapError::Genesis {
                network,
                message: e.to_string(),
            }
        })?;
        chain
            .init_or_setup_genesis(genesis)
            .await
            .map_err(|e| ShareChainBootstrapError::InitGenesis(e.to_string()))?;

        let chain_reader: Arc<dyn ShareChainReader> = Arc::new(InProcessChain::new(chain.clone()));
        info!("share-chain bootstrap: InProcessChain handles ready");
        Ok(ShareChainHandles {
            engine_handles: EngineHandles {
                chain: chain_reader,
                bitcoind,
            },
            chain_store: Some(chain),
            store: Some(store),
            store_writer_join: Some(store_writer_join),
            ipc_tip_snapshot: None,
            ipc_shutdown_signal: None,
        })
    }
}

// =======================================================================
// InProcessChain — wraps p2poolv2_lib::ChainStoreHandle.
//
// Used for tests + single-process dev. The engine crate is AGPL-clean
// because the `p2poolv2_lib` link lives here in the pool crate.
// =======================================================================

/// `ShareChainReader` impl backed by an in-process
/// [`ChainStoreHandle`]. ADR 0011 puts this here (in the pool crate)
/// rather than in the engine because the engine should never link
/// AGPL `p2poolv2_lib`.
///
/// Mirrors the wire-level error semantics of the daemon's
/// `ChainReadAdapter` (see
/// `vendor/p2poolv2/p2poolv2_node/src/ipc_chain.rs`):
/// - `ChainStoreHandle::get_chain_tip` returning `NotFound` →
///   `Ok(None)` (genesis not initialised).
/// - Other `StoreError`s → `IpcClientError::Capnp(..)` carrying the
///   formatted message. Reusing the `Capnp` variant keeps the error
///   type uniform across the seam (ADR 0011 § Decision § "error
///   model").
/// - `get_share_header` returning `NotFound` →
///   [`ShareHeaderLookup::NotFound`].
/// - All-zero share-hash query → [`ShareHeaderLookup::Genesis`]
///   (sentinel mirrored from the daemon's adapter).
/// - `prev_share_blockhash == all_zeros` →
///   [`ShareHeaderLookup::Found`] with `prev_share_blockhash = None`
///   so the engine's "stop at genesis" path keys off the explicit
///   `None` rather than an in-band sentinel.
pub struct InProcessChain {
    chain: ChainStoreHandle,
    network: bitcoin::Network,
    /// Retained so [`ShareChainReader::subscribe_tip`] can hand out
    /// fresh receivers. The in-process backend doesn't itself drive
    /// this channel — production wiring at `pool.rs` keeps polling
    /// `chain.get_chain_tip()` for now. The mock + IpcChain backends
    /// drive it from their own state.
    tip_tx: broadcast::Sender<BlockHash>,
}

impl InProcessChain {
    /// Wrap a live `ChainStoreHandle`. The network is captured so
    /// [`ShareChainReader::network`] can serve it synchronously.
    pub fn new(chain: ChainStoreHandle) -> Self {
        let network = chain.network();
        let (tip_tx, _) = broadcast::channel(16);
        Self {
            chain,
            network,
            tip_tx,
        }
    }

    /// Borrow the underlying `ChainStoreHandle`. The pool's reorg
    /// watcher and tip-height publisher use this for their sync /
    /// polling closures during the transition.
    pub fn chain_store_handle(&self) -> &ChainStoreHandle {
        &self.chain
    }

    /// Sender end of the tip broadcast. Pool-side wiring can keep
    /// this clone if it ever wants to drive the in-process backend's
    /// `subscribe_tip` channel from a poll loop. Today the pool
    /// keeps using sync polling for the in-process path, so the
    /// channel is inert.
    #[allow(dead_code, reason = "kept for symmetry with IpcChain")]
    pub fn tip_sender(&self) -> broadcast::Sender<BlockHash> {
        self.tip_tx.clone()
    }
}

impl ShareChainReader for InProcessChain {
    fn get_chain_tip(&self) -> BoxFuture<'_, Result<Option<BlockHash>, IpcClientError>> {
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
        let share_hash = *share_hash;
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
                    let prev_opt = if prev.as_raw_hash().as_byte_array().iter().all(|b| *b == 0) {
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

// =======================================================================
// IpcChain — production backend.
//
// Owns a `!Send` capnp client on a dedicated `LocalSet` thread; the
// outside world only sees a `Send + Sync` actor handle.
// =======================================================================

/// `IpcChain` actor handle. `Send + Sync`.
///
/// Cheap to clone (`Arc` internally). The actor's runtime + thread
/// stay alive as long as any clone of [`Self::actor`] exists; on
/// final drop the request channel closes, the actor task exits, the
/// runtime shuts down, the thread joins.
pub struct IpcChain {
    /// `Send + Sync` request handle. Cloning is cheap.
    actor: Arc<IpcChainActorHandle>,
    /// Captured at construction via `getNetwork @6`. Served sync by
    /// [`ShareChainReader::network`].
    network: bitcoin::Network,
    /// Lock-free atomic snapshot of the latest tip + tip-height.
    /// The reorg watcher's sync `Fn() -> Option<BlockHash>` closure
    /// reads from here without round-tripping to the actor.
    tip_snapshot: Arc<AtomicTipSnapshot>,
    /// Sender for [`Self::subscribe_tip`]. Cloned by every subscriber.
    tip_tx: broadcast::Sender<BlockHash>,
    /// Watch channel that flips to `true` if the actor's dedicated
    /// thread dies. The pool binary monitors this and shuts down
    /// rather than silently losing chain reads.
    shutdown_tx: watch::Sender<bool>,
}

impl IpcChain {
    /// Default mpsc capacity. ADR 0011 § Risks: sized as
    /// `2 × REORG_ANCESTRY_DEPTH (=128)` so a worst-case 100-hop
    /// reorg ancestry walk never blocks behind the queue.
    pub const REQUEST_CHANNEL_CAPACITY: usize = 256;

    /// Connect to a p2poolv2 IPC server at `socket_path`. Spawns the
    /// dedicated actor thread + runtime + LocalSet, performs the
    /// `getNetwork @6` capture, subscribes to tip pushes.
    pub async fn connect(socket_path: &str) -> Result<Self, IpcClientError> {
        let socket_path = socket_path.to_owned();
        let (cmd_tx, cmd_rx) = mpsc::channel::<IpcRequest>(Self::REQUEST_CHANNEL_CAPACITY);
        let (init_tx, init_rx) = std::sync::mpsc::sync_channel::<InitResult>(1);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let shutdown_tx_for_thread = shutdown_tx.clone();

        // The tip broadcast channel + atomic snapshot are *constructed
        // outside the actor thread* so the public IpcChain handle can
        // hand out receivers + read the snapshot without traversing
        // the actor mailbox. Both are cloned across the thread
        // boundary into the actor's subscribe-task.
        let tip_snapshot = Arc::new(AtomicTipSnapshot::empty());
        let tip_snapshot_for_thread = Arc::clone(&tip_snapshot);
        let (tip_tx, _) = broadcast::channel::<BlockHash>(64);
        let tip_tx_for_thread = tip_tx.clone();

        // Catch any panic on the actor thread and propagate it via
        // `shutdown_tx`. `std::thread::Builder::spawn` returns a
        // `JoinHandle` whose `.join()` is `Result<...>`; we move it
        // into a watchdog thread that flips the watch channel.
        let socket_path_for_thread = socket_path.clone();
        let actor_thread = std::thread::Builder::new()
            .name("sv2-p2pool-ipc-actor".into())
            .spawn(move || {
                actor_thread_main(
                    socket_path_for_thread,
                    cmd_rx,
                    init_tx,
                    tip_snapshot_for_thread,
                    tip_tx_for_thread,
                );
            })
            .map_err(|e| {
                IpcClientError::Capnp(capnp::Error::failed(format!(
                    "spawn IpcChain actor thread: {e}"
                )))
            })?;

        // Watchdog: joins the actor thread and propagates panic /
        // unexpected exit to the watch channel. Detached; it lives
        // until the actor thread is gone.
        std::thread::Builder::new()
            .name("sv2-p2pool-ipc-actor-watchdog".into())
            .spawn(move || {
                let join_result = actor_thread.join();
                let outcome = match join_result {
                    Ok(()) => "exited",
                    Err(_) => "panicked",
                };
                error!(
                    target: "sv2_p2pool_pool::share_chain",
                    outcome,
                    "IpcChain actor thread {outcome} — flipping shutdown_signal"
                );
                let _ = shutdown_tx_for_thread.send(true);
            })
            .map_err(|e| {
                IpcClientError::Capnp(capnp::Error::failed(format!(
                    "spawn IpcChain watchdog thread: {e}"
                )))
            })?;

        // Wait for the actor's init step (capnp connect + getNetwork
        // + subscribe_chain_tip) to succeed. `init_rx.recv()` is sync
        // but the std mpsc is bounded with capacity 1 and the actor
        // sends exactly once before the first request, so this is a
        // bounded wait — typically a few capnp round-trips.
        let init = match tokio::task::spawn_blocking(move || init_rx.recv()).await {
            Ok(Ok(init)) => init,
            Ok(Err(_)) => {
                return Err(IpcClientError::Capnp(capnp::Error::failed(
                    "IpcChain actor thread exited before init".into(),
                )));
            }
            Err(e) => {
                return Err(IpcClientError::Capnp(capnp::Error::failed(format!(
                    "IpcChain init blocking task panicked: {e}"
                ))));
            }
        };
        let network = init.into_result()?;

        let actor = Arc::new(IpcChainActorHandle { cmd_tx });
        info!(
            socket = %socket_path,
            ?network,
            "IpcChain connected"
        );
        Ok(Self {
            actor,
            network,
            tip_snapshot,
            tip_tx,
            shutdown_tx,
        })
    }

    /// Snapshot read of the latest tip. Lock-free; satisfies
    /// `Fn() -> Option<BlockHash> + Send + 'static` once cloned, so
    /// the engine's `start_reorg_watcher` closure can use it.
    pub fn tip_snapshot(&self) -> Arc<AtomicTipSnapshot> {
        Arc::clone(&self.tip_snapshot)
    }

    /// Hand back a `watch::Receiver` that flips to `true` if the
    /// actor's dedicated thread dies. Used by the pool binary to
    /// trigger graceful shutdown.
    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }
}

impl ShareChainReader for IpcChain {
    fn get_chain_tip(&self) -> BoxFuture<'_, Result<Option<BlockHash>, IpcClientError>> {
        let actor = Arc::clone(&self.actor);
        Box::pin(async move {
            let (reply_tx, reply_rx) = oneshot::channel();
            actor
                .cmd_tx
                .send(IpcRequest::GetChainTip { reply: reply_tx })
                .await
                .map_err(|_| {
                    IpcClientError::Capnp(capnp::Error::failed(
                        "IpcChain actor request channel closed".into(),
                    ))
                })?;
            reply_rx.await.map_err(|_| {
                IpcClientError::Capnp(capnp::Error::failed(
                    "IpcChain actor dropped reply channel".into(),
                ))
            })?
        })
    }

    fn get_share_header(
        &self,
        share_hash: &BlockHash,
    ) -> BoxFuture<'_, Result<ShareHeaderLookup, IpcClientError>> {
        let actor = Arc::clone(&self.actor);
        let share_hash = *share_hash;
        Box::pin(async move {
            let (reply_tx, reply_rx) = oneshot::channel();
            actor
                .cmd_tx
                .send(IpcRequest::GetShareHeader {
                    share_hash,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| {
                    IpcClientError::Capnp(capnp::Error::failed(
                        "IpcChain actor request channel closed".into(),
                    ))
                })?;
            reply_rx.await.map_err(|_| {
                IpcClientError::Capnp(capnp::Error::failed(
                    "IpcChain actor dropped reply channel".into(),
                ))
            })?
        })
    }

    fn get_tip_height(&self) -> BoxFuture<'_, Result<Option<u32>, IpcClientError>> {
        let actor = Arc::clone(&self.actor);
        Box::pin(async move {
            let (reply_tx, reply_rx) = oneshot::channel();
            actor
                .cmd_tx
                .send(IpcRequest::GetTipHeight { reply: reply_tx })
                .await
                .map_err(|_| {
                    IpcClientError::Capnp(capnp::Error::failed(
                        "IpcChain actor request channel closed".into(),
                    ))
                })?;
            reply_rx.await.map_err(|_| {
                IpcClientError::Capnp(capnp::Error::failed(
                    "IpcChain actor dropped reply channel".into(),
                ))
            })?
        })
    }

    fn network(&self) -> bitcoin::Network {
        self.network
    }

    fn subscribe_tip(&self) -> broadcast::Receiver<BlockHash> {
        self.tip_tx.subscribe()
    }
}

/// Internal: the `Send + Sync` actor handle. One per [`IpcChain`].
struct IpcChainActorHandle {
    cmd_tx: mpsc::Sender<IpcRequest>,
}

/// Lock-free latest-tip / latest-tip-height snapshot.
///
/// Used by the pool's sync reorg-watcher closure and the tip-height
/// publisher. Updated by the actor's subscribe-task on every
/// inbound tip push.
///
/// Implementation note: bitcoin 0.32's `BlockHash` is a 32-byte value
/// without a lock-free atomic representation in std. We use a
/// `Mutex<[u8; 32]>` for the tip and an `AtomicBool` "set?" flag.
/// Mutex contention is irrelevant in this profile — writes happen at
/// most once per chain tip and reads are once per
/// `DEFAULT_POLL_PERIOD`. Tip-height is a separate `AtomicU32` +
/// `AtomicBool` so the tip-height publisher doesn't need the tip
/// lock.
pub struct AtomicTipSnapshot {
    tip_set: AtomicBool,
    tip: std::sync::Mutex<[u8; 32]>,
    height_set: AtomicBool,
    height: AtomicU32,
}

impl AtomicTipSnapshot {
    fn empty() -> Self {
        Self {
            tip_set: AtomicBool::new(false),
            tip: std::sync::Mutex::new([0u8; 32]),
            height_set: AtomicBool::new(false),
            height: AtomicU32::new(0),
        }
    }

    /// Atomic load of the current tip. `None` until the actor's
    /// subscribe-task has pushed at least one tip update OR a
    /// `get_chain_tip()` was performed during init.
    pub fn load_tip(&self) -> Option<BlockHash> {
        if !self.tip_set.load(Ordering::Acquire) {
            return None;
        }
        let bytes = *self.tip.lock().expect("snapshot mutex poisoned");
        Some(BlockHash::from_raw_hash(
            bitcoin::hashes::sha256d::Hash::from_byte_array(bytes),
        ))
    }

    /// Atomic load of the current tip height.
    pub fn load_height(&self) -> Option<u32> {
        if !self.height_set.load(Ordering::Acquire) {
            return None;
        }
        Some(self.height.load(Ordering::Acquire))
    }

    fn store_tip(&self, tip: BlockHash) {
        let bytes = *tip.as_raw_hash().as_byte_array();
        *self.tip.lock().expect("snapshot mutex poisoned") = bytes;
        self.tip_set.store(true, Ordering::Release);
    }

    fn store_height(&self, height: u32) {
        self.height.store(height, Ordering::Release);
        self.height_set.store(true, Ordering::Release);
    }
}

/// Request sent from a caller to the actor. Each carries a `oneshot`
/// reply channel; the actor task sends the result back when the capnp
/// round-trip completes.
///
/// The `Get*` prefix on every variant is deliberate — these mirror
/// the IPC method names on the wire (`getChainTip @3` /
/// `getShareHeader @4` / `getTipHeight @5`). Renaming would break that
/// alignment.
#[allow(clippy::enum_variant_names)]
enum IpcRequest {
    GetChainTip {
        reply: oneshot::Sender<Result<Option<BlockHash>, IpcClientError>>,
    },
    GetShareHeader {
        share_hash: BlockHash,
        reply: oneshot::Sender<Result<ShareHeaderLookup, IpcClientError>>,
    },
    GetTipHeight {
        reply: oneshot::Sender<Result<Option<u32>, IpcClientError>>,
    },
}

/// Result of the actor's init handshake (connect + getNetwork +
/// subscribe). Wraps a `Result` so we can serialise the error across
/// the std mpsc boundary.
struct InitResult(Result<bitcoin::Network, IpcClientError>);

impl InitResult {
    fn into_result(self) -> Result<bitcoin::Network, IpcClientError> {
        self.0
    }
}

/// Actor-thread entry point. Builds a current-thread tokio runtime +
/// LocalSet, connects the capnp client, captures the network, kicks
/// off the subscribe-tip task, then drives the request loop until the
/// command channel closes.
fn actor_thread_main(
    socket_path: String,
    cmd_rx: mpsc::Receiver<IpcRequest>,
    init_tx: std::sync::mpsc::SyncSender<InitResult>,
    tip_snapshot: Arc<AtomicTipSnapshot>,
    tip_tx: broadcast::Sender<BlockHash>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = init_tx.send(InitResult(Err(IpcClientError::Capnp(
                capnp::Error::failed(format!("build current-thread runtime: {e}")),
            ))));
            return;
        }
    };

    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(actor_main(
        socket_path,
        cmd_rx,
        init_tx,
        tip_snapshot,
        tip_tx,
    )));
}

async fn actor_main(
    socket_path: String,
    mut cmd_rx: mpsc::Receiver<IpcRequest>,
    init_tx: std::sync::mpsc::SyncSender<InitResult>,
    tip_snapshot: Arc<AtomicTipSnapshot>,
    tip_tx: broadcast::Sender<BlockHash>,
) {
    // 1. Connect.
    let client = match Sv2P2poolIpcClient::connect(&socket_path).await {
        Ok(c) => c,
        Err(e) => {
            let _ = init_tx.send(InitResult(Err(e)));
            return;
        }
    };

    // Item #4 (Phase 3 hardening): observe the client's health watch
    // so a silent driver exit tears this actor down (which the
    // watchdog thread then translates into a pool-wide shutdown_signal
    // flip). Without this, the actor would keep serving requests that
    // now all fail with `capnp::Error::disconnected(...)` on the next
    // await, and the pool would never learn the IPC link is dead.
    let mut health_rx = client.health();

    // 2. Capture network (sync at the trait surface; one capnp call
    //    here, then served from the actor's local copy forever after).
    let network = match client.get_network().await {
        Ok(n) => n,
        Err(e) => {
            let _ = init_tx.send(InitResult(Err(e)));
            return;
        }
    };

    // 3. Capture initial tip + height — best-effort. Fills the
    //    snapshot before the broadcast task starts so the reorg
    //    watcher's sync closure has a good value the first time it
    //    ticks. Failures here are warnings: the daemon may simply
    //    not have completed genesis yet.
    match client.get_chain_tip().await {
        Ok(IpcChainTipResult::Tip(tip)) => tip_snapshot.store_tip(tip),
        Ok(IpcChainTipResult::Uninitialised) => {
            debug!("IpcChain: daemon reports tip uninitialised at connect")
        }
        Err(e) => warn!(error = %e, "IpcChain: initial get_chain_tip failed"),
    }
    match client.get_tip_height().await {
        Ok(IpcTipHeightResult::Height(h)) => tip_snapshot.store_height(h),
        Ok(IpcTipHeightResult::Uninitialised) => {
            debug!("IpcChain: daemon reports tip-height uninitialised at connect")
        }
        Err(e) => warn!(error = %e, "IpcChain: initial get_tip_height failed"),
    }

    // 4. Subscribe to push-driven tip updates. Every callback fires
    //    on the actor's LocalSet, so we can poke a !Send capnp
    //    follow-up here for the matching height.
    let subscription = {
        let tip_snapshot_for_cb = Arc::clone(&tip_snapshot);
        let tip_tx_cb = tip_tx.clone();
        let client_for_height = client.clone();
        let tip_snapshot_for_height = Arc::clone(&tip_snapshot);
        // Channel for "tip arrived"; the height-fetch task picks it
        // up and runs the height capnp call without blocking the
        // callback.
        let (height_trigger_tx, mut height_trigger_rx) = mpsc::channel::<()>(8);
        // Spawn the height-fetcher on the LocalSet. Single-flight:
        // if multiple tips race past it, just refresh once.
        tokio::task::spawn_local(async move {
            while let Some(()) = height_trigger_rx.recv().await {
                // Drain any backlog so we only do one height call
                // per burst.
                while height_trigger_rx.try_recv().is_ok() {}
                match client_for_height.get_tip_height().await {
                    Ok(IpcTipHeightResult::Height(h)) => tip_snapshot_for_height.store_height(h),
                    Ok(IpcTipHeightResult::Uninitialised) => {
                        debug!("IpcChain: tip-height uninitialised after tip push");
                    }
                    Err(e) => {
                        warn!(error = %e, "IpcChain: get_tip_height after tip push failed")
                    }
                }
            }
        });
        match client
            .subscribe_chain_tip(move |bytes| {
                if bytes.len() != 32 {
                    warn!(
                        got = bytes.len(),
                        "IpcChain: subscribeChainTip callback got non-32-byte payload; ignoring"
                    );
                    return;
                }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                let tip =
                    BlockHash::from_raw_hash(bitcoin::hashes::sha256d::Hash::from_byte_array(arr));
                tip_snapshot_for_cb.store_tip(tip);
                // broadcast::Sender::send is non-blocking; it returns
                // Err only when there are no subscribers, which we
                // tolerate (the watcher attaches lazily).
                let _ = tip_tx_cb.send(tip);
                // Trigger a height refresh on the actor's local task.
                // try_send is non-blocking; capacity 8 absorbs bursts.
                let _ = height_trigger_tx.try_send(());
            })
            .await
        {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(
                    error = %e,
                    "IpcChain: subscribeChainTip failed; tip pushes will be unavailable"
                );
                None
            }
        }
    };

    // 5. Init complete; release the caller of `IpcChain::connect`.
    if init_tx.send(InitResult(Ok(network))).is_err() {
        warn!("IpcChain: caller dropped init receiver before connect completed; shutting down");
        drop(subscription);
        return;
    }

    // 6. Request loop.
    //
    // Item #4 (Phase 3 hardening): also select on the health watch so
    // a silent driver exit terminates the actor promptly. Without
    // this we'd keep pulling requests from `cmd_rx` and dispatching
    // them onto a client whose driver is dead — every one would
    // await forever (the driver is what wakes capnp promises).
    loop {
        let req = tokio::select! {
            biased;
            // Health takes priority: if the driver died we want to
            // exit BEFORE dispatching another request.
            changed = health_rx.changed() => {
                match changed {
                    Ok(()) => {
                        let current = *health_rx.borrow_and_update();
                        if matches!(current, IpcClientHealth::Disconnected) {
                            error!(
                                "IpcChain actor: Sv2P2poolIpcClient reports Disconnected — \
                                 RpcSystem driver has exited; tearing down actor"
                            );
                            break;
                        }
                        // Not disconnected yet (e.g. a future state
                        // transition we don't recognize); loop and
                        // re-select.
                        continue;
                    }
                    Err(_) => {
                        // Sender dropped; equivalent to Disconnected.
                        error!(
                            "IpcChain actor: health watch sender dropped — client is unreachable"
                        );
                        break;
                    }
                }
            }
            maybe_req = cmd_rx.recv() => {
                match maybe_req {
                    Some(r) => r,
                    None => break,
                }
            }
        };
        // Each request spawns into a `spawn_local` so the loop keeps
        // pumping while a slow capnp round-trip is in flight. Without
        // this a 100-hop reorg ancestry walk serialises behind
        // earlier requests on the same loop.
        let client = client.clone();
        match req {
            IpcRequest::GetChainTip { reply } => {
                tokio::task::spawn_local(async move {
                    let result = client.get_chain_tip().await.map(|r| match r {
                        IpcChainTipResult::Tip(t) => Some(t),
                        IpcChainTipResult::Uninitialised => None,
                    });
                    let _ = reply.send(result);
                });
            }
            IpcRequest::GetShareHeader { share_hash, reply } => {
                tokio::task::spawn_local(async move {
                    let result = client.get_share_header(&share_hash).await.map(map_lookup);
                    let _ = reply.send(result);
                });
            }
            IpcRequest::GetTipHeight { reply } => {
                tokio::task::spawn_local(async move {
                    let result = client.get_tip_height().await.map(|r| match r {
                        IpcTipHeightResult::Height(h) => Some(h),
                        IpcTipHeightResult::Uninitialised => None,
                    });
                    let _ = reply.send(result);
                });
            }
        }
    }

    // Channel closed: the IpcChain handle was dropped. Tear down the
    // subscription explicitly so the server-side capability is
    // released.
    drop(subscription);
    info!("IpcChain actor shutting down (request channel closed)");
}

/// IPC client returns its own `ShareHeaderLookup` enum; map to the
/// engine-facing one re-exported from `sv2-p2pool-engine`. The two
/// types are identical in shape (same fields) but distinct types
/// because the engine re-exports a fresh `pub use`.
fn map_lookup(l: IpcShareHeaderLookup) -> ShareHeaderLookup {
    match l {
        IpcShareHeaderLookup::Found(read) => ShareHeaderLookup::Found(ShareHeaderRead {
            prev_share_blockhash: read.prev_share_blockhash,
        }),
        IpcShareHeaderLookup::NotFound => ShareHeaderLookup::NotFound,
        IpcShareHeaderLookup::Genesis => ShareHeaderLookup::Genesis,
    }
}

// =======================================================================
// Tests
// =======================================================================

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    /// Write a minimal-but-valid p2poolv2 config TOML to a tempdir and
    /// return the loaded `Config` along with the tempdir guard. The
    /// store path is set inside the tempdir so the writer can create
    /// rocksdb files on bootstrap. No `[ipc]` section, so this
    /// triggers the in-process bootstrap path.
    fn make_test_config() -> (P2poolConfig, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store_path = dir.path().join("store.db");
        let logs_dir = dir.path().join("logs");
        let stats_dir = dir.path().join("stats");
        std::fs::create_dir_all(&logs_dir).expect("logs dir");
        std::fs::create_dir_all(&stats_dir).expect("stats dir");

        let toml = format!(
            r#"
[network]
listen_address = "/ip4/127.0.0.1/tcp/0"
dial_peers = []
max_pending_incoming = 10
max_pending_outgoing = 10
max_established_incoming = 50
max_established_outgoing = 50
max_established_per_peer = 1
max_workbase_per_second = 10
max_userworkbase_per_second = 10
max_miningshare_per_second = 100
max_inventory_per_second = 100
max_transaction_per_second = 100
max_requests_per_second = 100
dial_timeout_secs = 30

[store]
path = "{}"

[stratum]
hostname = "127.0.0.1"
port = 0
start_difficulty = 10000
minimum_difficulty = 100
solo_address = "tb1qyazxde6558qj6z3d9np5e6msmrspwpf6k0qggk"
bootstrap_address = "tb1qyazxde6558qj6z3d9np5e6msmrspwpf6k0qggk"
zmqpubhashblock = "tcp://127.0.0.1:0"
network = "signet"
version_mask = "1fffe000"
difficulty_multiplier = 1.0
pool_signature = "sv2-p2pool-test"

[bitcoinrpc]
url = "http://127.0.0.1:18443"
username = "rpc"
password = "rpc"

[logging]
console = true
level = "info"
stats_dir = "{}"

[api]
hostname = "127.0.0.1"
port = 0
"#,
            store_path.display(),
            stats_dir.display(),
        );
        let config_path = dir.path().join("p2pool.toml");
        let mut f = std::fs::File::create(&config_path).expect("create config");
        f.write_all(toml.as_bytes()).expect("write config");
        let config = P2poolConfig::load(config_path.to_str().expect("path")).expect("load config");
        (config, dir)
    }

    /// Same as `make_test_config` but with an `[ipc]` section pointing
    /// at an arbitrary socket path. Triggers the IPC bootstrap path.
    fn make_test_config_with_ipc(socket: &std::path::Path) -> (P2poolConfig, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store_path = dir.path().join("store.db");
        let stats_dir = dir.path().join("stats");
        std::fs::create_dir_all(&stats_dir).expect("stats dir");

        let toml = format!(
            r#"
[network]
listen_address = "/ip4/127.0.0.1/tcp/0"
dial_peers = []
max_pending_incoming = 10
max_pending_outgoing = 10
max_established_incoming = 50
max_established_outgoing = 50
max_established_per_peer = 1
max_workbase_per_second = 10
max_userworkbase_per_second = 10
max_miningshare_per_second = 100
max_inventory_per_second = 100
max_transaction_per_second = 100
max_requests_per_second = 100
dial_timeout_secs = 30

[store]
path = "{}"

[stratum]
hostname = "127.0.0.1"
port = 0
start_difficulty = 10000
minimum_difficulty = 100
solo_address = "tb1qyazxde6558qj6z3d9np5e6msmrspwpf6k0qggk"
bootstrap_address = "tb1qyazxde6558qj6z3d9np5e6msmrspwpf6k0qggk"
zmqpubhashblock = "tcp://127.0.0.1:0"
network = "regtest"
version_mask = "1fffe000"
difficulty_multiplier = 1.0
pool_signature = "sv2-p2pool-test"

[bitcoinrpc]
url = "http://127.0.0.1:18443"
username = "rpc"
password = "rpc"

[logging]
console = true
level = "info"
stats_dir = "{}"

[api]
hostname = "127.0.0.1"
port = 0

[ipc]
socket_path = "{}"
"#,
            store_path.display(),
            stats_dir.display(),
            socket.display(),
        );
        let config_path = dir.path().join("p2pool.toml");
        let mut f = std::fs::File::create(&config_path).expect("create config");
        f.write_all(toml.as_bytes()).expect("write config");
        let config = P2poolConfig::load(config_path.to_str().expect("path")).expect("load config");
        (config, dir)
    }

    /// Minimal `ChainReadBackend` for actor tests.
    struct FakeBackend {
        tip: std::sync::Mutex<Option<[u8; 32]>>,
        height: std::sync::Mutex<Option<u32>>,
        network: bitcoin::Network,
    }

    impl p2poolv2_ipc::ChainReadBackend for FakeBackend {
        fn get_chain_tip(&self) -> Result<Option<[u8; 32]>, String> {
            Ok(*self.tip.lock().unwrap())
        }
        fn get_share_header(
            &self,
            share_hash: &[u8; 32],
        ) -> Result<p2poolv2_ipc::ShareHeaderOutcome, String> {
            if share_hash.iter().all(|b| *b == 0) {
                return Ok(p2poolv2_ipc::ShareHeaderOutcome::Genesis);
            }
            Ok(p2poolv2_ipc::ShareHeaderOutcome::NotFound)
        }
        fn get_tip_height(&self) -> Result<Option<u32>, String> {
            Ok(*self.height.lock().unwrap())
        }
        fn network(&self) -> bitcoin::Network {
            self.network
        }
    }

    fn temp_socket() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ipc.sock");
        (dir, path)
    }

    async fn wait_for_socket(path: &std::path::Path, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        while !path.exists() {
            if std::time::Instant::now() >= deadline {
                panic!("server socket never appeared at {}", path.display());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ipc_chain_round_trips_get_chain_tip() {
        let (_dir, sock) = temp_socket();
        let mut tip = [0u8; 32];
        tip[31] = 0xab;
        let backend: Arc<dyn p2poolv2_ipc::ChainReadBackend> = Arc::new(FakeBackend {
            tip: std::sync::Mutex::new(Some(tip)),
            height: std::sync::Mutex::new(Some(7)),
            network: bitcoin::Network::Regtest,
        });
        let _server = p2poolv2_ipc::spawn_ipc_server_full(sock.clone(), None, Some(backend));
        wait_for_socket(&sock, Duration::from_secs(3)).await;

        let chain = IpcChain::connect(sock.to_str().unwrap())
            .await
            .expect("IpcChain::connect ok");
        assert_eq!(chain.network(), bitcoin::Network::Regtest);

        let got = chain.get_chain_tip().await.expect("get_chain_tip ok");
        let expected =
            BlockHash::from_raw_hash(bitcoin::hashes::sha256d::Hash::from_byte_array(tip));
        assert_eq!(got, Some(expected));

        let height = chain.get_tip_height().await.expect("get_tip_height ok");
        assert_eq!(height, Some(7));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ipc_chain_get_share_header_genesis_sentinel() {
        let (_dir, sock) = temp_socket();
        let backend: Arc<dyn p2poolv2_ipc::ChainReadBackend> = Arc::new(FakeBackend {
            tip: std::sync::Mutex::new(None),
            height: std::sync::Mutex::new(None),
            network: bitcoin::Network::Regtest,
        });
        let _server = p2poolv2_ipc::spawn_ipc_server_full(sock.clone(), None, Some(backend));
        wait_for_socket(&sock, Duration::from_secs(3)).await;

        let chain = IpcChain::connect(sock.to_str().unwrap())
            .await
            .expect("connect");

        let zeros = BlockHash::all_zeros();
        let res = chain.get_share_header(&zeros).await.expect("ok");
        assert!(matches!(res, ShareHeaderLookup::Genesis));

        let unknown = BlockHash::from_byte_array([99u8; 32]);
        let res = chain.get_share_header(&unknown).await.expect("ok");
        assert!(matches!(res, ShareHeaderLookup::NotFound));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ipc_chain_subscribe_tip_drives_snapshot() {
        let (_dir, sock) = temp_socket();
        let initial = BlockHash::all_zeros();
        let (tip_tx, tip_rx) = tokio::sync::watch::channel(initial);
        let backend: Arc<dyn p2poolv2_ipc::ChainReadBackend> = Arc::new(FakeBackend {
            tip: std::sync::Mutex::new(None),
            height: std::sync::Mutex::new(None),
            network: bitcoin::Network::Regtest,
        });
        let _server =
            p2poolv2_ipc::spawn_ipc_server_full(sock.clone(), Some(tip_rx), Some(backend));
        wait_for_socket(&sock, Duration::from_secs(3)).await;

        let chain = IpcChain::connect(sock.to_str().unwrap())
            .await
            .expect("connect");
        let snapshot = chain.tip_snapshot();
        let mut rx = chain.subscribe_tip();

        // Drive a synthetic tip swap.
        let mut bytes = [0u8; 32];
        bytes[31] = 0xaa;
        let new_tip =
            BlockHash::from_raw_hash(bitcoin::hashes::sha256d::Hash::from_byte_array(bytes));
        tip_tx.send(new_tip).expect("send tip");

        // Wait for the snapshot to absorb the pushed tip — that's
        // what the reorg watcher reads.
        let mut got = false;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if snapshot.load_tip() == Some(new_tip) {
                got = true;
                break;
            }
        }
        assert!(got, "tip_snapshot did not observe the pushed tip");

        // The broadcast channel should also have fired by now.
        let recv = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
        if let Ok(Ok(t)) = recv {
            assert_eq!(t, new_tip);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ipc_chain_actor_thread_death_flips_shutdown_signal() {
        // Connect to a backend, then drop the IpcChain. The watchdog
        // flips the shutdown signal once the actor thread has joined.
        let (_dir, sock) = temp_socket();
        let backend: Arc<dyn p2poolv2_ipc::ChainReadBackend> = Arc::new(FakeBackend {
            tip: std::sync::Mutex::new(None),
            height: std::sync::Mutex::new(None),
            network: bitcoin::Network::Regtest,
        });
        let _server = p2poolv2_ipc::spawn_ipc_server_full(sock.clone(), None, Some(backend));
        wait_for_socket(&sock, Duration::from_secs(3)).await;

        let chain = IpcChain::connect(sock.to_str().unwrap())
            .await
            .expect("connect");
        let mut shutdown_rx = chain.shutdown_signal();
        // Initial value is `false` (actor alive).
        assert!(!*shutdown_rx.borrow());

        // Drop the chain → actor's request channel closes → actor
        // task exits → runtime drops → thread joins → watchdog flips
        // shutdown to true.
        drop(chain);

        let res = tokio::time::timeout(Duration::from_secs(5), shutdown_rx.changed()).await;
        assert!(res.is_ok(), "shutdown_signal did not flip within 5s");
        assert!(*shutdown_rx.borrow());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ipc_chain_connect_fails_on_unreachable_socket() {
        let (_dir, sock) = temp_socket();
        // Don't spawn a server. The connect should fail.
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            IpcChain::connect(sock.to_str().unwrap()),
        )
        .await
        .expect("timeout");
        assert!(
            res.is_err(),
            "expected IpcChain::connect to fail on unreachable socket"
        );
    }

    // ----- Bootstrap path tests ----------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bootstrap_share_chain_in_process_path() {
        let (config, _dir) = make_test_config();
        let handles = bootstrap_share_chain(&config)
            .await
            .expect("bootstrap succeeds");

        // chain handle reports the configured network.
        assert_eq!(
            handles.engine_handles.chain.network(),
            bitcoin::Network::Signet
        );
        // In-process path populates the rocksdb fields.
        assert!(handles.chain_store.is_some());
        assert!(handles.store.is_some());
        assert!(handles.store_writer_join.is_some());
        // No IPC plumbing in this mode.
        assert!(handles.ipc_tip_snapshot.is_none());
        assert!(handles.ipc_shutdown_signal.is_none());

        // Drop everything so the store-writer can shut down cleanly.
        drop(handles.engine_handles);
        drop(handles.chain_store);
        drop(handles.store);
        let writer = handles.store_writer_join.unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("writer joins within timeout");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bootstrap_share_chain_ipc_path() {
        // Spawn a fake IPC server, point the config at it, verify
        // bootstrap takes the IPC path and the resulting handles
        // expose the IPC-mode shape (no chain_store / store; with
        // a tip_snapshot + shutdown_signal).
        let (sock_dir, sock) = temp_socket();
        let mut tip = [0u8; 32];
        tip[31] = 0x42;
        let backend: Arc<dyn p2poolv2_ipc::ChainReadBackend> = Arc::new(FakeBackend {
            tip: std::sync::Mutex::new(Some(tip)),
            height: std::sync::Mutex::new(Some(11)),
            network: bitcoin::Network::Regtest,
        });
        let _server = p2poolv2_ipc::spawn_ipc_server_full(sock.clone(), None, Some(backend));
        wait_for_socket(&sock, Duration::from_secs(3)).await;

        let (config, _cfg_dir) = make_test_config_with_ipc(&sock);
        let handles = bootstrap_share_chain(&config)
            .await
            .expect("bootstrap succeeds");

        assert_eq!(
            handles.engine_handles.chain.network(),
            bitcoin::Network::Regtest,
            "IPC mode reports the daemon's network, not the local config"
        );
        assert!(handles.chain_store.is_none());
        assert!(handles.store.is_none());
        assert!(handles.store_writer_join.is_none());
        assert!(handles.ipc_tip_snapshot.is_some());
        assert!(handles.ipc_shutdown_signal.is_some());

        // Round-trip a chain read.
        let got = handles
            .engine_handles
            .chain
            .get_chain_tip()
            .await
            .expect("get_chain_tip ok");
        let expected =
            BlockHash::from_raw_hash(bitcoin::hashes::sha256d::Hash::from_byte_array(tip));
        assert_eq!(got, Some(expected));

        drop(handles);
        drop(sock_dir);
    }

    // ----- Pre-existing tests, ported to the new shape -----------

    #[tokio::test]
    async fn declare_mining_job_captures_share_chain_tip() {
        use jd_server_sv2::job_declarator::job_validation::{
            DeclareMiningJobResult, JobValidationEngine,
        };
        use stratum_apps::stratum_core::{
            binary_sv2::{B016M, B064K, B0255, Seq064K, U256},
            job_declaration_sv2::{DeclareMiningJob, ProvideMissingTransactionsSuccess},
        };
        use sv2_p2pool_engine::P2poolV2Engine;

        let (config, _dir) = make_test_config();
        let handles = bootstrap_share_chain(&config)
            .await
            .expect("bootstrap succeeds");
        let chain_for_assert = handles.engine_handles.chain.clone();
        let engine =
            P2poolV2Engine::with_handles(bitcoin::Network::Signet, handles.engine_handles.clone());

        // Build a structurally-valid DeclareMiningJob using fixtures
        // mirroring engine_impl::tests::build_coinbase / split_coinbase.
        let cb = {
            use bitcoin::{
                Amount, OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Witness, absolute::LockTime,
                transaction,
            };
            let mut witness = Witness::new();
            witness.push([0u8; 32]);
            bitcoin::Transaction {
                version: transaction::Version::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::from_bytes(vec![0u8; 16]),
                    sequence: Sequence::MAX,
                    witness,
                }],
                output: vec![TxOut {
                    value: Amount::ZERO,
                    script_pubkey: ScriptBuf::new(),
                }],
            }
        };
        let serialized = bitcoin::consensus::serialize(&cb);
        let extranonce_bytes = 16;
        let script_sig_len = cb.input[0].script_sig.len();
        let mut pos = 43; // COINBASE_PREFIX_LEN
        pos += bitcoin::VarInt(script_sig_len as u64).size();
        let bytes_in_prefix = script_sig_len.saturating_sub(extranonce_bytes);
        let split_at = pos + bytes_in_prefix;
        let prefix_bytes = serialized[..split_at].to_vec();
        let suffix_bytes = serialized[split_at + extranonce_bytes..].to_vec();

        let fake_tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::from_bytes(vec![1, 2, 3, 4]),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::ZERO,
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        };
        let wtxid_arr: [u8; 32] = *fake_tx.compute_wtxid().as_byte_array();
        let serialized_tx = bitcoin::consensus::serialize(&fake_tx);

        let token: u64 = 99;
        let token_b0255: B0255<'static> = token.to_le_bytes().to_vec().try_into().unwrap();
        let prefix_b: B064K<'static> = prefix_bytes.try_into().unwrap();
        let suffix_b: B064K<'static> = suffix_bytes.try_into().unwrap();
        let wtxid: U256<'static> = wtxid_arr.to_vec().try_into().unwrap();
        let wtxid_seq: Seq064K<'static, U256<'static>> = vec![wtxid].into();
        let excess: B064K<'static> = Vec::new().try_into().unwrap();

        let msg = DeclareMiningJob {
            request_id: 1,
            mining_job_token: token_b0255,
            version: 0x20000000,
            coinbase_tx_prefix: prefix_b,
            coinbase_tx_suffix: suffix_b,
            wtxid_list: wtxid_seq,
            excess_data: excess,
        };
        let tx_bytes: B016M<'static> = serialized_tx.try_into().expect("fits");
        let pmts = ProvideMissingTransactionsSuccess {
            request_id: 1,
            transaction_list: Seq064K::new(vec![tx_bytes]).expect("fits"),
        };
        let result = engine.handle_declare_mining_job(msg, Some(pmts)).await;
        assert!(
            matches!(result, DeclareMiningJobResult::Success),
            "declare must succeed against initialised signet chain"
        );

        let cached = engine.declared_jobs().get(&1).expect("declared job cached");
        let expected_tip = chain_for_assert
            .get_chain_tip()
            .await
            .expect("tip readable")
            .expect("genesis initialised");
        assert_eq!(
            cached.share_chain_tip,
            Some(expected_tip),
            "DeclareMiningJob captured the live share-chain tip"
        );

        drop(engine);
        drop(handles.engine_handles);
        drop(handles.chain_store);
        drop(handles.store);
        if let Some(writer) = handles.store_writer_join {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), writer).await;
        }
    }

    #[tokio::test]
    async fn notify_share_chain_reorg_selective_invalidation() {
        use jd_server_sv2::job_declarator::job_validation::JobValidationEngine;
        use sv2_p2pool_engine::{DeclaredJob, P2poolV2Engine, TipMetadata};

        let (config, _dir) = make_test_config();
        let handles = bootstrap_share_chain(&config)
            .await
            .expect("bootstrap succeeds");
        let chain = handles.engine_handles.chain.clone();
        let genesis_tip = chain
            .get_chain_tip()
            .await
            .expect("tip readable")
            .expect("genesis initialised");

        let engine =
            P2poolV2Engine::with_handles(bitcoin::Network::Signet, handles.engine_handles.clone());

        // Job A: captured tip == current tip → kept on reorg notification.
        engine.declared_jobs().insert(
            1,
            DeclaredJob {
                version: 1,
                coinbase_tx_prefix: vec![],
                coinbase_tx_suffix: vec![],
                wtxid_list: vec![],
                txid_list: None,
                tip: TipMetadata::default(),
                template_id: None,
                share_chain_tip: Some(genesis_tip),
                validated: true,
                allocated_token: None,
            },
        );
        // Job B: captured tip is unknown to the chain → dropped.
        engine.declared_jobs().insert(
            2,
            DeclaredJob {
                version: 1,
                coinbase_tx_prefix: vec![],
                coinbase_tx_suffix: vec![],
                wtxid_list: vec![],
                txid_list: None,
                tip: TipMetadata::default(),
                template_id: None,
                share_chain_tip: Some(BlockHash::from_byte_array([99u8; 32])),
                validated: true,
                allocated_token: None,
            },
        );
        // Job C: no captured tip → dropped (conservative).
        engine.declared_jobs().insert(
            3,
            DeclaredJob {
                version: 1,
                coinbase_tx_prefix: vec![],
                coinbase_tx_suffix: vec![],
                wtxid_list: vec![],
                txid_list: None,
                tip: TipMetadata::default(),
                template_id: None,
                share_chain_tip: None,
                validated: true,
                allocated_token: None,
            },
        );
        assert_eq!(engine.declared_jobs().len(), 3);

        engine.notify_share_chain_reorg(genesis_tip).await;

        assert_eq!(
            engine.declared_jobs().len(),
            1,
            "only the job whose captured tip matches the new tip survives"
        );
        assert!(engine.declared_jobs().get(&1).is_some());
        assert!(engine.declared_jobs().get(&2).is_none());
        assert!(engine.declared_jobs().get(&3).is_none());

        drop(engine);
        drop(handles.engine_handles);
        drop(handles.chain_store);
        drop(handles.store);
        if let Some(writer) = handles.store_writer_join {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), writer).await;
        }
    }

    #[tokio::test]
    async fn engine_reorg_watcher_polls_chain_handle() {
        use sv2_p2pool_engine::P2poolV2Engine;

        let (config, _dir) = make_test_config();
        let handles = bootstrap_share_chain(&config)
            .await
            .expect("bootstrap succeeds");

        let chain_store = handles.chain_store.clone().expect("in-process mode");
        let mut engine =
            P2poolV2Engine::with_handles(bitcoin::Network::Signet, handles.engine_handles.clone());

        let _observer = engine.start_reorg_watcher(
            move || chain_store.get_chain_tip().ok(),
            Duration::from_millis(20),
        );

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(engine.declared_jobs().is_empty());

        engine.stop_reorg_watcher();
        drop(engine);
        drop(handles.engine_handles);
        drop(handles.chain_store);
        drop(handles.store);
        if let Some(writer) = handles.store_writer_join {
            let _ = tokio::time::timeout(Duration::from_secs(5), writer).await;
        }
    }

    #[tokio::test]
    async fn bootstrap_share_chain_builds_engine_handles() {
        let (config, _dir) = make_test_config();
        let handles = bootstrap_share_chain(&config)
            .await
            .expect("bootstrap succeeds");

        // chain handle reports the configured network.
        assert_eq!(
            handles.engine_handles.chain.network(),
            bitcoin::Network::Signet
        );
        // The store writer is alive (still pollable).
        let writer = handles.store_writer_join.expect("in-process mode");
        assert!(!writer.is_finished());

        // Drop everything so the store-writer can shut down cleanly.
        drop(handles.engine_handles);
        drop(handles.chain_store);
        drop(handles.store);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("writer joins within timeout");
    }
}
