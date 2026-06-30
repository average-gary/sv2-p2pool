//! Cap'n Proto client for talking to a p2poolv2 IPC daemon.
//!
//! The server lives at `vendor/p2poolv2/p2poolv2_ipc` and exposes a
//! single `ShareChain` interface defined in
//! `vendor/p2poolv2/p2poolv2-capnp-types/proto/p2poolv2.capnp`. This
//! crate provides a typed async client for the seven methods the
//! schema currently defines:
//!
//! - [`Sv2P2poolIpcClient::validate_template`] — validate a candidate
//!   SV2 template against the share-chain tip.
//! - [`Sv2P2poolIpcClient::submit_solution`] — submit a solved block
//!   plus its share hash.
//! - [`Sv2P2poolIpcClient::subscribe_chain_tip`] — subscribe to tip
//!   changes (callback-style).
//! - [`Sv2P2poolIpcClient::get_chain_tip`] — read the current
//!   confirmed share-chain tip blockhash.
//! - [`Sv2P2poolIpcClient::get_share_header`] — look up a share
//!   header by its blockhash; returns the minimal `prev_share_blockhash`
//!   field plus discrete `NotFound` / `Genesis` variants.
//! - [`Sv2P2poolIpcClient::get_tip_height`] — read the confirmed
//!   share-chain tip height.
//! - [`Sv2P2poolIpcClient::get_network`] — read the bitcoin network
//!   the daemon was configured with (called once at startup).
//!
//! ## `!Send` constraint
//!
//! `capnp-rpc` is single-threaded by design — the [`RpcSystem`] driver
//! is `!Send` and must run on a [`tokio::task::LocalSet`]. The same
//! applies here: build the client inside `LocalSet::run_until` (or
//! similar) and don't try to share it across threads.
//!
//! ## License boundary (ADR 0010)
//!
//! The schema crate `p2poolv2-capnp-types` is dual-licensed
//! `MIT OR Apache-2.0`, so this client can take a path dep on it
//! without inheriting the AGPL of the p2poolv2 daemon.
//!
//! ## Status
//!
//! All seven IPC server methods now perform real work:
//!
//! - [`Sv2P2poolIpcClient::validate_template`] — structural pre-check
//!   (glues prefix+suffix and confirms it parses as a
//!   `bitcoin::Transaction`). Returns `InvalidCoinbase(<reason>)` on
//!   parse failure, `Ok` otherwise. Full share-chain admission
//!   (coinbase value, wtxid commitment) still requires a
//!   `ChainStoreHandle` plumbed into the daemon.
//! - [`Sv2P2poolIpcClient::submit_solution`] — real
//!   `shareHash == block_hash()` consistency check (deserialises the
//!   rawBlock, recomputes the hash, rejects on mismatch).
//! - [`Sv2P2poolIpcClient::subscribe_chain_tip`] — fans out tip
//!   changes from a `tokio::sync::watch::Receiver<BlockHash>` when
//!   the daemon launched the server with
//!   `spawn_ipc_server_with_tip_source(path, Some(rx))`. Without a
//!   wired tip source, the server preserves the original stub
//!   behaviour (subscriptions accepted but never fire).
//! - [`Sv2P2poolIpcClient::get_chain_tip`] /
//!   [`Sv2P2poolIpcClient::get_share_header`] /
//!   [`Sv2P2poolIpcClient::get_tip_height`] /
//!   [`Sv2P2poolIpcClient::get_network`] — delegate to the daemon's
//!   in-process `ChainStoreHandle` via the new `ChainReadBackend`
//!   adapter (added in the matching schema bump). On a daemon with
//!   no chain backend wired they return `IpcClientError::Capnp`
//!   carrying `capnp::Error::unimplemented`.

#![forbid(unsafe_code)]

use std::path::Path;
use std::rc::Rc;

use bitcoin::BlockHash;
use bitcoin::hashes::Hash as _;
use capnp::capability::Promise;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use p2poolv2_capnp_types::p2poolv2_capnp::{
    chain_tip_callback, chain_tip_result, network_result, share_chain, share_header_result,
    tip_height_result, validation_result,
};
use thiserror::Error;
use tokio::net::UnixStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};

/// Errors emitted by the IPC client.
#[derive(Debug, Error)]
pub enum IpcClientError {
    #[error("failed to connect to UDS at {path:?}: {source}")]
    Connect {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("capnp error: {0}")]
    Capnp(#[from] capnp::Error),
    /// Server returned a `ValidationResult` discriminant the schema
    /// crate doesn't know about — version skew between client and
    /// server schema.
    #[error("unknown ValidationResult variant: {0}")]
    UnknownValidationVariant(#[from] capnp::NotInSchema),
    /// Server returned a payload that should have been a 32-byte
    /// `BlockHash` but wasn't. Indicates a buggy server or version
    /// skew at the schema layer.
    #[error("invalid blockhash payload: expected 32 bytes, got {got}")]
    BlockHashDecode { got: usize },
}

/// Outcome of a [`Sv2P2poolIpcClient::get_chain_tip`] call.
#[derive(Debug, Clone)]
pub enum ChainTipResult {
    /// Daemon returned a confirmed share-chain tip.
    Tip(BlockHash),
    /// Daemon has not yet completed genesis setup.
    Uninitialised,
}

/// Outcome of a [`Sv2P2poolIpcClient::get_tip_height`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TipHeightResult {
    /// Daemon returned a confirmed tip height.
    Height(u32),
    /// Daemon has not yet completed genesis setup.
    Uninitialised,
}

/// The minimal `ShareHeader` subset the engine consumes today.
///
/// The capnp wire only carries `prev_share_blockhash`. The other
/// thirteen fields on the daemon's `p2poolv2_lib::ShareHeader`
/// (uncles, miner_bitcoin_address, merkle_root, bitcoin_header,
/// bits, time, donation, donation_address, fee, fee_address,
/// coinbase_value, coinbaseaux_flags, witness_commitment,
/// bitcoin_height, coinbase_nsecs, extranonce) are deliberately not
/// serialised — adding any of them is a schema bump, not a
/// drive-by client change. See ADR 0011 in this repo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareHeaderRead {
    /// Previous share block hash. `None` when the daemon's encoded
    /// value is the all-zeros sentinel (genesis predecessor); the
    /// engine treats that as "stop walking ancestors".
    pub prev_share_blockhash: Option<BlockHash>,
}

/// Outcome of a [`Sv2P2poolIpcClient::get_share_header`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareHeaderLookup {
    /// Header found; the engine reads only `prev_share_blockhash`.
    Found(ShareHeaderRead),
    /// No header for the requested share hash. The engine treats
    /// this as a truncated walk and falls back to invalidate-all.
    NotFound,
    /// The requested hash is the all-zeros genesis sentinel; the
    /// engine stops walking ancestors at this point.
    Genesis,
}

// Convert capnp's text-decode errors into a Capnp error so the
// `validate_template` decode path can use `?` uniformly.
fn text_to_string(text: capnp::text::Reader<'_>) -> Result<String, capnp::Error> {
    text.to_string()
        .map_err(|e| capnp::Error::failed(format!("invalid utf-8 in InvalidCoinbase reason: {e}")))
}

/// Outcome of a [`Sv2P2poolIpcClient::validate_template`] call.
#[derive(Debug, Clone)]
pub enum ValidationOutcome {
    /// Server accepted the template against the current tip.
    Ok,
    /// Server's tip has moved since the template was built.
    StaleChainTip,
    /// Coinbase failed structural / consensus checks. Carries the
    /// server's error message verbatim.
    InvalidCoinbase(String),
    /// Server is missing tx bodies; sends back the wtxids the client
    /// must provide via the next call.
    MissingTransactions(Vec<Vec<u8>>),
}

/// Connected client. Wraps a [`share_chain::Client`] capability handle.
///
/// `Clone`-able cheaply (capnp client capabilities are reference-counted),
/// but `!Send` because the underlying [`RpcSystem`] driver runs in a
/// `LocalSet`. The driver is spawned automatically on construction; it
/// runs until the `Sv2P2poolIpcClient` (the last clone) is dropped.
#[derive(Clone)]
pub struct Sv2P2poolIpcClient {
    client: share_chain::Client,
}

impl Sv2P2poolIpcClient {
    /// Connect to a p2poolv2 IPC server at `path`. Spawns the
    /// [`RpcSystem`] driver on the current `LocalSet` so the
    /// returned client is immediately usable.
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, IpcClientError> {
        let path = path.as_ref();
        let stream = UnixStream::connect(path)
            .await
            .map_err(|source| IpcClientError::Connect {
                path: path.to_path_buf(),
                source,
            })?;
        info!(socket = %path.display(), "p2poolv2 IPC client connected");

        let (reader, writer) = stream.into_split();
        let reader = reader.compat();
        let writer = writer.compat_write();

        let network = twoparty::VatNetwork::new(
            reader,
            writer,
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        );
        let mut rpc_system = RpcSystem::new(Box::new(network), None);
        let client: share_chain::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        // Drive the RpcSystem in the background. capnp-rpc's RpcSystem
        // is !Send + a Future; spawn_local lives on a LocalSet and is
        // the only way to drive it.
        tokio::task::spawn_local(async move {
            if let Err(e) = rpc_system.await {
                warn!(error = %e, "RpcSystem driver exited");
            } else {
                debug!("RpcSystem driver exited cleanly");
            }
        });

        Ok(Self { client })
    }

    /// Call `validateTemplate` against the server. Returns the
    /// structured [`ValidationOutcome`].
    pub async fn validate_template(
        &self,
        coinbase_prefix: &[u8],
        coinbase_suffix: &[u8],
        wtxid_list: &[Vec<u8>],
        missing_txs: &[Vec<u8>],
    ) -> Result<ValidationOutcome, IpcClientError> {
        let mut req = self.client.validate_template_request();
        let mut params = req.get();
        params.set_coinbase_prefix(coinbase_prefix);
        params.set_coinbase_suffix(coinbase_suffix);
        {
            let mut wtxids = params.reborrow().init_wtxid_list(wtxid_list.len() as u32);
            for (i, wtxid) in wtxid_list.iter().enumerate() {
                wtxids.set(i as u32, wtxid);
            }
        }
        {
            let mut txs = params.init_missing_txs(missing_txs.len() as u32);
            for (i, tx) in missing_txs.iter().enumerate() {
                txs.set(i as u32, tx);
            }
        }
        let reply = req.send().promise.await?;
        let result = reply.get()?.get_result()?;
        decode_validation_result(result)
    }

    /// Call `submitSolution`. Returns the server's `accepted` flag.
    pub async fn submit_solution(
        &self,
        raw_block: &[u8],
        share_hash: &[u8],
    ) -> Result<bool, IpcClientError> {
        let mut req = self.client.submit_solution_request();
        let mut params = req.get();
        params.set_raw_block(raw_block);
        params.set_share_hash(share_hash);
        let reply = req.send().promise.await?;
        Ok(reply.get()?.get_accepted())
    }

    /// Call `subscribeChainTip`, registering `on_new_tip` to be invoked
    /// for every reported tip change. The callback is owned by the
    /// server-side capability; cancellation comes from dropping the
    /// returned [`TipSubscription`].
    pub async fn subscribe_chain_tip<F>(
        &self,
        on_new_tip: F,
    ) -> Result<TipSubscription, IpcClientError>
    where
        F: Fn(Vec<u8>) + 'static,
    {
        let mut req = self.client.subscribe_chain_tip_request();
        let cb_client: chain_tip_callback::Client = capnp_rpc::new_client(CallbackImpl {
            on_new_tip: Box::new(on_new_tip),
        });
        // Hold a clone so we can decide when the callback drops; the
        // server only sees the one inside the request.
        let retainer = cb_client.clone();
        req.get().set_callback(cb_client);
        let _reply = req.send().promise.await?;
        Ok(TipSubscription {
            _callback: retainer,
        })
    }

    /// Call `getChainTip`. Returns the confirmed share-chain tip
    /// blockhash, or `Uninitialised` if the daemon has not yet
    /// completed genesis setup.
    pub async fn get_chain_tip(&self) -> Result<ChainTipResult, IpcClientError> {
        let req = self.client.get_chain_tip_request();
        let reply = req.send().promise.await?;
        let result = reply.get()?.get_result()?;
        Ok(match result.which()? {
            chain_tip_result::Which::Tip(bytes) => {
                let bytes = bytes?;
                ChainTipResult::Tip(decode_block_hash(bytes)?)
            }
            chain_tip_result::Which::Uninitialised(()) => ChainTipResult::Uninitialised,
        })
    }

    /// Call `getShareHeader`. Returns the engine-relevant subset of
    /// the daemon's `ShareHeader` plus discrete `NotFound` and
    /// `Genesis` variants so the caller can distinguish a missing
    /// header (truncated walk) from the genesis sentinel.
    pub async fn get_share_header(
        &self,
        share_hash: &BlockHash,
    ) -> Result<ShareHeaderLookup, IpcClientError> {
        let mut req = self.client.get_share_header_request();
        // Bitcoin block hashes are little-endian on the wire we use
        // (raw 32-byte sha256d output). The IPC schema treats them
        // as opaque 32-byte payloads so the encoding matches.
        let bytes = *share_hash.as_raw_hash().as_byte_array();
        req.get().set_share_hash(&bytes);
        let reply = req.send().promise.await?;
        let result = reply.get()?.get_result()?;
        Ok(match result.which()? {
            share_header_result::Which::Found(found) => {
                let found = found?;
                let prev_bytes = found.get_prev_share_blockhash()?;
                let prev = decode_block_hash(prev_bytes)?;
                // Map the daemon's all-zeros sentinel encoding to
                // an explicit `None`, so engine-side reorg-walk
                // logic doesn't have to know about it.
                let prev = if prev.as_raw_hash().as_byte_array().iter().all(|b| *b == 0) {
                    None
                } else {
                    Some(prev)
                };
                ShareHeaderLookup::Found(ShareHeaderRead {
                    prev_share_blockhash: prev,
                })
            }
            share_header_result::Which::NotFound(()) => ShareHeaderLookup::NotFound,
            share_header_result::Which::Genesis(()) => ShareHeaderLookup::Genesis,
        })
    }

    /// Call `getTipHeight`. Returns the confirmed share-chain tip
    /// height, or `Uninitialised` when the daemon has not yet
    /// completed genesis setup.
    pub async fn get_tip_height(&self) -> Result<TipHeightResult, IpcClientError> {
        let req = self.client.get_tip_height_request();
        let reply = req.send().promise.await?;
        let result = reply.get()?.get_result()?;
        Ok(match result.which()? {
            tip_height_result::Which::Height(h) => TipHeightResult::Height(h),
            tip_height_result::Which::Uninitialised(()) => TipHeightResult::Uninitialised,
        })
    }

    /// Call `getNetwork`. Returns the bitcoin network the daemon
    /// was configured with. Expected to be called exactly once at
    /// startup and cached by the caller (the daemon does not
    /// support hot-swapping networks).
    ///
    /// An `unknown` discriminant from a future schema version is
    /// surfaced as an `IpcClientError::Capnp(capnp::Error::failed)`
    /// rather than silently mapped to a default — the engine and
    /// pool wiring rely on the network being one of the bitcoin-rs
    /// variants for share-chain configuration.
    pub async fn get_network(&self) -> Result<bitcoin::Network, IpcClientError> {
        let req = self.client.get_network_request();
        let reply = req.send().promise.await?;
        let result = reply.get()?.get_result()?;
        Ok(match result.which()? {
            network_result::Which::Mainnet(()) => bitcoin::Network::Bitcoin,
            network_result::Which::Testnet(()) => bitcoin::Network::Testnet,
            network_result::Which::Testnet4(()) => bitcoin::Network::Testnet4,
            network_result::Which::Regtest(()) => bitcoin::Network::Regtest,
            network_result::Which::Signet(()) => bitcoin::Network::Signet,
            network_result::Which::Unknown(()) => {
                return Err(IpcClientError::Capnp(capnp::Error::failed(
                    "getNetwork: server returned `unknown` variant".into(),
                )));
            }
        })
    }
}

/// Decode a 32-byte blockhash payload. Mismatched lengths are a
/// schema-level invariant violation and surface as
/// [`IpcClientError::BlockHashDecode`] rather than collapsing into a
/// generic capnp error.
fn decode_block_hash(bytes: &[u8]) -> Result<BlockHash, IpcClientError> {
    if bytes.len() != 32 {
        return Err(IpcClientError::BlockHashDecode { got: bytes.len() });
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(bytes);
    Ok(BlockHash::from_raw_hash(
        bitcoin::hashes::sha256d::Hash::from_byte_array(arr),
    ))
}

/// Callback registration token. Drop it to release the client-side
/// capability holder; the server-side subscription lifetime is
/// independent.
pub struct TipSubscription {
    _callback: chain_tip_callback::Client,
}

/// Decode the union-typed `ValidationResult` into our [`ValidationOutcome`].
fn decode_validation_result(
    result: validation_result::Reader<'_>,
) -> Result<ValidationOutcome, IpcClientError> {
    Ok(match result.which()? {
        validation_result::Which::Ok(()) => ValidationOutcome::Ok,
        validation_result::Which::StaleChainTip(()) => ValidationOutcome::StaleChainTip,
        validation_result::Which::InvalidCoinbase(reason) => {
            ValidationOutcome::InvalidCoinbase(text_to_string(reason?)?)
        }
        validation_result::Which::MissingTransactions(list) => {
            let list = list?;
            let mut out = Vec::with_capacity(list.len() as usize);
            for i in 0..list.len() {
                out.push(list.get(i)?.to_vec());
            }
            ValidationOutcome::MissingTransactions(out)
        }
    })
}

/// Wraps a closure into a `chain_tip_callback::Server` cap.
struct CallbackImpl {
    on_new_tip: Box<dyn Fn(Vec<u8>)>,
}

impl chain_tip_callback::Server for CallbackImpl {
    #[allow(refining_impl_trait_internal)]
    fn on_new_tip(
        self: Rc<Self>,
        params: chain_tip_callback::OnNewTipParams,
        _results: chain_tip_callback::OnNewTipResults,
    ) -> Promise<(), capnp::Error> {
        let bytes = match params.get().and_then(|p| p.get_new_tip_hash()) {
            Ok(b) => b.to_vec(),
            Err(e) => return Promise::err(e),
        };
        (self.on_new_tip)(bytes);
        Promise::ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Pick a free unix socket path inside a tempdir.
    fn temp_socket() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ipc.sock");
        (dir, path)
    }

    /// Wait for `path` to appear (the server creates the socket lazily).
    async fn wait_for_socket(path: &std::path::Path, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        while !path.exists() {
            if std::time::Instant::now() >= deadline {
                panic!("server socket never appeared at {}", path.display());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn validate_template_rejects_unparseable_coinbase_via_uds() {
        // Phase 2-B: validate_template performs a structural pre-check.
        // Garbage prefix+suffix bytes do NOT deserialize as a
        // bitcoin::Transaction, so the server must return
        // InvalidCoinbase(<reason>). End-to-end across UDS.
        let (_dir, sock) = temp_socket();
        let _server = p2poolv2_ipc::spawn_ipc_server(sock.clone());

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                wait_for_socket(&sock, Duration::from_secs(2)).await;
                let client = Sv2P2poolIpcClient::connect(&sock).await.expect("connect");

                let outcome = client
                    .validate_template(b"prefix", b"suffix", &[], &[])
                    .await
                    .expect("validate_template ok");
                match outcome {
                    ValidationOutcome::InvalidCoinbase(reason) => {
                        assert!(
                            reason.contains("did not parse"),
                            "expected reason text to mention parse failure; got {reason}"
                        );
                    }
                    other => panic!("expected InvalidCoinbase, got {other:?}"),
                }
            })
            .await;
    }

    #[tokio::test]
    async fn validate_template_accepts_real_coinbase_via_uds() {
        // Build a valid coinbase tx, split at midpoint, drive the RPC.
        // The server's structural pre-check must accept it (Ok variant).
        use bitcoin::hashes::Hash as _;

        let (_dir, sock) = temp_socket();
        let _server = p2poolv2_ipc::spawn_ipc_server(sock.clone());

        let coinbase = bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::from_bytes(vec![0u8; 16]),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(50_0000_0000),
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        };
        // Touch a hash trait method so the import isn't dead.
        let _ = coinbase.compute_txid().as_raw_hash().as_byte_array();
        let serialized = bitcoin::consensus::serialize(&coinbase);
        let split = serialized.len() / 2;
        let prefix = serialized[..split].to_vec();
        let suffix = serialized[split..].to_vec();

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                wait_for_socket(&sock, Duration::from_secs(2)).await;
                let client = Sv2P2poolIpcClient::connect(&sock).await.expect("connect");

                let outcome = client
                    .validate_template(&prefix, &suffix, &[], &[])
                    .await
                    .expect("validate_template ok");
                assert!(
                    matches!(outcome, ValidationOutcome::Ok),
                    "expected Ok for a parseable coinbase; got {outcome:?}"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn submit_solution_accepts_when_share_hash_matches_block_hash() {
        // Phase 2-B unblock: submit_solution is no longer a pure stub.
        // Server-side now deserialises rawBlock, recomputes block_hash,
        // and verifies it matches the claimed shareHash. This test
        // exercises that round-trip end-to-end across a Unix socket.
        use bitcoin::hashes::Hash as _;

        let (_dir, sock) = temp_socket();
        let _server = p2poolv2_ipc::spawn_ipc_server(sock.clone());

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                wait_for_socket(&sock, Duration::from_secs(2)).await;
                let client = Sv2P2poolIpcClient::connect(&sock).await.expect("connect");

                let block = bitcoin::Block {
                    header: bitcoin::blockdata::block::Header {
                        version: bitcoin::blockdata::block::Version::from_consensus(1),
                        prev_blockhash: bitcoin::BlockHash::from_raw_hash(
                            bitcoin::hashes::Hash::all_zeros(),
                        ),
                        merkle_root: bitcoin::TxMerkleNode::from_raw_hash(
                            bitcoin::hashes::Hash::all_zeros(),
                        ),
                        time: 1_700_000_000,
                        bits: bitcoin::CompactTarget::from_consensus(0x207fffff),
                        nonce: 42,
                    },
                    txdata: vec![],
                };
                let raw = bitcoin::consensus::serialize(&block);
                let block_hash = *block.block_hash().as_raw_hash().as_byte_array();

                let accepted = client
                    .submit_solution(&raw, &block_hash)
                    .await
                    .expect("submit_solution ok");
                assert!(accepted, "matching shareHash MUST be accepted");

                // And the rejection path: same block, wrong shareHash.
                let mut bad_share = block_hash;
                bad_share[0] ^= 0xff;
                let accepted_bad = client
                    .submit_solution(&raw, &bad_share)
                    .await
                    .expect("submit_solution ok");
                assert!(!accepted_bad, "shareHash != block_hash MUST be rejected");

                // And the deserialise-failure path: garbage rawBlock.
                let accepted_garbage = client
                    .submit_solution(b"definitely not a block", &block_hash)
                    .await
                    .expect("submit_solution ok");
                assert!(!accepted_garbage, "unparseable rawBlock MUST be rejected");
            })
            .await;
    }

    /// End-to-end of subscribe_chain_tip with an injected tip source:
    /// drive a tokio::sync::watch sender from the test side, observe
    /// the on_new_tip callback firing on the client side. The whole
    /// thing rides over a real Unix socket via spawn_ipc_server_with_tip_source.
    #[tokio::test]
    async fn subscribe_chain_tip_fans_out_via_uds_round_trip() {
        use std::sync::Arc;
        use std::sync::Mutex;
        use std::time::Duration;

        use bitcoin::hashes::Hash as _;
        use tokio::sync::watch;

        let (_dir, sock) = temp_socket();
        let initial = bitcoin::BlockHash::from_raw_hash(bitcoin::hashes::Hash::all_zeros());
        let (tip_tx, tip_rx) = watch::channel(initial);
        let _server = p2poolv2_ipc::spawn_ipc_server_with_tip_source(sock.clone(), Some(tip_rx));

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                wait_for_socket(&sock, Duration::from_secs(2)).await;
                let client = Sv2P2poolIpcClient::connect(&sock).await.expect("connect");

                let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
                let received_clone = Arc::clone(&received);
                let _sub = client
                    .subscribe_chain_tip(move |bytes| {
                        received_clone.lock().unwrap().push(bytes);
                    })
                    .await
                    .expect("subscribe ok");

                let mk_tip = |last: u8| {
                    let mut h = [0u8; 32];
                    h[31] = last;
                    bitcoin::BlockHash::from_raw_hash(
                        bitcoin::hashes::sha256d::Hash::from_byte_array(h),
                    )
                };
                let tip_a = mk_tip(0xaa);
                let want_a = tip_a.as_raw_hash().as_byte_array().to_vec();

                tip_tx.send(tip_a).expect("send a");

                // Drive the client + server LocalSets until the
                // callback observed tip_a (or give up after a bounded
                // wait). The IPC server runs on a separate thread/runtime
                // so plain yields are not enough — short sleeps too.
                let mut got_a = false;
                for _ in 0..200 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    if received.lock().unwrap().contains(&want_a) {
                        got_a = true;
                        break;
                    }
                }
                assert!(
                    got_a,
                    "expected on_new_tip callback to fire within 1s; got {:x?}",
                    received.lock().unwrap()
                );
            })
            .await;
    }

    /// In-memory `ChainReadBackend` for round-trip tests. Mirrors
    /// the shape of the real daemon adapter without requiring a
    /// running p2poolv2 store.
    struct FakeBackend {
        tip: Option<[u8; 32]>,
        height: Option<u32>,
        network: bitcoin::Network,
        headers: std::collections::HashMap<[u8; 32], [u8; 32]>,
    }

    impl p2poolv2_ipc::ChainReadBackend for FakeBackend {
        fn get_chain_tip(&self) -> Result<Option<[u8; 32]>, String> {
            Ok(self.tip)
        }
        fn get_share_header(
            &self,
            share_hash: &[u8; 32],
        ) -> Result<p2poolv2_ipc::ShareHeaderOutcome, String> {
            if share_hash.iter().all(|b| *b == 0) {
                return Ok(p2poolv2_ipc::ShareHeaderOutcome::Genesis);
            }
            match self.headers.get(share_hash) {
                Some(prev) => Ok(p2poolv2_ipc::ShareHeaderOutcome::Found {
                    prev_share_blockhash: *prev,
                }),
                None => Ok(p2poolv2_ipc::ShareHeaderOutcome::NotFound),
            }
        }
        fn get_tip_height(&self) -> Result<Option<u32>, String> {
            Ok(self.height)
        }
        fn network(&self) -> bitcoin::Network {
            self.network
        }
    }

    #[tokio::test]
    async fn get_chain_tip_round_trip_via_uds() {
        use std::sync::Arc;

        let (_dir, sock) = temp_socket();

        let mut tip = [0u8; 32];
        tip[31] = 0xab;
        let backend: Arc<dyn p2poolv2_ipc::ChainReadBackend> = Arc::new(FakeBackend {
            tip: Some(tip),
            height: Some(7),
            network: bitcoin::Network::Regtest,
            headers: Default::default(),
        });
        let _server =
            p2poolv2_ipc::spawn_ipc_server_full(sock.clone(), None, Some(backend));

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                wait_for_socket(&sock, Duration::from_secs(2)).await;
                let client = Sv2P2poolIpcClient::connect(&sock).await.expect("connect");

                match client.get_chain_tip().await.expect("get_chain_tip ok") {
                    ChainTipResult::Tip(h) => {
                        let bytes = *h.as_raw_hash().as_byte_array();
                        assert_eq!(bytes, tip, "expected tip bytes to round-trip exactly");
                    }
                    ChainTipResult::Uninitialised => panic!("expected Tip variant"),
                }

                match client.get_tip_height().await.expect("get_tip_height ok") {
                    TipHeightResult::Height(h) => assert_eq!(h, 7),
                    TipHeightResult::Uninitialised => panic!("expected Height"),
                }

                let net = client.get_network().await.expect("get_network ok");
                assert_eq!(net, bitcoin::Network::Regtest);
            })
            .await;
    }

    #[tokio::test]
    async fn get_chain_tip_uninitialised_when_no_genesis() {
        use std::sync::Arc;

        let (_dir, sock) = temp_socket();
        let backend: Arc<dyn p2poolv2_ipc::ChainReadBackend> = Arc::new(FakeBackend {
            tip: None,
            height: None,
            network: bitcoin::Network::Regtest,
            headers: Default::default(),
        });
        let _server =
            p2poolv2_ipc::spawn_ipc_server_full(sock.clone(), None, Some(backend));

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                wait_for_socket(&sock, Duration::from_secs(2)).await;
                let client = Sv2P2poolIpcClient::connect(&sock).await.expect("connect");

                let tip = client.get_chain_tip().await.expect("ok");
                assert!(matches!(tip, ChainTipResult::Uninitialised));
                let h = client.get_tip_height().await.expect("ok");
                assert_eq!(h, TipHeightResult::Uninitialised);
            })
            .await;
    }

    #[tokio::test]
    async fn get_share_header_three_variants_round_trip() {
        use std::sync::Arc;

        let (_dir, sock) = temp_socket();

        let mut h = [0u8; 32];
        h[31] = 0x11;
        let mut prev = [0u8; 32];
        prev[31] = 0x22;
        let mut headers = std::collections::HashMap::new();
        headers.insert(h, prev);
        let backend: Arc<dyn p2poolv2_ipc::ChainReadBackend> = Arc::new(FakeBackend {
            tip: None,
            height: None,
            network: bitcoin::Network::Regtest,
            headers,
        });
        let _server =
            p2poolv2_ipc::spawn_ipc_server_full(sock.clone(), None, Some(backend));

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                wait_for_socket(&sock, Duration::from_secs(2)).await;
                let client = Sv2P2poolIpcClient::connect(&sock).await.expect("connect");

                let h_hash = bitcoin::BlockHash::from_raw_hash(
                    bitcoin::hashes::sha256d::Hash::from_byte_array(h),
                );
                match client
                    .get_share_header(&h_hash)
                    .await
                    .expect("get_share_header ok")
                {
                    ShareHeaderLookup::Found(read) => {
                        let prev_hash = read.prev_share_blockhash.expect("non-zero prev");
                        let bytes = *prev_hash.as_raw_hash().as_byte_array();
                        assert_eq!(bytes, prev);
                    }
                    other => panic!("expected Found, got {other:?}"),
                }

                // Genesis sentinel: all-zeros input must produce
                // ShareHeaderLookup::Genesis at the wire level.
                let zeros = bitcoin::BlockHash::from_raw_hash(
                    bitcoin::hashes::sha256d::Hash::from_byte_array([0u8; 32]),
                );
                match client.get_share_header(&zeros).await.expect("ok") {
                    ShareHeaderLookup::Genesis => {}
                    other => panic!("expected Genesis, got {other:?}"),
                }

                // Unknown hash → NotFound.
                let mut bogus = [0u8; 32];
                bogus[0] = 0x99;
                let bogus_hash = bitcoin::BlockHash::from_raw_hash(
                    bitcoin::hashes::sha256d::Hash::from_byte_array(bogus),
                );
                match client.get_share_header(&bogus_hash).await.expect("ok") {
                    ShareHeaderLookup::NotFound => {}
                    other => panic!("expected NotFound, got {other:?}"),
                }
            })
            .await;
    }

    #[tokio::test]
    async fn get_chain_tip_unimplemented_without_backend() {
        // The Phase-2 stub server with no chain backend wired must
        // surface `unimplemented` from the new chain-read methods —
        // not silently succeed. Confirms the negative path so we
        // don't accidentally regress the daemon's expected wiring.
        let (_dir, sock) = temp_socket();
        let _server = p2poolv2_ipc::spawn_ipc_server(sock.clone());

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                wait_for_socket(&sock, Duration::from_secs(2)).await;
                let client = Sv2P2poolIpcClient::connect(&sock).await.expect("connect");
                let res = client.get_chain_tip().await;
                assert!(res.is_err(), "expected error from unwired backend");
            })
            .await;
    }
}
