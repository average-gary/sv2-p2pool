//! Cap'n Proto client for talking to a p2poolv2 IPC daemon.
//!
//! The server lives at `vendor/p2poolv2/p2poolv2_ipc` and exposes a
//! single `ShareChain` interface defined in
//! `vendor/p2poolv2/p2poolv2-capnp-types/proto/p2poolv2.capnp`. This
//! crate provides a typed async client for the three methods the
//! schema currently defines:
//!
//! - [`Sv2P2poolIpcClient::validate_template`] — validate a candidate
//!   SV2 template against the share-chain tip.
//! - [`Sv2P2poolIpcClient::submit_solution`] — submit a solved block
//!   plus its share hash.
//! - [`Sv2P2poolIpcClient::subscribe_chain_tip`] — subscribe to tip
//!   changes (callback-style).
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
//! - [`Sv2P2poolIpcClient::submit_solution`] — server-side performs a
//!   real `shareHash == block_hash()` consistency check (deserialises
//!   the rawBlock, recomputes the hash, rejects on mismatch). Client
//!   surface unchanged.
//! - [`Sv2P2poolIpcClient::validate_template`] /
//!   [`Sv2P2poolIpcClient::subscribe_chain_tip`] — server-side still
//!   placeholder stubs awaiting a `ChainStoreHandle` plumbed into the
//!   IPC server, plus (for tip subscription) a tip-change broadcast
//!   channel inside `p2poolv2_lib::shares::chain` that does not yet
//!   exist.

#![forbid(unsafe_code)]

use std::path::Path;
use std::rc::Rc;

use capnp::capability::Promise;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use p2poolv2_capnp_types::p2poolv2_capnp::{chain_tip_callback, share_chain, validation_result};
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
    async fn validate_template_round_trips_against_stub() {
        let (_dir, sock) = temp_socket();
        let _server = p2poolv2_ipc::spawn_ipc_server(sock.clone());

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                wait_for_socket(&sock, Duration::from_secs(2)).await;
                let client = Sv2P2poolIpcClient::connect(&sock).await.expect("connect");

                // Stub returns Ok for any input — we just verify the
                // round-trip succeeds and the union decodes.
                let outcome = client
                    .validate_template(b"prefix", b"suffix", &[], &[])
                    .await
                    .expect("validate_template ok");
                match outcome {
                    ValidationOutcome::Ok
                    | ValidationOutcome::StaleChainTip
                    | ValidationOutcome::InvalidCoinbase(_)
                    | ValidationOutcome::MissingTransactions(_) => {
                        // Any decoded variant is acceptable — the stub's
                        // policy isn't this client's responsibility.
                    }
                }
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
}
