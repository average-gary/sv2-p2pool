//! `TdpHandle` — bridge between [`P2poolV2Engine`] and the SV2 Template
//! Distribution Protocol.
//!
//! The engine sits on the JDS side; ChannelManager sits on the Pool side.
//! TDP messages travel between Pool and Template Provider, but our engine
//! needs three things that live in TDP:
//!
//! - **Tip metadata** (`prev_hash`, `nbits`, `min_ntime`) — comes from
//!   `SetNewPrevHash`. ChannelManager already caches it as
//!   `last_new_prev_hash` (mod.rs:71-90).
//! - **Coinbase template metadata** — `NewTemplate` has it; ChannelManager
//!   caches as `last_future_template`.
//! - **Full transaction bodies** — fetched via
//!   `RequestTransactionData(template_id)` → `RequestTransactionDataSuccess.transaction_list`.
//!   The Pool→TP channel currently belongs to ChannelManager; nothing
//!   issues this request today.
//!
//! `TdpHandle` is the owned-by-engine struct that:
//!
//! 1. **Reads** snapshots of the latest `NewTemplate` and `SetNewPrevHash`
//!    via shared `Arc<RwLock<Option<...>>>` fields. The pool binary
//!    (Phase 2.5) populates these from incoming TP messages by tee'ing
//!    the existing `tp_to_channel_manager` stream.
//! 2. **Issues** `RequestTransactionData(template_id)` via a dedicated
//!    `Sender` into the Pool→TP channel.
//! 3. **Awaits** the matching `RequestTransactionDataSuccess` (or
//!    `RequestTransactionDataError`) by registering a per-`template_id`
//!    one-shot in `pending_requests` before sending. The pool binary's
//!    demux task delivers responses by removing the matching entry and
//!    sending into the one-shot.
//!
//! Phase 2.4 ships [`TdpHandle`] with a `MockTdp` for engine-level unit
//! tests; Phase 2.5 wires the real channel demux in `Pool::start`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bitcoin::{BlockHash, Transaction, hashes::Hash as _};
use stratum_apps::stratum_core::{
    parsers_sv2::TemplateDistribution,
    template_distribution_sv2::{
        NewTemplate, RequestTransactionData, RequestTransactionDataError,
        RequestTransactionDataSuccess, SetNewPrevHash,
    },
};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::TipMetadata;

/// Result of a `RequestTransactionData` exchange.
#[derive(Debug)]
pub enum TxDataResult {
    Success(RequestTransactionDataSuccess<'static>),
    Error(RequestTransactionDataError<'static>),
}

/// Default timeout for `request_tx_bodies`. The TP MUST respond
/// quickly; 5s is conservative for any network-tier deployment.
pub const DEFAULT_TX_BODIES_TIMEOUT: Duration = Duration::from_secs(5);

/// Sender side of a Pool→TP channel — used by the engine to push
/// `TemplateDistribution::RequestTransactionData` upstream. The pool
/// binary owns the matching receiver and forwards into the existing
/// TP-bound stream (`channel_manager_to_tp_sender`).
pub type TdpRequestSender = async_channel::Sender<TemplateDistribution<'static>>;

/// Bridge between [`crate::P2poolV2Engine`] and TDP.
///
/// Cloneable; cheap to share. The engine clones the handle once at
/// construction; the pool binary keeps a clone for the demux task.
#[derive(Clone)]
pub struct TdpHandle {
    inner: Arc<TdpHandleInner>,
}

struct TdpHandleInner {
    /// Pool→TP channel for issuing `RequestTransactionData`.
    tx_request_sender: TdpRequestSender,
    /// Pending request demux: keyed by `template_id`. The pool's
    /// demux task removes an entry when a response arrives and sends
    /// into the one-shot.
    pending_requests: Mutex<HashMap<u64, oneshot::Sender<TxDataResult>>>,
    /// Snapshot of the latest `SetNewPrevHash`. Updated by the pool
    /// binary's TP-message tee.
    last_prev_hash: RwLock<Option<SetNewPrevHash<'static>>>,
    /// Snapshot of the latest `NewTemplate`. Updated similarly.
    last_template: RwLock<Option<NewTemplate<'static>>>,
    /// Per-call timeout for [`TdpHandle::request_tx_bodies`].
    tx_bodies_timeout: Duration,
}

impl TdpHandle {
    /// Construct a new handle. The `tx_request_sender` is the engine's
    /// half of a Pool→TP channel; the pool binary owns the receiver and
    /// forwards onto the real TP wire.
    pub fn new(tx_request_sender: TdpRequestSender) -> Self {
        Self {
            inner: Arc::new(TdpHandleInner {
                tx_request_sender,
                pending_requests: Mutex::new(HashMap::new()),
                last_prev_hash: RwLock::new(None),
                last_template: RwLock::new(None),
                tx_bodies_timeout: DEFAULT_TX_BODIES_TIMEOUT,
            }),
        }
    }

    /// Override the default `request_tx_bodies` timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        // Only valid pre-clone; safe because we just allocated `inner`.
        let inner = Arc::get_mut(&mut self.inner)
            .expect("with_timeout called after clone — would mutate shared state");
        inner.tx_bodies_timeout = timeout;
        self
    }

    /// Snapshot the latest `(NewTemplate, SetNewPrevHash)` and project
    /// out `TipMetadata`. Returns `None` if either snapshot is missing
    /// (e.g. during early startup before the TP has sent us anything).
    pub fn current_tip(&self) -> Option<TipMetadata> {
        let prev_hash_msg = self.inner.last_prev_hash.read().ok()?.clone()?;
        let prev_hash = {
            let bytes: [u8; 32] = prev_hash_msg.prev_hash.to_vec().try_into().ok()?;
            BlockHash::from_byte_array(bytes)
        };
        Some(TipMetadata {
            prev_hash,
            nbits: prev_hash_msg.n_bits,
            min_ntime: prev_hash_msg.header_timestamp,
        })
    }

    /// Snapshot the most recent `template_id` we've received via
    /// `NewTemplate`. Used by `handle_declare_mining_job` to associate
    /// a declared job with its TP-side template id, so
    /// `request_tx_bodies` can later refer to it.
    pub fn current_template_id(&self) -> Option<u64> {
        self.inner
            .last_template
            .read()
            .ok()?
            .as_ref()
            .map(|t| t.template_id)
    }

    /// Issue `RequestTransactionData(template_id)` on the Pool→TP
    /// channel and await `RequestTransactionDataSuccess`. Returns the
    /// decoded, consensus-deserialized non-coinbase transaction list.
    pub async fn request_tx_bodies(&self, template_id: u64) -> Result<Vec<Transaction>, TdpError> {
        // 1. Register the per-template_id one-shot BEFORE sending,
        //    to avoid a race where the response arrives before we
        //    register.
        let (tx, rx) = oneshot::channel::<TxDataResult>();
        {
            let mut pending = self.inner.pending_requests.lock().expect("poisoned");
            if pending.contains_key(&template_id) {
                return Err(TdpError::DuplicateRequest(template_id));
            }
            pending.insert(template_id, tx);
        }

        // 2. Send the request.
        let request = RequestTransactionData { template_id };
        let send_result = self
            .inner
            .tx_request_sender
            .send(TemplateDistribution::RequestTransactionData(request))
            .await;
        if let Err(e) = send_result {
            // Clean up the pending entry on send failure.
            self.inner
                .pending_requests
                .lock()
                .expect("poisoned")
                .remove(&template_id);
            return Err(TdpError::SendFailed(e.to_string()));
        }

        // 3. Await the response (with timeout).
        let result = match tokio::time::timeout(self.inner.tx_bodies_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                // Sender dropped without sending — the demux task
                // failed. Clean up.
                self.inner
                    .pending_requests
                    .lock()
                    .expect("poisoned")
                    .remove(&template_id);
                return Err(TdpError::DemuxClosed);
            }
            Err(_) => {
                self.inner
                    .pending_requests
                    .lock()
                    .expect("poisoned")
                    .remove(&template_id);
                return Err(TdpError::Timeout {
                    template_id,
                    duration: self.inner.tx_bodies_timeout,
                });
            }
        };

        // 4. Decode the response.
        let success = match result {
            TxDataResult::Success(s) => s,
            TxDataResult::Error(e) => {
                let code = String::from_utf8_lossy(e.error_code.inner_as_ref()).into_owned();
                return Err(TdpError::TpRejected {
                    template_id,
                    error_code: code,
                });
            }
        };

        let mut txs = Vec::with_capacity(success.transaction_list.inner_as_ref().len());
        for tx_bytes in success.transaction_list.inner_as_ref() {
            let tx = bitcoin::consensus::Decodable::consensus_decode(&mut &tx_bytes[..])
                .map_err(|e| TdpError::DecodeFailed(e.to_string()))?;
            txs.push(tx);
        }
        Ok(txs)
    }

    /// Internal — the pool binary's demux task uses this to update the
    /// `last_prev_hash` snapshot on every incoming `SetNewPrevHash`.
    pub fn record_set_new_prev_hash(&self, msg: SetNewPrevHash<'static>) {
        if let Ok(mut guard) = self.inner.last_prev_hash.write() {
            *guard = Some(msg);
        }
    }

    /// Internal — the pool binary's demux task uses this to update the
    /// `last_template` snapshot on every incoming `NewTemplate`.
    pub fn record_new_template(&self, msg: NewTemplate<'static>) {
        if let Ok(mut guard) = self.inner.last_template.write() {
            *guard = Some(msg);
        }
    }

    /// Internal — the pool binary's demux task delivers responses by
    /// calling this. Removes the per-`template_id` one-shot and sends
    /// `result` into it. Returns `false` if no caller was waiting (the
    /// pool should drop the message in that case).
    pub fn deliver_response(&self, template_id: u64, result: TxDataResult) -> bool {
        let waiter = self
            .inner
            .pending_requests
            .lock()
            .expect("poisoned")
            .remove(&template_id);
        match waiter {
            Some(tx) => match tx.send(result) {
                Ok(()) => true,
                Err(_) => {
                    debug!(
                        template_id,
                        "deliver_response: receiver dropped before delivery"
                    );
                    false
                }
            },
            None => {
                warn!(
                    template_id,
                    "deliver_response: no waiter registered for template_id; dropping"
                );
                false
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum TdpError {
    #[error("a request for template_id={0} is already in-flight")]
    DuplicateRequest(u64),
    #[error("failed to send RequestTransactionData on Pool→TP channel: {0}")]
    SendFailed(String),
    #[error("response demux closed before responding")]
    DemuxClosed,
    #[error("template_id={template_id} did not respond within {duration:?}")]
    Timeout {
        template_id: u64,
        duration: Duration,
    },
    #[error("TP rejected RequestTransactionData(template_id={template_id}): {error_code}")]
    TpRejected {
        template_id: u64,
        error_code: String,
    },
    #[error("failed to decode transaction body: {0}")]
    DecodeFailed(String),
}

#[cfg(test)]
mod tests {
    use stratum_apps::stratum_core::binary_sv2::{Seq064K, U256};

    use super::*;

    fn dummy_set_new_prev_hash(
        prev_hash: [u8; 32],
        nbits: u32,
        min_ntime: u32,
    ) -> SetNewPrevHash<'static> {
        let prev_hash: U256<'static> = prev_hash.to_vec().try_into().expect("32 bytes");
        let target: U256<'static> = [0u8; 32].to_vec().try_into().expect("32 bytes");
        SetNewPrevHash {
            template_id: 1,
            prev_hash,
            header_timestamp: min_ntime,
            n_bits: nbits,
            target,
        }
    }

    #[test]
    fn current_tip_returns_none_until_set_new_prev_hash() {
        let (tx, _rx) = async_channel::unbounded();
        let handle = TdpHandle::new(tx);
        assert!(handle.current_tip().is_none());
    }

    #[test]
    fn current_tip_reflects_recorded_set_new_prev_hash() {
        let (tx, _rx) = async_channel::unbounded();
        let handle = TdpHandle::new(tx);
        let prev = dummy_set_new_prev_hash([7u8; 32], 0x207fffff, 1234567890);
        handle.record_set_new_prev_hash(prev);

        let tip = handle.current_tip().expect("tip set");
        assert_eq!(tip.nbits, 0x207fffff);
        assert_eq!(tip.min_ntime, 1234567890);
        assert_eq!(tip.prev_hash.as_byte_array(), &[7u8; 32]);
    }

    #[tokio::test]
    async fn request_tx_bodies_round_trips_via_demux() {
        let (req_tx, req_rx) = async_channel::unbounded();
        let handle = TdpHandle::new(req_tx);

        // Spawn a stub TP demux: when a request arrives, deliver an
        // empty Success.
        let handle_clone = handle.clone();
        let demux = tokio::spawn(async move {
            let req = req_rx.recv().await.expect("request received");
            let template_id = match req {
                TemplateDistribution::RequestTransactionData(r) => r.template_id,
                _ => panic!("unexpected message variant"),
            };
            let success = RequestTransactionDataSuccess {
                template_id,
                excess_data: Vec::<u8>::new().try_into().expect("empty fits"),
                transaction_list: Seq064K::new(Vec::new()).expect("empty fits"),
            };
            handle_clone.deliver_response(template_id, TxDataResult::Success(success));
        });

        let txs = handle
            .request_tx_bodies(42)
            .await
            .expect("request succeeds");
        assert!(txs.is_empty());
        demux.await.expect("demux completes");
    }

    #[tokio::test(start_paused = true)]
    async fn request_tx_bodies_times_out_when_no_response() {
        let (req_tx, _req_rx) = async_channel::unbounded();
        let handle = TdpHandle::new(req_tx).with_timeout(Duration::from_millis(50));
        let req = handle.request_tx_bodies(99);
        // Drive past the timeout.
        tokio::time::advance(Duration::from_millis(60)).await;
        let err = req.await.unwrap_err();
        assert!(matches!(
            err,
            TdpError::Timeout {
                template_id: 99,
                ..
            }
        ));
    }
}
