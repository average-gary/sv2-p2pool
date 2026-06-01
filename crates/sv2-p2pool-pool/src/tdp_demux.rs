//! TDP demux + merge tasks bridging sv2-apps's CM↔TP channel pair to the
//! engine's [`TdpHandle`].
//!
//! ## Why
//!
//! sv2-apps's `Pool::start` wires a single bidirectional pair of
//! `async_channel<TemplateDistribution<'static>>` between ChannelManager and
//! the Template Provider:
//!
//! ```text
//!     ┌─────────────┐  cm_to_tp  ┌──────────┐
//!     │ChannelMgr   │ ─────────▶ │   TP     │
//!     │             │ ◀───────── │          │
//!     └─────────────┘  tp_to_cm  └──────────┘
//! ```
//!
//! Our engine sits *outside* ChannelManager (it's the JDS-side
//! `JobValidationEngine`). To let it see TDP messages we splice in two
//! tasks that:
//!
//! - **Tee** every `tp_to_cm` message into a parallel observer that
//!   updates `TdpHandle::record_set_new_prev_hash`/`record_new_template`
//!   and demuxes `RequestTransactionDataSuccess/Error` to in-flight
//!   one-shots via `TdpHandle::deliver_response`. CM still receives
//!   100% of the original stream — its behavior is unchanged.
//! - **Merge** the engine's outbound `RequestTransactionData` requests
//!   onto the existing `cm_to_tp` channel, so the TP sees a single
//!   stream of `TemplateDistribution` messages from the pool.
//!
//! ```text
//!                  ┌──────────────┐
//!                  │  TdpHandle   │
//!                  └──┬────────┬──┘
//!                snapshot   request_tx_bodies
//!                     │        │
//!     ┌─────────────┐ │ tee    │ merge ┌──────────┐
//!     │ChannelMgr   │ ◀─[tee]──── + ──▶│   TP     │
//!     │             │ ─[merge]────────▶│          │
//!     └─────────────┘                  └──────────┘
//! ```
//!
//! Both tasks exit when their input channels close (graceful shutdown).

use async_channel::{Receiver, Sender};
use stratum_apps::stratum_core::parsers_sv2::TemplateDistribution;
use sv2_p2pool_engine::TdpHandle;
use sv2_p2pool_engine::tdp::TxDataResult;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

/// Spawn the tee task: drains `tp_input` (the original TP→Pool stream),
/// updates the engine's TDP snapshots / delivers tx-data responses, and
/// forwards every message unchanged to `cm_output` (the existing
/// ChannelManager-bound receiver).
///
/// Returns the spawned task so callers can include it in their
/// `TaskManager`.
pub fn spawn_tp_to_cm_tee(
    tp_input: Receiver<TemplateDistribution<'static>>,
    cm_output: Sender<TemplateDistribution<'static>>,
    tdp: TdpHandle,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        debug!("tdp_demux: tp_to_cm tee task started");
        while let Ok(msg) = tp_input.recv().await {
            // Observe TDP-relevant messages BEFORE forwarding so the
            // engine sees fresh state before any downstream message
            // handler that might depend on the snapshot.
            match &msg {
                TemplateDistribution::SetNewPrevHash(snph) => {
                    trace!(template_id = snph.template_id, "tdp_demux: SetNewPrevHash");
                    tdp.record_set_new_prev_hash(snph.clone().into_static());
                }
                TemplateDistribution::NewTemplate(nt) => {
                    trace!(template_id = nt.template_id, "tdp_demux: NewTemplate");
                    tdp.record_new_template(nt.clone().into_static());
                }
                TemplateDistribution::RequestTransactionDataSuccess(s) => {
                    trace!(
                        template_id = s.template_id,
                        "tdp_demux: RequestTransactionDataSuccess"
                    );
                    tdp.deliver_response(
                        s.template_id,
                        TxDataResult::Success(s.clone().into_static()),
                    );
                }
                TemplateDistribution::RequestTransactionDataError(e) => {
                    trace!(
                        template_id = e.template_id,
                        "tdp_demux: RequestTransactionDataError"
                    );
                    tdp.deliver_response(
                        e.template_id,
                        TxDataResult::Error(e.clone().into_static()),
                    );
                }
                _ => {}
            }

            if cm_output.send(msg).await.is_err() {
                warn!("tdp_demux: cm_output closed; tee exiting");
                break;
            }
        }
        debug!("tdp_demux: tp_to_cm tee task exited");
    })
}

/// Spawn the merge task: forwards every message from both `cm_input`
/// (ChannelManager → TP) and `engine_input` (engine
/// `RequestTransactionData` requests) onto a single `tp_output` toward
/// the Template Provider. Exits when both inputs are closed.
pub fn spawn_cm_and_engine_to_tp_merge(
    cm_input: Receiver<TemplateDistribution<'static>>,
    engine_input: Receiver<TemplateDistribution<'static>>,
    tp_output: Sender<TemplateDistribution<'static>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        debug!("tdp_demux: cm+engine -> tp merge task started");
        loop {
            tokio::select! {
                msg = cm_input.recv() => {
                    match msg {
                        Ok(m) => {
                            if tp_output.send(m).await.is_err() {
                                warn!("tdp_demux: tp_output closed; merge exiting");
                                break;
                            }
                        }
                        Err(_) => {
                            // CM input closed; continue draining engine_input.
                            debug!("tdp_demux: cm_input closed");
                            // If both are closed, exit.
                            if engine_input.is_closed() {
                                break;
                            }
                            // Drain whatever's left from engine_input.
                            while let Ok(m) = engine_input.recv().await {
                                if tp_output.send(m).await.is_err() {
                                    break;
                                }
                            }
                            break;
                        }
                    }
                }
                msg = engine_input.recv() => {
                    match msg {
                        Ok(m) => {
                            if tp_output.send(m).await.is_err() {
                                warn!("tdp_demux: tp_output closed; merge exiting");
                                break;
                            }
                        }
                        Err(_) => {
                            debug!("tdp_demux: engine_input closed");
                            if cm_input.is_closed() {
                                break;
                            }
                            while let Ok(m) = cm_input.recv().await {
                                if tp_output.send(m).await.is_err() {
                                    break;
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
        debug!("tdp_demux: cm+engine -> tp merge task exited");
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_channel::unbounded;
    use stratum_apps::stratum_core::{
        binary_sv2::{Seq064K, Seq0255},
        template_distribution_sv2::{
            NewTemplate, RequestTransactionData, RequestTransactionDataSuccess, SetNewPrevHash,
        },
    };

    use super::*;

    fn build_set_new_prev_hash(template_id: u64) -> SetNewPrevHash<'static> {
        SetNewPrevHash {
            template_id,
            prev_hash: [9u8; 32].to_vec().try_into().expect("32 bytes"),
            header_timestamp: 1_700_000_000,
            n_bits: 0x207fffff,
            target: [0u8; 32].to_vec().try_into().expect("32 bytes"),
        }
    }

    fn build_new_template(template_id: u64) -> NewTemplate<'static> {
        NewTemplate {
            template_id,
            future_template: false,
            version: 0x20000000,
            coinbase_tx_version: 2,
            coinbase_prefix: Vec::<u8>::new().try_into().expect("empty fits"),
            coinbase_tx_input_sequence: 0xffff_ffff,
            coinbase_tx_value_remaining: 50_0000_0000,
            coinbase_tx_outputs_count: 0,
            coinbase_tx_outputs: Vec::<u8>::new().try_into().expect("empty fits"),
            coinbase_tx_locktime: 0,
            merkle_path: Seq0255::new(Vec::new()).expect("empty fits"),
        }
    }

    #[tokio::test]
    async fn tee_updates_tdp_and_forwards_to_cm() {
        let (tp_in_tx, tp_in_rx) = unbounded::<TemplateDistribution<'static>>();
        let (cm_out_tx, cm_out_rx) = unbounded::<TemplateDistribution<'static>>();
        let (req_tx, _req_rx) = unbounded();
        let tdp = TdpHandle::new(req_tx);

        let _h = spawn_tp_to_cm_tee(tp_in_rx, cm_out_tx, tdp.clone());

        // Push SetNewPrevHash + NewTemplate; assert TdpHandle reflects
        // both AND CM sees both forwarded.
        tp_in_tx
            .send(TemplateDistribution::SetNewPrevHash(
                build_set_new_prev_hash(42),
            ))
            .await
            .unwrap();
        tp_in_tx
            .send(TemplateDistribution::NewTemplate(build_new_template(42)))
            .await
            .unwrap();

        let _m1 = tokio::time::timeout(Duration::from_secs(1), cm_out_rx.recv())
            .await
            .expect("cm receives msg 1")
            .expect("not closed");
        let _m2 = tokio::time::timeout(Duration::from_secs(1), cm_out_rx.recv())
            .await
            .expect("cm receives msg 2")
            .expect("not closed");

        // TdpHandle now has snapshots.
        assert!(tdp.current_tip().is_some());
        assert_eq!(tdp.current_template_id(), Some(42));
    }

    #[tokio::test]
    async fn tee_delivers_request_tx_data_response_to_pending_oneshot() {
        let (tp_in_tx, tp_in_rx) = unbounded::<TemplateDistribution<'static>>();
        let (cm_out_tx, _cm_out_rx) = unbounded::<TemplateDistribution<'static>>();
        let (req_tx, _req_rx) = unbounded();
        let tdp = TdpHandle::new(req_tx).with_timeout(Duration::from_secs(2));

        let _h = spawn_tp_to_cm_tee(tp_in_rx, cm_out_tx, tdp.clone());

        // Caller awaits a request (registers a one-shot).
        let waiter_tdp = tdp.clone();
        let waiter = tokio::spawn(async move { waiter_tdp.request_tx_bodies(7).await });

        // Briefly wait for the waiter to register before injecting the
        // response. The request-side send won't have a real TP listener,
        // but it doesn't need to — we deliver the response directly via
        // the tee.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let success = RequestTransactionDataSuccess {
            template_id: 7,
            excess_data: Vec::<u8>::new().try_into().expect("empty fits"),
            transaction_list: Seq064K::new(Vec::new()).expect("empty fits"),
        };
        tp_in_tx
            .send(TemplateDistribution::RequestTransactionDataSuccess(success))
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter completes")
            .expect("task didn't panic");
        assert!(result.is_ok(), "request_tx_bodies returned: {result:?}");
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn merge_forwards_both_streams_to_tp() {
        let (cm_in_tx, cm_in_rx) = unbounded::<TemplateDistribution<'static>>();
        let (eng_in_tx, eng_in_rx) = unbounded::<TemplateDistribution<'static>>();
        let (tp_out_tx, tp_out_rx) = unbounded::<TemplateDistribution<'static>>();

        let _h = spawn_cm_and_engine_to_tp_merge(cm_in_rx, eng_in_rx, tp_out_tx);

        // Send one message from each input.
        cm_in_tx
            .send(TemplateDistribution::SetNewPrevHash(
                build_set_new_prev_hash(1),
            ))
            .await
            .unwrap();
        eng_in_tx
            .send(TemplateDistribution::RequestTransactionData(
                RequestTransactionData { template_id: 99 },
            ))
            .await
            .unwrap();

        let mut got_cm = false;
        let mut got_eng = false;
        for _ in 0..2 {
            let msg = tokio::time::timeout(Duration::from_secs(1), tp_out_rx.recv())
                .await
                .expect("tp receives msg")
                .expect("not closed");
            match msg {
                TemplateDistribution::SetNewPrevHash(_) => got_cm = true,
                TemplateDistribution::RequestTransactionData(r) => {
                    assert_eq!(r.template_id, 99);
                    got_eng = true;
                }
                _ => panic!("unexpected variant"),
            }
        }
        assert!(got_cm && got_eng, "both streams must reach TP");
    }

    #[tokio::test]
    async fn tee_handle_aborts_cleanly() {
        // Verifies the JoinHandle returned by spawn_tp_to_cm_tee can
        // be aborted: the task stops promptly and awaiting the handle
        // returns. Pool::start relies on this for graceful shutdown.
        let (_tp_in_tx, tp_in_rx) = unbounded::<TemplateDistribution<'static>>();
        let (cm_out_tx, _cm_out_rx) = unbounded::<TemplateDistribution<'static>>();
        let (req_tx, _req_rx) = unbounded();
        let tdp = TdpHandle::new(req_tx);
        let h = spawn_tp_to_cm_tee(tp_in_rx, cm_out_tx, tdp);

        h.abort();
        let result = tokio::time::timeout(Duration::from_secs(1), h).await;
        assert!(result.is_ok(), "aborted handle joined within timeout");
        let inner = result.unwrap();
        assert!(
            inner.is_err() && inner.unwrap_err().is_cancelled(),
            "join error reflects abort"
        );
    }

    #[tokio::test]
    async fn merge_handle_aborts_cleanly() {
        let (_cm_in_tx, cm_in_rx) = unbounded::<TemplateDistribution<'static>>();
        let (_eng_in_tx, eng_in_rx) = unbounded::<TemplateDistribution<'static>>();
        let (tp_out_tx, _tp_out_rx) = unbounded::<TemplateDistribution<'static>>();
        let h = spawn_cm_and_engine_to_tp_merge(cm_in_rx, eng_in_rx, tp_out_tx);

        h.abort();
        let result = tokio::time::timeout(Duration::from_secs(1), h).await;
        assert!(result.is_ok(), "aborted handle joined within timeout");
        let inner = result.unwrap();
        assert!(
            inner.is_err() && inner.unwrap_err().is_cancelled(),
            "join error reflects abort"
        );
    }
}
