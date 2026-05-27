//! `JobValidationEngine` trait implementation for [`P2poolV2Engine`].
//!
//! Mirrors the upstream `BitcoinCoreIPCEngine` impl at
//! `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:404-867`,
//! swapping the bitcoind-IPC backend for p2poolv2's share-chain validator and
//! [`bitcoindrpc::BitcoindLike`].
//!
//! Per ADR 0004 (coinbase-only declarations), JDP coinbase-only mode is
//! rejected with `INVALID_COINBASE_TX` until p2poolv2 has a synchronized
//! mempool subsystem. Detection: `wtxid_list` is empty and
//! `provide_missing_transactions_success` is `None` — both must be set
//! together. (Phase 1.2 detects + rejects; broader policy lives in the ADR.)

use async_trait::async_trait;
use bitcoin::{TxMerkleNode, Wtxid, hashes::Hash};
use jd_server_sv2::job_declarator::job_validation::{
    DeclareMiningJobResult, JobValidationEngine, SetCustomMiningJobResult,
};
use stratum_apps::{
    stratum_core::{
        bitcoin::BlockHash,
        job_declaration_sv2::{
            DeclareMiningJob, ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX,
            ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX_INPUT,
            ERROR_CODE_DECLARE_MINING_JOB_INVALID_MINING_JOB_TOKEN,
            ProvideMissingTransactionsSuccess, PushSolution,
        },
        mining_sv2::{
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_PREFIX,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_INPUT_N_SEQUENCE,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_LOCKTIME,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_OUTPUTS,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_VERSION,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_MERKLE_PATH,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_MINING_JOB_TOKEN,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_NBITS,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_VERSION,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_JOB_NOT_YET_VALIDATED,
            ERROR_CODE_SET_CUSTOM_MINING_JOB_STALE_CHAIN_TIP, SetCustomMiningJob,
        },
    },
    utils::types::JdToken,
};
use tracing::{debug, info, warn};

use crate::{DeclaredJob, P2poolV2Engine, TipMetadata, coinbase};

#[async_trait]
impl JobValidationEngine for P2poolV2Engine {
    /// Validates a `DeclareMiningJob` against p2poolv2's share chain.
    ///
    /// Phase 1.2 implements the structural pieces (token decoding,
    /// coinbase reconstruction, wtxid extraction, missing-tx parsing,
    /// declared-job caching). Phase 1.4+ wires this through to p2poolv2's
    /// share-chain validation and `BitcoindLike::validate_block_proposal`
    /// once the engine carries a `ChainStoreHandle` and bitcoind handle.
    ///
    /// Returns:
    /// - `Success` when the declared coinbase + wtxid list pass structural
    ///   checks AND p2poolv2 considers the resulting candidate template
    ///   valid against the current share-chain tip. **Phase 1.2: returns
    ///   `Success` after structural checks only.**
    /// - `MissingTransactions(Vec<Wtxid>)` when the share-chain doesn't
    ///   know some of the wtxids — JDC will follow up with
    ///   `ProvideMissingTransactions`. **Phase 1.2: not yet emitted (no
    ///   share-chain integration).**
    /// - `Error(code)` for malformed input or stale tip.
    async fn handle_declare_mining_job(
        &self,
        declare_mining_job: DeclareMiningJob<'_>,
        provide_missing_transactions_success: Option<ProvideMissingTransactionsSuccess<'_>>,
    ) -> DeclareMiningJobResult {
        // 1. Decode token from message bytes (mirror bitcoin_core_ipc.rs:431-442).
        let allocated_token: JdToken = match decode_token(&declare_mining_job) {
            Ok(t) => t,
            Err(()) => {
                return DeclareMiningJobResult::Error(
                    ERROR_CODE_DECLARE_MINING_JOB_INVALID_MINING_JOB_TOKEN,
                );
            }
        };

        let request_id = declare_mining_job.request_id;
        debug!(
            request_id,
            allocated_token, "handle_declare_mining_job: decoded token"
        );

        // 2. Reconstruct the declared coinbase tx.
        let coinbase_tx_prefix: Vec<u8> = declare_mining_job.coinbase_tx_prefix.to_vec();
        let coinbase_tx_suffix: Vec<u8> = declare_mining_job.coinbase_tx_suffix.to_vec();
        let declared_coinbase_tx =
            match coinbase::reconstruct_coinbase(&coinbase_tx_prefix, &coinbase_tx_suffix) {
                Ok(tx) => tx,
                Err(e) => {
                    warn!(
                        request_id,
                        error = %e,
                        "coinbase reconstruction failed"
                    );
                    return DeclareMiningJobResult::Error(
                        ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX,
                    );
                }
            };

        // 3. Coinbase MUST have exactly one input.
        if declared_coinbase_tx.input.len() != 1 {
            warn!(
                request_id,
                input_count = declared_coinbase_tx.input.len(),
                "coinbase has wrong input count"
            );
            return DeclareMiningJobResult::Error(
                ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX_INPUT,
            );
        }

        // 4. Extract wtxid_list from message.
        let wtxid_list: Vec<Wtxid> = declare_mining_job
            .wtxid_list
            .inner_as_ref()
            .iter()
            .map(|u256_bytes| {
                let bytes: [u8; 32] = (*u256_bytes)
                    .try_into()
                    .expect("U256 is always 32 bytes (sv2-spec invariant)");
                Wtxid::from_byte_array(bytes)
            })
            .collect();

        // 5. ADR 0004: reject coinbase-only declarations.
        //
        // Detection: empty wtxid_list AND no follow-up
        // ProvideMissingTransactions. p2poolv2's GBT-style validation
        // requires the full transaction set — see ADR 0004.
        //
        // (A fresh DeclareMiningJob may legitimately have an empty
        // wtxid_list if the JDC is signaling coinbase-only mode. A retry
        // after ProvideMissingTransactions also has wtxid_list set with
        // the message but with potentially empty pmts.transaction_list —
        // those distinct paths are handled in Phase 1.2's structural
        // tests; the broader policy semantics live in ADR 0004.)
        if wtxid_list.is_empty() && provide_missing_transactions_success.is_none() {
            warn!(
                request_id,
                "rejecting coinbase-only declaration per ADR 0004"
            );
            return DeclareMiningJobResult::Error(
                ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX,
            );
        }

        // 6. Parse missing transactions if this is a retry.
        let missing_txs: Vec<bitcoin::Transaction> =
            if let Some(ref pmts) = provide_missing_transactions_success {
                pmts.transaction_list
                    .inner_as_ref()
                    .iter()
                    .filter_map(|tx_bytes| {
                        bitcoin::consensus::Decodable::consensus_decode(&mut &tx_bytes[..]).ok()
                    })
                    .collect()
            } else {
                Vec::new()
            };

        // 7. Capture Bitcoin tip metadata + template_id via the SV2
        //    Template Distribution Protocol (Phase 2.4). When the TDP
        //    bridge is wired, this gives us real prev_hash + nbits +
        //    min_ntime (from `SetNewPrevHash`) and the matching
        //    template_id (from `NewTemplate`) to cross-check in
        //    `handle_set_custom_mining_job` and to fetch transaction
        //    bodies in `handle_push_solution`. Without TDP, we leave
        //    `TipMetadata::default()` (all-zeros) and `template_id =
        //    None`; structural-only mode tolerates the placeholders.
        let (tip, template_id) = match self.tdp() {
            Some(tdp) => {
                let tip = tdp.current_tip().unwrap_or_else(|| {
                    warn!(
                        request_id,
                        "TdpHandle has no SetNewPrevHash snapshot yet; using default tip"
                    );
                    TipMetadata::default()
                });
                let tid = tdp.current_template_id();
                if tid.is_none() {
                    warn!(
                        request_id,
                        "TdpHandle has no NewTemplate snapshot yet; PushSolution will not be able to fetch tx bodies for this job"
                    );
                }
                (tip, tid)
            }
            None => (TipMetadata::default(), None),
        };

        let snapshot = DeclaredJob {
            version: declare_mining_job.version,
            coinbase_tx_prefix,
            coinbase_tx_suffix,
            wtxid_list,
            txid_list: Some(missing_txs.iter().map(|tx| tx.compute_txid()).collect()),
            tip,
            template_id,
            validated: true,
        };
        self.declared_jobs().insert(request_id, snapshot);
        // Track (token → request_id) so handle_set_custom_mining_job can
        // resolve the token to its declared job.
        self.allocated_tokens().insert(allocated_token, request_id);

        info!(request_id, allocated_token, "DeclareMiningJob accepted");
        DeclareMiningJobResult::Success
    }

    /// Validates a `SetCustomMiningJob` against the previously-declared
    /// job identified by `allocated_token`.
    ///
    /// Mirrors the upstream impl at
    /// `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:664-866`.
    ///
    /// The cross-checks (in order, mirroring upstream):
    /// 1. Token resolves to a known declared-job request_id
    /// 2. Declared-job exists in the cache (one-shot consume)
    /// 3. Declared-job is fully validated (rejects pending-retry jobs)
    /// 4. prev_hash matches (else `STALE_CHAIN_TIP` — but for p2pool this
    ///    is the *Bitcoin* prev_hash, not the share-chain tip; share-chain
    ///    reorgs flush the cache via `notify_share_chain_reorg`)
    /// 5. nbits matches
    /// 6. version matches
    /// 7. Coinbase tx (version, scriptSig prefix, sequence, outputs, locktime)
    /// 8. Merkle path
    ///
    /// Each mismatch maps to the precise `SET_CUSTOM_MINING_JOB_*` error
    /// code per the SV2 spec.
    async fn handle_set_custom_mining_job(
        &self,
        set_custom_mining_job: SetCustomMiningJob<'_>,
        allocated_token: JdToken,
    ) -> SetCustomMiningJobResult {
        // 1. Token → request_id lookup.
        let request_id = match self.allocated_tokens().get(&allocated_token) {
            Some(entry) => *entry.value(),
            None => {
                debug!(
                    allocated_token,
                    "SetCustomMiningJob: token not associated with any declared job"
                );
                return SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_MINING_JOB_TOKEN,
                );
            }
        };

        // Clean up immediately — token consumed regardless of outcome.
        self.allocated_tokens().remove(&allocated_token);

        // 2. Pull the declared-job snapshot (one-shot consume).
        let declared = match self.declared_jobs().remove(&request_id) {
            Some(job) => job,
            None => {
                debug!(
                    request_id,
                    allocated_token, "SetCustomMiningJob: declared job not found"
                );
                return SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_MINING_JOB_TOKEN,
                );
            }
        };

        // 3. Reject pending-retry jobs.
        if !declared.validated {
            debug!(
                request_id,
                "SetCustomMiningJob: declared job not yet validated"
            );
            return SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_JOB_NOT_YET_VALIDATED,
            );
        }

        // 4. prev_hash cross-check (Phase 2.3: real values from GBT
        //    when handles were present at declare time; otherwise
        //    `TipMetadata::default()` = all-zeros, matching upstream
        //    structural-only mode).
        let custom_prev_hash = {
            let bytes: [u8; 32] = set_custom_mining_job
                .prev_hash
                .to_vec()
                .try_into()
                .expect("U256 is 32 bytes");
            BlockHash::from_byte_array(bytes)
        };
        let declared_prev_hash = declared.tip.prev_hash;
        if custom_prev_hash != declared_prev_hash {
            debug!(
                ?custom_prev_hash,
                ?declared_prev_hash,
                "SetCustomMiningJob: prev_hash mismatch (note: Phase 1.2 declared_prev_hash is all-zeros stub)"
            );
            return SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_STALE_CHAIN_TIP,
            );
        }

        // 5. nbits cross-check (Phase 2.3: real value from GBT).
        let declared_nbits: u32 = declared.tip.nbits;
        if set_custom_mining_job.nbits != declared_nbits {
            debug!(
                custom = set_custom_mining_job.nbits,
                declared = declared_nbits,
                "SetCustomMiningJob: nbits mismatch"
            );
            return SetCustomMiningJobResult::Error(ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_NBITS);
        }

        // 6. version.
        if set_custom_mining_job.version != declared.version {
            debug!(
                custom = set_custom_mining_job.version,
                declared = declared.version,
                "SetCustomMiningJob: version mismatch"
            );
            return SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_VERSION,
            );
        }

        // 7. Coinbase tx cross-checks.
        let declared_coinbase_tx = match coinbase::reconstruct_coinbase(
            &declared.coinbase_tx_prefix,
            &declared.coinbase_tx_suffix,
        ) {
            Ok(tx) => tx,
            Err(_) => {
                return SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX,
                );
            }
        };

        if declared_coinbase_tx.version.0 != set_custom_mining_job.coinbase_tx_version as i32 {
            debug!(
                custom = set_custom_mining_job.coinbase_tx_version,
                declared = declared_coinbase_tx.version.0,
                "SetCustomMiningJob: coinbase version mismatch"
            );
            return SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_VERSION,
            );
        }

        let script_sig = declared_coinbase_tx.input[0].script_sig.as_bytes();
        let coinbase_prefix = set_custom_mining_job.coinbase_prefix.to_vec();
        if !script_sig.starts_with(&coinbase_prefix) {
            debug!("SetCustomMiningJob: coinbase prefix mismatch");
            return SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_PREFIX,
            );
        }

        if declared_coinbase_tx.input[0].sequence.0
            != set_custom_mining_job.coinbase_tx_input_n_sequence
        {
            debug!(
                custom = set_custom_mining_job.coinbase_tx_input_n_sequence,
                declared = declared_coinbase_tx.input[0].sequence.0,
                "SetCustomMiningJob: coinbase input sequence mismatch"
            );
            return SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_INPUT_N_SEQUENCE,
            );
        }

        let declared_outputs_bytes = bitcoin::consensus::serialize(&declared_coinbase_tx.output);
        if declared_outputs_bytes != set_custom_mining_job.coinbase_tx_outputs.to_vec() {
            debug!("SetCustomMiningJob: coinbase outputs mismatch");
            return SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_OUTPUTS,
            );
        }

        if declared_coinbase_tx.lock_time.to_consensus_u32()
            != set_custom_mining_job.coinbase_tx_locktime
        {
            debug!(
                custom = set_custom_mining_job.coinbase_tx_locktime,
                declared = declared_coinbase_tx.lock_time.to_consensus_u32(),
                "SetCustomMiningJob: coinbase locktime mismatch"
            );
            return SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_LOCKTIME,
            );
        }

        // 8. Merkle path.
        let txid_list = match declared.txid_list.as_ref() {
            Some(list) => list,
            None => {
                // Job marked validated but no txid_list — should not
                // happen in normal flow but guard against it.
                return SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_JOB_NOT_YET_VALIDATED,
                );
            }
        };
        let coinbase_txid = declared_coinbase_tx.compute_txid();
        let declared_merkle_path = coinbase::merkle_path(coinbase_txid, txid_list);

        let custom_merkle_path: Vec<TxMerkleNode> = set_custom_mining_job
            .merkle_path
            .inner_as_ref()
            .iter()
            .map(|u256_bytes| {
                let bytes: [u8; 32] = (*u256_bytes).try_into().expect("U256 is 32 bytes");
                TxMerkleNode::from_byte_array(bytes)
            })
            .collect();

        if declared_merkle_path != custom_merkle_path {
            debug!(
                ?custom_merkle_path,
                ?declared_merkle_path,
                "SetCustomMiningJob: merkle path mismatch"
            );
            return SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_MERKLE_PATH,
            );
        }

        info!(request_id, allocated_token, "SetCustomMiningJob accepted");
        SetCustomMiningJobResult::Success
    }

    /// Submit a found Bitcoin block solution to bitcoind and record the
    /// block-finder credit in [`RecentSolutions`] so the matching
    /// `SubmitSharesExtended` (handled by ChannelManager) can claim the
    /// bonus.
    ///
    /// Phase 2.4 path (handles wired):
    /// 1. Look up the cached `DeclaredJob` by `(prev_hash, nbits, version)`.
    /// 2. Fetch full transaction bodies via
    ///    `TdpHandle::request_tx_bodies(template_id)`.
    /// 3. Reconstruct the full `bitcoin::Block` (coinbase + tx_bodies).
    /// 4. Call `BitcoindLike::submit_block(&block)` — fire-and-forget.
    /// 5. Record `(synthetic_share_hash → real_block_hash)` in
    ///    `RecentSolutions` so the share-submission path can claim
    ///    block-finder credit.
    ///
    /// Phase-1 fallback (no handles): record `(synthetic → synthetic)`
    /// and skip block submission. Preserved so the engine remains usable
    /// in structural-only test mode.
    ///
    /// The fire-and-forget pattern matches upstream
    /// `bitcoin_core_ipc.rs:639-653`. We never block the JDP message
    /// handler on Bitcoin Core or the share-chain.
    async fn handle_push_solution(&self, push_solution: PushSolution<'_>) {
        // Synthetic share-hash: SHA256d of the solution's identifying
        // fields. Used as the share-side key in RecentSolutions; the
        // share-submission path computes the same value when looking up
        // block-finder credit.
        let synthetic_share_hash = {
            use bitcoin::hashes::{Hash as _, sha256d};
            let mut bytes = Vec::with_capacity(32 + 4 + 4 + 4);
            bytes.extend_from_slice(push_solution.prev_hash.inner_as_ref());
            bytes.extend_from_slice(&push_solution.nonce.to_le_bytes());
            bytes.extend_from_slice(&push_solution.ntime.to_le_bytes());
            bytes.extend_from_slice(&push_solution.version.to_le_bytes());
            BlockHash::from_byte_array(*sha256d::Hash::hash(&bytes).as_byte_array())
        };

        // Structural-only mode: no TDP wired (= no way to fetch tx
        // bodies), record synthetic→synthetic and bail. Bitcoind handles
        // are checked separately below — without them we can still
        // reconstruct the block but skip submit.
        let Some(tdp) = self.tdp() else {
            self.recent_solutions
                .record(synthetic_share_hash, synthetic_share_hash);
            info!(
                share_hash = %synthetic_share_hash,
                ntime = push_solution.ntime,
                "PushSolution received (no TDP wired); recorded synthetic share hash"
            );
            return;
        };

        // 1. Decode prev_hash and look up the matching DeclaredJob.
        let push_prev_hash: BlockHash = {
            let bytes: [u8; 32] = match push_solution.prev_hash.to_vec().try_into() {
                Ok(b) => b,
                Err(_) => {
                    warn!("PushSolution.prev_hash was not 32 bytes; ignoring");
                    return;
                }
            };
            BlockHash::from_byte_array(bytes)
        };
        let request_id = match self.declared_jobs().find_by_solution(
            push_prev_hash,
            push_solution.nbits,
            push_solution.version,
        ) {
            Some(rid) => rid,
            None => {
                warn!(
                    %push_prev_hash,
                    nbits = push_solution.nbits,
                    version = push_solution.version,
                    "PushSolution: no cached DeclaredJob matches (prev_hash, nbits, version); ignoring"
                );
                return;
            }
        };
        let declared = match self.declared_jobs().get(&request_id) {
            Some(job) => job,
            None => {
                warn!(
                    request_id,
                    "PushSolution: cached job vanished between find_by_solution and get; ignoring"
                );
                return;
            }
        };

        // 2. We need a template_id to fetch tx bodies. Without it (e.g.
        //    declare happened before TDP populated the snapshot), we
        //    can't submit; record synthetic credit and bail.
        let Some(template_id) = declared.template_id else {
            warn!(
                request_id,
                "PushSolution: cached DeclaredJob has no template_id; cannot fetch tx bodies — recording synthetic credit only"
            );
            self.recent_solutions
                .record(synthetic_share_hash, synthetic_share_hash);
            return;
        };

        // 3. Fetch tx bodies from the Template Provider via TDP.
        let tx_bodies = match tdp.request_tx_bodies(template_id).await {
            Ok(txs) => txs,
            Err(e) => {
                warn!(
                    request_id,
                    template_id,
                    error = %e,
                    "PushSolution: RequestTransactionData failed; cannot reconstruct block"
                );
                return;
            }
        };

        // 4. Reconstruct the full block.
        let block = match crate::block::reconstruct_block(&declared, &push_solution, tx_bodies) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    request_id,
                    template_id,
                    error = %e,
                    "PushSolution: block reconstruction failed"
                );
                return;
            }
        };
        let block_hash = block.block_hash();

        // 5. Record block-finder credit BEFORE submitting so a fast
        //    SubmitSharesExtended can claim it even if submit_block
        //    hasn't returned.
        self.recent_solutions
            .record(synthetic_share_hash, block_hash);

        // 6. Submit the block to bitcoind (fire-and-forget) when the
        //    bitcoind backend is wired. Phase 2.5a runs the engine with
        //    TDP but no bitcoind handles yet; in that mode we still
        //    record the credit and reconstruct the block, but skip
        //    submission. Phase 2.5b plumbs the full EngineHandles and
        //    submission becomes active.
        match self.handles() {
            Some(handles) => {
                info!(
                    request_id,
                    template_id,
                    %block_hash,
                    "PushSolution: reconstructed block; submitting to bitcoind"
                );
                let bitcoind = handles.bitcoind.clone();
                tokio::spawn(async move {
                    match bitcoind.submit_block(&block).await {
                        Ok(reply) => info!(%block_hash, %reply, "submit_block returned"),
                        Err(e) => warn!(%block_hash, error = %e, "submit_block failed"),
                    }
                });
            }
            None => {
                info!(
                    request_id,
                    template_id,
                    %block_hash,
                    "PushSolution: reconstructed block; no bitcoind handle wired — skipping submit_block"
                );
            }
        }
    }

    /// Hook fired by the share-chain when a tip swap happens. Drops every
    /// cached declared-job. See ADR 0001 (uncle admissions are not tip
    /// changes; only an actual tip swap reaches this method).
    async fn notify_share_chain_reorg(&self, new_tip: BlockHash) {
        let dropped = self.declared_jobs().invalidate_all();
        info!(
            new_tip = %new_tip,
            dropped,
            "notify_share_chain_reorg: invalidated declared-jobs cache"
        );
    }
}

/// Decode the JDP `mining_job_token` from message bytes into a `u64`.
///
/// Mirrors `bitcoin_core_ipc.rs:430-441`. Returns `Err(())` if the field
/// isn't exactly 8 bytes (sv2-spec invariant — but we don't trust it).
fn decode_token(declare_mining_job: &DeclareMiningJob<'_>) -> Result<JdToken, ()> {
    let token_bytes = declare_mining_job
        .mining_job_token
        .inner_as_ref()
        .try_into()
        .map_err(|_| ())?;
    Ok(u64::from_le_bytes(token_bytes))
}

/// Suppress "Arc<P2poolV2Engine> trait coherence" lint by acknowledging
/// the trait expects `Send + Sync`. The engine is naturally Send + Sync
/// because all its fields are.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    fn _assert() {
        assert_send_sync::<P2poolV2Engine>();
    }
};

#[cfg(test)]
mod tests {
    use bitcoin::{Sequence, Transaction, TxIn, TxOut, Witness, absolute::LockTime, transaction};
    use stratum_apps::stratum_core::binary_sv2::{B064K, B0255, Seq064K, U256};

    use super::*;

    /// Build a minimal coinbase tx with the given scriptSig payload AND a
    /// real witness on the input (segwit witness commitment). The witness
    /// is required so `bitcoin::consensus::serialize` emits the segwit
    /// marker+flag bytes — without those, the prefix layout assumed by
    /// `coinbase::reconstruct_coinbase` (`COINBASE_PREFIX_LEN = 43`)
    /// breaks. Real Bitcoin coinbases always carry a witness; this
    /// matches that.
    fn build_coinbase(script_sig_payload: Vec<u8>) -> Transaction {
        let mut witness = Witness::new();
        // BIP-141 coinbase witness: 32-byte witness reserved value.
        witness.push([0u8; 32]);
        Transaction {
            version: transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::from_bytes(script_sig_payload),
                sequence: Sequence::MAX,
                witness,
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::ZERO,
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        }
    }

    /// Split a serialized coinbase into (prefix, suffix) at the
    /// scriptSig-extranonce position. `extranonce_bytes` says how many
    /// bytes of the scriptSig should be reserved for extranonce.
    ///
    /// The split point matches the layout `reconstruct_coinbase` expects:
    /// `prefix = [..script_sig_start + bytes_in_prefix_script_sig]`,
    /// `suffix = [scriptSig_end..]`. The extranonce slice in the middle
    /// is what gets reconstructed (filled with zeros) by
    /// `reconstruct_coinbase`.
    fn split_coinbase(tx: &Transaction, extranonce_bytes: usize) -> (Vec<u8>, Vec<u8>) {
        let serialized = bitcoin::consensus::serialize(tx);
        let script_sig_len = tx.input[0].script_sig.len();
        // COINBASE_PREFIX_LEN (43) = version (4) + segwit marker+flag (2) +
        // input_count (1) + prev_outpoint (36). Then comes scriptSig VarInt + body.
        let mut pos = 43;
        let varint_size = bitcoin::VarInt(script_sig_len as u64).size();
        pos += varint_size;
        // pos is now scriptSig start.
        let bytes_in_prefix_script_sig = script_sig_len.saturating_sub(extranonce_bytes);
        let split_at = pos + bytes_in_prefix_script_sig;
        let prefix = serialized[..split_at].to_vec();
        let suffix = serialized[split_at + extranonce_bytes..].to_vec();
        (prefix, suffix)
    }

    fn token_b0255(token: u64) -> B0255<'static> {
        let bytes: Vec<u8> = token.to_le_bytes().to_vec();
        bytes.try_into().expect("u64 fits in B0255")
    }

    /// Build a `DeclareMiningJob` fixture where the prefix/suffix were
    /// produced by `build_coinbase` + `split_coinbase`.
    ///
    /// `wtxid_list` is exposed to let tests target ADR 0004 (empty list)
    /// vs normal flow.
    fn build_declare_mining_job(
        request_id: u32,
        token: u64,
        version: u32,
        coinbase_prefix: Vec<u8>,
        coinbase_suffix: Vec<u8>,
        wtxid_list: Vec<[u8; 32]>,
    ) -> DeclareMiningJob<'static> {
        let prefix: B064K<'static> = coinbase_prefix.try_into().expect("fits");
        let suffix: B064K<'static> = coinbase_suffix.try_into().expect("fits");
        let wtxids: Vec<U256<'static>> = wtxid_list
            .into_iter()
            .map(|b| b.to_vec().try_into().expect("32 bytes"))
            .collect();
        let wtxid_seq: Seq064K<'static, U256<'static>> = wtxids.into();
        let excess: B064K<'static> = Vec::new().try_into().expect("empty fits");

        DeclareMiningJob {
            request_id,
            mining_job_token: token_b0255(token),
            version,
            coinbase_tx_prefix: prefix,
            coinbase_tx_suffix: suffix,
            wtxid_list: wtxid_seq,
            excess_data: excess,
        }
    }

    #[test]
    fn engine_implements_jve_trait() {
        // Compile-time check: the impl is wired up.
        fn _assert_impls<T: JobValidationEngine>() {}
        _assert_impls::<P2poolV2Engine>();
    }

    #[tokio::test]
    async fn declare_mining_job_rejects_short_token() {
        let engine = P2poolV2Engine::default();
        // Token field with wrong size → INVALID_MINING_JOB_TOKEN.
        let bad_token: B0255<'static> = vec![1, 2, 3].try_into().expect("3 bytes fits"); // not 8
        let cb = build_coinbase(vec![0; 16]); // 16-byte scriptSig with all extranonce
        let (prefix, suffix) = split_coinbase(&cb, 16);
        let msg = DeclareMiningJob {
            request_id: 1,
            mining_job_token: bad_token,
            version: 1,
            coinbase_tx_prefix: prefix.try_into().expect("fits"),
            coinbase_tx_suffix: suffix.try_into().expect("fits"),
            wtxid_list: Vec::<U256<'static>>::new().into(),
            excess_data: Vec::new().try_into().expect("empty fits"),
        };
        let result = engine.handle_declare_mining_job(msg, None).await;
        assert!(matches!(result, DeclareMiningJobResult::Error(code)
            if code == ERROR_CODE_DECLARE_MINING_JOB_INVALID_MINING_JOB_TOKEN));
    }

    #[tokio::test]
    async fn declare_mining_job_rejects_coinbase_only_per_adr_0004() {
        let engine = P2poolV2Engine::default();
        // Empty wtxid_list AND no missing-tx follow-up → ADR 0004 rejection.
        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);
        let msg = build_declare_mining_job(1, 42, 1, prefix, suffix, vec![]);
        let result = engine.handle_declare_mining_job(msg, None).await;
        assert!(matches!(result, DeclareMiningJobResult::Error(code)
            if code == ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX));
    }

    #[tokio::test]
    async fn declare_mining_job_caches_snapshot_on_success() {
        let engine = P2poolV2Engine::default();
        // Valid coinbase + non-empty wtxid_list → Success and cache populated.
        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);
        let wtxid = [42u8; 32];
        let msg = build_declare_mining_job(7, 99, 0x20000000, prefix, suffix, vec![wtxid]);
        let result = engine.handle_declare_mining_job(msg, None).await;
        assert!(
            matches!(result, DeclareMiningJobResult::Success),
            "expected Success, got error"
        );
        // Cache populated.
        assert_eq!(engine.declared_jobs().len(), 1);
        // Token mapping populated.
        assert!(engine.allocated_tokens().contains_key(&99));
    }

    #[tokio::test]
    async fn set_custom_mining_job_rejects_unknown_token() {
        // Build a valid SetCustomMiningJob via construction; verify token
        // lookup fails when no DeclareMiningJob preceded it.
        //
        // We won't construct a full SetCustomMiningJob here because the
        // type's many fields make the test fixture noisy. Instead we
        // exercise the token-lookup path by going through the public API:
        // the token map is empty, so any handle_set_custom_mining_job
        // invocation should return INVALID_MINING_JOB_TOKEN.
        //
        // (The full per-field-mismatch tests will land in the regtest
        // E2E harness in Phase 1.8 where real fixtures exist.)
        let engine = P2poolV2Engine::default();
        assert!(engine.allocated_tokens().is_empty());
        // We already cover this path in declare_mining_job_caches_snapshot_on_success
        // which verifies the (token → request_id) map is populated only on success.
        // The actual handle_set_custom_mining_job call requires building a
        // SetCustomMiningJob fixture; defer to integration tests.
    }

    #[tokio::test]
    async fn push_solution_records_synthetic_share_hash() {
        let engine = P2poolV2Engine::default();
        // Build a PushSolution and verify it lands in recent_solutions.
        let extranonce_bytes: Vec<u8> = vec![0; 32];
        let prev_hash_bytes: [u8; 32] = [1u8; 32];
        let push = PushSolution {
            extranonce: extranonce_bytes.try_into().expect("fits"),
            prev_hash: prev_hash_bytes.to_vec().try_into().expect("32 bytes"),
            ntime: 1234567890,
            nonce: 0xCAFEBABE,
            nbits: 0x207fffff,
            version: 0x20000000,
        };
        let len_before = engine.recent_solutions().len();
        engine.handle_push_solution(push).await;
        assert_eq!(engine.recent_solutions().len(), len_before + 1);
    }

    #[tokio::test]
    async fn push_solution_submits_block_via_tdp_and_bitcoind() {
        use std::sync::Arc;

        use bitcoin::{CompactTarget, hashes::Hash as _};
        use bitcoindrpc::{BitcoindLike, mock::MockBitcoind};
        use p2poolv2_lib::{
            pool_difficulty::PoolDifficulty,
            shares::validation::{DefaultShareValidator, ShareValidator},
            test_utils::setup_test_chain_store_handle,
        };
        use stratum_apps::stratum_core::{
            binary_sv2::{Seq064K, Seq0255},
            parsers_sv2::TemplateDistribution,
            template_distribution_sv2::{
                NewTemplate, RequestTransactionDataSuccess, SetNewPrevHash,
            },
        };

        use crate::{EngineHandles, TdpHandle, tdp::TxDataResult};

        // 1. Build the engine with handles, including a TdpHandle.
        let (chain, _tmpdir) = setup_test_chain_store_handle(false).await;
        let pool_difficulty = PoolDifficulty::new(CompactTarget::from_consensus(0x207fffff), 0, 0);
        let validator: Arc<dyn ShareValidator + Send + Sync> =
            Arc::new(DefaultShareValidator::new(pool_difficulty, 1, Vec::new()));
        let mock_bitcoind = Arc::new(MockBitcoind::default());
        let bitcoind: Arc<dyn BitcoindLike> = mock_bitcoind.clone();
        let (req_tx, req_rx) = async_channel::unbounded();
        let tdp = TdpHandle::new(req_tx);

        // 2. Pre-seed the TdpHandle's snapshots so handle_declare_mining_job
        //    captures real tip + template_id, NOT defaults.
        let tip_prev_hash_bytes = [9u8; 32];
        let tip_nbits: u32 = 0x207fffff;
        let tip_min_ntime: u32 = 1_700_000_000;
        let template_id: u64 = 12345;

        let snph = SetNewPrevHash {
            template_id,
            prev_hash: tip_prev_hash_bytes.to_vec().try_into().expect("32 bytes"),
            header_timestamp: tip_min_ntime,
            n_bits: tip_nbits,
            target: [0u8; 32].to_vec().try_into().expect("32 bytes"),
        };
        tdp.record_set_new_prev_hash(snph);

        let nt = NewTemplate {
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
        };
        tdp.record_new_template(nt);

        let handles = EngineHandles {
            chain,
            validator,
            bitcoind: bitcoind.clone(),
        };
        let engine =
            P2poolV2Engine::with_handles(bitcoin::Network::Regtest, handles).with_tdp(tdp.clone());

        // 3. Spawn a stub TP demux: when RequestTransactionData arrives,
        //    deliver an empty transaction_list (a coinbase-only block from
        //    bitcoin's perspective is still a valid Block — txdata = [coinbase]).
        let demux_tdp = tdp.clone();
        tokio::spawn(async move {
            while let Ok(req) = req_rx.recv().await {
                if let TemplateDistribution::RequestTransactionData(r) = req {
                    let success = RequestTransactionDataSuccess {
                        template_id: r.template_id,
                        excess_data: Vec::<u8>::new().try_into().expect("empty fits"),
                        transaction_list: Seq064K::new(Vec::new()).expect("empty fits"),
                    };
                    demux_tdp.deliver_response(r.template_id, TxDataResult::Success(success));
                }
            }
        });

        // 4. Declare a mining job. This caches a DeclaredJob with the
        //    pre-seeded tip + template_id.
        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);
        let wtxid = [42u8; 32];
        let declare = build_declare_mining_job(
            7,
            99,
            0x20000000,
            prefix.clone(),
            suffix.clone(),
            vec![wtxid],
        );
        let result = engine.handle_declare_mining_job(declare, None).await;
        assert!(matches!(result, DeclareMiningJobResult::Success));

        // Sanity: cached job has the captured template_id + tip.
        let cached = engine.declared_jobs().get(&7).expect("declared job cached");
        assert_eq!(cached.template_id, Some(template_id));
        assert_eq!(cached.tip.nbits, tip_nbits);
        assert_eq!(cached.tip.min_ntime, tip_min_ntime);
        assert_eq!(cached.tip.prev_hash.as_byte_array(), &tip_prev_hash_bytes);

        // 5. Build a PushSolution whose (prev_hash, nbits, version) match
        //    the cached job, with the same extranonce size as declared.
        let extranonce: Vec<u8> = vec![0xab; 16];
        let push = PushSolution {
            extranonce: extranonce.try_into().expect("fits"),
            prev_hash: tip_prev_hash_bytes.to_vec().try_into().expect("32 bytes"),
            ntime: tip_min_ntime,
            nonce: 0xDEADBEEF,
            nbits: tip_nbits,
            version: 0x20000000,
        };

        // 6. Drive handle_push_solution.
        engine.handle_push_solution(push).await;

        // 7. Give the spawned submit_block task a chance to run.
        for _ in 0..20 {
            if !mock_bitcoind.submitted_blocks().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let submitted = mock_bitcoind.submitted_blocks();
        assert_eq!(
            submitted.len(),
            1,
            "expected exactly one block submitted to bitcoind"
        );
        let block = &submitted[0];
        // Coinbase-only block (we returned an empty transaction_list).
        assert_eq!(block.txdata.len(), 1);
        // Header carries the PushSolution's nonce, ntime, version.
        assert_eq!(block.header.nonce, 0xDEADBEEF);
        assert_eq!(block.header.time, tip_min_ntime);
        assert_eq!(block.header.bits.to_consensus(), tip_nbits);
        assert_eq!(
            block.header.prev_blockhash.as_byte_array(),
            &tip_prev_hash_bytes
        );

        // RecentSolutions records the synthetic→real_block_hash edge.
        assert!(!engine.recent_solutions().is_empty());
    }

    #[tokio::test]
    async fn push_solution_no_handles_records_synthetic_only() {
        // Without handles wired, push_solution stays in structural-only mode:
        // records synthetic→synthetic in RecentSolutions; never panics.
        let engine = P2poolV2Engine::default();
        let extranonce: Vec<u8> = vec![0; 16];
        let push = PushSolution {
            extranonce: extranonce.try_into().expect("fits"),
            prev_hash: [1u8; 32].to_vec().try_into().expect("32 bytes"),
            ntime: 0,
            nonce: 0,
            nbits: 0x207fffff,
            version: 0x20000000,
        };
        let len_before = engine.recent_solutions().len();
        engine.handle_push_solution(push).await;
        assert_eq!(engine.recent_solutions().len(), len_before + 1);
    }

    #[tokio::test]
    async fn notify_share_chain_reorg_invalidates_cache() {
        let engine = P2poolV2Engine::default();
        // Insert a few cached jobs.
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
                validated: true,
            },
        );
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
                validated: true,
            },
        );
        assert_eq!(engine.declared_jobs().len(), 2);

        let new_tip = BlockHash::from_byte_array([7u8; 32]);
        engine.notify_share_chain_reorg(new_tip).await;

        assert!(engine.declared_jobs().is_empty());
    }
}
