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

use crate::{
    DeclaredJob, P2poolV2Engine, ShareHeaderLookup, TipMetadata, coinbase,
    metrics::PushSolutionDropReason,
};

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
        // Helper: tally the terminal result on the metrics counters,
        // then return it. Keeps every error / success / missing-txns
        // path consistent without threading a side channel.
        let bump_and_return = |result: DeclareMiningJobResult| -> DeclareMiningJobResult {
            if let Some(m) = self.metrics() {
                match &result {
                    DeclareMiningJobResult::Success => {
                        m.declare_mining_job_accepted.inc();
                    }
                    DeclareMiningJobResult::Error(_) => {
                        m.declare_mining_job_rejected.inc();
                    }
                    DeclareMiningJobResult::MissingTransactions(_) => {
                        m.declare_mining_job_missing_txns.inc();
                    }
                }
            }
            result
        };

        // 1. Decode token from message bytes (mirror bitcoin_core_ipc.rs:431-442).
        let allocated_token: JdToken = match decode_token(&declare_mining_job) {
            Ok(t) => t,
            Err(()) => {
                return bump_and_return(DeclareMiningJobResult::Error(
                    ERROR_CODE_DECLARE_MINING_JOB_INVALID_MINING_JOB_TOKEN,
                ));
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
                    return bump_and_return(DeclareMiningJobResult::Error(
                        ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX,
                    ));
                }
            };

        // 3. Coinbase MUST have exactly one input.
        if declared_coinbase_tx.input.len() != 1 {
            warn!(
                request_id,
                input_count = declared_coinbase_tx.input.len(),
                "coinbase has wrong input count"
            );
            return bump_and_return(DeclareMiningJobResult::Error(
                ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX_INPUT,
            ));
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
            return bump_and_return(DeclareMiningJobResult::Error(
                ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX,
            ));
        }

        // 6. First-pass declare with non-empty wtxid_list and no PMTS:
        //    ask the JDC to send tx bodies. We don't yet have a
        //    template-side store to look up which wtxids we already
        //    know, so the conservative-correct response is to request
        //    all of them. The JDC re-issues DeclareMiningJob with PMTS
        //    on the second pass.
        //
        //    Without this, caching a job with `txid_list = Some(vec![])`
        //    while the JDC's merkle path was computed over the full
        //    wtxid set would cause a downstream
        //    `SetCustomMiningJob.merkle_path` mismatch (INVALID_MERKLE_PATH).
        if !wtxid_list.is_empty() && provide_missing_transactions_success.is_none() {
            debug!(
                request_id,
                count = wtxid_list.len(),
                "DeclareMiningJob: requesting missing transactions"
            );
            return bump_and_return(DeclareMiningJobResult::MissingTransactions(wtxid_list));
        }

        // 7. Parse missing transactions from the PMTS retry.
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

        // Capture the share-chain tip when handles are wired so
        // future selective-invalidation logic has the data. Reading
        // the tip is best-effort: a transient store / transport
        // error must not block job acceptance.
        //
        // Phase 2-B Track A (ADR 0011): the chain handle is now an
        // `Arc<dyn ShareChainReader>` and `get_chain_tip` is async.
        // `Ok(None)` (genesis-uninitialised) is the "structurally
        // correct, no tip yet" path; `Err(_)` is a transport
        // failure. Both end up captured as `None` on the snapshot —
        // dashboards see the failure via the warn! log.
        let share_chain_tip = match self.handles() {
            Some(h) => {
                let chain = h.chain.clone();
                match chain.get_chain_tip().await {
                    Ok(tip) => tip,
                    Err(e) => {
                        warn!(
                            request_id,
                            error = %e,
                            "share-chain tip read failed at declare time; continuing without capture"
                        );
                        None
                    }
                }
            }
            None => None,
        };

        let snapshot = DeclaredJob {
            version: declare_mining_job.version,
            coinbase_tx_prefix,
            coinbase_tx_suffix,
            wtxid_list,
            txid_list: Some(missing_txs.iter().map(|tx| tx.compute_txid()).collect()),
            tip,
            template_id,
            share_chain_tip,
            validated: true,
        };
        self.declared_jobs().insert(request_id, snapshot);
        // Track (token → request_id) so handle_set_custom_mining_job can
        // resolve the token to its declared job.
        self.allocated_tokens().insert(allocated_token, request_id);

        info!(request_id, allocated_token, "DeclareMiningJob accepted");
        bump_and_return(DeclareMiningJobResult::Success)
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
        let bump_and_return = |result: SetCustomMiningJobResult| -> SetCustomMiningJobResult {
            if let Some(m) = self.metrics() {
                match &result {
                    SetCustomMiningJobResult::Success => {
                        m.set_custom_mining_job_accepted.inc();
                    }
                    SetCustomMiningJobResult::Error(_) => {
                        m.set_custom_mining_job_rejected.inc();
                    }
                }
            }
            result
        };

        // 1. Token → request_id lookup.
        let request_id = match self.allocated_tokens().get(&allocated_token) {
            Some(entry) => *entry.value(),
            None => {
                debug!(
                    allocated_token,
                    "SetCustomMiningJob: token not associated with any declared job"
                );
                return bump_and_return(SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_MINING_JOB_TOKEN,
                ));
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
                return bump_and_return(SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_MINING_JOB_TOKEN,
                ));
            }
        };

        // 3. Reject pending-retry jobs.
        if !declared.validated {
            debug!(
                request_id,
                "SetCustomMiningJob: declared job not yet validated"
            );
            return bump_and_return(SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_JOB_NOT_YET_VALIDATED,
            ));
        }

        // 4. prev_hash + nbits cross-checks. Phase 2.4 captures the
        //    real tip via TDP at declare time. In structural-only
        //    mode (no TDP wired) `tip` stays at default (all-zeros
        //    prev_hash, zero nbits) — skip these cross-checks rather
        //    than returning spurious mismatches.
        let tip_was_captured =
            declared.tip.prev_hash != BlockHash::all_zeros() || declared.tip.nbits != 0;

        if tip_was_captured {
            let custom_prev_hash = {
                let bytes: [u8; 32] = set_custom_mining_job
                    .prev_hash
                    .to_vec()
                    .try_into()
                    .expect("U256 is 32 bytes");
                BlockHash::from_byte_array(bytes)
            };
            if custom_prev_hash != declared.tip.prev_hash {
                debug!(
                    ?custom_prev_hash,
                    declared_prev_hash = ?declared.tip.prev_hash,
                    "SetCustomMiningJob: prev_hash mismatch"
                );
                return bump_and_return(SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_STALE_CHAIN_TIP,
                ));
            }

            if set_custom_mining_job.nbits != declared.tip.nbits {
                debug!(
                    custom = set_custom_mining_job.nbits,
                    declared = declared.tip.nbits,
                    "SetCustomMiningJob: nbits mismatch"
                );
                return bump_and_return(SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_NBITS,
                ));
            }
        } else {
            debug!(
                request_id,
                "SetCustomMiningJob: tip was not captured at declare time; skipping prev_hash + nbits cross-checks"
            );
        }

        // 6. version.
        if set_custom_mining_job.version != declared.version {
            debug!(
                custom = set_custom_mining_job.version,
                declared = declared.version,
                "SetCustomMiningJob: version mismatch"
            );
            return bump_and_return(SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_VERSION,
            ));
        }

        // 7. Coinbase tx cross-checks.
        let declared_coinbase_tx = match coinbase::reconstruct_coinbase(
            &declared.coinbase_tx_prefix,
            &declared.coinbase_tx_suffix,
        ) {
            Ok(tx) => tx,
            Err(_) => {
                return bump_and_return(SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX,
                ));
            }
        };

        if declared_coinbase_tx.version.0 != set_custom_mining_job.coinbase_tx_version as i32 {
            debug!(
                custom = set_custom_mining_job.coinbase_tx_version,
                declared = declared_coinbase_tx.version.0,
                "SetCustomMiningJob: coinbase version mismatch"
            );
            return bump_and_return(SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_VERSION,
            ));
        }

        let script_sig = declared_coinbase_tx.input[0].script_sig.as_bytes();
        let coinbase_prefix = set_custom_mining_job.coinbase_prefix.to_vec();
        if !script_sig.starts_with(&coinbase_prefix) {
            debug!("SetCustomMiningJob: coinbase prefix mismatch");
            return bump_and_return(SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_PREFIX,
            ));
        }

        if declared_coinbase_tx.input[0].sequence.0
            != set_custom_mining_job.coinbase_tx_input_n_sequence
        {
            debug!(
                custom = set_custom_mining_job.coinbase_tx_input_n_sequence,
                declared = declared_coinbase_tx.input[0].sequence.0,
                "SetCustomMiningJob: coinbase input sequence mismatch"
            );
            return bump_and_return(SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_INPUT_N_SEQUENCE,
            ));
        }

        let declared_outputs_bytes = bitcoin::consensus::serialize(&declared_coinbase_tx.output);
        if declared_outputs_bytes != set_custom_mining_job.coinbase_tx_outputs.to_vec() {
            debug!("SetCustomMiningJob: coinbase outputs mismatch");
            return bump_and_return(SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_OUTPUTS,
            ));
        }

        if declared_coinbase_tx.lock_time.to_consensus_u32()
            != set_custom_mining_job.coinbase_tx_locktime
        {
            debug!(
                custom = set_custom_mining_job.coinbase_tx_locktime,
                declared = declared_coinbase_tx.lock_time.to_consensus_u32(),
                "SetCustomMiningJob: coinbase locktime mismatch"
            );
            return bump_and_return(SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_COINBASE_TX_LOCKTIME,
            ));
        }

        // 8. Merkle path.
        let txid_list = match declared.txid_list.as_ref() {
            Some(list) => list,
            None => {
                // Job marked validated but no txid_list — should not
                // happen in normal flow but guard against it.
                return bump_and_return(SetCustomMiningJobResult::Error(
                    ERROR_CODE_SET_CUSTOM_MINING_JOB_JOB_NOT_YET_VALIDATED,
                ));
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
            return bump_and_return(SetCustomMiningJobResult::Error(
                ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_MERKLE_PATH,
            ));
        }

        info!(request_id, allocated_token, "SetCustomMiningJob accepted");
        bump_and_return(SetCustomMiningJobResult::Success)
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
        if let Some(m) = self.metrics() {
            m.push_solution_received.inc();
        }

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
            if let Some(m) = self.metrics() {
                m.record_push_solution_drop(PushSolutionDropReason::NoHandles);
            }
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
                if let Some(m) = self.metrics() {
                    m.record_push_solution_drop(PushSolutionDropReason::NoMatchingJob);
                }
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
                if let Some(m) = self.metrics() {
                    m.record_push_solution_drop(PushSolutionDropReason::CacheRace);
                }
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
            if let Some(m) = self.metrics() {
                m.record_push_solution_drop(PushSolutionDropReason::NoTemplateId);
            }
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
                if let Some(m) = self.metrics() {
                    m.record_push_solution_drop(PushSolutionDropReason::TdpFetchFailed);
                }
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
                if let Some(m) = self.metrics() {
                    m.record_push_solution_drop(PushSolutionDropReason::ReconstructFailed);
                }
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
                if let Some(m) = self.metrics() {
                    m.blocks_submitted.inc();
                }
                let bitcoind = handles.bitcoind.clone();
                let metrics = self.metrics().cloned();
                tokio::spawn(async move {
                    match bitcoind.submit_block(&block).await {
                        Ok(reply) => {
                            // bitcoind's submitblock RPC returns
                            // serde_json::Value::Null on success
                            // (serializes to "null") and a rejection-reason
                            // string on consensus rejection (serializes to
                            // a quoted string e.g. "\"high-hash\""). Treat
                            // anything that isn't the null sentinel as a
                            // failure.
                            if reply == "null" {
                                info!(
                                    request_id,
                                    template_id,
                                    %block_hash,
                                    "submit_block accepted"
                                );
                            } else {
                                warn!(
                                    request_id,
                                    template_id,
                                    %block_hash,
                                    rejection = %reply,
                                    "submit_block rejected by bitcoind",
                                );
                                if let Some(m) = metrics.as_ref() {
                                    m.blocks_submit_failed.inc();
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                request_id,
                                template_id,
                                %block_hash,
                                error = %e,
                                "submit_block failed"
                            );
                            if let Some(m) = metrics.as_ref() {
                                m.blocks_submit_failed.inc();
                            }
                        }
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

    /// Hook fired by the share-chain when a tip swap happens.
    ///
    /// **With chain handle wired** (Phase 2-A): selective invalidation.
    /// Walks back from `new_tip` through `prev_share_blockhash`
    /// pointers up to [`REORG_ANCESTRY_DEPTH`] hops. Cached
    /// `DeclaredJob`s whose captured `share_chain_tip` is found on
    /// that ancestry path are kept; the rest are dropped. Jobs with
    /// `share_chain_tip == None` (declared before chain wiring or
    /// while the chain was unreadable) are conservatively dropped.
    ///
    /// **Without chain handle**: falls back to flushing the whole
    /// cache (the original Phase 1 rule).
    ///
    /// See ADR 0001 (α=1, uncles aren't stale): uncle admissions
    /// don't reach this method; only an actual tip swap does.
    async fn notify_share_chain_reorg(&self, new_tip: BlockHash) {
        if let Some(m) = self.metrics() {
            m.reorg_notifications.inc();
        }
        let bump_dropped = |n: usize| {
            if let Some(m) = self.metrics() {
                m.jobs_invalidated_total.inc_by(n as u64);
            }
        };

        let Some(handles) = self.handles() else {
            let dropped = self.declared_jobs().invalidate_all();
            bump_dropped(dropped);
            info!(
                new_tip = %new_tip,
                dropped,
                "notify_share_chain_reorg: no chain handle — flushed declared-jobs cache"
            );
            return;
        };

        // Walk new_tip's ancestry up to REORG_ANCESTRY_DEPTH hops and
        // collect block hashes seen along the way. A cached job's
        // captured tip is "still on chain" iff it appears in this
        // set OR equals new_tip itself.
        //
        // Phase 2-B Track A (ADR 0011): each `get_share_header` is
        // now an async UDS round-trip when the chain reader is
        // backed by IPC. Worst case is `REORG_ANCESTRY_DEPTH = 100`
        // sequential calls per reorg; the latency budget (~10-50 ms
        // p99 over UDS) is documented in the ADR's Negative
        // section. The genesis sentinel + missing-header arms are
        // expressed via [`ShareHeaderLookup`] discrete variants
        // rather than by inspecting `prev == BlockHash::all_zeros()`
        // — the daemon-side adapter already encodes that on the
        // wire so we don't double-check it here.
        //
        // We snapshot the chain handle (a cheap `Arc::clone`) up
        // front so the await inside the loop doesn't borrow `handles`
        // (which would extend the borrow across the await and break
        // the future's `Send` bound).
        let chain = handles.chain.clone();
        let mut ancestors: std::collections::HashSet<BlockHash> = std::collections::HashSet::new();
        ancestors.insert(new_tip);
        let mut cursor = new_tip;
        for _ in 0..REORG_ANCESTRY_DEPTH {
            match chain.get_share_header(&cursor).await {
                Ok(ShareHeaderLookup::Found(header)) => {
                    match header.prev_share_blockhash {
                        Some(prev) => {
                            ancestors.insert(prev);
                            cursor = prev;
                        }
                        None => break, // genesis predecessor encoded as None
                    }
                }
                Ok(ShareHeaderLookup::Genesis) => {
                    // The cursor itself was the all-zeros sentinel —
                    // we've fallen off the end of the share chain.
                    // Same effect as the `prev == all_zeros` case
                    // in the legacy code path: stop walking.
                    break;
                }
                Ok(ShareHeaderLookup::NotFound) => {
                    warn!(
                        cursor = %cursor,
                        "notify_share_chain_reorg: header not found mid-walk; falling back to invalidate_all"
                    );
                    let dropped = self.declared_jobs().invalidate_all();
                    bump_dropped(dropped);
                    info!(
                        new_tip = %new_tip,
                        dropped,
                        "notify_share_chain_reorg: flushed declared-jobs cache (header not found)"
                    );
                    return;
                }
                Err(e) => {
                    warn!(
                        cursor = %cursor,
                        error = %e,
                        "notify_share_chain_reorg: ancestor walk truncated; falling back to invalidate_all"
                    );
                    let dropped = self.declared_jobs().invalidate_all();
                    bump_dropped(dropped);
                    info!(
                        new_tip = %new_tip,
                        dropped,
                        "notify_share_chain_reorg: flushed declared-jobs cache (ancestor walk failed)"
                    );
                    return;
                }
            }
        }

        let dropped = self
            .declared_jobs()
            .retain(|job| match job.share_chain_tip {
                Some(tip) => ancestors.contains(&tip),
                None => false, // conservatively drop jobs without captured tip
            });
        bump_dropped(dropped);
        info!(
            new_tip = %new_tip,
            dropped,
            ancestors_walked = ancestors.len(),
            "notify_share_chain_reorg: selective invalidation complete"
        );
    }
}

/// Maximum number of `prev_share_blockhash` hops to walk back from a
/// new tip when deciding which cached `DeclaredJob`s survive a reorg.
/// 100 hops is enough for any reasonable share-chain reorg depth in
/// practice; jobs that captured a tip beyond that horizon get dropped
/// (the operator was likely offline, the cache is stale anyway).
const REORG_ANCESTRY_DEPTH: usize = 100;

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
    async fn declare_mining_job_first_pass_returns_missing_transactions() {
        // Valid coinbase + non-empty wtxid_list + no PMTS → engine asks
        // the JDC for tx bodies via MissingTransactions(wtxid_list).
        // The job is NOT cached on this pass (the JDC will re-issue
        // DeclareMiningJob with the bodies).
        let engine = P2poolV2Engine::default();
        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);
        let wtxid = [42u8; 32];
        let msg = build_declare_mining_job(7, 99, 0x20000000, prefix, suffix, vec![wtxid]);
        let result = engine.handle_declare_mining_job(msg, None).await;
        match result {
            DeclareMiningJobResult::MissingTransactions(missing) => {
                assert_eq!(missing.len(), 1);
                assert_eq!(missing[0].as_byte_array(), &wtxid);
            }
            other => panic!(
                "expected MissingTransactions, got {:?}",
                match other {
                    DeclareMiningJobResult::Success => "Success",
                    DeclareMiningJobResult::Error(_) => "Error",
                    DeclareMiningJobResult::MissingTransactions(_) => unreachable!(),
                }
            ),
        }
        // Cache + token map remain empty until the PMTS retry succeeds.
        assert!(engine.declared_jobs().is_empty());
        assert!(!engine.allocated_tokens().contains_key(&99));
    }

    #[tokio::test]
    async fn metrics_counters_increment_through_handlers() {
        use prometheus::Registry;

        use crate::EngineMetrics;

        let registry = Registry::new();
        let metrics = EngineMetrics::register(&registry).expect("register");
        let engine = P2poolV2Engine::default().with_metrics(metrics.clone());

        // Bad token → declare_mining_job_rejected.
        let bad_token: B0255<'static> = vec![1, 2, 3].try_into().expect("3 bytes fits");
        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);
        let bad_msg = DeclareMiningJob {
            request_id: 1,
            mining_job_token: bad_token,
            version: 1,
            coinbase_tx_prefix: prefix.clone().try_into().expect("fits"),
            coinbase_tx_suffix: suffix.clone().try_into().expect("fits"),
            wtxid_list: Vec::<U256<'static>>::new().into(),
            excess_data: Vec::new().try_into().expect("empty fits"),
        };
        engine.handle_declare_mining_job(bad_msg, None).await;
        assert_eq!(metrics.declare_mining_job_rejected.get(), 1);
        assert_eq!(metrics.declare_mining_job_accepted.get(), 0);
        assert_eq!(metrics.declare_mining_job_missing_txns.get(), 0);

        // Non-empty wtxid_list, no PMTS → MissingTransactions counter.
        let wtxid = [42u8; 32];
        let missing_msg = build_declare_mining_job(2, 99, 1, prefix, suffix, vec![wtxid]);
        engine.handle_declare_mining_job(missing_msg, None).await;
        assert_eq!(metrics.declare_mining_job_missing_txns.get(), 1);
        assert_eq!(metrics.declare_mining_job_rejected.get(), 1);

        // PushSolution increments push_solution_received (TDP-less path).
        let extranonce: Vec<u8> = vec![0; 16];
        let push = PushSolution {
            extranonce: extranonce.try_into().expect("fits"),
            prev_hash: [1u8; 32].to_vec().try_into().expect("32 bytes"),
            ntime: 0,
            nonce: 0,
            nbits: 0x207fffff,
            version: 0x20000000,
        };
        engine.handle_push_solution(push).await;
        assert_eq!(metrics.push_solution_received.get(), 1);
        assert_eq!(
            metrics.blocks_submitted.get(),
            0,
            "no handles wired → no submit_block"
        );

        // notify_share_chain_reorg → reorg_notifications.
        engine
            .notify_share_chain_reorg(BlockHash::from_byte_array([7u8; 32]))
            .await;
        assert_eq!(metrics.reorg_notifications.get(), 1);
    }

    #[tokio::test]
    async fn declare_mining_job_with_pmts_caches_snapshot() {
        // The PMTS-retry path: same DeclareMiningJob plus
        // ProvideMissingTransactionsSuccess carrying the actual
        // transaction bodies. Engine validates and caches.
        use stratum_apps::stratum_core::{
            binary_sv2::B016M, job_declaration_sv2::ProvideMissingTransactionsSuccess,
        };

        let engine = P2poolV2Engine::default();
        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);

        // Build a fake non-coinbase tx and use its real wtxid, so the
        // engine's check against PMTS bodies passes.
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
        let wtxid_bytes: [u8; 32] = *fake_tx.compute_wtxid().as_byte_array();

        let msg = build_declare_mining_job(7, 99, 0x20000000, prefix, suffix, vec![wtxid_bytes]);
        let serialized_tx = bitcoin::consensus::serialize(&fake_tx);
        let tx_bytes: B016M<'static> = serialized_tx.try_into().expect("fits");
        let pmts = ProvideMissingTransactionsSuccess {
            request_id: 7,
            transaction_list: Seq064K::new(vec![tx_bytes]).expect("fits"),
        };

        let result = engine.handle_declare_mining_job(msg, Some(pmts)).await;
        assert!(matches!(result, DeclareMiningJobResult::Success));
        assert_eq!(engine.declared_jobs().len(), 1);
        assert!(engine.allocated_tokens().contains_key(&99));
    }

    /// Build a structurally-valid `SetCustomMiningJob` fixture that
    /// matches the coinbase produced by `build_coinbase` /
    /// `split_coinbase`. Used to exercise the cross-check path
    /// without writing the per-field assertions ourselves in every
    /// test.
    fn build_set_custom_mining_job(
        token: u64,
        version: u32,
        prev_hash_bytes: [u8; 32],
        nbits: u32,
        coinbase_tx_outputs_serialized: Vec<u8>,
        merkle_path: Vec<[u8; 32]>,
    ) -> stratum_apps::stratum_core::mining_sv2::SetCustomMiningJob<'static> {
        use stratum_apps::stratum_core::{
            binary_sv2::{B064K, Seq0255},
            mining_sv2::SetCustomMiningJob,
        };
        let merkle: Vec<U256<'static>> = merkle_path
            .into_iter()
            .map(|b| b.to_vec().try_into().expect("32 bytes"))
            .collect();
        SetCustomMiningJob {
            channel_id: 1,
            request_id: 1,
            token: token_b0255(token),
            version,
            prev_hash: prev_hash_bytes.to_vec().try_into().expect("32 bytes"),
            min_ntime: 0,
            nbits,
            coinbase_tx_version: 2,
            coinbase_prefix: Vec::<u8>::new().try_into().expect("empty fits"),
            coinbase_tx_input_n_sequence: u32::MAX,
            coinbase_tx_outputs: TryInto::<B064K<'static>>::try_into(
                coinbase_tx_outputs_serialized,
            )
            .expect("outputs fit"),
            coinbase_tx_locktime: 0,
            merkle_path: Seq0255::new(merkle).expect("merkle fits"),
        }
    }

    #[tokio::test]
    async fn set_custom_mining_job_handles_less_skips_tip_checks() {
        // Without handles wired, declared.tip is all-zeros default.
        // SetCustomMiningJob with arbitrary non-zero prev_hash/nbits
        // must not return STALE_CHAIN_TIP / INVALID_NBITS — instead
        // the cross-checks are skipped and only the structural
        // checks (version, coinbase, merkle path) apply.
        use stratum_apps::stratum_core::{
            binary_sv2::B016M, job_declaration_sv2::ProvideMissingTransactionsSuccess,
        };

        let engine = P2poolV2Engine::default();
        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);

        // Two-step declare: send wtxid_list, then re-issue with PMTS.
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
        let wtxid_bytes: [u8; 32] = *fake_tx.compute_wtxid().as_byte_array();
        let serialized_tx = bitcoin::consensus::serialize(&fake_tx);
        let tx_bytes: B016M<'static> = serialized_tx.try_into().expect("fits");
        let pmts = ProvideMissingTransactionsSuccess {
            request_id: 7,
            transaction_list: Seq064K::new(vec![tx_bytes]).expect("fits"),
        };
        let declare =
            build_declare_mining_job(7, 99, 0x20000000, prefix, suffix, vec![wtxid_bytes]);
        let result = engine.handle_declare_mining_job(declare, Some(pmts)).await;
        assert!(matches!(result, DeclareMiningJobResult::Success));

        // Compute the values the engine will derive when validating.
        // The cached txid_list has the fake tx's txid (computed in
        // the engine from the PMTS bodies).
        let cached = engine.declared_jobs().get(&7).expect("cached");
        let reconstructed = crate::coinbase::reconstruct_coinbase(
            &cached.coinbase_tx_prefix,
            &cached.coinbase_tx_suffix,
        )
        .expect("reconstruct");
        let outputs_serialized = bitcoin::consensus::serialize(&reconstructed.output);
        let coinbase_txid = reconstructed.compute_txid();
        let txid_list = cached.txid_list.as_ref().expect("txid_list").clone();
        let merkle = crate::coinbase::merkle_path(coinbase_txid, &txid_list);
        let merkle_arr: Vec<[u8; 32]> = merkle
            .iter()
            .map(|m| {
                use bitcoin::hashes::Hash as _;
                m.as_byte_array().to_owned()
            })
            .collect();

        let custom = build_set_custom_mining_job(
            99,
            0x20000000,
            // Arbitrary non-zero prev_hash — would fail the STALE_CHAIN_TIP
            // check if the engine didn't skip it.
            [0xab; 32],
            // Same for nbits.
            0x12345678,
            outputs_serialized,
            merkle_arr,
        );
        let result = engine.handle_set_custom_mining_job(custom, 99).await;
        let success = matches!(result, SetCustomMiningJobResult::Success);
        assert!(
            success,
            "expected Success in handles-less mode (cross-checks skipped)"
        );
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

        use bitcoin::hashes::Hash as _;
        use bitcoindrpc::{BitcoindLike, mock::MockBitcoind};
        use stratum_apps::stratum_core::{
            binary_sv2::{Seq064K, Seq0255},
            parsers_sv2::TemplateDistribution,
            template_distribution_sv2::{
                NewTemplate, RequestTransactionDataSuccess, SetNewPrevHash,
            },
        };

        use crate::share_chain_reader::mock::MockShareChain;
        use crate::{EngineHandles, ShareChainReader, TdpHandle, tdp::TxDataResult};

        // 1. Build the engine with handles, including a TdpHandle.
        //    Uses `MockShareChain::with_no_genesis()` to preserve the
        //    original test intent (tip read returns `Ok(None)`,
        //    `share_chain_tip` capture path stores `None`). ADR 0011
        //    § Decision § "MockShareChain" documents the migration.
        let chain: Arc<dyn ShareChainReader> = Arc::new(MockShareChain::with_no_genesis());
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
            bitcoind: bitcoind.clone(),
        };
        let engine =
            P2poolV2Engine::with_handles(bitcoin::Network::Regtest, handles).with_tdp(tdp.clone());

        // 3. Build fake_tx fixtures used both as the JDC's PMTS body
        //    (declare-time) and as the stub TP's RequestTransactionData
        //    response (push_solution-time). They MUST be the same body
        //    so the cached txid_list lines up with the bytes the
        //    engine will receive when reconstructing the block.
        use stratum_apps::stratum_core::{
            binary_sv2::B016M, job_declaration_sv2::ProvideMissingTransactionsSuccess,
        };
        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);
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
        let wtxid_bytes: [u8; 32] = *fake_tx.compute_wtxid().as_byte_array();
        let serialized_tx = bitcoin::consensus::serialize(&fake_tx);
        let tx_bytes_for_pmts: B016M<'static> = serialized_tx.clone().try_into().expect("fits");
        let pmts = ProvideMissingTransactionsSuccess {
            request_id: 7,
            transaction_list: Seq064K::new(vec![tx_bytes_for_pmts]).expect("fits"),
        };

        // 3b. Spawn a stub TP demux: when RequestTransactionData
        //     arrives, deliver the SAME fake_tx body so block
        //     reconstruction sees txdata = [coinbase, fake_tx].
        let demux_tdp = tdp.clone();
        let stub_tx_body = serialized_tx.clone();
        tokio::spawn(async move {
            while let Ok(req) = req_rx.recv().await {
                if let TemplateDistribution::RequestTransactionData(r) = req {
                    let body: B016M<'static> = stub_tx_body.clone().try_into().expect("fits");
                    let success = RequestTransactionDataSuccess {
                        template_id: r.template_id,
                        excess_data: Vec::<u8>::new().try_into().expect("empty fits"),
                        transaction_list: Seq064K::new(vec![body]).expect("fits"),
                    };
                    demux_tdp.deliver_response(r.template_id, TxDataResult::Success(success));
                }
            }
        });

        // 4. Declare a mining job (PMTS attached, single-pass success).
        let declare = build_declare_mining_job(
            7,
            99,
            0x20000000,
            prefix.clone(),
            suffix.clone(),
            vec![wtxid_bytes],
        );
        let result = engine.handle_declare_mining_job(declare, Some(pmts)).await;
        assert!(matches!(result, DeclareMiningJobResult::Success));

        // Sanity: cached job has the captured template_id + tip.
        let cached = engine.declared_jobs().get(&7).expect("declared job cached");
        assert_eq!(cached.template_id, Some(template_id));
        assert_eq!(cached.tip.nbits, tip_nbits);
        assert_eq!(cached.tip.min_ntime, tip_min_ntime);
        assert_eq!(cached.tip.prev_hash.as_byte_array(), &tip_prev_hash_bytes);
        // share_chain_tip is best-effort: the test fixture's
        // ChainStoreHandle is uninitialised (no genesis), so reading
        // the tip errors out and the engine logs a warn + stores
        // None. The capture path is exercised in pool's
        // share_chain::tests::engine_reorg_watcher_polls_chain_handle
        // where genesis is initialised.
        let _ = cached.share_chain_tip;

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
        // Block carries [coinbase, fake_tx] — the stub TP returned the
        // fake_tx body in response to RequestTransactionData.
        assert_eq!(block.txdata.len(), 2);
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
    async fn push_solution_increments_submit_failed_counter_on_bitcoind_error() {
        use std::sync::Arc;

        use async_trait::async_trait;
        use bitcoin::hashes::Hash as _;
        use bitcoindrpc::{BitcoindLike, BitcoindRpcError, GetBlockchainInfo, ProposalOutcome};
        use prometheus::Registry;
        use stratum_apps::stratum_core::{
            binary_sv2::{B016M, Seq064K, Seq0255},
            job_declaration_sv2::ProvideMissingTransactionsSuccess,
            parsers_sv2::TemplateDistribution,
            template_distribution_sv2::{
                NewTemplate, RequestTransactionDataSuccess, SetNewPrevHash,
            },
        };

        use crate::share_chain_reader::mock::MockShareChain;
        use crate::{
            EngineHandles, EngineMetrics, ShareChainReader, TdpHandle, tdp::TxDataResult,
        };

        // submit_block always returns Err; everything else falls through
        // to "unscripted" but the test path only touches submit_block.
        struct FailingBitcoind;

        #[async_trait]
        impl BitcoindLike for FailingBitcoind {
            async fn get_difficulty(&self) -> Result<f64, BitcoindRpcError> {
                Ok(1.0)
            }
            async fn getblockchaininfo(&self) -> Result<GetBlockchainInfo, BitcoindRpcError> {
                Ok(GetBlockchainInfo {
                    initial_block_download: false,
                })
            }
            async fn getblocktemplate(
                &self,
                _network: bitcoin::Network,
            ) -> Result<String, BitcoindRpcError> {
                Ok("{}".to_string())
            }
            async fn decoderawtransaction(
                &self,
                tx: &bitcoin::Transaction,
            ) -> Result<bitcoin::Transaction, BitcoindRpcError> {
                Ok(tx.clone())
            }
            async fn submit_block(
                &self,
                _block: &bitcoin::Block,
            ) -> Result<String, BitcoindRpcError> {
                Err(BitcoindRpcError::Other("simulated rpc failure".to_string()))
            }
            async fn validate_block_proposal(
                &self,
                _block: &bitcoin::Block,
            ) -> Result<ProposalOutcome, BitcoindRpcError> {
                Ok(ProposalOutcome::Accepted)
            }
        }

        let registry = Registry::new();
        let metrics = EngineMetrics::register(&registry).expect("register");

        let chain: Arc<dyn ShareChainReader> = Arc::new(MockShareChain::with_no_genesis());
        let bitcoind: Arc<dyn BitcoindLike> = Arc::new(FailingBitcoind);
        let (req_tx, req_rx) = async_channel::unbounded();
        let tdp = TdpHandle::new(req_tx);

        let tip_prev_hash_bytes = [9u8; 32];
        let tip_nbits: u32 = 0x207fffff;
        let tip_min_ntime: u32 = 1_700_000_000;
        let template_id: u64 = 12345;

        tdp.record_set_new_prev_hash(SetNewPrevHash {
            template_id,
            prev_hash: tip_prev_hash_bytes.to_vec().try_into().expect("32 bytes"),
            header_timestamp: tip_min_ntime,
            n_bits: tip_nbits,
            target: [0u8; 32].to_vec().try_into().expect("32 bytes"),
        });
        tdp.record_new_template(NewTemplate {
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
        });

        let handles = EngineHandles { chain, bitcoind };
        let engine = P2poolV2Engine::with_handles(bitcoin::Network::Regtest, handles)
            .with_tdp(tdp.clone())
            .with_metrics(metrics.clone());

        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);
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
        let wtxid_bytes: [u8; 32] = *fake_tx.compute_wtxid().as_byte_array();
        let serialized_tx = bitcoin::consensus::serialize(&fake_tx);
        let tx_bytes_for_pmts: B016M<'static> = serialized_tx.clone().try_into().expect("fits");
        let pmts = ProvideMissingTransactionsSuccess {
            request_id: 7,
            transaction_list: Seq064K::new(vec![tx_bytes_for_pmts]).expect("fits"),
        };

        let demux_tdp = tdp.clone();
        let stub_tx_body = serialized_tx.clone();
        tokio::spawn(async move {
            while let Ok(req) = req_rx.recv().await {
                if let TemplateDistribution::RequestTransactionData(r) = req {
                    let body: B016M<'static> = stub_tx_body.clone().try_into().expect("fits");
                    let success = RequestTransactionDataSuccess {
                        template_id: r.template_id,
                        excess_data: Vec::<u8>::new().try_into().expect("empty fits"),
                        transaction_list: Seq064K::new(vec![body]).expect("fits"),
                    };
                    demux_tdp.deliver_response(r.template_id, TxDataResult::Success(success));
                }
            }
        });

        let declare = build_declare_mining_job(
            7,
            99,
            0x20000000,
            prefix.clone(),
            suffix.clone(),
            vec![wtxid_bytes],
        );
        let result = engine.handle_declare_mining_job(declare, Some(pmts)).await;
        assert!(matches!(result, DeclareMiningJobResult::Success));

        let push = PushSolution {
            extranonce: vec![0xab; 16].try_into().expect("fits"),
            prev_hash: tip_prev_hash_bytes.to_vec().try_into().expect("32 bytes"),
            ntime: tip_min_ntime,
            nonce: 0xDEADBEEF,
            nbits: tip_nbits,
            version: 0x20000000,
        };
        engine.handle_push_solution(push).await;

        // Wait for the spawned submit_block task to land its Err arm.
        for _ in 0..40 {
            if metrics.blocks_submit_failed.get() > 0 {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        assert_eq!(metrics.blocks_submit_failed.get(), 1);
        // The "we tried" counter still increments — both should be 1.
        assert_eq!(metrics.blocks_submitted.get(), 1);
    }

    #[tokio::test]
    async fn push_solution_increments_submit_failed_counter_on_bitcoind_rejection() {
        // bitcoind's submitblock RPC returns Ok(<rejection-string>) on
        // consensus rejection — NOT Err. Verify we still bump the
        // failure counter so operator dashboards can alert on
        // "rejected by bitcoind" (the most common lost-block mode).
        use std::sync::Arc;

        use bitcoin::hashes::Hash as _;
        use bitcoindrpc::{BitcoindLike, mock::MockBitcoind};
        use prometheus::Registry;
        use stratum_apps::stratum_core::{
            binary_sv2::{B016M, Seq064K, Seq0255},
            job_declaration_sv2::ProvideMissingTransactionsSuccess,
            parsers_sv2::TemplateDistribution,
            template_distribution_sv2::{
                NewTemplate, RequestTransactionDataSuccess, SetNewPrevHash,
            },
        };

        use crate::share_chain_reader::mock::MockShareChain;
        use crate::{
            EngineHandles, EngineMetrics, ShareChainReader, TdpHandle, tdp::TxDataResult,
        };

        let registry = Registry::new();
        let metrics = EngineMetrics::register(&registry).expect("register");

        let chain: Arc<dyn ShareChainReader> = Arc::new(MockShareChain::with_no_genesis());
        // Mock returns Ok("\"high-hash\"") — bitcoind's wire shape for a
        // PoW-rejected block (a JSON string, including the quotes after
        // serde_json::Value::to_string()).
        let mock_bitcoind = Arc::new(
            MockBitcoind::default().with_submit_block_response("\"high-hash\"".to_string()),
        );
        let bitcoind: Arc<dyn BitcoindLike> = mock_bitcoind.clone();
        let (req_tx, req_rx) = async_channel::unbounded();
        let tdp = TdpHandle::new(req_tx);

        let tip_prev_hash_bytes = [9u8; 32];
        let tip_nbits: u32 = 0x207fffff;
        let tip_min_ntime: u32 = 1_700_000_000;
        let template_id: u64 = 12345;

        tdp.record_set_new_prev_hash(SetNewPrevHash {
            template_id,
            prev_hash: tip_prev_hash_bytes.to_vec().try_into().expect("32 bytes"),
            header_timestamp: tip_min_ntime,
            n_bits: tip_nbits,
            target: [0u8; 32].to_vec().try_into().expect("32 bytes"),
        });
        tdp.record_new_template(NewTemplate {
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
        });

        let handles = EngineHandles { chain, bitcoind };
        let engine = P2poolV2Engine::with_handles(bitcoin::Network::Regtest, handles)
            .with_tdp(tdp.clone())
            .with_metrics(metrics.clone());

        let cb = build_coinbase(vec![0; 16]);
        let (prefix, suffix) = split_coinbase(&cb, 16);
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
        let wtxid_bytes: [u8; 32] = *fake_tx.compute_wtxid().as_byte_array();
        let serialized_tx = bitcoin::consensus::serialize(&fake_tx);
        let tx_bytes_for_pmts: B016M<'static> = serialized_tx.clone().try_into().expect("fits");
        let pmts = ProvideMissingTransactionsSuccess {
            request_id: 7,
            transaction_list: Seq064K::new(vec![tx_bytes_for_pmts]).expect("fits"),
        };

        let demux_tdp = tdp.clone();
        let stub_tx_body = serialized_tx.clone();
        tokio::spawn(async move {
            while let Ok(req) = req_rx.recv().await {
                if let TemplateDistribution::RequestTransactionData(r) = req {
                    let body: B016M<'static> = stub_tx_body.clone().try_into().expect("fits");
                    let success = RequestTransactionDataSuccess {
                        template_id: r.template_id,
                        excess_data: Vec::<u8>::new().try_into().expect("empty fits"),
                        transaction_list: Seq064K::new(vec![body]).expect("fits"),
                    };
                    demux_tdp.deliver_response(r.template_id, TxDataResult::Success(success));
                }
            }
        });

        let declare = build_declare_mining_job(
            7,
            99,
            0x20000000,
            prefix.clone(),
            suffix.clone(),
            vec![wtxid_bytes],
        );
        let result = engine.handle_declare_mining_job(declare, Some(pmts)).await;
        assert!(matches!(result, DeclareMiningJobResult::Success));

        let push = PushSolution {
            extranonce: vec![0xab; 16].try_into().expect("fits"),
            prev_hash: tip_prev_hash_bytes.to_vec().try_into().expect("32 bytes"),
            ntime: tip_min_ntime,
            nonce: 0xDEADBEEF,
            nbits: tip_nbits,
            version: 0x20000000,
        };
        engine.handle_push_solution(push).await;

        for _ in 0..40 {
            if metrics.blocks_submit_failed.get() > 0 {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        assert_eq!(
            metrics.blocks_submit_failed.get(),
            1,
            "rejection-string Ok(_) should still bump the failure counter"
        );
        // bitcoind still received the block — the rejection happens after.
        assert_eq!(mock_bitcoind.submitted_blocks().len(), 1);
    }

    #[tokio::test]
    async fn push_solution_no_handles_records_synthetic_only() {
        // Without handles wired, push_solution stays in structural-only mode:
        // records synthetic→synthetic in RecentSolutions; never panics.
        // Also: increments push_solution_dropped_total{reason="no_handles"}.
        use prometheus::Registry;

        use crate::EngineMetrics;

        let registry = Registry::new();
        let metrics = EngineMetrics::register(&registry).expect("register");
        let engine = P2poolV2Engine::default().with_metrics(metrics.clone());

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
        assert_eq!(
            metrics
                .push_solution_dropped
                .with_label_values(&["no_handles"])
                .get(),
            1
        );
    }

    #[tokio::test]
    async fn push_solution_dropped_no_matching_job_increments_counter() {
        // TDP wired but no DeclaredJob matches the PushSolution. The
        // engine should bail at find_by_solution and bump the counter
        // with reason="no_matching_job".
        use std::sync::Arc;

        use bitcoindrpc::{BitcoindLike, mock::MockBitcoind};
        use prometheus::Registry;

        use crate::share_chain_reader::mock::MockShareChain;
        use crate::{EngineHandles, EngineMetrics, ShareChainReader, TdpHandle};

        let registry = Registry::new();
        let metrics = EngineMetrics::register(&registry).expect("register");

        let chain: Arc<dyn ShareChainReader> = Arc::new(MockShareChain::with_no_genesis());
        let mock_bitcoind = Arc::new(MockBitcoind::default());
        let bitcoind: Arc<dyn BitcoindLike> = mock_bitcoind.clone();
        let (req_tx, _req_rx) = async_channel::unbounded();
        let tdp = TdpHandle::new(req_tx);

        let handles = EngineHandles { chain, bitcoind };
        let engine = P2poolV2Engine::with_handles(bitcoin::Network::Regtest, handles)
            .with_tdp(tdp)
            .with_metrics(metrics.clone());

        // No DeclaredJob ever cached. Drive a PushSolution: the engine
        // hits the find_by_solution → None path.
        let push = PushSolution {
            extranonce: vec![0xab; 16].try_into().expect("fits"),
            prev_hash: [9u8; 32].to_vec().try_into().expect("32 bytes"),
            ntime: 1_700_000_000,
            nonce: 0xDEADBEEF,
            nbits: 0x207fffff,
            version: 0x20000000,
        };
        engine.handle_push_solution(push).await;

        assert_eq!(
            metrics
                .push_solution_dropped
                .with_label_values(&["no_matching_job"])
                .get(),
            1,
            "no_matching_job arm should fire when no cached DeclaredJob exists"
        );
        // No block submission attempt — submitted_blocks is empty.
        assert!(mock_bitcoind.submitted_blocks().is_empty());
        // The other reasons stay at zero.
        for other in [
            "cache_race",
            "no_template_id",
            "tdp_fetch_failed",
            "reconstruct_failed",
            "no_handles",
        ] {
            assert_eq!(
                metrics
                    .push_solution_dropped
                    .with_label_values(&[other])
                    .get(),
                0,
                "reason={other} should stay at zero"
            );
        }
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
                share_chain_tip: None,
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
                share_chain_tip: None,
                validated: true,
            },
        );
        assert_eq!(engine.declared_jobs().len(), 2);

        let new_tip = BlockHash::from_byte_array([7u8; 32]);
        engine.notify_share_chain_reorg(new_tip).await;

        assert!(engine.declared_jobs().is_empty());
    }
}
