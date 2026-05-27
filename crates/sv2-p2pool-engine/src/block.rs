//! Block (header) reconstruction from `PushSolution` + cached `DeclaredJob`.
//!
//! When a JDC sends a `PushSolution`, we need to assemble the corresponding
//! `bitcoin::Block` so we can submit it to bitcoind via
//! `BitcoindLike::submit_block`. The full block requires:
//!
//! 1. The reconstructed coinbase tx (from prefix + extranonce + suffix)
//! 2. All non-coinbase transactions (lookup against an external mempool)
//! 3. A header with: version, prev_hash, merkle_root, ntime, nbits, nonce
//!
//! **Phase 2.2 scope**: header reconstruction + coinbase reconstruction +
//! merkle-root computation from a known `txid_list`. The full
//! `bitcoin::Block` requires a tx-source (mempool, GBT lookup, or
//! p2poolv2's share-block transaction storage) — that lands in Phase 2.4
//! once we know which tx-source we have access to.
//!
//! # Mirror of upstream
//!
//! The upstream `bitcoin-core-sv2` JDP handler at
//! `vendor/sv2-apps/bitcoin-core-sv2/src/job_declaration_protocol/handlers.rs:`
//! has `handle_push_solution` as a `// todo` stub. We're implementing
//! this for the first time anywhere in the SV2 ecosystem.

use bitcoin::{
    Block, BlockHash, CompactTarget, Transaction, TxMerkleNode, Txid,
    block::{Header, Version},
    consensus::Encodable,
    hashes::Hash,
};
use stratum_apps::stratum_core::job_declaration_sv2::PushSolution;

use crate::{DeclaredJob, coinbase};

/// Reconstruct the candidate Bitcoin block header from a `PushSolution`
/// + the cached `DeclaredJob` it refers to.
///
/// This computes the actual `merkle_root` from the coinbase txid (which
/// depends on the chosen extranonce) plus the cached `txid_list`. The
/// other header fields come straight from `PushSolution`.
///
/// The caller is responsible for matching `PushSolution` to its
/// `DeclaredJob` — `PushSolution` doesn't carry a token or request_id,
/// so the match is by `(prev_hash, nbits, version)` against in-flight
/// declared jobs (Phase 2.4).
pub fn reconstruct_header(
    declared_job: &DeclaredJob,
    push_solution: &PushSolution<'_>,
) -> Result<Header, BlockReconstructError> {
    // 1. The cached declared-job must have a populated txid_list. Set
    //    to Some(_) by `handle_declare_mining_job` after Success.
    let txid_list = declared_job
        .txid_list
        .as_ref()
        .ok_or(BlockReconstructError::DeclaredJobNotValidated)?;

    // 2. Reconstruct the coinbase with the chosen extranonce.
    let extranonce_bytes = push_solution.extranonce.inner_as_ref();
    let coinbase_tx = coinbase::reconstruct_coinbase_with_extranonce(
        &declared_job.coinbase_tx_prefix,
        extranonce_bytes,
        &declared_job.coinbase_tx_suffix,
    )
    .map_err(BlockReconstructError::Coinbase)?;
    let coinbase_txid = coinbase_tx.compute_txid();

    // 3. Compute the merkle root over [coinbase_txid, ...txid_list].
    let merkle_root = compute_merkle_root_from_txids(coinbase_txid, txid_list);

    // 4. Decode prev_hash from PushSolution's U256.
    let prev_blockhash: BlockHash = {
        let bytes: [u8; 32] = push_solution
            .prev_hash
            .to_vec()
            .try_into()
            .map_err(|_| BlockReconstructError::BadPrevHash)?;
        BlockHash::from_byte_array(bytes)
    };

    // 5. Assemble the header.
    Ok(Header {
        version: Version::from_consensus(push_solution.version as i32),
        prev_blockhash,
        merkle_root,
        time: push_solution.ntime,
        bits: CompactTarget::from_consensus(push_solution.nbits),
        nonce: push_solution.nonce,
    })
}

/// Reconstruct the full candidate `bitcoin::Block` from a `PushSolution`,
/// the cached `DeclaredJob`, and the non-coinbase transaction bodies
/// fetched via `RequestTransactionData(template_id)` from the Template
/// Provider.
///
/// Returns a fully-formed `bitcoin::Block` ready to hand to
/// `BitcoindLike::submit_block`.
///
/// Cross-check: the txids of `tx_bodies` MUST match (in order) the cached
/// `DeclaredJob.txid_list`. If they don't, the TP gave us a transaction
/// list inconsistent with the declared job and we refuse to assemble a
/// block.
pub fn reconstruct_block(
    declared_job: &DeclaredJob,
    push_solution: &PushSolution<'_>,
    tx_bodies: Vec<Transaction>,
) -> Result<Block, BlockReconstructError> {
    let txid_list = declared_job
        .txid_list
        .as_ref()
        .ok_or(BlockReconstructError::DeclaredJobNotValidated)?;

    if tx_bodies.len() != txid_list.len() {
        return Err(BlockReconstructError::TxBodyCountMismatch {
            expected: txid_list.len(),
            got: tx_bodies.len(),
        });
    }
    for (idx, (body, expected_txid)) in tx_bodies.iter().zip(txid_list.iter()).enumerate() {
        let computed = body.compute_txid();
        if computed != *expected_txid {
            return Err(BlockReconstructError::TxBodyTxidMismatch {
                index: idx,
                expected: *expected_txid,
                got: computed,
            });
        }
    }

    let extranonce_bytes = push_solution.extranonce.inner_as_ref();
    let coinbase_tx = coinbase::reconstruct_coinbase_with_extranonce(
        &declared_job.coinbase_tx_prefix,
        extranonce_bytes,
        &declared_job.coinbase_tx_suffix,
    )
    .map_err(BlockReconstructError::Coinbase)?;
    let coinbase_txid = coinbase_tx.compute_txid();

    let merkle_root = compute_merkle_root_from_txids(coinbase_txid, txid_list);

    let prev_blockhash: BlockHash = {
        let bytes: [u8; 32] = push_solution
            .prev_hash
            .to_vec()
            .try_into()
            .map_err(|_| BlockReconstructError::BadPrevHash)?;
        BlockHash::from_byte_array(bytes)
    };

    let header = Header {
        version: Version::from_consensus(push_solution.version as i32),
        prev_blockhash,
        merkle_root,
        time: push_solution.ntime,
        bits: CompactTarget::from_consensus(push_solution.nbits),
        nonce: push_solution.nonce,
    };

    let mut txdata = Vec::with_capacity(1 + tx_bodies.len());
    txdata.push(coinbase_tx);
    txdata.extend(tx_bodies);

    Ok(Block { header, txdata })
}

/// Compute the Bitcoin merkle root from `[coinbase_txid, ...txid_list]`.
///
/// Bitcoin's merkle tree algorithm: at each level, hash adjacent pairs;
/// if there's an odd number, the last element is duplicated.
///
/// This mirrors `bitcoin::Block::compute_merkle_root` but takes raw
/// `Txid`s rather than full transactions, which is what we have at
/// reconstruction time.
fn compute_merkle_root_from_txids(coinbase_txid: Txid, txid_list: &[Txid]) -> TxMerkleNode {
    let mut hashes: Vec<TxMerkleNode> = Vec::with_capacity(1 + txid_list.len());
    hashes.push(coinbase_txid.into());
    for txid in txid_list {
        hashes.push((*txid).into());
    }

    if hashes.is_empty() {
        // Should never happen since we always push the coinbase.
        return TxMerkleNode::all_zeros();
    }

    while hashes.len() > 1 {
        let half = hashes.len().div_ceil(2);
        let mut next_level = Vec::with_capacity(half);
        for idx in 0..half {
            let left = hashes[2 * idx];
            // Bitcoin duplicates the last odd element.
            let right = hashes[std::cmp::min(2 * idx + 1, hashes.len() - 1)];
            let mut engine = TxMerkleNode::engine();
            left.consensus_encode(&mut engine)
                .expect("in-memory writers don't error");
            right
                .consensus_encode(&mut engine)
                .expect("in-memory writers don't error");
            next_level.push(TxMerkleNode::from_engine(engine));
        }
        hashes = next_level;
    }

    hashes[0]
}

/// Errors from block (header) reconstruction.
#[derive(Debug, thiserror::Error)]
pub enum BlockReconstructError {
    #[error("declared-job has no txid_list (was not validated to Success)")]
    DeclaredJobNotValidated,
    #[error("coinbase reconstruction failed: {0}")]
    Coinbase(#[from] coinbase::CoinbaseReconstructError),
    #[error("PushSolution.prev_hash was not 32 bytes")]
    BadPrevHash,
    #[error("tx_bodies count mismatch: expected {expected}, got {got}")]
    TxBodyCountMismatch { expected: usize, got: usize },
    #[error("tx_bodies[{index}] txid mismatch: expected {expected}, got {got}")]
    TxBodyTxidMismatch {
        index: usize,
        expected: Txid,
        got: Txid,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Coinbase-only block: txid_list is empty, so merkle root == coinbase txid.
    #[test]
    fn merkle_root_single_coinbase() {
        let coinbase = Txid::from_byte_array([7u8; 32]);
        let root = compute_merkle_root_from_txids(coinbase, &[]);
        assert_eq!(root, TxMerkleNode::from(coinbase));
    }

    /// Two-tx block: merkle root = SHA256d(coinbase || other).
    #[test]
    fn merkle_root_two_txs() {
        let coinbase = Txid::from_byte_array([1u8; 32]);
        let other = Txid::from_byte_array([2u8; 32]);
        let root = compute_merkle_root_from_txids(coinbase, &[other]);

        // Hand-compute the expected value: SHA256d(coinbase || other).
        let mut engine = TxMerkleNode::engine();
        TxMerkleNode::from(coinbase)
            .consensus_encode(&mut engine)
            .unwrap();
        TxMerkleNode::from(other)
            .consensus_encode(&mut engine)
            .unwrap();
        let expected = TxMerkleNode::from_engine(engine);
        assert_eq!(root, expected);
    }

    /// Three-tx block: tests the odd-elements-duplication rule. The right
    /// child of the last pair at the first level should be the third
    /// element duplicated.
    #[test]
    fn merkle_root_three_txs_duplicates_odd() {
        let cb = Txid::from_byte_array([1u8; 32]);
        let t1 = Txid::from_byte_array([2u8; 32]);
        let t2 = Txid::from_byte_array([3u8; 32]);
        let root = compute_merkle_root_from_txids(cb, &[t1, t2]);

        // Hand-compute: level 1 = [hash(cb || t1), hash(t2 || t2)],
        // level 0 (root) = hash(level1[0] || level1[1]).
        fn h(a: TxMerkleNode, b: TxMerkleNode) -> TxMerkleNode {
            let mut engine = TxMerkleNode::engine();
            a.consensus_encode(&mut engine).unwrap();
            b.consensus_encode(&mut engine).unwrap();
            TxMerkleNode::from_engine(engine)
        }

        let cb_n: TxMerkleNode = cb.into();
        let t1_n: TxMerkleNode = t1.into();
        let t2_n: TxMerkleNode = t2.into();
        let level1_left = h(cb_n, t1_n);
        let level1_right = h(t2_n, t2_n);
        let expected = h(level1_left, level1_right);

        assert_eq!(root, expected);
    }
}
