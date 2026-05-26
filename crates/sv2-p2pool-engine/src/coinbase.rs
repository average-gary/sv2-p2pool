//! Coinbase reconstruction + merkle-path helpers.
//!
//! Mirrors the equivalent logic in the upstream
//! `BitcoinCoreIPCEngine::DeclaredCustomJob::{get_coinbase_tx, get_merkle_path}`
//! at `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:100-204`,
//! lifted into free functions so our engine + tests can use them without
//! taking a dep on the upstream private type.
//!
//! These helpers are kept narrow on purpose: input is bytes (prefix +
//! suffix as published by the JDC), output is a parsed [`bitcoin::Transaction`]
//! and (for the merkle-path helper) a `Vec<TxMerkleNode>` ready to compare
//! against `SetCustomMiningJob.merkle_path`.

use bitcoin::{
    Transaction, TxMerkleNode, Txid,
    consensus::{Decodable, Encodable},
    hashes::Hash,
};

/// Coinbase prefix layout assumed by the coinbase-reconstruction helper.
///
/// Coinbase structure: `version(4) + marker+flag(2) + input_count(1) +
/// outpoint(32) + index(4) = 43` bytes, followed by a `scriptSig` length
/// `VarInt` and the `scriptSig` body.
const COINBASE_PREFIX_LEN: usize = 43;

/// Reconstruct the declared coinbase transaction by concatenating
/// `prefix + zero-padded extranonce + suffix`.
///
/// The extranonce size is implied: it's the difference between the
/// `scriptSig` length encoded in the prefix's `VarInt` and the bytes of
/// `scriptSig` already present in the prefix.
///
/// Returns `Err(CoinbaseReconstructError::*)` on malformed input — caller
/// maps to `INVALID_COINBASE_TX`.
pub fn reconstruct_coinbase(
    coinbase_tx_prefix: &[u8],
    coinbase_tx_suffix: &[u8],
) -> Result<Transaction, CoinbaseReconstructError> {
    if coinbase_tx_prefix.len() < COINBASE_PREFIX_LEN {
        return Err(CoinbaseReconstructError::PrefixTooShort);
    }

    let script_sig_size: usize = {
        let mut cursor = &coinbase_tx_prefix[COINBASE_PREFIX_LEN..];
        bitcoin::VarInt::consensus_decode(&mut cursor)
            .map_err(|_| CoinbaseReconstructError::BadScriptSigVarInt)?
            .0 as usize
    };

    let varint_size = bitcoin::VarInt(script_sig_size as u64).size();
    let script_sig_offset = COINBASE_PREFIX_LEN + varint_size;

    if coinbase_tx_prefix.len() < script_sig_offset {
        return Err(CoinbaseReconstructError::PrefixTooShort);
    }

    let script_sig_bytes_in_prefix = coinbase_tx_prefix.len() - script_sig_offset;
    if script_sig_bytes_in_prefix > script_sig_size {
        return Err(CoinbaseReconstructError::ScriptSigOverflow);
    }
    let full_extranonce_size = script_sig_size - script_sig_bytes_in_prefix;

    let mut declared_coinbase_tx = coinbase_tx_prefix.to_vec();
    declared_coinbase_tx.extend(std::iter::repeat_n(0u8, full_extranonce_size));
    declared_coinbase_tx.extend_from_slice(coinbase_tx_suffix);

    Transaction::consensus_decode(&mut &declared_coinbase_tx[..])
        .map_err(|_| CoinbaseReconstructError::Decode)
}

/// Errors from [`reconstruct_coinbase`].
#[derive(Debug, thiserror::Error)]
pub enum CoinbaseReconstructError {
    #[error("coinbase prefix shorter than minimum")]
    PrefixTooShort,
    #[error("malformed scriptSig VarInt in coinbase prefix")]
    BadScriptSigVarInt,
    #[error("scriptSig bytes in prefix exceed declared scriptSig size")]
    ScriptSigOverflow,
    #[error("failed to decode reconstructed coinbase as a Transaction")]
    Decode,
}

/// Compute the merkle-path "branch" used to derive the block header's
/// merkle root from the coinbase position (index 0).
///
/// `coinbase_txid` is the coinbase's `Txid` (computed from a successful
/// [`reconstruct_coinbase`]); `txid_list` is the list of non-coinbase
/// txids in tree order. Returns the sibling hashes at each level from
/// leaf to root, ready to compare against `SetCustomMiningJob.merkle_path`.
///
/// Mirrors `DeclaredCustomJob::get_merkle_path` line-for-line; lifted out
/// so we can unit-test without driving the trait.
pub fn merkle_path(coinbase_txid: Txid, txid_list: &[Txid]) -> Vec<TxMerkleNode> {
    let mut hashes: Vec<TxMerkleNode> = Vec::with_capacity(1 + txid_list.len());
    hashes.push(coinbase_txid.into());
    for txid in txid_list {
        hashes.push((*txid).into());
    }

    if hashes.len() == 1 {
        return Vec::new();
    }

    let mut branch = Vec::new();
    while hashes.len() > 1 {
        branch.push(hashes[1]);

        let half = hashes.len().div_ceil(2);
        let mut next_level = Vec::with_capacity(half);
        for idx in 0..half {
            let left = hashes[2 * idx];
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

    branch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconstruct_rejects_short_prefix() {
        let err = reconstruct_coinbase(&[0u8; 10], &[]).unwrap_err();
        assert!(matches!(err, CoinbaseReconstructError::PrefixTooShort));
    }

    #[test]
    fn merkle_path_single_tx_returns_empty_branch() {
        let coinbase = Txid::from_byte_array([1u8; 32]);
        let branch = merkle_path(coinbase, &[]);
        assert!(branch.is_empty());
    }

    #[test]
    fn merkle_path_two_txs_returns_one_sibling() {
        let coinbase = Txid::from_byte_array([1u8; 32]);
        let other = Txid::from_byte_array([2u8; 32]);
        let branch = merkle_path(coinbase, &[other]);
        assert_eq!(branch.len(), 1);
        assert_eq!(branch[0], TxMerkleNode::from(other));
    }
}
