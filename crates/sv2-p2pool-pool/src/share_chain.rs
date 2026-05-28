//! Phase 2.5b: minimum-slice share-chain backend bootstrap.
//!
//! Builds the three handles our engine consumes —
//! [`ChainStoreHandle`] (read-only share-chain access),
//! [`ShareValidator`] (share validation), and [`BitcoindLike`] (block
//! submission + future GBT proposals) — without booting the full
//! p2poolv2 [`NodeHandle`] (libp2p networking, ZMQ listener, GBT
//! poller, Stratum server, metrics, monitoring).
//!
//! The full Node bring-up is deferred: nothing in our engine's current
//! code path calls into p2poolv2's libp2p side. Once
//! `handle_declare_mining_job` starts validating against the share
//! chain — which requires a running Node organising blocks across
//! peers — that piece will land alongside.
//!
//! ## Lifecycle
//!
//! [`bootstrap_share_chain`] returns:
//! - The [`EngineHandles`] struct (ready to feed into
//!   [`P2poolV2Engine::with_handles`]).
//! - The [`StoreWriter`] thread join handle (so the caller can keep it
//!   alive for the lifetime of the pool and surface failures).
//! - The [`Arc<Store>`] (so the caller can run background-task
//!   maintenance against the same rocksdb instance).

use std::sync::Arc;

use bitcoindrpc::{BitcoindLike, BitcoindRpcClient};
use p2poolv2_lib::{
    config::Config as P2poolConfig,
    pool_difficulty::PoolDifficulty,
    shares::{
        chain::chain_store_handle::ChainStoreHandle,
        share_block::ShareBlock,
        validation::{DefaultShareValidator, ShareValidator},
    },
    store::{
        Store,
        writer::{StoreHandle, StoreWriter, write_channel},
    },
};
use sv2_p2pool_engine::EngineHandles;
use tokio::task::JoinHandle;
use tracing::info;

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
    #[error("failed to build pool difficulty: {0}")]
    PoolDifficulty(String),
    #[error("failed to construct bitcoind RPC client: {0:?}")]
    BitcoindClient(bitcoindrpc::BitcoindRpcError),
}

/// Live handles produced by [`bootstrap_share_chain`].
///
/// `engine_handles` is what the engine constructor consumes. The
/// `store` and `store_writer_join` are kept by the binary so they live
/// as long as the pool. Dropping them will tear down the share-chain
/// store; the binary holds them for the duration of `Pool::start`.
pub struct ShareChainHandles {
    pub engine_handles: EngineHandles,
    pub store: Arc<Store>,
    pub store_writer_join: JoinHandle<()>,
}

/// Bootstrap the minimum slice: open the rocksdb store, init genesis,
/// build a `DefaultShareValidator`, build a `BitcoindRpcClient`, and
/// pack them into [`EngineHandles`].
///
/// The `StoreWriter` is spawned via `tokio::task::spawn_blocking` (it
/// runs on a dedicated OS thread because rocksdb writes are sync). The
/// returned `JoinHandle<()>` should be kept alive by the caller; if it
/// stops before the pool shuts down, the store is gone.
pub async fn bootstrap_share_chain(
    p2pool_config: &P2poolConfig,
) -> Result<ShareChainHandles, ShareChainBootstrapError> {
    let network = p2pool_config.stratum.network;
    info!(
        network = %network,
        store_path = %p2pool_config.store.path,
        "share-chain bootstrap: opening store"
    );

    // 1. Open rocksdb. `Store::new(path, read_only=false)`.
    let store = Arc::new(
        Store::new(p2pool_config.store.path.clone(), false).map_err(|e| {
            ShareChainBootstrapError::OpenStore {
                path: p2pool_config.store.path.clone(),
                message: e.to_string(),
            }
        })?,
    );

    // 2. Spawn the StoreWriter on a dedicated blocking thread. It owns
    //    every serialized write to rocksdb.
    let (write_tx, write_rx) = write_channel();
    let store_for_writer = store.clone();
    let store_writer_join = tokio::task::spawn_blocking(move || {
        let writer = StoreWriter::new(store_for_writer, write_rx);
        writer.run();
        info!("share-chain bootstrap: StoreWriter exited");
    });

    // 3. Wrap the store + writer-channel in a StoreHandle, then a
    //    ChainStoreHandle. Init genesis if missing.
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

    // 4. Build the share validator. PoolDifficulty::build reads the
    //    current chain tip; the multiplier + signature come from the
    //    p2pool config.
    let pool_difficulty = PoolDifficulty::build(&chain)
        .map_err(|e| ShareChainBootstrapError::PoolDifficulty(e.to_string()))?;
    let difficulty_multiplier = p2pool_config.stratum.difficulty_multiplier as u128;
    let pool_signature = p2pool_config
        .stratum
        .pool_signature
        .as_deref()
        .unwrap_or("")
        .as_bytes()
        .to_vec();
    let validator: Arc<dyn ShareValidator + Send + Sync> = Arc::new(DefaultShareValidator::new(
        pool_difficulty,
        difficulty_multiplier,
        pool_signature,
    ));

    // 5. Build the bitcoind RPC client.
    let rpc = &p2pool_config.bitcoinrpc;
    let bitcoind_client = BitcoindRpcClient::new(&rpc.url, &rpc.username, &rpc.password)
        .map_err(ShareChainBootstrapError::BitcoindClient)?;
    let bitcoind: Arc<dyn BitcoindLike> = Arc::new(bitcoind_client);

    info!("share-chain bootstrap: handles ready");
    Ok(ShareChainHandles {
        engine_handles: EngineHandles {
            chain,
            validator,
            bitcoind,
        },
        store,
        store_writer_join,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    /// Write a minimal-but-valid p2poolv2 config TOML to a tempdir and
    /// return the loaded `Config` along with the tempdir guard. The
    /// store path is set inside the tempdir so the writer can create
    /// rocksdb files on bootstrap.
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

    #[tokio::test]
    async fn declare_mining_job_captures_share_chain_tip() {
        use jd_server_sv2::job_declarator::job_validation::{
            DeclareMiningJobResult, JobValidationEngine,
        };
        use stratum_apps::stratum_core::{
            binary_sv2::{B064K, B0255, Seq064K, U256},
            job_declaration_sv2::DeclareMiningJob,
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
        // We re-create a minimal coinbase here rather than depending
        // on the engine's pub(crate) test helpers.
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
        // Layout matches the engine's test helpers: prefix ends right
        // before the 16-byte extranonce reservation; suffix starts
        // immediately after.
        let extranonce_bytes = 16;
        let script_sig_len = cb.input[0].script_sig.len();
        let mut pos = 43; // COINBASE_PREFIX_LEN
        pos += bitcoin::VarInt(script_sig_len as u64).size();
        let bytes_in_prefix = script_sig_len.saturating_sub(extranonce_bytes);
        let split_at = pos + bytes_in_prefix;
        let prefix_bytes = serialized[..split_at].to_vec();
        let suffix_bytes = serialized[split_at + extranonce_bytes..].to_vec();

        let token: u64 = 99;
        let token_b0255: B0255<'static> = token.to_le_bytes().to_vec().try_into().unwrap();
        let prefix_b: B064K<'static> = prefix_bytes.try_into().unwrap();
        let suffix_b: B064K<'static> = suffix_bytes.try_into().unwrap();
        let wtxid: U256<'static> = [42u8; 32].to_vec().try_into().unwrap();
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
        let result = engine.handle_declare_mining_job(msg, None).await;
        assert!(
            matches!(result, DeclareMiningJobResult::Success),
            "declare must succeed against initialised signet chain"
        );

        let cached = engine.declared_jobs().get(&1).expect("declared job cached");
        let expected_tip = chain_for_assert.get_chain_tip().expect("tip readable");
        assert_eq!(
            cached.share_chain_tip,
            Some(expected_tip),
            "DeclareMiningJob captured the live share-chain tip"
        );

        drop(engine);
        drop(handles.store);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handles.store_writer_join)
            .await;
    }

    #[tokio::test]
    async fn engine_reorg_watcher_polls_chain_handle() {
        use std::time::Duration;

        use sv2_p2pool_engine::P2poolV2Engine;

        let (config, _dir) = make_test_config();
        let handles = bootstrap_share_chain(&config)
            .await
            .expect("bootstrap succeeds");

        let chain = handles.engine_handles.chain.clone();
        let mut engine =
            P2poolV2Engine::with_handles(bitcoin::Network::Signet, handles.engine_handles.clone());

        // The watcher polls `chain.get_chain_tip()` on the configured
        // schedule. With a live chain initialised at genesis,
        // get_chain_tip should always succeed and return the same
        // value — exercising the closure shape we use in Pool::start
        // without needing to drive a synthetic tip swap.
        let _observer = engine.start_reorg_watcher(
            move || chain.get_chain_tip().ok(),
            Duration::from_millis(20),
        );

        // Let the watcher tick a few times under real time. The
        // cache must remain empty (the tip never changes) and the
        // watcher must not panic in the closure.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(engine.declared_jobs().is_empty());

        engine.stop_reorg_watcher();
        drop(engine);
        drop(handles.store);
        let _ = tokio::time::timeout(Duration::from_secs(5), handles.store_writer_join).await;
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
        assert!(!handles.store_writer_join.is_finished());

        // Drop everything so the store-writer can shut down cleanly.
        drop(handles.engine_handles);
        drop(handles.store);
        // Awaiting the join handle here triggers the writer to exit
        // because all senders are dropped.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handles.store_writer_join)
            .await
            .expect("writer joins within timeout");
    }
}
