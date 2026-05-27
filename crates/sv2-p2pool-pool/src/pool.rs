//! Pool runtime.
//!
//! Mirrors the upstream `pool_sv2::PoolSv2` lifecycle at
//! `vendor/sv2-apps/pool-apps/pool/src/lib/mod.rs:46-359`, swapping the
//! hardcoded `BitcoinCoreIPCEngine` for our [`sv2_p2pool_engine::P2poolV2Engine`].
//!
//! # What this owns
//!
//! - The shared `CancellationToken` + `TaskManager`
//! - The TP ↔ ChannelManager + Downstream ↔ ChannelManager async-channels
//! - The optional embedded `JobDeclarator` (always present for our pool —
//!   without JDP, p2pool has nothing to validate against the share chain)
//! - The `ChannelManager` itself
//! - The Template Provider connection (Bitcoin Core IPC or upstream SV2 TP)
//!
//! # What this does NOT own
//!
//! - The p2poolv2 `Node` / `ChainStoreHandle`. Phase 1.6 holds the engine
//!   without a wired-in share chain (the trait methods stub the
//!   share-chain validation). Phase 2 widens the engine constructor.
//! - Configuration parsing — that's `args.rs`.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle as ThreadJoinHandle;

use async_channel::unbounded;
use bitcoin_core_sv2::template_distribution_protocol::CancellationToken;
use jd_server_sv2::job_declarator::{JobDeclarator, job_validation::JobValidationEngine};
use p2poolv2_lib::config::Config as P2poolConfig;
use pool_sv2::{
    channel_manager::ChannelManager,
    config::PoolConfig,
    error::PoolErrorKind,
    template_receiver::{
        bitcoin_core::{BitcoinCoreSv2TDPConfig, connect_to_bitcoin_core},
        sv2_tp::Sv2Tp,
    },
};
use stratum_apps::{
    stratum_core::bitcoin::consensus::Encodable, task_manager::TaskManager,
    tp_type::TemplateProviderType, utils::types::GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS,
};
use sv2_p2pool_engine::TdpHandle;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::{PoolBuilder, share_chain, tdp_demux};

/// Top-level pool runtime.
///
/// Construct via [`PoolBuilder::build_pool`]; drive via [`Pool::start`].
#[derive(Debug, Clone)]
pub struct Pool {
    config: PoolConfig,
    /// Phase 2.5b: optional p2poolv2 share-chain config. When present,
    /// [`Pool::start`] bootstraps real `EngineHandles` (chain +
    /// validator + bitcoind) and constructs the engine via
    /// `with_handles`. When absent, the engine runs in TDP-only mode
    /// (Phase 2.5a behaviour preserved for tests).
    p2pool_config: Option<P2poolConfig>,
    cancellation_token: CancellationToken,
    shutdown_notify: Arc<Notify>,
    is_alive: Arc<AtomicBool>,
}

impl Pool {
    pub(crate) fn new(config: PoolConfig) -> Self {
        Self {
            config,
            p2pool_config: None,
            cancellation_token: CancellationToken::new(),
            shutdown_notify: Arc::new(Notify::new()),
            is_alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Attach a p2poolv2 share-chain config, enabling real
    /// `EngineHandles` bootstrap on `Pool::start`.
    pub fn with_p2pool_config(mut self, p2pool_config: P2poolConfig) -> Self {
        self.p2pool_config = Some(p2pool_config);
        self
    }

    /// Run the pool until cancelled or `Ctrl+C`.
    ///
    /// Mirrors `PoolSv2::start` but uses our `P2poolV2Engine` as the
    /// `JobValidationEngine`. The substantive difference from upstream
    /// is the engine construction in step 3.
    pub async fn start(&self) -> Result<(), PoolErrorKind> {
        // 1. Coinbase outputs (encoded for ChannelManager).
        let coinbase_outputs = vec![self.config.get_txout()];
        let mut encoded_outputs = vec![];
        coinbase_outputs
            .consensus_encode(&mut encoded_outputs)
            .expect("invalid coinbase output in config");

        let cancellation_token = self.cancellation_token.clone();
        let task_manager = Arc::new(TaskManager::new());

        // 2. Channel pairs.
        //
        // Phase 2.5a inserts the TDP demux between sv2-apps's CM and the
        // configured TP:
        //
        //   TP → tp_to_demux_* → [tee task] → tp_to_cm_*       (CM input)
        //                                  ↘ TdpHandle snapshots / pending one-shots
        //
        //   CM → cm_to_tp_*  ──┐
        //                      ├─ [merge task] → merged_to_tp_*  (TP input)
        //   engine → eng_to_tp ┘
        //
        // The CM still sees the full original message stream (tee is
        // forward-preserving). The engine's RequestTransactionData
        // requests get merged onto the same outbound stream.
        let (downstream_to_cm_sender, downstream_to_cm_receiver) = unbounded();

        let (cm_to_tp_sender, cm_to_tp_receiver) = unbounded();
        let (engine_to_tp_sender, engine_to_tp_receiver) = unbounded();
        let (merged_to_tp_sender, merged_to_tp_receiver) = unbounded();

        let (tp_to_demux_sender, tp_to_demux_receiver) = unbounded();
        let (tp_to_cm_sender, tp_to_cm_receiver) = unbounded();
        debug!("channels initialized");

        // 3. Build the engine + the TDP bridge first so we can pass the
        //    engine into JDS construction with the bridge already wired.
        let jds_config = self.config.build_jds_config()?.ok_or_else(|| {
            PoolErrorKind::Configuration(
                "[jds] config is required for sv2-p2pool — without it, the engine cannot \
                 validate jobs against the share chain"
                    .to_string(),
            )
        })?;

        info!("building embedded JDS with P2poolV2Engine backend");
        let tdp = TdpHandle::new(engine_to_tp_sender);

        // Phase 2.5b: when a p2pool config is attached, bootstrap real
        // EngineHandles (chain + validator + bitcoind) for full
        // share-chain integration. The store + writer-thread join must
        // outlive the engine, so we keep them on the stack here.
        let mut share_chain_handles: Option<share_chain::ShareChainHandles> = None;
        let engine_concrete = if let Some(p2pool_config) = self.p2pool_config.as_ref() {
            let handles = share_chain::bootstrap_share_chain(p2pool_config)
                .await
                .map_err(|e| PoolErrorKind::Configuration(format!("share-chain bootstrap: {e}")))?;
            info!("share-chain handles wired into engine");
            let engine_handles = handles.engine_handles.clone();
            share_chain_handles = Some(handles);
            PoolBuilder::new(self.config_network())
                .build_engine_with_handles(engine_handles)
                .with_tdp(tdp.clone())
        } else {
            info!("no p2pool config attached; engine runs in TDP-only mode");
            PoolBuilder::new(self.config_network())
                .build_engine()
                .with_tdp(tdp.clone())
        };
        let engine: Arc<dyn JobValidationEngine> = Arc::new(engine_concrete);

        // 3b. Spawn the TDP demux tasks. These bridge the CM↔TP channel
        //    pair to the engine's TdpHandle.
        let _tee_handle = tdp_demux::spawn_tp_to_cm_tee(
            tp_to_demux_receiver,
            tp_to_cm_sender.clone(),
            tdp.clone(),
        );
        let _merge_handle = tdp_demux::spawn_cm_and_engine_to_tp_merge(
            cm_to_tp_receiver.clone(),
            engine_to_tp_receiver,
            merged_to_tp_sender,
        );
        info!("TDP demux tasks spawned");
        let jd = JobDeclarator::new(
            engine,
            cancellation_token.clone(),
            jds_config.coinbase_reward_script().clone(),
            task_manager.clone(),
        )
        .await
        .map_err(PoolErrorKind::Jds)?;

        jd.clone()
            .start(cancellation_token.clone(), task_manager.clone())
            .await
            .map_err(|e| PoolErrorKind::Jds(e.into()))?;

        jd.clone()
            .start_downstream_server(
                *jds_config.authority_public_key(),
                *jds_config.authority_secret_key(),
                jds_config.cert_validity_sec(),
                *jds_config.listen_address(),
                task_manager.clone(),
                cancellation_token.clone(),
                jds_config.supported_extensions().to_vec(),
                jds_config.required_extensions().to_vec(),
            )
            .await
            .map_err(|e| PoolErrorKind::Jds(e.into()))?;
        info!(
            "JDS listening for JDP connections on {}",
            jds_config.listen_address()
        );
        let job_declarator_for_shutdown = jd.clone();

        // 4. Build ChannelManager. Identical to upstream — share-chain
        //    integration happens inside the embedded engine, not here.
        let channel_manager = ChannelManager::new(
            self.config.clone(),
            cm_to_tp_sender.clone(),
            tp_to_cm_receiver,
            downstream_to_cm_receiver,
            encoded_outputs.clone(),
            Some(jd),
        )
        .await?;

        let channel_manager_clone = channel_manager.clone();
        let mut bitcoin_core_join_handle: Option<ThreadJoinHandle<()>> = None;
        let mut bitcoin_core_cancellation_token: Option<CancellationToken> = None;

        // 5. Wire up the Template Provider (TP) — either upstream SV2
        //    TP or local bitcoind IPC. The TP's inbound stream is the
        //    merged (CM + engine) outbound; its outbound stream feeds
        //    the demux task (which tees to CM + engine).
        match self.config.template_provider_type().clone() {
            TemplateProviderType::Sv2Tp {
                address,
                public_key,
            } => {
                let sv2_tp = Sv2Tp::new(
                    address.clone(),
                    public_key,
                    merged_to_tp_receiver,
                    tp_to_demux_sender,
                    cancellation_token.clone(),
                    task_manager.clone(),
                )
                .await?;
                sv2_tp
                    .start(address, cancellation_token.clone(), task_manager.clone())
                    .await?;
                info!("SV2 TP setup done");
            }
            TemplateProviderType::BitcoinCoreIpc {
                network,
                data_dir,
                fee_threshold,
                min_interval,
            } => {
                let unix_socket_path = stratum_apps::tp_type::resolve_ipc_socket_path(
                    &network, data_dir,
                )
                .ok_or_else(|| {
                    PoolErrorKind::Configuration(
                        "could not determine Bitcoin data directory; set data_dir in config"
                            .to_string(),
                    )
                })?;
                info!(
                    "using Bitcoin Core IPC socket at {}",
                    unix_socket_path.display()
                );

                let btc_token = CancellationToken::new();
                let btc_config = BitcoinCoreSv2TDPConfig {
                    unix_socket_path,
                    fee_threshold,
                    min_interval,
                    incoming_tdp_receiver: merged_to_tp_receiver.clone(),
                    outgoing_tdp_sender: tp_to_demux_sender.clone(),
                    cancellation_token: btc_token.clone(),
                };
                bitcoin_core_cancellation_token = Some(btc_token);
                bitcoin_core_join_handle = Some(
                    connect_to_bitcoin_core(
                        btc_config,
                        cancellation_token.clone(),
                        task_manager.clone(),
                    )
                    .await,
                );
            }
        }

        // 6. Start ChannelManager + downstream server.
        channel_manager
            .start(
                cancellation_token.clone(),
                task_manager.clone(),
                coinbase_outputs,
            )
            .await?;
        channel_manager_clone
            .start_downstream_server(
                *self.config.authority_public_key(),
                *self.config.authority_secret_key(),
                self.config.cert_validity_sec(),
                *self.config.listen_address(),
                task_manager.clone(),
                cancellation_token.clone(),
                downstream_to_cm_sender,
            )
            .await?;
        info!("downstream server started; waiting for shutdown signal");

        // 7. Wait for Ctrl-C or external cancellation.
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl+C received; initiating graceful shutdown");
                cancellation_token.cancel();
            }
            _ = cancellation_token.cancelled() => {}
        }

        // 8. Graceful shutdown.
        info!("shutting down embedded JDS");
        job_declarator_for_shutdown.shutdown();
        if let Some(token) = bitcoin_core_cancellation_token {
            token.cancel();
        }
        if let Some(handle) = bitcoin_core_join_handle {
            info!("waiting for BitcoinCoreSv2TDP thread");
            match handle.join() {
                Ok(_) => info!("BitcoinCoreSv2TDP shutdown complete"),
                Err(e) => error!(?e, "BitcoinCoreSv2TDP thread error"),
            }
        }

        warn!("graceful shutdown: waiting {GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS}s for tasks");
        match tokio::time::timeout(
            std::time::Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS),
            task_manager.join_all(),
        )
        .await
        {
            Ok(_) => info!("all tasks joined cleanly"),
            Err(_) => {
                warn!("tasks did not finish within timeout; aborting");
                task_manager.abort_all().await;
                task_manager.join_all().await;
                warn!("forced shutdown complete");
            }
        }
        // Drop share-chain handles last: this closes the StoreWriter
        // channel, which causes the writer thread to exit and rocksdb
        // to flush. Explicit drop to make the lifecycle visible.
        if let Some(handles) = share_chain_handles.take() {
            drop(handles.engine_handles);
            drop(handles.store);
            // Await the writer thread; it should exit promptly once its
            // channel sender is dropped.
            match handles.store_writer_join.await {
                Ok(()) => info!("StoreWriter task joined"),
                Err(e) => warn!(?e, "StoreWriter task did not join cleanly"),
            }
        }

        self.shutdown_notify.notify_waiters();
        self.is_alive.store(false, Ordering::Relaxed);
        info!("pool shutdown complete");
        Ok(())
    }

    /// External cancellation.
    pub async fn shutdown(&self) {
        if !self.is_alive.load(Ordering::Relaxed) {
            return;
        }
        let notified = self.shutdown_notify.notified();
        self.cancellation_token.cancel();
        notified.await;
    }

    /// Network derived from the configured TP type.
    fn config_network(&self) -> bitcoin::Network {
        match self.config.template_provider_type() {
            TemplateProviderType::BitcoinCoreIpc { network, .. } => match network {
                stratum_apps::tp_type::BitcoinNetwork::Mainnet => bitcoin::Network::Bitcoin,
                stratum_apps::tp_type::BitcoinNetwork::Testnet4 => bitcoin::Network::Testnet,
                stratum_apps::tp_type::BitcoinNetwork::Signet => bitcoin::Network::Signet,
                stratum_apps::tp_type::BitcoinNetwork::Regtest => bitcoin::Network::Regtest,
            },
            // Upstream SV2 TP: we don't know the network at this layer.
            // Default to regtest since that's what Phase 1 targets;
            // production will have BitcoinCoreIpc.
            TemplateProviderType::Sv2Tp { .. } => bitcoin::Network::Regtest,
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        debug!("Pool dropped");
        self.cancellation_token.cancel();
    }
}
