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
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::PoolBuilder;

/// Top-level pool runtime.
///
/// Construct via [`PoolBuilder::build_pool`]; drive via [`Pool::start`].
#[derive(Debug, Clone)]
pub struct Pool {
    config: PoolConfig,
    cancellation_token: CancellationToken,
    shutdown_notify: Arc<Notify>,
    is_alive: Arc<AtomicBool>,
}

impl Pool {
    pub(crate) fn new(config: PoolConfig) -> Self {
        Self {
            config,
            cancellation_token: CancellationToken::new(),
            shutdown_notify: Arc::new(Notify::new()),
            is_alive: Arc::new(AtomicBool::new(true)),
        }
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
        let (downstream_to_cm_sender, downstream_to_cm_receiver) = unbounded();
        let (cm_to_tp_sender, cm_to_tp_receiver) = unbounded();
        let (tp_to_cm_sender, tp_to_cm_receiver) = unbounded();
        debug!("channels initialized");

        // 3. Build embedded JDS using OUR engine.
        //
        // This is the core difference from upstream PoolSv2. Where the
        // upstream picks `BitcoinCoreIPCEngine` based on
        // template_provider_type, we hand in our `P2poolV2Engine`
        // unconditionally. JDS is required (no `if jds_config { ... }`
        // gate) because without share-chain validation our pool has
        // nothing to do.
        let jds_config = self.config.build_jds_config()?.ok_or_else(|| {
            PoolErrorKind::Configuration(
                "[jds] config is required for sv2-p2pool — without it, the engine cannot \
                 validate jobs against the share chain"
                    .to_string(),
            )
        })?;

        info!("building embedded JDS with P2poolV2Engine backend");
        let engine: Arc<dyn JobValidationEngine> =
            PoolBuilder::new(self.config_network()).build_engine_arc();
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
        //    TP or local bitcoind IPC.
        match self.config.template_provider_type().clone() {
            TemplateProviderType::Sv2Tp {
                address,
                public_key,
            } => {
                let sv2_tp = Sv2Tp::new(
                    address.clone(),
                    public_key,
                    cm_to_tp_receiver,
                    tp_to_cm_sender,
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
                    incoming_tdp_receiver: cm_to_tp_receiver.clone(),
                    outgoing_tdp_sender: tp_to_cm_sender.clone(),
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
