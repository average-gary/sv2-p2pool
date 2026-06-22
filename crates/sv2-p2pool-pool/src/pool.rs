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
use prometheus::Registry;
use stratum_apps::{
    stratum_core::bitcoin::consensus::Encodable, task_manager::TaskManager,
    tp_type::TemplateProviderType, utils::types::GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS,
};
use sv2_p2pool_engine::{EngineMetrics, TdpHandle};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::{PoolBuilder, metrics_endpoint, share_chain, tdp_demux};

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
    /// Prometheus registry the engine's [`EngineMetrics`] are
    /// registered on. Cloneable (internally `Arc`), so an external
    /// HTTP `/metrics` endpoint can read it without taking the lock
    /// path through `Pool`.
    metrics_registry: Registry,
    /// Optional listen address for the built-in `/metrics` HTTP
    /// endpoint. When set, [`Pool::start`] spawns
    /// [`metrics_endpoint::spawn_metrics_endpoint`] against the
    /// pool's `metrics_registry`. Use `127.0.0.1:0` in tests for an
    /// OS-assigned port.
    metrics_addr: Option<std::net::SocketAddr>,
    cancellation_token: CancellationToken,
    shutdown_notify: Arc<Notify>,
    is_alive: Arc<AtomicBool>,
}

impl Pool {
    pub(crate) fn new(config: PoolConfig) -> Self {
        Self {
            config,
            p2pool_config: None,
            metrics_registry: Registry::new(),
            metrics_addr: None,
            cancellation_token: CancellationToken::new(),
            shutdown_notify: Arc::new(Notify::new()),
            is_alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Enable the built-in `/metrics` endpoint at `addr`.
    pub fn with_metrics_addr(mut self, addr: std::net::SocketAddr) -> Self {
        self.metrics_addr = Some(addr);
        self
    }

    /// Attach a p2poolv2 share-chain config, enabling real
    /// `EngineHandles` bootstrap on `Pool::start`.
    pub fn with_p2pool_config(mut self, p2pool_config: P2poolConfig) -> Self {
        self.p2pool_config = Some(p2pool_config);
        self
    }

    /// Borrow the Prometheus registry the engine's counters are
    /// registered on. The binary's monitoring server (or any external
    /// `/metrics` mount) can `gather()` against this for export.
    pub fn metrics_registry(&self) -> &Registry {
        &self.metrics_registry
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
        let mut engine_concrete = if let Some(p2pool_config) = self.p2pool_config.as_ref() {
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

        // Register engine counters on the pool's Prometheus registry.
        // Pool::start can be called multiple times in tests; on a
        // duplicate registration we log + skip (no metrics rather than
        // a panic).
        match EngineMetrics::register(&self.metrics_registry) {
            Ok(metrics) => {
                engine_concrete = engine_concrete.with_metrics(metrics);
                info!("engine metrics registered on Pool::metrics_registry");
            }
            Err(e) => {
                warn!(error = %e, "engine metrics registration failed — continuing without metrics");
            }
        }

        // Spawn the /metrics HTTP endpoint when configured. Failure to
        // bind logs + continues; the engine still ticks counters on
        // the registry, just without a scrape target.
        let mut metrics_endpoint_handle: Option<tokio::task::JoinHandle<()>> = None;
        if let Some(addr) = self.metrics_addr {
            match metrics_endpoint::spawn_metrics_endpoint(addr, self.metrics_registry.clone())
                .await
            {
                Ok((bound, handle)) => {
                    info!(addr = %bound, "metrics endpoint started");
                    metrics_endpoint_handle = Some(handle);
                }
                Err(e) => {
                    warn!(error = %e, "metrics endpoint failed to bind — continuing without HTTP scrape");
                }
            }
        }

        // Spawn the share-chain reorg watcher when a chain handle is
        // available. Polls a sync `Fn() -> Option<BlockHash>` at
        // `DEFAULT_POLL_PERIOD` and invalidates the engine's
        // declared_jobs cache on every detected tip swap. ADR 0001
        // applies — uncle admissions are not tip changes; only an
        // actual tip swap reaches the invalidator.
        //
        // Phase 2-B Track A (ADR 0011 step 7) gives us two backends:
        //
        //  - `IpcChain` (production): the actor's subscribe-task pushes
        //    every new tip into a lock-free `AtomicTipSnapshot`. The
        //    watcher's closure becomes `move || tip_snapshot.load_tip()`
        //    — no UDS round-trip per tick. The tip-height publisher
        //    likewise reads `snapshot.load_height()`, which the actor
        //    refreshes on every push.
        //
        //  - `InProcessChain` (tests / single-process): the underlying
        //    `ChainStoreHandle` is exposed on
        //    `ShareChainHandles.chain_store`. We keep the legacy sync
        //    polling there because there's no daemon to push from.
        let mut tip_height_publisher_handle: Option<tokio::task::JoinHandle<()>> = None;
        let mut ipc_shutdown_watcher_handle: Option<tokio::task::JoinHandle<()>> = None;
        if let Some(handles) = share_chain_handles.as_ref() {
            if let Some(snapshot) = handles.ipc_tip_snapshot.clone() {
                // IpcChain mode: lock-free tip read driven by the
                // actor's subscribe-task.
                let snapshot_for_watcher = snapshot.clone();
                engine_concrete.start_reorg_watcher(
                    move || snapshot_for_watcher.load_tip(),
                    sv2_p2pool_engine::DEFAULT_POLL_PERIOD,
                );
                info!("share-chain reorg watcher started (IpcChain snapshot)");

                if let Some(metrics) = engine_concrete.metrics().cloned() {
                    let snapshot_for_publisher = snapshot.clone();
                    let period = sv2_p2pool_engine::DEFAULT_POLL_PERIOD;
                    let handle = tokio::spawn(async move {
                        let mut ticker = tokio::time::interval(period);
                        ticker
                            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        loop {
                            ticker.tick().await;
                            match snapshot_for_publisher.load_height() {
                                Some(h) => metrics.share_chain_tip_height.set(h as i64),
                                None => metrics.share_chain_tip_height.set(-1),
                            }
                        }
                    });
                    tip_height_publisher_handle = Some(handle);
                    info!("share-chain tip-height publisher started (IpcChain snapshot)");
                }

                // The IpcChain actor lives on a dedicated OS thread.
                // If it dies (panic, peer hang-up, runtime shutdown)
                // we cancel the pool rather than silently lose chain
                // reads. The watch::Receiver flips to `true` on the
                // first such event.
                if let Some(mut shutdown_rx) = handles.ipc_shutdown_signal.clone() {
                    let cancel = cancellation_token.clone();
                    let handle = tokio::spawn(async move {
                        // Skip the initial `false` value; only react
                        // to a transition to `true`.
                        loop {
                            if *shutdown_rx.borrow() {
                                error!(
                                    "IpcChain actor thread reports shutdown — cancelling pool"
                                );
                                cancel.cancel();
                                return;
                            }
                            if shutdown_rx.changed().await.is_err() {
                                // Sender dropped — actor handle gone.
                                debug!("IpcChain shutdown channel closed; exiting watcher");
                                return;
                            }
                        }
                    });
                    ipc_shutdown_watcher_handle = Some(handle);
                    info!("IpcChain shutdown watcher started");
                }
            } else if let Some(chain_store) = handles.chain_store.clone() {
                // InProcessChain mode: legacy sync polling against
                // the rocksdb-backed handle.
                let chain_store_for_watcher = chain_store.clone();
                engine_concrete.start_reorg_watcher(
                    move || chain_store_for_watcher.get_chain_tip().ok(),
                    sv2_p2pool_engine::DEFAULT_POLL_PERIOD,
                );
                info!("share-chain reorg watcher started (InProcessChain polling)");

                if let Some(metrics) = engine_concrete.metrics().cloned() {
                    let chain_store_for_publisher = chain_store.clone();
                    let period = sv2_p2pool_engine::DEFAULT_POLL_PERIOD;
                    let handle = tokio::spawn(async move {
                        let mut ticker = tokio::time::interval(period);
                        ticker
                            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        loop {
                            ticker.tick().await;
                            match chain_store_for_publisher.get_tip_height() {
                                Ok(Some(h)) => metrics.share_chain_tip_height.set(h as i64),
                                Ok(None) => metrics.share_chain_tip_height.set(-1),
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        "share_chain_tip_height: get_tip_height failed"
                                    );
                                    metrics.share_chain_tip_height.set(-1);
                                }
                            }
                        }
                    });
                    tip_height_publisher_handle = Some(handle);
                    info!("share-chain tip-height publisher started (InProcessChain polling)");
                }
            }
        }

        // Spawn the RecentSolutions sweeper unconditionally — it
        // bounds memory regardless of which other handles are wired,
        // and dropping the engine aborts the task.
        engine_concrete.start_recent_solutions_sweeper(
            sv2_p2pool_engine::DEFAULT_RECENT_SOLUTIONS_SWEEP_INTERVAL,
        );
        info!("recent-solutions sweeper started");

        let engine: Arc<dyn JobValidationEngine> = Arc::new(engine_concrete);

        // 3b. Spawn the TDP demux tasks. These bridge the CM↔TP channel
        //    pair to the engine's TdpHandle. JoinHandles are kept so
        //    we can abort them in the graceful-shutdown phase below;
        //    without that, a runtime-reuse path (hot reload, embedding
        //    Pool in a longer-lived process) would leak the tasks.
        let tee_handle = tdp_demux::spawn_tp_to_cm_tee(
            tp_to_demux_receiver,
            tp_to_cm_sender.clone(),
            tdp.clone(),
        );
        let merge_handle = tdp_demux::spawn_cm_and_engine_to_tp_merge(
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
        let channel_manager_for_monitoring = channel_manager.clone();
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

        // 6b. Start the upstream monitoring server when configured.
        // Mirrors `PoolSv2::start` at
        // `vendor/sv2-apps/pool-apps/pool/src/lib/mod.rs:162-194`.
        // ChannelManager already implements `Sv2ClientsMonitoring`
        // (per `vendor/sv2-apps/pool-apps/pool/src/lib/monitoring.rs`)
        // so all per-channel `shares_accepted` / `shares_rejected`
        // counters from upstream's PrometheusMetrics are populated
        // automatically once a downstream connects. The pool's own
        // `/metrics` endpoint stays where it is — this is a separate
        // surface for the upstream JSON + Prometheus endpoints.
        if let Some(monitoring_addr) = self.config.monitoring_address() {
            info!(
                "initializing upstream monitoring server on http://{}",
                monitoring_addr
            );
            let refresh = std::time::Duration::from_secs(
                self.config.monitoring_cache_refresh_secs().unwrap_or(15),
            );
            match stratum_apps::monitoring::MonitoringServer::new(
                monitoring_addr,
                None,
                Some(Arc::new(channel_manager_for_monitoring)),
                refresh,
            ) {
                Ok(monitoring_server) => {
                    let cancellation_for_run = cancellation_token.clone();
                    let shutdown_signal = async move {
                        cancellation_for_run.cancelled().await;
                    };
                    let cancellation_for_err = cancellation_token.clone();
                    task_manager.spawn(async move {
                        if let Err(e) = monitoring_server.run(shutdown_signal).await {
                            error!(error = %e, "monitoring server error");
                            cancellation_for_err.cancel();
                        }
                    });
                }
                Err(e) => warn!(error = %e, "failed to initialize monitoring server"),
            }
        }

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

        // Abort the TDP demux tasks. They aren't tracked by
        // task_manager (sv2-apps's TaskManager, not ours) so they
        // need explicit cleanup. Aborting is safe — the tasks have
        // no shared state that requires graceful drain; in-flight
        // forwards are lost on shutdown either way.
        tee_handle.abort();
        merge_handle.abort();
        let _ = tee_handle.await;
        let _ = merge_handle.await;
        info!("TDP demux tasks aborted");

        // Abort the /metrics endpoint if it was started.
        if let Some(handle) = metrics_endpoint_handle.take() {
            handle.abort();
            let _ = handle.await;
            info!("metrics endpoint aborted");
        }

        // Abort the tip-height publisher if it was started.
        if let Some(handle) = tip_height_publisher_handle.take() {
            handle.abort();
            let _ = handle.await;
            info!("share-chain tip-height publisher aborted");
        }

        // Abort the IpcChain shutdown-watcher if it was started.
        if let Some(handle) = ipc_shutdown_watcher_handle.take() {
            handle.abort();
            let _ = handle.await;
            info!("IpcChain shutdown watcher aborted");
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
        // Drop share-chain handles last. For the in-process path
        // this closes the StoreWriter channel, which causes the
        // writer thread to exit and rocksdb to flush. For IpcChain
        // it drops the actor handle, which causes the actor thread
        // to exit and the runtime to shut down. Explicit drop in
        // both modes to make the lifecycle visible.
        if let Some(handles) = share_chain_handles.take() {
            drop(handles.engine_handles);
            drop(handles.chain_store);
            drop(handles.store);
            drop(handles.ipc_tip_snapshot);
            drop(handles.ipc_shutdown_signal);
            if let Some(writer) = handles.store_writer_join {
                match writer.await {
                    Ok(()) => info!("StoreWriter task joined"),
                    Err(e) => warn!(?e, "StoreWriter task did not join cleanly"),
                }
            } else {
                info!("share-chain handles (IpcChain mode) dropped");
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

    /// Network derived from the configured TP type. When a p2poolv2
    /// share-chain config is attached, prefer its
    /// `stratum.network` since the share-chain side is the source of
    /// truth for which network the pool is participating in.
    pub(crate) fn config_network(&self) -> bitcoin::Network {
        if let Some(p2pool) = self.p2pool_config.as_ref() {
            return p2pool.stratum.network;
        }
        match self.config.template_provider_type() {
            TemplateProviderType::BitcoinCoreIpc { network, .. } => match network {
                stratum_apps::tp_type::BitcoinNetwork::Mainnet => bitcoin::Network::Bitcoin,
                stratum_apps::tp_type::BitcoinNetwork::Testnet4 => bitcoin::Network::Testnet4,
                stratum_apps::tp_type::BitcoinNetwork::Signet => bitcoin::Network::Signet,
                stratum_apps::tp_type::BitcoinNetwork::Regtest => bitcoin::Network::Regtest,
            },
            // Upstream SV2 TP without a p2pool config attached: we
            // don't know the network at this layer. Default to
            // testnet4 (the supported deployment target).
            TemplateProviderType::Sv2Tp { .. } => bitcoin::Network::Testnet4,
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        debug!("Pool dropped");
        self.cancellation_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal PoolConfig configured for BitcoinCoreIpc on a
    /// given network. Shares structure with the test helper in
    /// builder::tests but that one is in a sibling test module so
    /// we duplicate here rather than re-export through pub(crate).
    fn pool_config_with_network(network: &str) -> PoolConfig {
        let toml = format!(
            r#"
authority_public_key = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72"
authority_secret_key = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n"
cert_validity_sec = 3600
listen_address = "127.0.0.1:0"
coinbase_reward_script = "addr(tb1qa0sm0hxzj0x25rh8gw5xlzwlsfvvyz8u96w3p8)"
server_id = 1
pool_signature = "test"
shares_per_minute = 6.0
share_batch_size = 10
supported_extensions = []
required_extensions = []
monitoring_address = "127.0.0.1:0"
monitoring_cache_refresh_secs = 15

[template_provider_type.BitcoinCoreIpc]
network = "{network}"
fee_threshold = 100
min_interval = 5

[jds]
listen_address = "127.0.0.1:0"
"#
        );
        toml::from_str(&toml).expect("PoolConfig deserialize")
    }

    #[test]
    fn config_network_testnet4_does_not_collapse_to_legacy_testnet() {
        // Regression: an earlier mapping returned Network::Testnet
        // (legacy testnet3) when the operator configured testnet4.
        let pool = Pool::new(pool_config_with_network("testnet4"));
        assert_eq!(pool.config_network(), bitcoin::Network::Testnet4);
    }

    #[test]
    fn config_network_signet_round_trips() {
        let pool = Pool::new(pool_config_with_network("signet"));
        assert_eq!(pool.config_network(), bitcoin::Network::Signet);
    }

    #[test]
    fn config_network_regtest_round_trips() {
        let pool = Pool::new(pool_config_with_network("regtest"));
        assert_eq!(pool.config_network(), bitcoin::Network::Regtest);
    }
}
