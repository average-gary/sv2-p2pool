//! Phase 3a driving E2E: boot the full SV2 stack, attach an SV1 CPU
//! miner via the translator proxy, and verify a `SubmitSharesExtended`
//! reaches the pool's ChannelManager. Witness is the upstream
//! `MonitoringServer`'s `sv2_client_shares_accepted_total` Prometheus
//! gauge, which is updated per channel when the pool accepts a share.
//!
//! Topology:
//!
//! ```text
//! mujina (SV1, CPU)  →  translator_sv2 (SV1↔SV2)  →  jd_client_sv2  →  sv2-p2pool  →  bitcoind testnet4
//!                                                                          │
//!                                                                          └→  p2poolv2
//! ```
//!
//! Caveat: upstream's monitoring server populates the per-channel
//! Prometheus gauges only when its snapshot cache refreshes (every
//! `monitoring_cache_refresh_secs`, default 15s in our test config).
//! A share that lands at time T is observable no earlier than the
//! next cache tick — up to ~15s of additional latency on top of the
//! actual share-arrival time.
//!
//! Run locally (after building the prerequisite binaries):
//!
//! ```sh
//! cargo build --bin sv2-p2pool
//! cargo build --manifest-path vendor/p2poolv2/Cargo.toml --bin p2poolv2
//! cargo build --manifest-path vendor/sv2-apps/miner-apps/Cargo.toml \
//!     --bin jd_client_sv2 --bin translator_sv2
//! cargo build --manifest-path /path/to/mujina/Cargo.toml --bin mujina-minerd
//!
//! BITCOIND_EXE=/path/to/multiprocess/bitcoind \
//!   MUJINA_MINERD_EXE=/path/to/mujina/target/debug/mujina-minerd \
//!   cargo test -p sv2-p2pool-testenv --test e2e_share_submission \
//!     -- --ignored --nocapture
//! ```

use std::time::Duration;

use sv2_p2pool_testenv::{
    JdClientDBuilder, MujinaMinerDBuilder, Sv2P2poolDBuilder, TestEnvBuilder, TranslatorSv2DBuilder,
};

#[test]
#[ignore = "requires BITCOIND_EXE + P2POOLV2_EXE + SV2_P2POOL_EXE + JD_CLIENT_EXE + TRANSLATOR_SV2_EXE + MUJINA_MINERD_EXE + bitcoind built with multiprocess support"]
fn share_reaches_pool_channel_manager() {
    let env = TestEnvBuilder::new()
        .with_network(bitcoin::Network::Testnet4)
        .with_ipcbind()
        .build()
        .expect("bitcoind testnet4 starts with ipcbind");

    let p2pool = env.with_p2poolv2().expect("p2poolv2 starts");
    eprintln!(
        "p2poolv2: api={} stratum={} libp2p_port={}",
        p2pool.api_addr, p2pool.stratum_addr, p2pool.libp2p_port
    );

    let sv2 = Sv2P2poolDBuilder::new(&env.bitcoind)
        .with_network(bitcoin::Network::Testnet4)
        .with_bitcoin_data_dir(env.bitcoind.workdir())
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("sv2-p2pool starts");
    eprintln!(
        "sv2-p2pool: jds={} mining={} metrics={} monitoring={}",
        sv2.jds_addr, sv2.mining_addr, sv2.metrics_addr, sv2.monitoring_addr
    );

    let jdc = JdClientDBuilder::new(&env.bitcoind, &sv2)
        .with_network(bitcoin::Network::Testnet4)
        .with_bitcoin_data_dir(env.bitcoind.workdir())
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("jd_client_sv2 starts");
    eprintln!("jd_client_sv2: listening={}", jdc.listening_addr);

    let translator = TranslatorSv2DBuilder::new(&jdc)
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("translator_sv2 starts");
    eprintln!(
        "translator_sv2: downstream_addr={}",
        translator.downstream_addr
    );

    // Mujina speaks SV1; point it at the translator's SV1 port.
    let mujina = MujinaMinerDBuilder::new()
        .with_pool_url(format!("stratum+tcp://{}", translator.downstream_addr))
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("mujina-minerd starts");
    eprintln!(
        "mujina-minerd: api_addr={} pool_url={}",
        mujina.api_addr,
        mujina.pool_url.as_deref().unwrap_or("<dummy>")
    );

    // Witness: the upstream MonitoringServer exposes per-channel
    // `sv2_client_shares_accepted_total{channel_id="…",…}` whenever
    // ChannelManager accepts a SubmitSharesExtended. Any positive
    // value on any label set proves a share completed the
    // SV1→SV2→ChannelManager path.
    //
    // 90s budget covers: SV1 handshake, JDP DeclareMiningJob round
    // trip, first job propagation, CPU hashing at the (low) channel
    // target the translator hands out, AND up to one
    // monitoring_cache_refresh_secs (15s) interval before the gauge
    // updates. If this test starts flaking, the cache-refresh window
    // is the most likely culprit — drop monitoring_cache_refresh_secs
    // in Sv2P2poolDBuilder before extending the timeout.
    let observed = sv2
        .wait_for_monitoring_metric(
            |body| {
                body.lines().any(|line| {
                    !line.starts_with('#')
                        && line.starts_with("sv2_client_shares_accepted_total")
                        && line
                            .rsplit(' ')
                            .next()
                            .and_then(|v| v.parse::<f64>().ok())
                            .is_some_and(|v| v > 0.0)
                })
            },
            Duration::from_secs(90),
        )
        .expect("monitoring scrape succeeds");
    assert!(
        observed,
        "expected SubmitSharesExtended to reach pool within 90s\n\
         monitoring /metrics body:\n{}\n\
         engine /metrics body:\n{}",
        sv2.scrape_monitoring_metrics_body().unwrap_or_default(),
        sv2.scrape_metrics_body().unwrap_or_default(),
    );
    eprintln!("✓ SubmitSharesExtended landed in pool ChannelManager");
}
