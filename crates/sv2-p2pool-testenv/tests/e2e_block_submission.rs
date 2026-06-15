//! Phase 3b driving E2E: boot the full SV2 stack on regtest with a
//! CPU miner and verify a block is reconstructed and handed to
//! `bitcoind.submit_block`. Witness is the engine counter
//! `sv2_p2pool_engine_blocks_submitted_total > 0` (incremented at
//! `engine_impl.rs:694` when `block::reconstruct_block` succeeds and
//! the submit is dispatched).
//!
//! Topology — same as Phase 3a, but on regtest so the network block
//! target is `0x207fffff` (effectively-no-PoW); a CPU thread clears
//! it in milliseconds:
//!
//! ```text
//! mujina (SV1, CPU)  →  translator_sv2 (SV1↔SV2)  →  jd_client_sv2  →  sv2-p2pool  →  bitcoind regtest
//!                                                                          │
//!                                                                          └→  p2poolv2 (regtest genesis)
//! ```
//!
//! The chain target meets ChannelManager difficulty, then meets the
//! template's network target on the same hash → JDC emits
//! `PushSolution` → engine reconstructs the block → submit_block.
//! Whether bitcoind accepts the block (consensus-valid coinbase, etc.)
//! is out of scope for the witness; on failure the test dumps both
//! `/metrics` bodies so `blocks_submit_failed_total` and the
//! `push_solution_dropped_total{reason}` breakdown are visible.
//!
//! Run locally:
//!
//! ```sh
//! cargo build --bin sv2-p2pool
//! cargo build --manifest-path vendor/p2poolv2/Cargo.toml --bin p2poolv2
//! cargo build --manifest-path vendor/sv2-apps/miner-apps/Cargo.toml \
//!     --bin jd_client_sv2 --bin translator_sv2
//!
//! BITCOIND_EXE=/path/to/multiprocess/bitcoind \
//!   MUJINA_MINERD_EXE=/path/to/mujina-minerd \
//!   cargo test -p sv2-p2pool-testenv --test e2e_block_submission \
//!     -- --ignored --nocapture
//! ```

use std::time::Duration;

use sv2_p2pool_testenv::{
    JdClientDBuilder, MujinaMinerDBuilder, P2poolV2DBuilder, Sv2P2poolDBuilder, TestEnvBuilder,
    TranslatorSv2DBuilder,
};

#[test]
#[ignore = "requires BITCOIND_EXE + P2POOLV2_EXE + SV2_P2POOL_EXE + JD_CLIENT_EXE + TRANSLATOR_SV2_EXE + MUJINA_MINERD_EXE + bitcoind built with multiprocess support"]
fn block_submitted_to_bitcoind() {
    let env = TestEnvBuilder::new()
        .with_network(bitcoin::Network::Regtest)
        .with_ipcbind()
        .build()
        .expect("bitcoind regtest starts with ipcbind");

    let p2pool = P2poolV2DBuilder::new(&env.bitcoind)
        .with_network(bitcoin::Network::Regtest)
        .build()
        .expect("p2poolv2 starts on regtest");
    eprintln!(
        "p2poolv2: api={} stratum={} libp2p_port={}",
        p2pool.api_addr, p2pool.stratum_addr, p2pool.libp2p_port
    );

    let sv2 = Sv2P2poolDBuilder::new(&env.bitcoind)
        .with_network(bitcoin::Network::Regtest)
        .with_bitcoin_data_dir(env.bitcoind.workdir())
        .with_low_difficulty()
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("sv2-p2pool starts on regtest");
    eprintln!(
        "sv2-p2pool: jds={} mining={} metrics={} monitoring={}",
        sv2.jds_addr, sv2.mining_addr, sv2.metrics_addr, sv2.monitoring_addr
    );

    let jdc = JdClientDBuilder::new(&env.bitcoind, &sv2)
        .with_network(bitcoin::Network::Regtest)
        .with_bitcoin_data_dir(env.bitcoind.workdir())
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("jd_client_sv2 starts");
    eprintln!("jd_client_sv2: listening={}", jdc.listening_addr);

    // Pair the low share-chain difficulty (set above via
    // with_low_difficulty) with a low translator hashrate floor so
    // every CPU hash both clears the SV2 channel target AND the
    // regtest network target.
    let translator = TranslatorSv2DBuilder::new(&jdc)
        .with_min_individual_miner_hashrate(1.0)
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("translator_sv2 starts");
    eprintln!(
        "translator_sv2: downstream_addr={}",
        translator.downstream_addr
    );

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

    // Witness: engine /metrics — `sv2_p2pool_engine_blocks_submitted_total`
    // increments inside handle_push_solution when reconstruct_block
    // succeeds and bitcoind.submit_block is dispatched (regardless of
    // whether bitcoind ultimately accepts the block).
    //
    // 60s budget covers: SV1 handshake, JDP handshake, first job
    // propagation, plus CPU hashing on the trivial regtest target.
    // No upstream snapshot-cache lag on this metric — the engine
    // counter is updated synchronously.
    let observed = sv2
        .wait_for_metric(
            |body| {
                body.lines().any(|line| {
                    !line.starts_with('#')
                        && line.starts_with("sv2_p2pool_engine_blocks_submitted_total")
                        && line
                            .rsplit(' ')
                            .next()
                            .and_then(|v| v.parse::<u64>().ok())
                            .is_some_and(|v| v > 0)
                })
            },
            Duration::from_secs(60),
        )
        .expect("engine metrics scrape succeeds");
    assert!(
        observed,
        "expected engine to submit a block within 60s\n\
         engine /metrics body:\n{}\n\
         monitoring /metrics body:\n{}",
        sv2.scrape_metrics_body().unwrap_or_default(),
        sv2.scrape_monitoring_metrics_body().unwrap_or_default(),
    );
    eprintln!("✓ block submitted to bitcoind");
}
