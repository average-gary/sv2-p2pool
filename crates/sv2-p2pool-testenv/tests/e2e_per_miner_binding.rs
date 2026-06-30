//! Phase 3c Step 5/6 driving E2E: two JDCs with distinct `user_identity`
//! values both land shares against the same pool, and the upstream
//! `MonitoringServer` exposes two distinct `user_identity` labels on
//! `sv2_client_shares_accepted_total` — proving the per-miner identity
//! flows from each JDC's `DeclareMiningJob.user_identifier` through the
//! pool's ChannelManager (which is the key the engine's
//! `handle_allocate_mining_job_token` uses to bind a per-miner payout
//! script — see ADR 0013).
//!
//! ## What this test does and does NOT assert today
//!
//! What it asserts:
//!
//! - Two JDCs configured with `user_identity = "miner-alice"` and
//!   `user_identity = "miner-bob"` can be brought up against the same
//!   `sv2-p2pool` + `p2poolv2` pair on regtest.
//! - Both miners land at least one share each at the pool, observable as
//!   two distinct `user_identity` label values on
//!   `sv2_client_shares_accepted_total`.
//! - The engine's `handle_allocate_mining_job_token` path is exercised
//!   end-to-end per miner.
//!
//! What it does NOT assert (yet):
//!
//! - **Distinct per-token coinbase scripts**. The engine's
//!   `resolve_payout_script` returns `None` for every `user_identifier`
//!   in production today (the accounting selector is a documented
//!   follow-up, see ADR 0002 § Follow-ups + ADR 0013 § Status). With the
//!   resolver returning `None`, every miner falls back to the pool-wide
//!   `coinbase_reward_script` — so the coinbase `script_pubkey` of any
//!   block submitted on this run is *identical* across miners, and a
//!   "distinct script" assertion would be vacuously false.
//!
//!   When the accounting selector lands (or a test-only stub feature
//!   flag is added), the witness extends to a per-channel scrape +
//!   coinbase-byte comparison. The hooks are all in place: the engine's
//!   `lookup_payout_script(JdToken)` already reads the per-miner binding
//!   the resolver would populate, and `handle_push_solution` consults
//!   that lookup at block-reconstruction time.
//!
//! ## Topology
//!
//! ```text
//! mujina-alice (SV1) → translator-alice (SV1↔SV2) → jdc-alice ──┐
//!                                                                ├→ sv2-p2pool → bitcoind regtest
//! mujina-bob   (SV1) → translator-bob   (SV1↔SV2) → jdc-bob   ──┘                     │
//!                                                                                      └→ p2poolv2 (regtest)
//! ```
//!
//! Each `(mujina, translator, jdc)` triple is independent; the only
//! shared component is the pool. Both translators have
//! `aggregate_channels = false` (per-miner channel), so the
//! ChannelManager opens two distinct extended channels, each carrying
//! its JDC's `user_identity`.
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
//!   cargo test -p sv2-p2pool-testenv --test e2e_per_miner_binding \
//!     -- --ignored --nocapture
//! ```

use std::time::Duration;

use sv2_p2pool_testenv::{
    JdClientDBuilder, MujinaMinerDBuilder, Sv2P2poolDBuilder, TestEnvBuilder, TranslatorSv2DBuilder,
};

#[test]
#[ignore = "requires BITCOIND_EXE + P2POOLV2_EXE + SV2_P2POOL_EXE + JD_CLIENT_EXE + TRANSLATOR_SV2_EXE + MUJINA_MINERD_EXE + bitcoind built with multiprocess support"]
fn two_jdcs_with_distinct_user_identity_both_land_shares() {
    const ALICE: &str = "miner-alice";
    const BOB: &str = "miner-bob";

    let env = TestEnvBuilder::new()
        .with_network(bitcoin::Network::Regtest)
        .with_ipcbind()
        .build()
        .expect("bitcoind regtest starts with ipcbind");

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

    // Two independent JDCs, each tagged with a distinct user_identity.
    // The JDC plumbs this through every DeclareMiningJob.user_identifier
    // it sends — that's the key the engine's
    // handle_allocate_mining_job_token binds payout scripts under.
    let jdc_alice = JdClientDBuilder::new(&env.bitcoind, &sv2)
        .with_network(bitcoin::Network::Regtest)
        .with_bitcoin_data_dir(env.bitcoind.workdir())
        .with_user_identity(ALICE)
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("jdc-alice starts");
    eprintln!(
        "jdc-alice: listening={} user_identity={}",
        jdc_alice.listening_addr, ALICE
    );

    let jdc_bob = JdClientDBuilder::new(&env.bitcoind, &sv2)
        .with_network(bitcoin::Network::Regtest)
        .with_bitcoin_data_dir(env.bitcoind.workdir())
        .with_user_identity(BOB)
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("jdc-bob starts");
    eprintln!(
        "jdc-bob: listening={} user_identity={}",
        jdc_bob.listening_addr, BOB
    );

    // One translator + CPU miner per JDC. We pair regtest's trivial
    // network target with `min_individual_miner_hashrate = 1.0` so the
    // channel target the translator hands out is also trivial — every
    // CPU hash clears both targets.
    let translator_alice = TranslatorSv2DBuilder::new(&jdc_alice)
        .with_user_identity(ALICE)
        .with_min_individual_miner_hashrate(1.0)
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("translator-alice starts");
    let translator_bob = TranslatorSv2DBuilder::new(&jdc_bob)
        .with_user_identity(BOB)
        .with_min_individual_miner_hashrate(1.0)
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("translator-bob starts");
    eprintln!(
        "translators: alice={} bob={}",
        translator_alice.downstream_addr, translator_bob.downstream_addr
    );

    let mujina_alice = MujinaMinerDBuilder::new()
        .with_pool_url(format!(
            "stratum+tcp://{}",
            translator_alice.downstream_addr
        ))
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("mujina-alice starts");
    let mujina_bob = MujinaMinerDBuilder::new()
        .with_pool_url(format!("stratum+tcp://{}", translator_bob.downstream_addr))
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("mujina-bob starts");
    eprintln!(
        "mujinas: alice api={} bob api={}",
        mujina_alice.api_addr, mujina_bob.api_addr
    );

    // Witness: scrape upstream MonitoringServer /metrics and wait until
    // BOTH user_identity label values appear on
    // `sv2_client_shares_accepted_total` with a positive count. The
    // upstream server publishes per-channel labels — the same metric
    // backing `e2e_share_submission.rs`, but here we discriminate on
    // the `user_identity="…"` label rather than just any value > 0.
    //
    // 120s budget covers: SV1 handshake × 2, JDP DeclareMiningJob round
    // trip × 2, first-job propagation, CPU hashing (trivial on regtest),
    // AND up to one monitoring_cache_refresh_secs interval (default 15s
    // in our config) before each gauge updates.
    let observed = sv2
        .wait_for_monitoring_metric(
            |body| {
                let saw = |needle: &str| -> bool {
                    body.lines().any(|line| {
                        !line.starts_with('#')
                            && line.starts_with("sv2_client_shares_accepted_total")
                            && line.contains(needle)
                            && line
                                .rsplit(' ')
                                .next()
                                .and_then(|v| v.parse::<f64>().ok())
                                .is_some_and(|v| v > 0.0)
                    })
                };
                saw(&format!("user_identity=\"{ALICE}\""))
                    && saw(&format!("user_identity=\"{BOB}\""))
            },
            Duration::from_secs(120),
        )
        .expect("monitoring scrape succeeds");
    assert!(
        observed,
        "expected BOTH user_identity={ALICE:?} AND user_identity={BOB:?} to land shares within 120s\n\
         monitoring /metrics body:\n{}\n\
         engine /metrics body:\n{}",
        sv2.scrape_monitoring_metrics_body().unwrap_or_default(),
        sv2.scrape_metrics_body().unwrap_or_default(),
    );
    eprintln!("two distinct user_identity values landed shares");

    // Anti-flake: hold the spawners alive past the assertion so Drop
    // tears them down in a defined order (mujina last → translator →
    // JDC → pool → bitcoind). The compiler would drop these in the
    // reverse declaration order anyway, but binding to `_` here makes
    // the intent explicit and silences "unused" warnings for the
    // mujina handles that exist purely for their side-effect of
    // generating SV1 share traffic.
    let _ = (mujina_alice, mujina_bob, translator_alice, translator_bob);
}
