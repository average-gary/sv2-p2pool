//! End-to-end test: boot bitcoind + p2poolv2 + sv2-p2pool + jd_client_sv2
//! and verify the chain connects up.
//!
//! All tests are `#[ignore]`d because they require external binaries
//! on disk (set `BITCOIND_EXE`, `P2POOLV2_EXE`, `SV2_P2POOL_EXE`,
//! `JD_CLIENT_EXE` env vars OR run `cargo build` for the workspace
//! and the sv2-apps miner-apps subworkspace first). The nightly CI
//! workflow at `.github/workflows/nightly.yml` builds these binaries
//! and runs `cargo test --workspace -- --ignored`.
//!
//! Run locally:
//!
//! ```sh
//! # Build all the things first.
//! cargo build --bin sv2-p2pool
//! cargo build --manifest-path vendor/p2poolv2/Cargo.toml --bin p2poolv2
//! cargo build --manifest-path vendor/sv2-apps/miner-apps/Cargo.toml \
//!     --bin jd_client_sv2 --package jd_client_sv2
//!
//! cargo test -p sv2-p2pool-testenv --test e2e_full_stack -- --ignored
//! ```

use std::time::Duration;

use sv2_p2pool_testenv::{JdClientDBuilder, P2poolV2DBuilder, Sv2P2poolDBuilder, TestEnv};

/// Boots all four components in order: bitcoind → p2poolv2 → sv2-p2pool
/// → jd_client_sv2. Each spawner's readiness signal is "TCP accept on
/// the listen port"; if any fails, the test errors out and Drop tears
/// down the previously-started processes.
///
/// Network: testnet4 (the supported deployment target — see
/// docs/architecture.md "Phase 2-A status by component").
#[test]
#[ignore = "requires BITCOIND_EXE + P2POOLV2_EXE + SV2_P2POOL_EXE + JD_CLIENT_EXE"]
fn full_stack_boots_against_testnet4_bitcoind() {
    // Bitcoind must run with -ipcbind for the pool's IPC TP. corepc-node
    // doesn't expose -ipcbind in its config builder, so this test is
    // expected to fail at sv2-p2pool's IPC connect step until either
    // (a) corepc-node grows ipcbind support, or (b) we wrap bitcoind
    // ourselves with the right flags. Marked #[ignore] for that reason
    // too — it primarily exercises the spawner orchestration.
    let env = TestEnv::new().expect("bitcoind starts");

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
        "sv2-p2pool: jds={} mining={}",
        sv2.jds_addr, sv2.mining_addr
    );

    let jdc = JdClientDBuilder::new(&env.bitcoind, &sv2)
        .with_network(bitcoin::Network::Testnet4)
        .with_bitcoin_data_dir(env.bitcoind.workdir())
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("jd_client_sv2 starts");
    eprintln!("jd_client_sv2: listening={}", jdc.listening_addr);

    // Sanity: each component held its listen port for at least the
    // readiness window. Without driving real SV2 traffic this is the
    // most we can assert end-to-end at the spawner layer; the next
    // step (Phase 3) is to send a SubmitSharesExtended through JDC and
    // observe it in p2poolv2's chain.
}

/// Smaller smoke test: just bitcoind + p2poolv2. Doesn't require our
/// own binary or jd_client_sv2 — useful for debugging the p2poolv2
/// spawner in isolation.
#[test]
#[ignore = "requires BITCOIND_EXE + P2POOLV2_EXE"]
fn p2poolv2_boots_against_testnet4_bitcoind() {
    let env = TestEnv::new().expect("bitcoind starts");
    let _p2pool = P2poolV2DBuilder::new(&env.bitcoind)
        .with_network(bitcoin::Network::Testnet4)
        .build()
        .expect("p2poolv2 starts");
}
