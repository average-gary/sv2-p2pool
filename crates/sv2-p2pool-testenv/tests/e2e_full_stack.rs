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

use sv2_p2pool_testenv::{
    JdClientDBuilder, P2poolV2DBuilder, Sv2P2poolDBuilder, TestEnv, TestEnvBuilder,
};

/// Boots all four components in order: bitcoind → p2poolv2 → sv2-p2pool
/// → jd_client_sv2. Each spawner's readiness signal is "TCP accept on
/// the listen port"; if any fails, the test errors out and Drop tears
/// down the previously-started processes.
///
/// Bitcoind is launched in **testnet4** mode (with multiprocess
/// support) so:
/// - p2poolv2's testnet4 genesis applies (the only natively-supported
///   network from `Bitcoin / Testnet4 / Signet`).
/// - sv2-apps's `resolve_ipc_socket_path` for `network = "testnet4"`
///   matches the `-ipcbind=unix:<workdir>/testnet4/node.sock` we pass
///   to bitcoind.
///
/// Tradeoff vs regtest: no `mine_blocks` / `invalidate_block`. We can
/// still verify boot + handshake; mining-driven E2E (share submission)
/// requires either a real testnet4 sync or a shim block source.
#[test]
#[ignore = "requires BITCOIND_EXE + P2POOLV2_EXE + SV2_P2POOL_EXE + JD_CLIENT_EXE + bitcoind built with multiprocess support"]
fn full_stack_boots_against_testnet4_bitcoind() {
    let env = TestEnvBuilder::new()
        .with_network(bitcoin::Network::Testnet4)
        .with_ipcbind()
        .build()
        .expect("bitcoind testnet4 starts with ipcbind");
    let socket = env
        .ipc_socket_path
        .as_ref()
        .expect("ipcbind requested -> socket path set");
    eprintln!("bitcoind ipc socket: {}", socket.display());

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
