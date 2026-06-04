//! Smoke test for [`sv2_p2pool_testenv::TranslatorSv2D`]: boots
//! bitcoind + p2poolv2 + sv2-p2pool + jd_client_sv2 + translator_sv2,
//! verifies the translator's SV1 downstream port accepts a TCP
//! connection.
//!
//! Run locally (after building the prerequisite binaries):
//!
//! ```sh
//! cargo build --bin sv2-p2pool
//! cargo build --manifest-path vendor/p2poolv2/Cargo.toml --bin p2poolv2
//! cargo build --manifest-path vendor/sv2-apps/miner-apps/Cargo.toml \
//!     --bin jd_client_sv2 --bin translator_sv2
//!
//! cargo test -p sv2-p2pool-testenv --test e2e_translator -- --ignored
//! ```

use std::net::TcpStream;
use std::time::Duration;

use sv2_p2pool_testenv::{
    JdClientDBuilder, Sv2P2poolDBuilder, TestEnvBuilder, TranslatorSv2DBuilder,
};

#[test]
#[ignore = "requires BITCOIND_EXE + P2POOLV2_EXE + SV2_P2POOL_EXE + JD_CLIENT_EXE + TRANSLATOR_SV2_EXE + bitcoind built with multiprocess support"]
fn translator_boots_atop_jd_client_sv2() {
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
    eprintln!("sv2-p2pool: jds={} mining={}", sv2.jds_addr, sv2.mining_addr);

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

    // Ready signal in the spawner is TCP-accept on downstream_port. As
    // an extra-loud assertion, dial it again from the test body so a
    // future regression in wait_for_ready is caught explicitly.
    TcpStream::connect_timeout(&translator.downstream_addr, Duration::from_secs(2))
        .expect("translator downstream port accepts");
}
