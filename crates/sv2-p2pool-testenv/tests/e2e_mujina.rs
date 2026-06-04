//! Smoke test for [`sv2_p2pool_testenv::MujinaMinerD`]: boots
//! mujina-minerd in standalone mode (no upstream pool) against its
//! built-in dummy job source and verifies the REST API binds.
//!
//! This test deliberately doesn't spin up the full SV2 stack — that
//! lives in `e2e_share_submission`. Here we only prove the spawner +
//! env-var plumbing work, decoupled from translator/JDC/pool readiness.
//!
//! Run locally:
//!
//! ```sh
//! cargo build --manifest-path /path/to/mujina/Cargo.toml --bin mujina-minerd
//! MUJINA_MINERD_EXE=/path/to/mujina/target/debug/mujina-minerd \
//!   cargo test -p sv2-p2pool-testenv --test e2e_mujina -- --ignored
//! ```

use std::net::TcpStream;
use std::time::Duration;

use sv2_p2pool_testenv::MujinaMinerDBuilder;

#[test]
#[ignore = "requires MUJINA_MINERD_EXE"]
fn mujina_boots_in_dummy_mode() {
    let mujina = MujinaMinerDBuilder::new()
        .with_ready_timeout(Duration::from_secs(15))
        .build()
        .expect("mujina-minerd starts");
    eprintln!("mujina-minerd: api_addr={}", mujina.api_addr);

    // Sanity-dial the API port post-readiness — catches a regression
    // where wait_for_ready returns Ok but the address isn't actually
    // accepting (very unlikely, but cheap to assert).
    TcpStream::connect_timeout(&mujina.api_addr, Duration::from_secs(2))
        .expect("mujina API port accepts");
}
