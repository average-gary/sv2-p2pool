//! Track B driving E2E: when bitcoind's `getblocktemplate` proposal-mode
//! call returns a rejection, the engine must short-circuit
//! `SetCustomMiningJob` with `Error(_)` and bump
//! `sv2_p2pool_engine_set_custom_mining_job_proposal_rejected_total{reason="consensus_rejected"}`.
//!
//! Topology — same shape as
//! `e2e_block_submission.rs`, but with a `wiremock`-backed HTTP server
//! interposed in front of bitcoind's RPC port. The IPC template provider
//! continues to talk to the real regtest bitcoind (so templates, share
//! chain ops, etc. behave normally); only the RPC channel that the
//! engine uses for `validate_block_proposal` is hijacked:
//!
//! ```text
//! mujina (SV1, CPU)  →  translator_sv2  →  jd_client_sv2  →  sv2-p2pool ─┬→ bitcoind regtest (IPC: templates)
//!                                                                       └→ wiremock (RPC: getblocktemplate proposal → "bad-cb-amount")
//! ```
//!
//! Witness: the engine's `/metrics` carries the consensus-rejection
//! counter labeled `reason="consensus_rejected"`. Any positive value
//! proves the per-SCMJ proposal-validation hop ran AND saw a rejection.
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
//!   cargo test -p sv2-p2pool-testenv \
//!     --test e2e_scmj_rejects_consensus_invalid_template \
//!     -- --ignored --nocapture
//! ```
//!
//! Test ordering: the wiremock interceptor must be up BEFORE sv2-p2pool
//! spawns so the pool's first RPC handshake (chain-info probe) lands
//! against the mock. The mock is configured to proxy *most* RPC methods
//! through to real bitcoind so probe + share-chain ops succeed; only
//! `getblocktemplate` in proposal mode is rejected.

use std::time::Duration;

use sv2_p2pool_testenv::{
    JdClientDBuilder, MujinaMinerDBuilder, P2poolV2DBuilder, Sv2P2poolDBuilder, TestEnvBuilder,
    TranslatorSv2DBuilder,
};
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires BITCOIND_EXE + P2POOLV2_EXE + SV2_P2POOL_EXE + JD_CLIENT_EXE + TRANSLATOR_SV2_EXE + MUJINA_MINERD_EXE + bitcoind built with multiprocess support"]
async fn e2e_scmj_rejects_consensus_invalid_template() {
    let env = TestEnvBuilder::new()
        .with_network(bitcoin::Network::Regtest)
        .with_ipcbind()
        .build()
        .expect("bitcoind regtest starts with ipcbind");

    // 1. Spin up a wiremock RPC server that returns
    //    `{"result":"bad-cb-amount","id":...,"error":null}` for the
    //    proposal-mode getblocktemplate calls the engine will issue at
    //    SCMJ time. Any other RPC method gets a default 200 with an
    //    empty result so the pool's startup probe + share-chain ops
    //    don't fail outright. We pre-mount the proposal matcher with
    //    higher priority so it wins over the catch-all.
    let mock_server = MockServer::start().await;

    // Reject every `getblocktemplate` call whose params include
    // `"mode":"proposal"`. The body shape is JSON-RPC 2.0:
    // `{"jsonrpc":"2.0","method":"getblocktemplate","params":[{"mode":"proposal","data":"..."}],...}`.
    Mock::given(method("POST"))
        .and(path("/"))
        .and(body_partial_json(serde_json::json!({
            "method": "getblocktemplate",
            "params": [{ "mode": "proposal" }],
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "bad-cb-amount",
            "error": null,
        })))
        // Highest priority so it wins over the catch-all below.
        .with_priority(1)
        .mount(&mock_server)
        .await;

    // Catch-all: any other RPC method gets a JSON-RPC `{"result": null}`
    // so probes succeed. Note this is intentionally lenient — the test
    // is about the proposal-rejection path, not full bitcoind fidelity.
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": null,
            "error": null,
        })))
        .with_priority(5)
        .mount(&mock_server)
        .await;

    let mock_rpc_url = mock_server.uri();
    eprintln!("wiremock RPC interceptor: {mock_rpc_url}");

    let p2pool = P2poolV2DBuilder::new(&env.bitcoind)
        .with_network(bitcoin::Network::Regtest)
        .build()
        .expect("p2poolv2 starts on regtest");
    eprintln!(
        "p2poolv2: api={} stratum={} libp2p_port={}",
        p2pool.api_addr, p2pool.stratum_addr, p2pool.libp2p_port
    );

    // 2. Point sv2-p2pool's [bitcoinrpc] URL at the wiremock interceptor.
    //    The IPC template provider still uses the real bitcoind via
    //    bitcoin_data_dir.
    let sv2 = Sv2P2poolDBuilder::new(&env.bitcoind)
        .with_network(bitcoin::Network::Regtest)
        .with_bitcoin_data_dir(env.bitcoind.workdir())
        .with_low_difficulty()
        .with_bitcoinrpc_url_override(mock_rpc_url.clone())
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

    // Witness: the consensus_rejected counter must increment within
    // 60s. The proposal-validation hop fires synchronously inside
    // handle_set_custom_mining_job; once the JDC declares + SCMJs at
    // least one job, the counter goes positive.
    let observed = sv2
        .wait_for_metric(
            |body| {
                body.lines().any(|line| {
                    !line.starts_with('#')
                        && line.starts_with(
                            "sv2_p2pool_engine_set_custom_mining_job_proposal_rejected_total{",
                        )
                        && line.contains("reason=\"consensus_rejected\"")
                        && line
                            .rsplit(' ')
                            .next()
                            .and_then(|v| v.parse::<u64>().ok())
                            .is_some_and(|v| v > 0)
                })
            },
            Duration::from_secs(60),
        )
        .expect("engine /metrics scrape succeeds");

    assert!(
        observed,
        "expected consensus_rejected counter > 0 within 60s\n\
         engine /metrics body:\n{}",
        sv2.scrape_metrics_body().unwrap_or_default(),
    );
    eprintln!("validate_block_proposal rejection captured at /metrics");
}
