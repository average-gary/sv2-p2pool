# Architecture

This is a 1-pager. The full spec — including the SV2 ↔ p2poolv2 message-by-message mapping, ADRs, and the `JobValidationEngine` implementation skeleton — is maintained in the project author's research wiki at `~/wiki/topics/sv2-p2pool-integration/`.

## Components

```
crates/sv2-p2pool-engine    # JobValidationEngine impl — translates SV2 JDP messages into p2poolv2 share-chain calls
crates/sv2-p2pool-pool      # Pool runtime + binary entry point. Bypasses PoolSv2::start.
crates/sv2-p2pool-ipc       # capnp client for talking to a p2poolv2 daemon (Phase 2; not yet active)
crates/sv2-p2pool-testenv   # Regtest test harness (Phase 1.8). corepc-node-based.
```

## Vendored upstreams

```
vendor/sv2-apps/    github.com/stratum-mining/sv2-apps   MIT/Apache-2.0
                    pinned to maintainer fork average-gary/sv2-apps:phase-1
                    (carries feat/jve-reorg-notify on top of upstream main)

vendor/p2poolv2/    github.com/p2poolv2/p2poolv2         AGPL-3.0
                    pinned to maintainer fork average-gary/p2pool-v2:phase-1
                    (carries feat/bitcoind-trait + feat/capnp-ipc on top of upstream main)
```

These are git submodules pinned to specific commits. To update:

```sh
cd vendor/sv2-apps && git fetch && git checkout <new-commit>
cd ../..
cargo check --workspace                  # rebuild against new commit
cargo test --workspace                   # ensure nothing broke
git add vendor/sv2-apps && git commit -m "bump sv2-apps to <new-commit>"
```

## Trait we implement

`jd_server_sv2::job_declarator::job_validation::JobValidationEngine` — defined in `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/mod.rs`. The reference implementation `BitcoinCoreIPCEngine` lives at `bitcoin_core_ipc.rs:404-867`; our `P2poolV2Engine` mirrors its structure, swapping the bitcoind-IPC backend for direct calls into `p2poolv2_lib` and `BitcoindLike` (the trait we contributed via the upstream `feat/bitcoind-trait` branch).

## Why we bypass `PoolSv2::start()`

`PoolSv2::start()` (at `vendor/sv2-apps/pool-apps/pool/src/lib/mod.rs:91-110`) hard-codes engine selection — only `BitcoinCoreIPCEngine` can be constructed from its config-driven match arm. Our `Pool::start` (at `crates/sv2-p2pool-pool/src/pool.rs`) mirrors `PoolSv2::start` line-for-line, but constructs `JobDeclarator::new(engine, ...)` with our `P2poolV2Engine` using the public constructor that accepts any `Arc<dyn JobValidationEngine>`.

## Pool boot sequence

`crates/sv2-p2pool-pool/src/main.rs`:
1. `process_cli_args()` — load `PoolConfig` from TOML (default: `./sv2-p2pool.toml`).
2. `PoolBuilder::new(network).build_pool(config)` — construct `Pool`.
3. `pool.start().await` — boot:
   - Build `P2poolV2Engine` (our `JobValidationEngine` impl).
   - Build `JobDeclarator::new(engine, ...)`.
   - Build `ChannelManager`.
   - Connect Template Provider (Bitcoin Core IPC or upstream SV2 TP).
   - Start downstream listeners.
   - Wait for `Ctrl+C` or external cancellation.

The shared-chain handles (`ChainStoreHandle`, `Arc<dyn BitcoindLike>`, `Arc<dyn ShareValidator>`) are NOT yet plumbed into the engine. Phase 2 widens `PoolBuilder` to accept them — until then, the trait methods do structural validation only and stub the share-chain integration.

## Chain-read seam (Phase 2-B Track A, ADR 0011)

The engine reads share-chain state exclusively through the `ShareChainReader` trait at `crates/sv2-p2pool-engine/src/share_chain_reader.rs`. The trait is dyn-compatible (hand-written `Pin<Box<Future<...>>>` returns, no `async-trait` macro on the build graph), `Send + Sync`, and carries five methods: async `get_chain_tip` / `get_share_header` / `get_tip_height` for capnp round-trips, sync `network()` for the value captured once at construction, and sync `subscribe_tip()` returning a fresh `broadcast::Receiver<BlockHash>` for push-driven tip distribution. `EngineHandles.chain` is `Arc<dyn ShareChainReader>` — the engine has no compile-time knowledge of the backend.

Two backends live in the pool crate (the AGPL boundary):

- `sv2_p2pool::share_chain::InProcessChain` wraps a live `ChainStoreHandle`. Used by single-process dev and the rocksdb-backed `#[tokio::test]`s in `share_chain.rs`. The legacy genesis sentinel (all-zeros) is mapped to `ShareHeaderLookup::Genesis` at this adapter so the in-process and IPC paths behave identically.
- `sv2_p2pool::share_chain::IpcChain` is the production backend. It owns a `!Send` `Sv2P2poolIpcClient` on a dedicated `std::thread` running a current-thread `tokio` runtime + `LocalSet`. Outside callers send `IpcRequest` messages over a bounded mpsc (`REQUEST_CHANNEL_CAPACITY = 256`, sized as 2 × `REORG_ANCESTRY_DEPTH` so a worst-case 100-hop reorg ancestry walk never blocks). A subscribe-tip task on the same `LocalSet` fans the daemon's `subscribeChainTip @2` callback into both a lock-free `AtomicTipSnapshot` (read by the reorg watcher's sync `Fn() -> Option<BlockHash>` closure at `pool.rs`) and a `broadcast::Sender<BlockHash>` (read by `subscribe_tip()`). A detached watchdog thread joins the actor and flips a `watch::Sender<bool>` if the actor exits, so the pool binary can drive a clean shutdown rather than silently losing chain reads. ADR 0011 documents the full rationale, error model (single `IpcClientError` across the seam, `ShareHeaderLookup { Found, NotFound, Genesis }` for the discriminated wire union), and migration shape.

Selection between the two is config-driven: when `p2pool_config.ipc.socket_path` is set, `bootstrap_share_chain` connects an `IpcChain` to that socket; otherwise it opens a fresh rocksdb store and uses `InProcessChain`. The end-to-end exercise of the IPC seam lives at `crates/sv2-p2pool-testenv/tests/e2e_ipc_chain.rs` (boots a real capnp daemon via `p2poolv2_ipc::spawn_ipc_server_full`, drives `IpcChain` over UDS, covers `get_chain_tip`, a 100-hop reorg ancestor walk including mid-walk truncation, and tip-subscription delivery under a 50-update burst).

## Phasing

- **Phase 0** (shipped): repo bootstrap, submodules, scaffolded crates.
- **Phase 1** (shipped): `P2poolV2Engine` impl + Pool runtime + binary + regtest harness skeleton.
  - 1.0 — submodule integration branches
  - 1.1 — engine skeleton fields (PR #23)
  - 1.2-1.4 — `JobValidationEngine` trait impl (PR #25)
  - 1.5 — `PoolBuilder` + lib/bin split (PR #26)
  - 1.6-1.7 — Pool runtime + binary entry point (PR #27)
  - 1.8 — `sv2-p2pool-testenv` skeleton + smoke test
- **Phase 2-A** (shipped): in-process share-chain integration + SV2-native tip + tx bodies + spawners.
  - 2.1 — `EngineHandles` + `with_handles` constructor (PR #29)
  - 2.2 — block reconstruction module (PR #30)
  - 2.3 — real tip metadata via bitcoind GBT (PR #31, superseded by 2.4)
  - 2.4 — `TdpHandle` for SV2-native tip + tx bodies; full `handle_push_solution` (PR #32)
  - 2.5a — TDP demux + `TdpHandle` wiring in `Pool::start` (PR #33)
  - 2.5b — minimum-slice `EngineHandles` bootstrap (rocksdb + ChainStoreHandle + DefaultShareValidator + BitcoindRpcClient) (PR #34)
  - 2.6 — `P2poolV2D` testenv spawner with three-tier discovery (PR #35)
  - 2.7 — Network parameterization (default Testnet4) + `Sv2P2poolD` testenv spawner (PR #36)
  - 2.8 — docs refresh + `--ignored` E2E in CI nightly
  - 2.9 — `JdClientD` spawner + ipcbind support in the testenv (PRs #38 / #39)
  - 2.10 — testnet4 switch in testenv + reorg-watcher wiring (PRs #40 / #41)
  - 2.11 — selective `DeclaredJob` invalidation on share-chain reorg (PRs #42 / #43)
  - 2.12 — `MissingTransactions` flow + `SetCustomMiningJob` handles-less mode (PRs #44 / #45)
  - 2.13 — `RecentSolutions` sweeper, demux abort on shutdown, validator-handle cleanup (PRs #46 / #48 / #50)
  - 2.14 — IPC client skeleton against the upstream stub (PR #49)
  - 2.15 — Engine Prometheus counters + `/metrics` endpoint + `--log-file` wiring (PRs #51 / #52 / #53 / #54)
  - 2.16 — `config_network` testnet4 fix (PR #55)
  - 2.17 — Production observability: cache-size + sweeper-liveness gauges, `blocks_submit_failed_total` (transport errors + consensus rejections), `push_solution_dropped_total{reason}` labeled counter, `share_chain_tip_height` gauge, `/healthz` endpoint, log-field correlation, reorg-counter wiring fix (PRs #61 / #62 / #63 / #64 / #65 / #66 / #71 / #72 / #74)
  - 2.18 — Phase 2-B server-side: real `submit_solution` / `subscribe_chain_tip` / `validate_template` (PRs #68 / #69 / #70), E2E verifies JDP handshake via /metrics scrape (PR #67), warn-only bitcoind probe at boot (PR #73)
- **Phase 2-B** ✅ shipped: capnp IPC integration on the engine side, completed in Phase 2-B Track A (ADR 0011). PRs #68 / #69 / #70 made all three pre-existing IPC server methods real (`submit_solution` verifies `shareHash == block_hash()`; `subscribe_chain_tip` fans out from an injected `watch::Receiver<BlockHash>`; `validate_template` does a structural coinbase-parse pre-check). PR #67 added `--metrics-addr` plumbing to the testenv and upgraded the full-stack E2E to verify the JDP handshake actually flows. Track A then extended the capnp schema with `getChainTip @3` / `getShareHeader @4` / `getTipHeight @5` / `getNetwork @6` (each result a discriminated union mirroring the `ValidationResult` precedent), surfaced them on `Sv2P2poolIpcClient`, introduced the `ShareChainReader` async trait as the engine's only chain-read seam (`Arc<dyn ShareChainReader>` on `EngineHandles.chain`), and shipped two backends behind it: `InProcessChain` (wraps `ChainStoreHandle`; convenient for tests + the single-process dev path) and `IpcChain` (production — owns the `!Send` capnp client on a dedicated `LocalSet` thread, exposes a `Send + Sync` actor handle, drives the reorg watcher's sync closure from a lock-free `AtomicTipSnapshot`, and propagates actor-thread panics through a `watch::Sender<bool>` so a dead chain connection isn't silently swallowed). The engine crate no longer links `p2poolv2_lib`; the AGPL boundary now sits at the pool crate per ADR 0010.
- **Phase 3a** (shipped): driving E2E test against testnet4. Adds two testenv spawners (`TranslatorSv2D` for SV1↔SV2 proxying, `MujinaMinerD` for an SV1 CPU miner) and starts the upstream sv2-apps `MonitoringServer` in `Pool::start` so per-channel `sv2_client_shares_accepted_total` is scrapable. `e2e_share_submission` composes mujina → translator → JDC → pool and asserts a share lands in ChannelManager (modulo the upstream snapshot-cache refresh window, ~15s by default). Needs bitcoind built with multiprocess support on disk to actually run.
- **Phase 3b** (shipped): drive through `handle_push_solution` / `submit_block` on regtest. Adds regtest genesis support to the vendored p2poolv2 fork (`feat/regtest-support` branch — `REGTEST_GENESIS_DATA` in `shares/genesis/mod.rs` plus a widened assert in `shares/share_block/mod.rs`) so the share chain boots on regtest, where the network target is `0x207fffff` and a CPU thread clears it in milliseconds. Adds `Sv2P2poolDBuilder::with_low_difficulty()` to drop `start_difficulty`/`minimum_difficulty` to 1 so a CPU miner clears the channel target too. `e2e_block_submission` is the witness — asserts `sv2_p2pool_engine_blocks_submitted_total > 0` within 60s.
- **Phase 3c** (testnet shipped, mainnet pending accounting selector): per-miner payout binding via the upstream `JobValidationEngine` trait extension. ADR 0002 § Follow-up Option 4 has landed in our maintainer fork of sv2-apps on `feat/per-miner-payout-trait`; the engine's `handle_allocate_mining_job_token` runs duplicate-binding + size-budget checks and writes to `token_payout` whenever the resolver returns a script. Today the resolver returns `None` for every miner (the accounting selector is the remaining mainnet blocker), so all miners fall back to the pool-wide `coinbase_reward_script`. See [ADR 0013](adr/0013-per-miner-payout-binding.md) for the full wiring and the mainnet upgrade path.

## Status by component

- ✅ **Engine**: full `JobValidationEngine` impl. `handle_declare_mining_job` returns `MissingTransactions` first-pass per spec, captures TDP tip + share-chain tip on success, caches a `DeclaredJob`. `handle_set_custom_mining_job` cross-checks every field (with handles-less fallback). `handle_push_solution` looks up the cached job, fetches tx bodies via TDP, reconstructs the block, submits to bitcoind. `notify_share_chain_reorg` walks ancestry to selectively invalidate.
- ✅ **Pool runtime**: TDP demux + reorg watcher + RecentSolutions sweeper + bootstrapped `EngineHandles` (rocksdb chain + bitcoind RPC). Graceful shutdown aborts every spawned task.
- ✅ **Observability**: Prometheus collector set (`IntCounter`s for material events including `blocks_submit_failed_total` covering both transport errors and consensus rejections, `IntCounterVec` for `push_solution_dropped_total{reason}` with six stable labels, `IntCounterVec` for `set_custom_mining_job_proposal_rejected{reason}` (`consensus_rejected` / `rpc_error`) + companion `set_custom_mining_job_validation_skipped` counter and `set_custom_mining_job_validation_seconds` histogram for the ADR 0012 SCMJ-time bitcoind pre-flight, `IntGauge`s for declared-jobs/recent-solutions cache sizes, sweeper liveness timestamp, and share-chain tip height) exposed via a built-in `/metrics` HTTP endpoint (`--metrics-addr`). Sibling `/healthz` for orchestrator probes (Dockerfile + docker-compose wired). `--log-file` honoured via `init_logging`; submit_block log lines carry `request_id` + `template_id` for correlation with upstream JDP/TDP traffic.
- ✅ **Testenv**: `BitcoinD` + `P2poolV2D` + `Sv2P2poolD` + `JdClientD` + `TranslatorSv2D` + `MujinaMinerD` spawners. `with_ipcbind` for Bitcoin Core multiprocess; testnet4-default with `Sv2P2poolDBuilder::with_low_difficulty()` for regtest tests. Smoke tests exercise full-stack boot; `e2e_share_submission` drives shares from a CPU miner into ChannelManager (testnet4); `e2e_block_submission` drives a regtest CPU miner all the way to `bitcoind.submit_block`.
- ✅ **IPC client crate**: connects to the upstream p2poolv2 IPC server, surfaces `validate_template` / `submit_solution` / `subscribe_chain_tip`. All three server methods perform real work: `validate_template` does a structural coinbase parse-check (returns `InvalidCoinbase` on failure); `submit_solution` verifies `shareHash == block_hash()`; `subscribe_chain_tip` fans out tip changes from a `tokio::sync::watch::Receiver<BlockHash>` injected via `spawn_ipc_server_with_tip_source`. Full share-chain admission (coinbase value, wtxid commitment, share-chain ancestry) still requires a `ChainStoreHandle` plumbed into the daemon.
- ✅ **Engine validation against upstream**: `handle_set_custom_mining_job` pre-flights the candidate block through `BitcoindLike::validate_block_proposal` (BIP 23 proposal mode) before the JDS acks the custom job, using the in-process `EngineHandles.bitcoind` rather than a new IPC round-trip. Consensus rejections and RPC errors are surfaced via labeled metric `sv2_p2pool_engine_set_custom_mining_job_proposal_rejected{reason}` (stable labels `consensus_rejected` / `rpc_error`), latency tracked in the `…_validation_seconds` histogram, and structural-only fallbacks (no TDP / no `template_id` / no handles) counted by `…_validation_skipped`. See [ADR 0012](adr/0012-validate-block-proposal-at-scmj.md).
- ✅ **Full driving E2E**: 3a (share submission via mujina + translator on testnet4) and 3b (block submission on regtest, via the fork's regtest-genesis support) both wired. Both still require a multiprocess bitcoind on disk to run.
- ⏳→✅ **Per-miner payout binding** (ADR 0002, ADR 0013): testnet binding live — the engine's `handle_allocate_mining_job_token` (via the upstream `feat/per-miner-payout-trait` extension) writes the per-token payout map, the `TokenPayoutEvictor` hook drains it from `TokenManager` evictions, and the testenv E2E `e2e_per_miner_binding` proves two distinct `user_identity` labels land shares end-to-end. Mainnet upgrade path is the accounting selector that maps `user_identifier → Option<ScriptBuf>` — documented in [ADR 0013](adr/0013-per-miner-payout-binding.md) § Follow-ups.

## Local development

```sh
# Build
cargo check --workspace --locked
cargo test --workspace

# Run binary (Phase 2-A: requires BOTH config files)
cargo run --bin sv2-p2pool -- \
    --config ./config/pool.example.toml \
    --p2pool-config ./config/p2pool.example.toml

# Regtest harness smoke (requires BITCOIND_EXE or auto-download)
cargo test -p sv2-p2pool-testenv -- --ignored

# Full ignored-test run (requires BITCOIND_EXE + P2POOLV2_EXE +
# `cargo build --bin sv2-p2pool` for the workspace target lookup)
cargo test --workspace -- --ignored
```

See [docs/running.md](running.md) for an operator-oriented quickstart.

For local CI iteration without burning GitHub-hosted runner minutes, see the `Local CI` section in the [README](../README.md) for `act` setup.
