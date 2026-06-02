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
- **Phase 2-B**: capnp IPC integration on the engine side. The client crate is now functional against the upstream stub (PR #49); next step is replacing the in-process `EngineHandles.chain` with IPC calls to an out-of-process p2poolv2 daemon. Blocked on the daemon's stub returning real share-chain results.
- **Phase 3**: full driving E2E test against testnet4 (drives `SubmitSharesExtended` end-to-end), the ADR 0002 token-payout interceptor (needs a JDC TLV extension or upstream sv2-apps trait change), deployment recipes (systemd, docker-compose).

## Status by component

- ✅ **Engine**: full `JobValidationEngine` impl. `handle_declare_mining_job` returns `MissingTransactions` first-pass per spec, captures TDP tip + share-chain tip on success, caches a `DeclaredJob`. `handle_set_custom_mining_job` cross-checks every field (with handles-less fallback). `handle_push_solution` looks up the cached job, fetches tx bodies via TDP, reconstructs the block, submits to bitcoind. `notify_share_chain_reorg` walks ancestry to selectively invalidate.
- ✅ **Pool runtime**: TDP demux + reorg watcher + RecentSolutions sweeper + bootstrapped `EngineHandles` (rocksdb chain + bitcoind RPC). Graceful shutdown aborts every spawned task.
- ✅ **Observability**: Prometheus `IntCounter` set covering every material event, exposed via a built-in `/metrics` HTTP endpoint (`--metrics-addr`). `--log-file` honoured via `init_logging`.
- ✅ **Testenv**: `BitcoinD` + `P2poolV2D` + `Sv2P2poolD` + `JdClientD` spawners. `with_ipcbind` for Bitcoin Core multiprocess; testnet4-default. Smoke tests exercise full-stack boot.
- ✅ **IPC client crate**: connects to the upstream p2poolv2 IPC server stub, surfaces `validate_template` / `submit_solution` / `subscribe_chain_tip`.
- ⏳ **Engine validation against upstream**: `validate_block_proposal` against bitcoind isn't called at declare time. Open question whether to lift it from in-process p2poolv2_lib via the IPC client or keep direct.
- ⏳ **Full driving E2E**: needs bitcoind built with multiprocess support to run; currently the spawner-orchestration test demonstrates boot but stops short of share submission.
- ⏳ **Per-miner payout binding** (ADR 0002): the engine's `lookup_payout_script` is wired but the JDC-side allocation interceptor needs an upstream sv2-apps TLV extension.

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
