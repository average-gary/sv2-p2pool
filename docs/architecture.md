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
- **Phase 2-B**: capnp IPC PR to p2poolv2; switch binary to talk over UDS. The engine's IPC client crate (`crates/sv2-p2pool-ipc`) is scaffolded but not yet active.
- **Phase 3**: production hardening, observability, deployment recipes, full driving E2E test (`JdClientD` spawner + integration-tests).

## Phase 2-A status by component

- ✅ Engine: full `JobValidationEngine` impl with real `handle_declare_mining_job` (TDP-snapshot tip + cached `template_id`) and `handle_push_solution` (lookup → `RequestTransactionData` → block reconstruction → `submit_block`).
- ✅ Pool binary: loads both `--config` (sv2-apps PoolConfig) and `--p2pool-config` (p2poolv2 share-chain config). `Pool::start` spawns the TDP demux tasks and bootstraps EngineHandles when the second config is supplied.
- ✅ Testenv: `P2poolV2D` and `Sv2P2poolD` spawners. Default network is Testnet4 (the live deployment target with a public dashboard).
- ⏳ Full p2poolv2 `NodeHandle` (libp2p networking, ZMQ listener, GBT poller, Stratum server, metrics, monitoring) — deferred until share-chain validation moves into `handle_declare_mining_job`. None of these are consumed by the current engine code path.
- ⏳ End-to-end driving test (`JdClientD` spawner + integration-tests crate) — deferred to Phase 3 alongside the production-hardening work.

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
