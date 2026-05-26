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
  - 1.9 — this doc update
- **Phase 2**: wire `ChainStoreHandle` + `BitcoindLike` into the engine; capnp IPC PR to p2poolv2; switch binary to talk over UDS.
- **Phase 3**: production hardening, observability, deployment recipes.

## Local development

```sh
# Build
cargo check --workspace --locked
cargo test --workspace

# Run binary
cargo run --bin sv2-p2pool -- --config ./config/pool.example.toml

# Regtest harness smoke (requires BITCOIND_EXE or auto-download)
cargo test -p sv2-p2pool-testenv -- --ignored
```

For local CI iteration without burning GitHub-hosted runner minutes, see the `Local CI` section in the [README](../README.md) for `act` setup.
