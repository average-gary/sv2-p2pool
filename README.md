# sv2-p2pool

A Stratum V2 mining pool that uses [p2poolv2](https://github.com/p2poolv2/p2poolv2) as its share-accounting and payout backend, composed on top of the [stratum-mining/sv2-apps](https://github.com/stratum-mining/sv2-apps) reference application stack.

**Status: usable on testnet4.** The pool boots end-to-end against bitcoind multiprocess + a p2poolv2 share-chain config, speaks the full SV2 Job Declarator Protocol per spec, finds blocks via `submit_block`, and exports Prometheus metrics. Production hardening is ongoing — see [docs/architecture.md](docs/architecture.md) for the per-component status table.

## What it does

The sv2-apps stack defines a `JobValidationEngine` trait — pluggable backend for the SV2 Job Declarator Server. The reference impl `BitcoinCoreIPCEngine` talks to bitcoind directly. This repo adds a second impl, `P2poolV2Engine`, that:

- Speaks SV2 JDP to JDCs (`DeclareMiningJob` → `MissingTransactions` / `Success`, `SetCustomMiningJob`, `PushSolution`).
- Issues `RequestTransactionData(template_id)` over the SV2 Template Distribution Protocol when reconstructing a found block — no out-of-band JSON-RPC.
- Submits the reconstructed `bitcoin::Block` to bitcoind via `submit_block`.
- Watches the p2poolv2 share-chain tip and selectively invalidates cached `DeclaredJob`s on reorg (walks `prev_share_blockhash` ancestry).
- Lets miners receive coinbase payouts directly from blocks the pool finds — no custodial pool wallet (per [ADR 0002](docs/adr/0002-jdtoken-payout-script.md)).
- Exports a Prometheus counter set + a built-in `/metrics` HTTP endpoint.

## Quickstart

For a copy-pasteable systemd or docker-compose install, see [deploy/README.md](deploy/README.md).

```sh
# Build
cargo build --release --bin sv2-p2pool

# Configure (start from the examples)
cp deploy/config/pool.example.toml /etc/sv2-p2pool/pool.toml
cp deploy/config/p2pool.example.toml /etc/sv2-p2pool/p2pool.toml
$EDITOR /etc/sv2-p2pool/pool.toml          # auth keys, payout address
$EDITOR /etc/sv2-p2pool/p2pool.toml        # bitcoinrpc creds, dial_peers

# Run
sv2-p2pool \
    --config /etc/sv2-p2pool/pool.toml \
    --p2pool-config /etc/sv2-p2pool/p2pool.toml \
    --metrics-addr 127.0.0.1:9000
```

Bitcoin Core 28.0+ with multiprocess support is required for the IPC template provider — see [docs/running.md](docs/running.md#bitcoin-core-ipc).

## Layout

```
crates/
├── sv2-p2pool-engine/   # JobValidationEngine impl + share-chain validation
├── sv2-p2pool-ipc/      # Cap'n Proto client for the p2poolv2 IPC server
├── sv2-p2pool-pool/     # the pool runtime + binary (replaces PoolSv2::start)
└── sv2-p2pool-testenv/  # corepc-node-style spawners for full-stack tests

docs/
├── architecture.md     # design + phasing
├── running.md          # operator quickstart
└── adr/                # architecture decision records

deploy/
├── README.md           # install instructions
├── systemd/            # sv2-p2pool.service
├── docker-compose.yml  # all-in-one stack
└── config/             # pool.example.toml + p2pool.example.toml
```

## Vendoring

Both upstreams are git submodules under `vendor/` so we can pin specific commits and patch locally:

```
vendor/sv2-apps/   — github.com/stratum-mining/sv2-apps (MIT/Apache-2.0)
vendor/p2poolv2/   — github.com/p2poolv2/p2poolv2 (AGPL-3.0)
```

To clone:

```sh
git clone --recurse-submodules https://github.com/average-gary/sv2-p2pool
# or after cloning:
git submodule update --init --recursive
```

Standard cargo deps (capnp, tokio, prometheus, etc.) are pulled from crates.io — they are not vendored.

## License

This repo is **AGPL-3.0-or-later** because the pool binary links `p2poolv2_lib` (AGPL-3.0). The `vendor/sv2-apps/` submodule remains MIT/Apache-2.0 in isolation; combining it with this crate places the combined work under AGPL.

If you operate the pool publicly, AGPL §13 requires you to make the source available to network users. Hosting this Git repo (or a fork) publicly satisfies that obligation.

The Cap'n Proto schema crate (`vendor/p2poolv2/p2poolv2-capnp-types`) is dual-licensed `MIT OR Apache-2.0` so non-AGPL clients can talk to the daemon — see [ADR 0010](docs/adr/0010-capnp-schema-hosting.md).

## Testing

```sh
# Unit + integration (75 tests)
cargo test --workspace

# Full-stack #[ignore]d smoke tests (requires BITCOIND_EXE + P2POOLV2_EXE
# + SV2_P2POOL_EXE + JD_CLIENT_EXE on PATH or via env vars)
cargo test --workspace -- --ignored
```

A nightly CI workflow (`.github/workflows/nightly.yml`) builds the dependency binaries from the submodules and runs the full ignored set.

## Local CI

CI runs on GitHub Actions (`.github/workflows/ci.yml`), but you can iterate on it locally via [`act`](https://github.com/nektos/act) without burning GitHub-hosted runner minutes.

```sh
brew install act          # macOS; see https://nektosact.com/installation/ for Linux
git submodule update --init --recursive   # required: workflow uses submodules: recursive
act pull_request          # full pull_request event
act -j workspace          # just the workspace build/test/lint job
```

Defaults live in `.actrc` at the repo root.

## Phases

- [x] **Phase 0** — repo bootstrap, submodules, scaffolding
- [x] **Phase 1** — `P2poolV2Engine` + full pool binary; signet smoke test
- [x] **Phase 2-A** — in-process share-chain integration; SV2-native tip + tx bodies via TDP; full handle_push_solution; reorg watcher; metrics; deployment recipes
- [ ] **Phase 2-B** — capnp IPC integration on the engine side (client crate exists; awaiting upstream stub-to-real wiring)
- [ ] **Phase 3** — full driving E2E test (share submission end-to-end), per-miner payout binding (ADR 0002), production deployments
