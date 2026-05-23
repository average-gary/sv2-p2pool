# Architecture

This is a 1-pager. The full spec — including the SV2 ↔ p2poolv2 message-by-message mapping and the `JobValidationEngine` implementation skeleton — is maintained in the project author's research wiki.

## Components

```
crates/sv2-p2pool-engine    # JobValidationEngine impl — translates SV2 JDP messages into p2poolv2 share-chain calls
crates/sv2-p2pool-ipc       # capnp client for talking to a p2poolv2 daemon (Phase 2; not yet active)
crates/sv2-p2pool-pool      # the binary; full PoolSv2 replacement
```

## Vendored upstreams

```
vendor/sv2-apps/    github.com/stratum-mining/sv2-apps   MIT/Apache-2.0
vendor/p2poolv2/    github.com/p2poolv2/p2poolv2         AGPL-3.0
```

These are git submodules pinned to specific commits. To update one:

```sh
cd vendor/sv2-apps && git fetch origin && git checkout <new-commit>
cd ../..
cargo check --workspace                  # rebuild against new commit
cargo test --workspace                   # ensure nothing broke
git add vendor/sv2-apps && git commit -m "bump sv2-apps to <new-commit>"
```

## Trait we implement

`jd_server_sv2::job_declarator::job_validation::JobValidationEngine` — defined in `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/mod.rs`. The reference implementation `BitcoinCoreIPCEngine` lives at `bitcoin_core_ipc.rs:404-867`; our `P2poolV2Engine` mirrors its structure, swapping the bitcoind-IPC backend for direct calls into `p2poolv2_lib`.

## Why we bypass `PoolSv2::start()`

`PoolSv2::start()` (at `vendor/sv2-apps/pool-apps/pool/src/lib/mod.rs:91-110`) hard-codes engine selection — only `BitcoinCoreIPCEngine` can be constructed from its config-driven match arm. Our pool binary instead assembles `JobDeclarator::new(engine, ...)` directly using the public constructor, which accepts any `Arc<dyn JobValidationEngine>`.

## Phasing

- **Phase 0 (this commit)**: repo bootstrap, submodules, scaffolded crates, workspace spike.
- **Phase 1**: `P2poolV2Engine` implementation; full pool binary; signet smoke test.
- **Phase 2**: capnp IPC PR to p2poolv2; switch the binary to talk over UDS.
- **Phase 3**: production hardening, observability, deployment recipes.
