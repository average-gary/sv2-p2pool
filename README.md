# sv2-p2pool

A Stratum V2 mining pool that uses [p2poolv2](https://github.com/p2poolv2/p2poolv2) as its share-accounting and payout backend, by composing the [stratum-mining/sv2-apps](https://github.com/stratum-mining/sv2-apps) reference application stack.

**Status: pre-alpha.** Phase 0 bootstrap. Not usable yet.

## What this is

The sv2-apps stack defines a `JobValidationEngine` trait — pluggable backend for the SV2 Job Declarator Server. Today its only implementation is `BitcoinCoreIPCEngine` (talks to bitcoind). This repo adds a second implementation, `P2poolV2Engine`, that delegates job validation, share accounting, and payout selection to a running p2poolv2 share-chain node.

The result is a single pool binary that:
- Speaks SV2 to miners (via JDC + JDP, optionally direct mining-protocol channels)
- Routes share accounting into p2poolv2's peer-to-peer share chain
- Lets miners receive coinbase payouts directly from blocks the pool finds — no custodial pool wallet

## Vendoring

Both upstreams are git submodules under `vendor/` so we can pin specific commits and locally hack if needed:

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

Standard cargo deps (capnp, tokio, etc.) are pulled from crates.io — they are not vendored.

## Layout

```
crates/
├── sv2-p2pool-engine/   # JobValidationEngine impl over p2poolv2
├── sv2-p2pool-ipc/      # capnp client (Phase 2)
└── sv2-p2pool-pool/     # the pool binary; full PoolSv2 replacement
```

## License

This repo is **AGPL-3.0-or-later** because it links `p2poolv2_lib` (AGPL-3.0). The `vendor/sv2-apps/` submodule remains MIT/Apache-2.0 in isolation; combining it with this crate places the combined work under AGPL.

If you operate the pool publicly, AGPL §13 requires you to make the source available to network users. Hosting this Git repo (or a fork) publicly satisfies that obligation.

## Plan and design notes

The full spec, including the SV2 ↔ p2poolv2 message-by-message mapping and `JobValidationEngine` skeleton, lives in the project author's research wiki. A sanitized version will land in `docs/architecture.md` once Phase 1 is underway.

## Phases

- [x] **Phase 0** — repo bootstrap, submodules, scaffolding, workspace spike
- [ ] **Phase 1** — `P2poolV2Engine` + full pool binary; signet smoke test
- [ ] **Phase 2** — Cap'n Proto IPC PR to p2poolv2; switch pool to talk over UDS
- [ ] **Phase 3** — production hardening
