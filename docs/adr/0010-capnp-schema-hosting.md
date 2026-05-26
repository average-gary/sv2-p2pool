# 0010. Cap'n Proto schema crate hosting for `p2poolv2-capnp-types`

- Status: Accepted
- Date: 2026-05-26
- Deciders: sv2-p2pool maintainers, p2poolv2 maintainers (pending upstream review)
- Tags: phase-2, design, upstream, ipc, capnp

## Context and Problem Statement

Phase 2 of the integration plan adds a Cap'n Proto IPC server to `p2poolv2`, mirroring how `bitcoind` exposes the Template Distribution Protocol to `bitcoin-core-sv2`. The IPC schema (`p2poolv2.capnp`) plus the Rust bindings generated from it must be packaged as a crate — call it `p2poolv2-capnp-types` — that both the p2poolv2 IPC *server* and our `sv2-p2pool-ipc` *client* depend on. The question is **where that crate is hosted and released from**.

The decision blocks issue #7 (the upstream IPC PR): the location of the schema source-of-truth determines the PR's file layout and the consumer's `Cargo.toml` shape.

## Decision Drivers

- **Single source of truth** for the schema. Two copies guarantee drift.
- **Versioning ergonomics** for both the server (p2poolv2) and clients (us, future implementers).
- **Upstream-friendliness**: the IPC PR should look conventional, not exotic.
- **Avoid forcing consumers to vendor p2poolv2** just to get the schema (p2poolv2 is AGPL; the schema crate itself is plain data + capnp-generated code that should be MIT/Apache, exactly like `bitcoin-capnp-types`).
- **Release cadence**: schema bumps should be cheap; they shouldn't require a full p2poolv2 release.

## Considered Options

- **A. In-tree in `p2poolv2/p2poolv2`, not separately published** — schema crate lives at `p2poolv2/crates/p2poolv2-capnp-types/`, consumed by p2poolv2's IPC server via path; external consumers depend on it via git URL.
- **C. In-tree in `p2poolv2/p2poolv2`, published independently to crates.io** — schema crate lives in p2poolv2's repo, but is released to crates.io on its own cadence, with its own SemVer. **This is the `bitcoin-capnp-types` shape.**
- **D. Defer — keep the schema in-repo for now, no separate crate** (YAGNI). Revisit when a third consumer materializes.

Option B (host the schema in `sv2-p2pool`, p2poolv2 imports it) was rejected at the framing stage: the IPC *server* is the canonical implementer and lives in p2poolv2. Hosting the schema downstream of its server contradicts the precedent (`bitcoin-capnp-types` ships from the same org as Bitcoin Core's IPC server, not from a consumer) and would make the upstream PR awkward (p2poolv2 would gain a hard dep on a crate from a third-party org).

## Decision Outcome

**Chosen: Option C — schema crate lives in `p2poolv2/p2poolv2` and is published independently to crates.io as `p2poolv2-capnp-types`.**

This matches the `bitcoin-capnp-types` precedent already in use inside our vendored tree: `vendor/sv2-apps/bitcoin-core-sv2/Cargo.toml:26` consumes it as a plain `bitcoin-capnp-types = "0.2.0"` cargo dep alongside `capnp = "0.25.0"` (line 15) and `capnp-rpc = "0.25.0"` (line 16). The plan §4.4 explicitly calls for this shape: *"Companion crate `p2poolv2-capnp-types` published to crates.io so external implementers (us, others) can depend on the schema without vendoring p2poolv2"* (`~/wiki/topics/sv2-p2pool-integration/output/plan-sv2-p2pool-repo-2026-05-22.md` §4.4).

### Consequences

Positive:
- Single source of truth — schema source lives next to its only canonical implementer (the IPC server in p2poolv2).
- External consumers (us in `sv2-p2pool-ipc`; future SV2 pools or tooling) depend on `p2poolv2-capnp-types = "x.y"` — no submodule, no AGPL exposure from the schema dep itself.
- Schema can be bumped without cutting a full p2poolv2 release (separate crate, separate `cargo publish`).
- Upstream PR shape is conventional: a new sibling crate in the existing workspace, plus a `[publish]` flow.

Negative / accepted:
- Two release artifacts to coordinate (`p2poolv2` daemon and `p2poolv2-capnp-types`). Mitigated by the same human owners.
- Versioning discipline required: schema bumps must follow SemVer, since the daemon and external clients pin different versions in lockstep is unrealistic.

### Implementation notes (issue #7)

- License `p2poolv2-capnp-types` as `MIT OR Apache-2.0` (matches `bitcoin-capnp-types`); the AGPL boundary stays at the daemon binary, not at the schema/data crate.
- Publish on the first IPC server release; before then, our `sv2-p2pool-ipc` consumes it via `git = "..."` pin to the upstream branch (mirroring how `bitcoin-core-sv2/Cargo.toml:24` git-pins `stratum-core` during co-development).
- Record the chosen schema capability IDs (`@0xc0ffee...`) in this ADR's followup once generated.

## Links

- Plan: `~/wiki/topics/sv2-p2pool-integration/output/plan-sv2-p2pool-repo-2026-05-22.md` §2.2 (schema sketch), §4.4 (IPC PR scope), §10 (open question 10).
- Precedent: `vendor/sv2-apps/bitcoin-core-sv2/Cargo.toml:26` — `bitcoin-capnp-types = "0.2.0"`.
- Issue: #10 (this decision); blocks #7 (capnp IPC PR).
