# 0012. `validate_block_proposal` at `SetCustomMiningJob` time

- Status: Accepted
- Date: 2026-06-24
- Deciders: sv2-p2pool maintainers
- Tags: phase-3, engine, validation, bitcoind

## Context and Problem Statement

The engine's `JobValidationEngine` impl cross-checks every field of a `SetCustomMiningJob` (SCMJ) against the cached `DeclaredJob` — version, prev-hash, nbits, ntime, merkle path, coinbase reconstruction. None of those checks call **bitcoind**. A JDC that publishes a structurally well-formed template that *bitcoind would still reject* (over-paid coinbase, stale prev-block, inconclusive witness commitment) is accepted by the engine, advertised by the JDS, and only fails when a share clears the network target and `submit_block` rejects it — i.e. block credit is lost.

The Phase 2-B IPC server's `validate_template` is a structural coinbase parse-check only; it does not run `getblocktemplate`-proposal mode against bitcoind. The engine itself holds an `Arc<dyn BitcoindLike>` (via `EngineHandles`) which exposes `validate_block_proposal` — the BIP 23 proposal-mode wrapper that returns a `ProposalOutcome::{Accepted, Duplicate, Rejected(reason)}` verdict.

The question: **when, and via which path, should the engine consult bitcoind to pre-flight the candidate block?**

## Decision Drivers

- Catch consensus failures *before* the JDS acks the custom job.
- Don't double-validate (`handle_push_solution` already submits the full block).
- Don't introduce a new IPC round-trip when the engine already holds the handle in-process.
- Stay handles-less-test compatible — existing engine unit tests run without `EngineHandles` wired.

## Considered Options

- **A. Validate at SCMJ-time, in-process, via `BitcoindLike::validate_block_proposal`.** Add a `block::build_candidate_block` constructor that synthesises a proposal-mode `bitcoin::Block` from the SCMJ header + zero-extranonce coinbase + TDP-fetched tx bodies, then call bitcoind directly.
- **B. Validate at `DeclareMiningJob`-time.** Rejected: at declare-time the JDC has not yet selected `min_ntime` / `nonce` placeholders; bitcoind would return `inconclusive` rather than a definitive verdict.
- **C. Validate at `handle_push_solution`-time only (status quo).** Rejected: a bad template wastes miner work across every share submitted against it. Pre-flighting eliminates the wasted window.
- **D. Route the call through the IPC server's `validate_template`.** Rejected: requires extending the cap'n proto schema to carry a full proposal block when we already hold the `BitcoindLike` handle in-process. Pure overhead.

## Decision Outcome

**Chosen: Option A — pre-flight at SCMJ-time via in-process `BitcoindLike::validate_block_proposal`, with a structural-only fallback when handles / TDP / `template_id` are absent.**

The handler runs *after* the existing structural cross-checks (so a malformed SCMJ never reaches bitcoind). Inputs to the candidate block:
- Header: `version`, `prev_hash`, `nbits` from the cached `DeclaredJob`; `min_ntime` from SCMJ; `nonce = 0`.
- Coinbase: zero-extranonce reconstructed from the cached coinbase prefix/suffix.
- Body: TDP-fetched `tx_bodies` keyed by the cached `template_id`; the reconstructed `txid` list is cross-checked against the declared `tx_short_hash_list` to guard against a TP whose tx set diverged from the JDC's snapshot.

Outcome mapping:
- `Ok(Accepted)` / `Ok(Duplicate)` → SCMJ `Success`.
- `Ok(Rejected(reason))` → SCMJ `Error(INVALID_COINBASE_TX)`; bumps `set_custom_mining_job_proposal_rejected{reason="consensus_rejected"}`. The bitcoind reason string is logged at `warn` (NOT a label — cardinality).
- `Err(BitcoindRpcError::*)` → same SCMJ error; bumps `…{reason="rpc_error"}`.
- TDP fetch failure → structural-only acceptance + `set_custom_mining_job_validation_skipped` tick. The JDC didn't misbehave; we just lost upstream context.

When `EngineHandles` are absent or the cached job has no `template_id`, the handler skips validation and accepts on structural checks alone. This preserves the existing handles-less test surface.

### Consequences

Positive:
- Misconfigured-JDC templates are rejected before the JDS advertises the custom job. No wasted miner work.
- `consensus_rejected` is the most actionable operator signal — a non-zero rate flags a JDC with a wrong payout script.
- New `set_custom_mining_job_validation_seconds` histogram surfaces bitcoind-RPC tail latency, which dominates SCMJ handler p99.
- No schema work — purely engine-internal.

Negative / accepted:
- SCMJ handler latency now includes one bitcoind round-trip (typically <10ms on a local node; worst-case minutes if bitcoind is wedged). Mitigated by the histogram + by the fact that SCMJ is not on the share-submission hot path.
- Operators must watch `validation_skipped / accepted` to confirm full-validation mode is active. Drift toward skipped silently downgrades correctness.
- TDP fetch failures are demoted to "skip + accept" rather than reject. Defensible (the JDC didn't misbehave) but means a flaky TP can mask a bad template. Tracked via `_skipped`.

### Implementation notes

- New `block::build_candidate_block` (sibling of `reconstruct_block` in `crates/sv2-p2pool-engine/src/block.rs`); same txid cross-check guards both paths.
- Three new collectors in `crates/sv2-p2pool-engine/src/metrics.rs`: `set_custom_mining_job_proposal_rejected` (`IntCounterVec` labeled by `ScmjProposalRejectReason`), `set_custom_mining_job_validation_seconds` (`Histogram`), `set_custom_mining_job_validation_skipped` (`IntCounter`).
- Unit tests use `MockBitcoind::with_proposal_outcome` for scripted Accepted / Rejected / RPC-error paths.
- E2E witness (`crates/sv2-p2pool-testenv/tests/e2e_scmj_rejects_consensus_invalid_template.rs`) wires a wiremock-backed bitcoind RPC stub that returns `"bad-cb-amount"` and asserts the counter ticks. `#[ignore]` per testenv convention.

## Links

- Trait: `vendor/p2poolv2/.../BitcoindLike::validate_block_proposal` (returns `ProposalOutcome`).
- Sibling: ADR 0010 (capnp schema hosting) — the IPC schema deliberately does NOT carry full proposal blocks, which is why this validation stays in-process.
- Counter conventions: `docs/running.md` Observability table.
