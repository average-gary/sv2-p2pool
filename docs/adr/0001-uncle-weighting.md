# 1. Uncle weighting for SubmitSharesSuccess.new_shares_sum

- Status: Proposed
- Date: 2026-05-26

## Context

SV2's `SubmitSharesSuccess` carries a flat scalar `new_shares_sum` plus
`new_submits_accepted_count`. The wire format makes no distinction between
share classes — every accepted share folds into the same accumulator.

p2poolv2's share-chain is a *chain with uncles*: a share that loses the
longest-share-chain race may still be admitted as an uncle and credited
toward payout. The integration spec flags this as a model gap:

> p2poolv2's chain-with-uncles means a share that fails the *longest-chain*
> test may still be admissible as an **uncle**. SV2 has no
> "accepted-as-uncle" code.
> — `~/wiki/topics/sv2-p2pool-integration/wiki/topics/share-accounting-mapping.md`,
> §"Critical note: uncles"

The `JobValidationEngine` trait
(`vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/mod.rs:29-53`)
returns `DeclareMiningJobResult { Success | Error | MissingTransactions }` —
no per-class share count. Any aggregation must fit the existing
`SubmitSharesSuccess` counters exposed by the monitoring layer at
`vendor/sv2-apps/stratum-apps/src/monitoring/snapshot_cache.rs:45-74`
as `sv2_*_shares_*_total`.

## Decision Drivers

- Miners MUST NOT lose payout credit for shares admitted as uncles
  (p2poolv2's central value proposition vs. SV1 pools).
- MUST stay backwards-compatible with the unmodified
  `SubmitSharesSuccess` wire schema and the sv2-apps monitoring contract.
- SHOULD avoid forcing an upstream-PR dependency on sv2-apps for the
  Phase-1 milestone.
- SHOULD remain auditable: a downstream observer reading the SV2 stream
  alone should be able to reconstruct miner credit without a side channel.
- Trait-API stability: ad-hoc trait extensions ripple through every
  `JobValidationEngine` impl (today only `BitcoinCoreIPCEngine`).

## Considered Options

### Option A — Flat aggregation, uncle = full main-chain credit (α = 1)

Treat any share admitted by `shares::validation::validate(...)` — main or
uncle — as a single increment to `new_submits_accepted_count` and add its
full difficulty to `new_shares_sum`. No `stale-share` rejection for shares
admitted as uncles.

- **Pros**: zero wire-format change; trait return type unchanged; trivially
  monotonic; downstream observers see exactly what the share-chain admits;
  matches the spec recommendation verbatim.
- **Cons**: collapses information — observers cannot distinguish "block of
  100 uncles" from "block of 100 main-chain shares" without out-of-band
  data; mildly overpays uncles relative to a strict longest-chain payout
  rule.

### Option B — Weighted aggregation, uncle = α × main-chain (α from share-chain rule)

Apply a coefficient `α ∈ (0, 1]` (or possibly > 1 by future rule) per uncle
admission, sourced from p2poolv2's share-chain consensus rule. `new_shares_sum
+= α · share_diff` for uncles, full weight for main-chain.

- **Pros**: faithful to whatever payout weighting the share-chain itself
  enforces; preserves a single scalar per the SV2 wire schema.
- **Cons**: p2poolv2's TLA+ spec at `spec/ShareChain.tla` formalises uncle
  *organisation* but does NOT pin down a payout weight — the constant α is
  unspecified upstream; cementing one in our backend would either fork the
  payout rule or hard-code an assumption we cannot defend; same observability
  loss as A; makes the `JobValidationEngine` impl carry policy that morally
  belongs to `accounting/`.

### Option C — Richer trait return: extend `JobValidationEngine` with per-class counts

Add a sibling result path carrying `{main_count, main_diff_sum,
uncle_count, uncle_diff_sum}` and let `Sv2MiningServer` decide how to fold
those into the wire scalar.

- **Pros**: lossless; observability preserved upstream; lets the server
  evolve policy without re-touching backends.
- **Cons**: requires an upstream sv2-apps PR — the trait is defined there
  (`mod.rs:29`) and shared with `BitcoinCoreIPCEngine`; out of scope for
  Phase 1, which is explicitly local-only.

## Decision Outcome

**Chosen: Option A — flat aggregation, uncle counted at full credit, no
`stale-share` for uncle-admitted shares.**

Rationale: this is the minimal mapping that preserves miner credit (driver
1), is wire-compatible (driver 2), and ships without an upstream sv2-apps
PR (driver 3). The integration spec explicitly recommends it
(`share-accounting-mapping.md` §"Critical note: uncles"). Option B requires
a policy constant the upstream share-chain spec does not provide. Option C
is the right *long-term* shape but is upstream-PR-bound and therefore
ineligible for Phase 1; we capture it as a follow-up so the door stays
open.

## Consequences

**Positive**

- `P2poolV2Engine` can be implemented entirely against the existing
  `JobValidationEngine` trait surface (`mod.rs:29-53`), unblocking the
  Phase-1 skeleton in `share-accounting-mapping.md` §"Recommended
  `JobValidationEngine` skeleton".
- Monitoring contract holds: every uncle admission increments
  `sv2_*_shares_accepted_total` exactly like a main-chain share
  (`snapshot_cache.rs:45-74` series remain populated).

**Negative**

- The SV2 stream loses the main/uncle distinction. Operators who need it
  must read p2poolv2's own metrics surface (separate scrape).
- If the share-chain ever adopts an explicit uncle weight α < 1, this ADR
  must be revisited and miners will see a one-time accounting shift.

**Follow-ups**

- Implement `P2poolV2Engine::handle_*` per the spec skeleton; suppress the
  `stale-share` SV2 rejection path when `shares::chain` admits as uncle.
- Open a tracking issue for Option C (upstream `JobValidationEngine`
  per-class return) once Phase-1 lands; keep this ADR's "Status" as
  `Proposed` until the engine implementation is merged, then promote to
  `Accepted`.
- Open-question (3) in `share-accounting-mapping.md` is hereby resolved by
  this ADR; the wiki entry should be updated on next compile.
