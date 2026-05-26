# 1. Uncle weighting for SubmitSharesSuccess.new_shares_sum

- Status: Proposed
- Date: 2026-05-26
- Supersedes: —
- Superseded-by: —

## Context

SV2's `SubmitSharesSuccess` carries a flat scalar `new_shares_sum` plus
`new_submits_accepted_count`. The wire format makes no distinction between
share classes — every accepted share folds into one accumulator.

p2poolv2's share-chain is a *chain with uncles*: a share that loses the
longest-share-chain race may still be admitted as an uncle and credited
toward payout. The spec flags this gap:

> p2poolv2's chain-with-uncles means a share that fails the *longest-chain*
> test may still be admissible as an **uncle**. SV2 has no
> "accepted-as-uncle" code.
> — `~/wiki/topics/sv2-p2pool-integration/wiki/topics/share-accounting-mapping.md`,
> §"Critical note: uncles"

The `JobValidationEngine` trait
(`vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/mod.rs:29-53`)
returns `DeclareMiningJobResult { Success | Error | MissingTransactions }`
— no per-class share count. Any aggregation must fit the existing
`SubmitSharesSuccess` counters in
`vendor/sv2-apps/stratum-apps/src/monitoring/snapshot_cache.rs`
(`sv2_*_shares_accepted_total` handling, ~lines 278+ / 394+).

## Decision Drivers

- Miners MUST NOT lose payout credit for uncle-admitted shares
  (p2poolv2's value proposition vs. SV1 pools).
- MUST stay wire-compatible with `SubmitSharesSuccess` and the sv2-apps
  monitoring contract.
- SHOULD avoid an upstream sv2-apps PR for Phase 1.
- SHOULD remain auditable from the SV2 stream alone.
- Trait-API stability: extensions ripple through every
  `JobValidationEngine` impl (today only `BitcoinCoreIPCEngine`).

## Considered Options

### Option A — Flat aggregation, uncle = full main-chain credit (α = 1)

Any share admitted by `shares::validation::validate(...)` — main or
uncle — increments `new_submits_accepted_count` and adds its full
difficulty to `new_shares_sum`. No `stale-share` for uncle admissions.

- **Pros**: zero wire change; trait return unchanged; trivially monotonic;
  matches the spec recommendation verbatim.
- **Cons**: collapses information — observers cannot distinguish uncles
  from main-chain shares without out-of-band data; mildly overpays uncles
  vs. a strict longest-chain rule.

### Option B — Weighted aggregation, uncle = α × main-chain (α from share-chain rule)

Apply `α ∈ (0, 1]` per uncle admission, sourced from p2poolv2's share-chain
consensus rule.

- **Pros**: faithful to whatever weighting the share-chain enforces;
  single scalar preserved.
- **Cons**: p2poolv2's `spec/ShareChain.tla` formalises uncle
  *organisation* but does NOT pin α — cementing a value in our backend
  forks payout policy or hard-codes an undefendable assumption; same
  observability loss as A; pushes accounting policy into
  `JobValidationEngine`.

### Option C — Richer trait return: extend `JobValidationEngine` with per-class counts

Add a result path carrying `{main_count, main_diff_sum, uncle_count,
uncle_diff_sum}`; let `Sv2MiningServer` fold them into the wire scalar.

- **Pros**: lossless; observability preserved upstream; server evolves
  policy without re-touching backends.
- **Cons**: requires an upstream sv2-apps PR — the trait is shared with
  `BitcoinCoreIPCEngine`, which has no notion of "uncle"; teaching every
  impl a p2pool-specific class leaks pool-payout policy into a generic
  job-validation surface and bloats the trait for all future backends.
  Also out of scope for Phase-1, local-only.

### Option D — Option A plus a per-class internal Prometheus counter

Option A's wire behaviour, plus a backend-local
`p2pool_uncle_shares_total{kind="main"|"uncle"}` counter (and matching
diff-sum) from `P2poolV2Engine`, scraped alongside `sv2_*_shares_accepted_total`.

- **Pros**: recovers the main/uncle distinction Option A collapses, with
  no upstream PR or wire change; strictly dominates "do nothing about
  observability"; one labelled counter to implement.
- **Cons**: a second metrics namespace operators must learn; the
  SV2-only auditor still cannot reconstruct the split (driver 4 only
  partially met); risk of drift if future paths increment one surface
  but not the other.

## Decision Outcome

**Chosen: Option A — α = 1, no `stale-share` for uncle admissions.**
Option D is recommended as a non-blocking follow-up.

**Reconciling α = 1 with the wiki's "uncle-weighted":** the wiki's
normative requirement (share-accounting-mapping.md §"Critical note") is
the non-emission of `stale-share` for uncle admissions; "uncle-weighted"
asserts uncles carry *a* weight, not that α < 1. Since
`spec/ShareChain.tla` does not pin α (Option B), α = 1 is the unique
choice consistent with the wiki *and* with the absence of an
upstream-specified weight. If `ShareChain.tla` later pins α, revisit.

Rationale: minimal mapping that preserves miner credit (driver 1),
wire-compatible (driver 2), no upstream PR (driver 3). Option B needs a
constant the spec does not provide. Option C is the right long-term shape
but upstream-PR-bound. Option D is additive — tracked as a follow-up.

## Consequences

**Positive**

- `P2poolV2Engine` implementable against the existing `JobValidationEngine`
  surface (`mod.rs:29-53`), unblocking the Phase-1 skeleton.
- Monitoring contract holds: every uncle admission increments
  `sv2_*_shares_accepted_total` like a main-chain share.

**Negative**

- The SV2 stream loses the main/uncle distinction; operators needing it
  scrape p2poolv2's own surface — Option D formalises this.
- If the share-chain later pins α < 1, miners see a one-time accounting
  shift.

**Follow-ups**

- Implement `P2poolV2Engine::handle_*`; suppress `stale-share` when
  `shares::chain` admits as uncle.
- Adopt Option D's `p2pool_uncle_shares_total` counter (non-blocking).
- Track Option C upstream once Phase-1 lands; promote Status to
  `Accepted` when the engine merges.
- Resolves [share-accounting-mapping.md §"Open questions" item 3](../../../wiki/topics/sv2-p2pool-integration/wiki/topics/share-accounting-mapping.md#open-questions);
  wiki updates on next compile.
