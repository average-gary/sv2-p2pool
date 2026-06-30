# 0013. Per-miner payout binding via the upstream `JobValidationEngine` extension

- Status: Accepted (testnet)
- Date: 2026-06-30
- Deciders: sv2-p2pool maintainers
- Tags: phase-3c, design, upstream, jds, payout
- Supersedes: ADR 0002 § Decision § 1 (the in-binary wrapper sketch). Reuses
  the same map shape; moves the writer from the binary into the trait
  impl.

## Context and Problem Statement

ADR 0002 proposed three options for binding each `JdToken` to a per-miner
`ScriptBuf` and accepted Option 1 (in-binary wrapper) for Phase 1, while
filing Option 4 (extend the upstream `JobValidationEngine` trait) as the
upstream follow-up.

Option 4 has now landed in our maintainer fork of sv2-apps on the
`feat/per-miner-payout-trait` branch (commit `ac25c4cf` in
`vendor/sv2-apps`, see Phase 3c Step 1 / Step 2 commits on this repo).
This ADR records the wiring and the residual scope that must close
before mainnet.

## Decision

**Per-miner payout binding ships on testnet via the upstream trait
extension. Mainnet ships behind the accounting selector landing.**

Concretely, on the engine side
(`crates/sv2-p2pool-engine/src/engine_impl.rs`):

1. `JobValidationEngine::handle_allocate_mining_job_token(&self, token,
   user_identifier, coinbase_output_max_additional_size)` defensively
   re-validates the upstream NFKC + ASCII-whitespace normalization on
   `user_identifier`, falls back to `None` on empty-after-trim.
2. Asks the engine-private `resolve_payout_script(normalized) ->
   Option<ScriptBuf>` for a per-miner script. **Today this returns
   `None` for every input** — the accounting selector that maps
   `user_identifier → ScriptBuf` is a documented follow-up. The
   `#[cfg(test)]` resolver-override hook lets unit tests drive every
   branch of the function (see Step 2 commit, 9 unit tests).
3. On `Some(script)`:
   - Duplicate-binding guard: if the same `user_identifier` is already
     bound to a different live `JdToken`, fall back to `None` (the JDC
     for the prior allocation has already received the prior `TxOut` and
     is mining against it; overwriting would silently re-credit).
   - Size-budget guard: reject scripts whose `TxOut`-serialized size
     exceeds `coinbase_output_max_additional_size` so the JDS never
     emits an oversize coinbase.
   - Insert into the forward map `token_payout: DashMap<JdToken,
     ScriptBuf>` and the inverse index `user_identifier_index:
     DashMap<String, JdToken>`.
4. `TokenPayoutEvictor` impl drains both maps when the JDS's
   `TokenManager` evicts either an allocated-but-never-active token or
   an active token (the latter keyed by the original allocated
   `JdToken` per the trait contract).

On the pool side
(`crates/sv2-p2pool-pool/src/pool.rs`):

1. `JobDeclarator::new_with_payout_evictor(engine, …,
   Some(payout_evictor))` installs the engine as both
   `Arc<dyn JobValidationEngine>` and `Arc<dyn TokenPayoutEvictor>` —
   one `Arc`, two trait-object views.
2. The pool-wide `coinbase_reward_script` from `[jds]` config remains
   the fallback when `handle_allocate_mining_job_token` returns `None`.

## Status

- **Testnet**: live. The trait extension is wired; the engine receives
  every `AllocateMiningJobToken` and runs the duplicate-binding,
  size-budget, and eviction code paths. Today every miner falls back to
  the pool-wide script because the resolver returns `None`, but the
  binding *infrastructure* is fully exercised by the unit tests (9
  tests under `engine_impl::tests`) and by the `#[ignore]`d testenv
  scenario `e2e_per_miner_binding::two_jdcs_with_distinct_user_identity_both_land_shares`
  (witness: two distinct `user_identity` labels on
  `sv2_client_shares_accepted_total`).
- **Mainnet**: gated on the **accounting selector** landing. That work
  is the resolver body — a single function
  `user_identifier → Option<ScriptBuf>` consulted by
  `resolve_payout_script`. Until it ships, every miner gets the
  pool-wide `coinbase_reward_script` and no entries are written to
  `token_payout` in production.

## Consequences

Positive:

- Layering matches Option 4: payout selection lives in the engine, not
  in a binary-level wrapper. No parallel `TokenManager` to keep in sync.
- The trait extension lets a future change to the selector ship without
  touching the JDS internals — it stays a private engine helper.
- The `TokenPayoutEvictor` hook means token-map eviction is driven by
  the JDS's own bookkeeping (allocated-TTL + active-TTL janitor), so
  the maps cannot grow unbounded.
- Defensive normalization + duplicate-binding + size-budget checks all
  fail closed to the pool-wide fallback — the JDS never emits an
  invalid `TxOut` because of engine bugs.

Negative / accepted:

- The `#[cfg(test)]` resolver-override is only reachable from the
  engine crate's own tests; the testenv E2E test (which spawns the
  built binary) can't toggle distinct scripts today. That's why the
  E2E assertion stops at "both user_identity labels land shares" — the
  per-token coinbase-script distinctness will be assertable once the
  accounting selector lands (or a feature-gated production stub is
  added, currently out of scope).
- `user_identifier_index` evictions walk the map with `DashMap::retain`
  on every drop — O(n) over a small map, but if the pool ever runs
  thousands of concurrent miners a reverse pointer will be needed
  (documented inline in `engine_impl.rs`).
- The duplicate-binding guard is keyed by `user_identifier` alone, not
  `(user_identifier, downstream_id)`. If two downstreams legitimately
  share an identity (mobile + desktop with the same wallet), the
  second one falls back to pool-wide.

## Follow-ups

1. **Accounting selector** — implement
   `P2poolV2Engine::resolve_payout_script` against
   `p2poolv2_lib::accounting::payout_selection`. This is the only
   remaining production blocker for the per-miner binding (and the
   stated mainnet gate).
2. **Per-channel coinbase-script witness** — once (1) ships, extend
   `e2e_per_miner_binding` to scrape the per-channel coinbase
   `script_pubkey` (likely via the upstream MonitoringServer's
   per-channel snapshot) and assert distinct bytes per miner.
3. **Reverse pointer in `user_identifier_index`** — replace the
   `DashMap::retain` scan in `drop_token_payout` with an
   `Arc<DashMap<JdToken, String>>` so eviction is O(1). Defer until
   the pool hits multi-thousand concurrent miners.

## Links

- ADR 0002 (`docs/adr/0002-jdtoken-payout-script.md`) — the original
  framing and follow-up commitment that this ADR closes (Option 4).
- Vendored trait extension: `vendor/sv2-apps` commit `ac25c4cf` on
  branch `feat/per-miner-payout-trait` — adds
  `JobDeclarator::new_with_payout_evictor` and the
  `TokenPayoutEvictor` trait.
- Engine impl: `crates/sv2-p2pool-engine/src/engine_impl.rs`
  (`handle_allocate_mining_job_token` + `impl TokenPayoutEvictor for
  P2poolV2Engine`), commit `f4161f6` on this repo.
- E2E witness: `crates/sv2-p2pool-testenv/tests/e2e_per_miner_binding.rs`.
- Pool wiring: `crates/sv2-p2pool-pool/src/pool.rs` —
  `JobDeclarator::new_with_payout_evictor` call site.
