# 0014 — Accounting Payout Resolver Wakeup

- Status: Accepted
- Date: 2026-07-01
- Related: [0002 — JDToken → payout script][adr-0002],
  [0013 — Per-miner payout binding][adr-0013]
- Supersedes/Replaces: nothing
- Depends on (soft): Tier 1 #3 (deactivate-then-validate)

[adr-0002]: 0002-jdtoken-payout-script.md
[adr-0013]: 0013-per-miner-payout-binding.md

## Context

Since Phase 3-c the engine has carried the machinery for per-miner
payout scripts (`TokenPayoutMap`, `UserIdentifierIndex`,
`handle_allocate_mining_job_token`, `TokenPayoutEvictor` — see ADRs
0002 + 0013). But the actual per-user script lookup —
`P2poolV2Engine::resolve_payout_script` — has stayed a stub that
returns `None` for every input. The only way to feed it a live
value was a `#[cfg(test)]` closure hook
(`set_test_payout_resolver`), so unit tests exercised the surrounding
branches (duplicate binding, size budget) while production
deliberately fell back to the pool-wide `coinbase_reward_script` on
every allocation.

The Phase 3 follow-up landed a `[jds] deactivate-then-validate`
reorder (Tier 1 #3), tightened SCMJ proposal handling
(Tier 2 items #4-8), extended reorg observability (Tier 4 #11-12),
and generally made the accounting scaffolding "load-bearing". The
resolver stub was flagged in that pass as Tier 5 #13 — the last
step before the accounting selector can actually credit different
miners with different coinbases.

## Decision

Wake up the resolver by shipping a **narrow synchronous trait**,
threaded through `PoolBuilder` → `Pool` → `P2poolV2Engine`, driven
by an additive **`[payout.static]` TOML section** in the pool
config, observed via a **monotonic Prometheus counter**, and
protected by a **DashMap TOCTOU tightening** that co-ships in the
same PR.

### Trait shape

```rust
pub trait PayoutScriptResolver: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn resolve(&self, user_identifier: &str) -> Option<ScriptBuf>;
}
```

- **No `Debug` bound.** Payout scripts credit real Bitcoin payouts;
  a derive-Debug on future implementors would leak them into log
  aggregators. Static-string discrimination via `name()` covers the
  operator-visibility need at the Pool-startup INFO log (a single
  boot-time entry — no cardinality risk).
- **Sync-only contract.** The caller
  (`handle_allocate_mining_job_token`) holds no yield point across
  the call; blocking here stalls the JDS Tokio worker. Enforced
  observably via the
  `sv2_p2pool_engine_payout_resolver_resolve_duration_seconds`
  histogram — a future implementor reaching for `block_on(reqwest::get)`
  is visible as a long tail rather than a silent throughput
  regression. Implementors that need external data MUST maintain an
  in-memory cache refreshed out-of-band (background task swapping
  an `ArcSwap` view is the canonical pattern).

### Stock implementations

- **`NullResolver`.** Returns `None` for every input.
  `name() == "null"`. Default installed by `P2poolV2Engine::new`.
  Preserves byte-for-byte the pool-wide-fallback semantics
  deployments have today.
- **`StaticMapResolver`.** Table-driven from an in-memory
  `DashMap<String, ScriptBuf>`. Keys are stored normalised
  (`trim → nfkc`), matching what `handle_allocate_mining_job_token`
  does to inbound `user_identifier` values. Constructor rejects
  empty user_identifiers and any two raw keys that collide after
  normalisation (error carries both raw keys so operators can spot
  the offending config lines). `name() == "static-map"`.

### `[payout.static]` TOML surface

```toml
[[payout.static.entries]]
user_identifier = "miner-alice"
script_hex      = "0014ab..."

[[payout.static.entries]]
user_identifier = "miner-bob"
script_hex      = "0014cd..."
```

Deployments that omit the section entirely get the `NullResolver`
default. The section is parsed by `sv2-p2pool-pool::payout_config`
from the SAME TOML file the upstream `pool_sv2::config::PoolConfig`
is parsed from — additive, no fork of the upstream type. Validation
errors on invalid hex, empty user_identifiers, and post-normalisation
duplicate keys surface at binary boot time.

This TOML surface is the first PRODUCTION caller of the trait, which
is what earns the trait's keep against a bare `Fn` closure: the
constant `[payout.static]` shape gives the trait an observable
public contract via config files, not just an internal seam.

### Wiring path

```
main.rs
  → parse [payout.static]
  → payout_config::build_resolver → Arc<dyn PayoutScriptResolver>
  → PoolBuilder::with_payout_resolver(resolver)
  → PoolBuilder::build_pool_with_p2pool_config
      → Pool { payout_resolver: Some(resolver), ... }
  → Pool::start
      → let resolver_for_engine = self.resolver_for_start();  // NullResolver default
      → engine_concrete = engine_concrete.with_payout_resolver(resolver_for_engine);
      → info!(resolver = resolver.name(), "installing payout-script resolver on engine");
```

The critical wiring fix lives in `Pool::start`. Prior revision
constructed a fresh `PoolBuilder::new(self.config_network())`
inside `Pool::start` to build the engine — a resolver installed on
the outer builder did NOT persist through that inner
`PoolBuilder::new`. The fix: park the resolver on the `Pool`
struct itself and apply it directly on `engine_concrete` in
`Pool::start`, bypassing the inner builder entirely. Locked by
the `pool_start_applies_resolver_from_pool_field` unit test.

### DashMap TOCTOU tightening (co-ship)

`handle_allocate_mining_job_token` (engine_impl.rs) previously did:

```
if user_identifier_index.get(uid).is_some_and(different_token) { return None; }
// ...
token_payout.insert(token, script);         // write #1
user_identifier_index.insert(uid, token);   // write #2
```

Under two concurrent JDCs allocating for the same normalised
`user_identifier`, this could:

- Install an orphan inverse-index entry pointing at a `token_payout`
  binding another writer overwrote, OR
- Race the `get`-then-insert check and overwrite a live binding.

The race was harmless while the resolver was hardcoded to `None`
(the whole path early-returned before touching the maps). Now that
a real `Some`-returning resolver ships, the race becomes reachable,
so the fix co-ships:

```
user_identifier_index
    .entry(normalised.clone())
    .or_insert_with(|| {
        token_payout.insert(token, script.clone());   // atomic with the reservation
        is_new_binding = true;
        token
    });
```

Ordering: reserve the inverse-index slot first (via `entry(uid)
.or_insert_with`). If the slot is already held by a different
token, fall back — do NOT touch `token_payout`. If the slot is
freshly claimed, register the forward binding inside the
`or_insert_with` closure — under DashMap's per-entry lock, so no
observer can see a partial state. Locked by the
`payout_resolver_toctou_no_orphan_under_contention` engine unit
test which fires 64 concurrent allocations for the same user and
asserts final consistency.

### Metrics witness

Two new collectors on the engine's Prometheus registry:

- **`sv2_p2pool_engine_payout_binding_installed_total{user_identifier}`**
  — `IntCounterVec`, unbounded label space, children lazy-created
  on first insertion. Bumped exactly once per NEW binding
  (`is_new_binding == true` inside the `entry().or_insert_with`
  closure); collisions on the duplicate-binding guard do NOT
  double-increment. Monotonic and survives
  `TokenPayoutEvictor::on_active_evicted → drop_token_payout`
  eviction.
- **`sv2_p2pool_engine_payout_resolver_resolve_duration_seconds`**
  — `Histogram`. Every `resolve_payout_script` call inside
  `handle_allocate_mining_job_token` gets a `start_timer` /
  `observe_duration` wrapping. Makes the sync-only contract
  observable.

### E2E witness (metrics-based, TOML-driven)

`crates/sv2-p2pool-testenv/tests/e2e_per_miner_binding.rs` runs the
two-JDC topology (mujina → translator → jdc → sv2-p2pool → bitcoind
regtest) with a `[payout.static]` TOML block written into the
generated `pool.toml` at spawn time. `Sv2P2poolDBuilder`'s new
`with_payout_static_map(HashMap<String, ScriptBuf>)` preserves the
`Command::new` subprocess model — the resolver stands up inside
the spawned pool from TOML, not injected in-process.

After the existing user_identity share-label preflight passes, the
test scrapes the engine's `/metrics` endpoint and asserts:

```
sv2_p2pool_engine_payout_binding_installed_total{user_identifier="miner-alice"} >= 1
sv2_p2pool_engine_payout_binding_installed_total{user_identifier="miner-bob"}   >= 1
```

Since the counter increments exactly once per binding install AND
the resolver maps each user to a distinct 22-byte P2WPKH-shaped
script, a nonzero counter for each user proves the resolver was
consulted per-user with the correct script.

**Why not a `#[doc(hidden)] pub fn payout_bindings_snapshot()`
accessor?** `token_payout` entries are transient — every
successful SCMJ evicts via `TokenPayoutEvictor::on_active_evicted →
drop_token_payout` (Defect 2 in the Phase 3 audit). A state-snapshot
witness races against eviction: the base rate is racy, not a CI
timing flake. The monotonic counter is durable — that's why it
replaces the accessor rather than augmenting it.

A nightly-only `#[ignore]`d bonus test (`e2e_per_miner_binding_block_coinbase_nightly`,
currently a stub) is reserved for the stronger end-to-end
witness: capture a submitted `bitcoin::Block` and assert
`coinbase output[0].script_pubkey` matches the resolver's script
for the finder. Mirrors the `e2e_ipc_chain` nightly-only pattern.

### Tier 1 #3 dependency framing (soft, not hard)

Tier 1 #3 reorders `TokenManager::deactivate` to run AFTER
validation but keeps it unconditional. Successful SCMJs still evict
via `on_active_evicted → drop_token_payout`, so `token_payout`
entries remain transient. What Tier 1 #3 buys us here is defensive:
the resolver's binding is readable to the validator during
validation, and a validation-rejected SCMJ still cleans up the
token deterministically (no leak beyond the pre-existing 10s TTL).
This ADR's monotonic-counter witness does NOT depend on
`token_payout` being live at scrape time — it depends only on the
increment having fired once per binding install, which is durable.

So the dependency downgrades from HARD to SOFT: both should land,
but this ADR is not blocked on Tier 1 #3 given the counter-based
witness.

## Consequences

### Positive

- Per-miner payout binding is finally consulted in production, not
  just tests.
- Operators get a first-class TOML surface (`[payout.static]`) with
  compile-time-agnostic validation (invalid hex / duplicate keys
  fail at binary boot, not at runtime).
- The DashMap TOCTOU race — latent for as long as the resolver was
  stubbed — is closed atomically with the resolver's wakeup.
- Two new metrics make the trait's contract observable:
  `payout_binding_installed_total{user_identifier}` for E2E
  witness, `payout_resolver_resolve_duration_seconds` for
  sync-contract enforcement.
- No production breakage: deployments that omit `[payout.static]`
  get the `NullResolver` default, which returns the same
  byte-for-byte pool-wide-fallback semantics as today.

### Negative

- `payout_binding_installed_total`'s label space is unbounded (one
  child per miner). Operators with very large miner sets should
  aggregate on the dashboard rather than sum-by-label. Documented
  in-crate on the counter's doc comment.
- The trait carries a `Send + Sync + 'static` bound (needed for the
  `Arc<dyn PayoutScriptResolver>` storage). Simple `Fn` closures
  work in tests via a `#[cfg(test)]` blanket impl; a production
  implementor that wants captured non-`'static` state has to
  own it in an `Arc`.

### Rejected alternatives

- **Cross-crate re-exports.** `sv2-p2pool-engine` and
  `sv2-p2pool-pool` both need the trait in scope. The engine crate
  is the natural home; the pool crate uses `sv2_p2pool_engine::PayoutScriptResolver`
  via re-export from the engine crate root. Testenv already depends
  on the engine crate transitively, so no explicit re-export chain
  is needed.
- **Named `FnResolver<F>` shim.** Would let test sites pass raw
  closures without an `Arc::new` at the call site. Not worth the
  named type: a `#[cfg(test)]` blanket impl over `Fn(&str) ->
  Option<ScriptBuf>` covers the 4 existing test sites with one
  extra `std::sync::Arc::new(...)` per site.
- **`#[doc(hidden)] pub fn payout_bindings_snapshot()` accessor.**
  Ruled out on eviction-race grounds (see above); replaced by the
  monotonic counter.
- **In-process pool entry point + test-hooks Cargo feature.** Would
  let tests skip the subprocess spawn and drive the resolver
  directly. Redundant now that the TOML surface exists — the E2E
  test writes the same `[payout.static]` block a production
  operator would.

## Implementation

See files:

- `crates/sv2-p2pool-engine/src/payout_resolver.rs` — trait +
  stock implementations.
- `crates/sv2-p2pool-engine/src/lib.rs` — `payout_resolver` field
  on `P2poolV2Engine`, `with_payout_resolver` builder,
  `resolve_payout_script` delegating to the trait through the
  histogram.
- `crates/sv2-p2pool-engine/src/engine_impl.rs` — TOCTOU
  tightening + `payout_binding_installed_total` increment.
- `crates/sv2-p2pool-engine/src/metrics.rs` — both new
  collectors.
- `crates/sv2-p2pool-pool/src/payout_config.rs` — TOML
  deserialisation for `[payout.static]`.
- `crates/sv2-p2pool-pool/src/builder.rs` — resolver threaded
  through every `build_pool*` path.
- `crates/sv2-p2pool-pool/src/pool.rs` — `payout_resolver` field
  on `Pool`, applied directly on `engine_concrete` in `Pool::start`.
- `crates/sv2-p2pool-pool/src/main.rs` — `[payout]` parse + wire
  into `PoolBuilder`.
- `crates/sv2-p2pool-testenv/src/sv2_p2pool_d.rs` —
  `with_payout_static_map` writes the TOML block at spawn time.
- `crates/sv2-p2pool-testenv/tests/e2e_per_miner_binding.rs` —
  primary metrics-based witness + nightly-only block-level stub.
- `config/pool.example.toml` — commented `[payout.static]`
  example block.
