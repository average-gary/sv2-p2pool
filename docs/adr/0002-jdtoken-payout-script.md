# ADR 0002: JdToken to payout-script binding

- Status: Proposed
- Date: 2026-05-26
- Closes: #2

## Context

The upstream `JobValidationEngine` trait explicitly does not handle `AllocateMiningJobToken` (`vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/mod.rs:26`). Allocation is performed by `JobDeclarationMessageHandler::handle_allocate_mining_job_token` in `vendor/sv2-apps/.../job_declaration_message_handler.rs:44-99`, which assembles a single `TxOut { value: 0, script_pubkey: self.coinbase_reward_script.script_pubkey() }` per allocation — that is, a **single pool-wide payout script** plumbed in from `pool-apps/jd-server/src/lib/config.rs:40` and held in `JobDeclarator` at `mod.rs:115`. There is currently no per-token state inside the validation engine beyond what the `BitcoinCoreIPCEngine` reference impl caches itself.

For p2poolv2, every miner publishes its own `coinbase_reward_script`. Each `JdToken` therefore needs to be bound to a **different** `ScriptBuf` so that `handle_push_solution` (and `handle_declare_mining_job`'s coinbase-output validation) can credit the correct p2pool miner.

Token lifetimes are short. `ALLOCATED_TOKEN_TIMEOUT_SECS` is nominally 10 minutes (currently inflated to 24h via `TEMPORARY_TIMEOUT_MULTIPLIER`, sv2-apps#335) and `ACTIVE_TOKEN_TIMEOUT_SECS = 10s` (`vendor/sv2-apps/.../job_declarator/mod.rs:50-53`). On JDS restart, all in-flight tokens are already invalid because the JDC must re-establish a Noise session and re-allocate.

## Considered options

### Option 1 — In-memory `Arc<DashMap<JdToken, ScriptBuf>>` in our pool binary

A sibling map next to the existing `TokenManager.allocated_tokens` / `active_tokens` (`vendor/sv2-apps/.../token_management/mod.rs:51-52`), populated at `AllocateMiningJobToken` time and looked up from `P2poolV2Engine` via an `Arc` clone. Janitor-driven eviction reuses `ALLOCATED_TOKEN_TIMEOUT_SECS`.

- Pros: matches the existing `DashMap`-everywhere style; zero new storage layers; eviction story already solved by the janitor; bound naturally tracks token TTL.
- Cons: lost on restart — but tokens are already invalidated by Noise teardown, so this is not a real loss; cross-process JDS scaling would need a different design (out of scope).

### Option 2 — Persist in p2poolv2's RocksDB store

Write `(JdToken → ScriptBuf)` into p2poolv2's existing `store/` rocksdb.

- Pros: survives restart.
- Cons: couples the JDS engine to p2poolv2's storage layout for state with a 10-minute upper-bound TTL; rocksdb writes on every allocation are wasteful for ephemeral data; restart durability is moot because tokens die with the Noise session anyway.

### Option 3 — Separate sled/sqlite store inside the pool binary

Engine-private durable map.

- Pros: durable without coupling to p2poolv2.
- Cons: same TTL/restart argument as Option 2; an extra moving part, schema, and migration story for state that fits trivially in memory.

### Option 4 — Extend the upstream `JobValidationEngine` trait

Add `handle_allocate_mining_job_token(&self, token, downstream_id) -> ScriptBuf` so the engine drives the binding. Cleanest layering, no parallel map.

- Pros: payout selection lives where it belongs; no need to reach into JDS internals from our binary.
- Cons: requires an upstream PR to sv2-apps and a sv2-spec discussion (the trait was deliberately scoped to exclude `AllocateMiningJobToken`). Phase 1 cannot block on upstream cycles. A constraint of this work is "do NOT push to upstream sv2-apps."

## Decision

**Option 1 for Phase 1; file Option 4 as the upstream follow-up.**

Concretely:

1. In our pool binary (not a vendor patch), construct `JobDeclarator` with a thin wrapper that holds an additional `Arc<DashMap<JdToken, ScriptBuf>>` named `token_payout`. The wrapper intercepts `handle_allocate_mining_job_token` to (a) call `token_manager.allocate(client_id)`, (b) ask p2poolv2's `accounting::payout_selection` for the per-miner script, (c) `token_payout.insert(token, script)`, then (d) emit `AllocateMiningJobTokenSuccess` using that script in place of the pool-wide `coinbase_reward_script`.
2. `P2poolV2Engine` receives the same `Arc<DashMap<JdToken, ScriptBuf>>` at construction time. `handle_declare_mining_job` looks up `token_payout.get(&allocated_token)` to validate that the declared coinbase output's `script_pubkey` matches; `handle_push_solution` looks it up to credit the right share/miner in `accounting`.
3. Eviction: when `TokenManager::deallocate`, `deactivate`, or the janitor expires a token, mirror the removal in `token_payout`. The simplest implementation reuses the existing janitor by passing `token_payout` into `spawn_janitor_task` and calling `token_payout.remove(...)` whenever a token is evicted from `allocated_tokens` or `active_tokens`. `allocated_from_active` (`token_management/mod.rs:139-150`) gives the original `JdToken` for active-token lookups.
4. Open an upstream issue in sv2-apps proposing the trait extension in Option 4 with this ADR as motivation. Cut over when it lands.

## Consequences

- **Positive.** Phase 1 unblocked with no upstream-PR dependency; one new `DashMap`; eviction reuses an existing janitor; engine layering matches the rest of the codebase.
- **Negative.** Two parallel maps keyed by `JdToken` (token-state vs payout-script) must stay in sync — mitigated by collocating their mutations in the wrapper. JDS scaling beyond a single process needs a different design (deferred).
- **Follow-ups.**
  1. Upstream sv2-apps issue: extend `JobValidationEngine` to own token allocation (Option 4). **Landed in Phase 3c — see [ADR 0013](0013-per-miner-payout-binding.md).** The trait extension and the engine wiring ship on testnet behind the accounting selector; the in-binary wrapper sketched in this ADR's Decision §1 is superseded by the engine-side `handle_allocate_mining_job_token` impl.
  2. Engine README documents the wrapper and the lifetime invariant.
  3. Reorg-revocation hook (open question §5 in the wiki article) is a separate ADR.

## Citations

- Wiki: `~/wiki/topics/sv2-p2pool-integration/wiki/topics/share-accounting-mapping.md` §3.1 ("Recommended `JobValidationEngine` skeleton") — the `token_payout: Arc<DashMap<JdToken, ScriptBuf>>` field this ADR formalizes; open question §4 on `JdToken` ↔ payout-script binding.
- `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/mod.rs:26` — trait excludes `AllocateMiningJobToken`.
- `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_declaration_message_handler.rs:44-99` — current allocation handler with single pool-wide script.
- `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/mod.rs:50-53,115` — token TTLs; pool-wide `coinbase_reward_script` field.
- `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/token_management/mod.rs:51-52,139-150,186-234` — existing `DashMap`s and janitor.
- `vendor/sv2-apps/pool-apps/jd-server/src/lib/config.rs:40` — pool-wide `coinbase_reward_script` config field.
