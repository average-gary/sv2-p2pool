# ADR 0004: Coinbase-only declaration handling

- Status: Proposed
- Date: 2026-05-26
- Closes: #4

## Context

SV2's Job Declaration Protocol (spec 06) defines two declaration modes:

1. **Full-Template** — JDC sends `DeclareMiningJob` with a populated `wtxid_list`.
2. **Coinbase-only** — `wtxid_list` is empty; the pool sees only the coinbase. A privacy mode the spec explicitly supports.

p2poolv2's share-chain consensus (`p2poolv2_lib::shares::validation`) is GBT-style: every share carries a full `Vec<ShareTransaction>` and the validator recomputes the bitcoin merkle root and BIP141 witness commitment from that list. An SV2 `JobValidationEngine` backed by p2poolv2 therefore cannot, in coinbase-only mode, perform the validation that share-chain admission requires.

## Confirmation that full wtxids are required

Verified against `vendor/p2poolv2/p2poolv2_lib/src/shares/`:

- `share_block/mod.rs:301-312` — `ShareBlock { transactions: Vec<ShareTransaction>, ... }`. The full transaction set is part of the share, not a wtxid digest.
- `validation/mod.rs:241-256` (`validate_merkle_root`) — recomputes the merkle root from `share.transactions`.
- `validation/mod.rs:541-584` (`validate_share_witness_commitment`) — recomputes the BIP141 witness root from non-coinbase share transactions.
- `validation/mod.rs:282-310` (`validate_scripts_values_and_sigops`) — runs `bitcoinconsensus::verify` on every non-coinbase tx.
- `validation/mod.rs:799-828` (`validate_share_block`) — composes all of the above; no path admits a share without full transactions.

Conclusion: p2poolv2 cannot validate a share-chain block from `(coinbase + empty wtxid list)`. Full-Template is the only mode that maps onto the existing validator.

## Decision drivers

- **Consensus correctness.** A share that cannot be reconstructed cannot be admitted to the share-chain.
- **Spec compatibility.** Rejecting a spec-defined mode is a known concession; per spec 06's honesty-incentive section, a JDC can switch pools.
- **Privacy posture.** p2poolv2 already gossips every share publicly, so the privacy gain from coinbase-only would be partial at best.
- **Implementation complexity.** A "synthesize wtxids from local mempool" path adds a synchronous mempool dependency p2poolv2 does not currently have.

## Considered options

### Option 1 — Reject all coinbase-only declarations with `INVALID_COINBASE_TX`

Map empty `wtxid_list` to `ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX`.

Pros: trivial; one branch in `handle_declare_mining_job`; preserves consensus invariants.
Cons: code is semantically wrong — the coinbase itself is fine; what is invalid is the *job shape*. No spec-defined `UNSUPPORTED_DECLARATION_MODE` exists today.

### Option 2 — Accept by reconstructing wtxids from p2poolv2's local mempool

Pros: matches spec semantics; preserves a privacy mode.
Cons: p2poolv2 has no synchronized mempool; significant engineering; latency on every `DeclareMiningJob`; race between miner's mempool view and ours produces nondeterministic rejections; introduces silent-mismatch bugs. Out of scope for Phase 1.

### Option 3 — Hybrid: accept coinbase-only iff template ≡ p2poolv2's current template

If JDC's `prev_hash` and time window match p2poolv2's own freshly-built template, fill in the known wtxid set.

Pros: cheap when it works.
Cons: works only in a narrow convergence window; silently degrades to Option 1 the rest of the time; conditional behavior is harder to reason about than a flat reject.

### Option 4 — Defer with a clear "unsupported" signal

Reject coinbase-only declarations using the closest existing error code, document the limitation, and file an upstream spec/sv2-apps issue proposing a dedicated `UNSUPPORTED_DECLARATION_MODE` code. Cut over when it lands.

Pros: honest signal to JDC; clean upgrade path; concrete motivation for the upstream issue.
Cons: uses an imperfect existing code in the interim.

## Decision

**Option 4.**

In Phase 1, `P2poolV2Engine::handle_declare_mining_job` rejects any `DeclareMiningJob` whose effective wtxid set (after `ProvideMissingTransactionsSuccess` resolution) is empty. The engine returns `ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX` — the closest existing code in `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:32-37` — accompanied by a structured log line `coinbase-only declarations not supported by p2poolv2 share-chain` so JDC operators can diagnose without reading our source.

In parallel we open an upstream sv2-spec/sv2-apps issue proposing `ERROR_CODE_DECLARE_MINING_JOB_UNSUPPORTED_DECLARATION_MODE`. When that lands we cut over.

Option 2 is the only path that genuinely supports coinbase-only and is filed as a follow-up tied to a future "p2poolv2 mempool subsystem" workstream. Option 3 is rejected as too clever for the consensus-correctness invariant it must uphold.

## Consequences

- **Positive.** Share-chain validator stays simple; no new "validate without full block" path; no synchronization races. Implementation is one early-return branch.
- **Negative.** SV2 JDCs that default to coinbase-only must be configured for Full-Template when pointing at a p2poolv2-backed JDS. We must document this prominently.
- **Follow-ups.**
  1. Upstream issue proposing a dedicated error code.
  2. Engine README and `JobValidationEngine` rustdoc note the restriction.
  3. Re-evaluate when/if p2poolv2 gains a mempool subsystem.

## Citations

- Wiki: `~/wiki/topics/sv2-p2pool-integration/wiki/topics/share-accounting-mapping.md` — open question §6.
- Wiki: `~/wiki/topics/sv2-p2pool-integration/raw/papers/2026-05-22-sv2-spec-job-declaration-protocol.md:19-21` — coinbase-only vs Full-Template.
- `vendor/p2poolv2/p2poolv2_lib/src/shares/share_block/mod.rs:301-312` — full `Vec<ShareTransaction>`.
- `vendor/p2poolv2/p2poolv2_lib/src/shares/validation/mod.rs:541-584` — witness commitment requires full bodies.
- `vendor/p2poolv2/p2poolv2_lib/src/shares/validation/mod.rs:799-828` — `validate_share_block`.
- `vendor/sv2-apps/pool-apps/jd-server/src/lib/job_declarator/job_validation/bitcoin_core_ipc.rs:425-637` — reference `handle_declare_mining_job`.
- `vendor/sv2-apps/.../bitcoin_core_ipc.rs:32-37` — current `DeclareMiningJob` error codes.
