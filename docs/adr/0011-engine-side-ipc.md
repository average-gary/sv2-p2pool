# 0011. Engine-side IPC migration — replace in-process `ChainStoreHandle` with a `ShareChainReader` trait + capnp `IpcChain` actor

- Status: Accepted
- Date: 2026-06-23
- Deciders: sv2-p2pool maintainers
- Tags: phase-2-b, design, ipc, capnp, agpl, engine

## Context and Problem Statement

ADR 0010 established the AGPL boundary at the p2poolv2 daemon binary by dual-licensing the capnp schema (`p2poolv2-capnp-types`, MIT/Apache-2.0) and giving us a non-AGPL `sv2-p2pool-ipc` client. Phase 2-A then shipped Phase-2-A's share-chain integration in-process — `EngineHandles.chain: ChainStoreHandle` (`crates/sv2-p2pool-engine/src/lib.rs:283` pre-Track-A), with `ChainStoreHandle` constructed from `p2poolv2_lib` inside the same process (`crates/sv2-p2pool-pool/src/share_chain.rs:109` pre-Track-A). That left the AGPL-licensed `p2poolv2_lib` linked into both the engine crate AND the pool binary, contradicting the boundary ADR 0010 set.

The remaining engineering question for Phase 2-B was **how** to drop `p2poolv2_lib` from the engine crate without a multi-release migration: the call-site audit eventually surfaced five live reads through `EngineHandles.chain` (two in `engine_impl.rs`, two in `pool.rs`, one in `share_chain.rs`); the production reorg watcher takes a sync `Fn() -> Option<BlockHash>` so a purely-async trait would not compose with it; and the capnp `Sv2P2poolIpcClient` is `!Send` while the pool runtime is multi-threaded and uses `tokio::spawn` (Send-required).

## Decision Drivers

- **Single AGPL boundary** at the pool crate (per ADR 0010). The engine crate must not transitively link `p2poolv2_lib`.
- **All five live chain-reads** covered by one trait surface — no parallel code paths.
- **Sync reorg-watcher signature preserved** so `Engine::start_reorg_watcher`'s public API doesn't churn.
- **No `async-trait`** in the engine's hot path. The 100-hop reorg ancestor walk should not pay `Box::pin`-per-call macro overhead unnecessarily.
- **No feature-flag rollout, no parallel `InProcessChain` build alongside `IpcChain` in shipped releases.** The AGPL goal forbids keeping the in-process path alive in production indefinitely.
- **Discriminated wire errors**: the engine's reorg walker distinguishes "genesis reached", "header not found", and "transport error". A bare `getShareHeader -> (header)` collapses all three into `capnp::Error` — that semantic must survive on the wire.

## Considered Options

- **A. Defer engine-side IPC; keep `ChainStoreHandle` in `EngineHandles`** until the daemon ships a chain-read schema. *Rejected*: the AGPL-clean target promised by ADR 0010 stays blocked indefinitely, and Track B's upcoming `validate_block_proposal` work needs a clean trait seam to slot into. The schema is still pre-1.0 (placeholder file ID at `p2poolv2.capnp:8-11`) so additions are cheap right now.
- **B. Make the chain-reader trait sync, call `block_on` inside the engine to drive IPC.** *Rejected*: `block_on` inside an async runtime is a real anti-pattern and would deadlock on the multi-threaded pool runtime when called from inside a Tokio worker.
- **C. Make the chain-reader trait async and propagate `!Send` from the capnp client up through the engine.** *Rejected*: it cascades `!Send` into `EngineHandles`, then into `start_reorg_watcher`'s `Fn() -> Option<BlockHash> + Send + 'static`, then into every `tokio::spawn` site in `pool.rs`. The fan-out is unbounded.
- **D. Async trait + a `Send`-safe actor wrapper for the `!Send` capnp client; feed the sync reorg watcher from an atomic snapshot updated by the actor's `subscribe_chain_tip` task.** *Chosen.*

## Decision Outcome

**Chosen: Option D — `ShareChainReader` async trait + `IpcChain` actor wrapping a `!Send` `Sv2P2poolIpcClient` on a dedicated `LocalSet` thread, with a lock-free `AtomicTipSnapshot` feeding the sync reorg watcher.**

### Trait surface

`pub trait ShareChainReader: Send + Sync` lives at `crates/sv2-p2pool-engine/src/share_chain_reader.rs`. Five methods cover the full audit of in-process reads:

```rust
trait ShareChainReader: Send + Sync {
    fn get_chain_tip(&self) -> BoxFuture<'_, Result<Option<BlockHash>, IpcClientError>>;
    fn get_share_header(&self, share_hash: &BlockHash)
        -> BoxFuture<'_, Result<ShareHeaderLookup, IpcClientError>>;
    fn get_tip_height(&self) -> BoxFuture<'_, Result<Option<u32>, IpcClientError>>;
    fn network(&self) -> bitcoin::Network;                          // sync
    fn subscribe_tip(&self) -> broadcast::Receiver<BlockHash>;      // sync
}
```

`BoxFuture<'_, T>` is `Pin<Box<dyn Future<Output = T> + Send + '_>>` — hand-written so the trait is dyn-compatible without pulling in the `async-trait` macro crate. `ShareHeaderLookup { Found(ShareHeaderRead), NotFound, Genesis }` is a Rust enum mapped 1:1 from a capnp discriminated union; the engine's reorg walker matches on the three variants directly rather than guessing from `capnp::Error`. `network()` is sync because the value is captured once at construction via the daemon's `getNetwork @6`; `subscribe_tip()` is sync because it just hands out a fresh `broadcast::Receiver` cloned from a `Sender` retained on the impl. The other three are async because the production `IpcChain` impl awaits UDS round-trips.

`EngineHandles.chain` becomes `Arc<dyn ShareChainReader>`. The engine has no compile-time knowledge of the backend. The single error type across the seam is `sv2_p2pool_ipc::IpcClientError` — no separate `ShareChainError`.

### Schema additions

Four method numbers + four structs added to `p2poolv2.capnp` (companion PR upstream, fallback documented if upstream review stalls):

```capnp
interface ShareChain {
  validateTemplate     @0 ...;   # pre-existing
  submitSolution       @1 ...;   # pre-existing
  subscribeChainTip    @2 ...;   # pre-existing
  getChainTip          @3 () -> (result :ChainTipResult);
  getShareHeader       @4 (shareHash :Data) -> (result :ShareHeaderResult);
  getTipHeight         @5 () -> (result :TipHeightResult);
  getNetwork           @6 () -> (network :Network);
}
struct ChainTipResult     { union { tip @0 :Data; uninitialised @1 :Void; } }
struct TipHeightResult    { union { height @0 :UInt32; uninitialised @1 :Void; } }
struct ShareHeaderResult  { union { found @0 :ShareHeaderRead; notFound @1 :Void; genesis @2 :Void; } }
struct ShareHeaderRead    { prevShareBlockhash @0 :Data; }   # 32 bytes; the only field the engine reads
enum   Network            { mainnet @0; testnet @1; testnet4 @2; signet @3; regtest @4; }
```

`ShareHeaderRead` deliberately carries only `prev_share_blockhash`. The remaining fields on `p2poolv2_lib::ShareHeader` (uncles, miner_bitcoin_address, merkle_root, bitcoin_header, bits, time, donation/fee, coinbase_value, coinbaseaux_flags, witness_commitment, bitcoin_height, coinbase_nsecs, extranonce) are not on the wire — one field on the wire matches one field consumed by the engine. A comment in the schema lists what's intentionally absent so a future contributor doesn't widen the surface without thinking.

### Backends — two, not three

- `sv2_p2pool::share_chain::IpcChain` (production): owns the `!Send` capnp client on a dedicated `std::thread` running a current-thread tokio runtime + `LocalSet`. Outside callers communicate over a bounded `mpsc::Sender<IpcRequest>` with `oneshot` reply channels (`REQUEST_CHANNEL_CAPACITY = 256` = `2 × REORG_ANCESTRY_DEPTH(=128)` so a worst-case 100-hop reorg ancestry walk never blocks). A subscribe-tip task on the same `LocalSet` fans the daemon's `subscribeChainTip @2` callback into both a lock-free `AtomicTipSnapshot` (read by the reorg watcher's sync closure) and a `broadcast::Sender<BlockHash>` (read by `subscribe_tip()`). A detached watchdog thread joins the actor and flips a `watch::Sender<bool>` so the pool binary can drive a clean shutdown if the actor dies. Each request `spawn_local`s its capnp round-trip so the request loop keeps pumping during a long ancestor walk.
- `sv2_p2pool::share_chain::InProcessChain` (single-process dev + tests): wraps a real `ChainStoreHandle`. Mirrors `IpcChain`'s error and sentinel semantics (all-zeros share-hash → `Genesis`; `NotFound` from the store → `NotFound`; other `StoreError`s → `IpcClientError::Capnp(..)` so the trait's error type is uniform across backends).

Both live in the pool crate (the AGPL boundary). The engine crate's `Cargo.toml` does not list `p2poolv2_lib` as a dependency — verified by `cargo tree -p sv2-p2pool-engine` not containing `p2poolv2_lib`.

A `#[cfg(test)] MockShareChain` lives next to the trait for engine-internal unit tests (replaces the previous `setup_test_chain_store_handle` fixture; provides `with_no_genesis()` to preserve the `Ok(None)` test intent at the original genesis-uninitialised call site).

### Reorg watcher + tip-height publisher migration

`Engine::start_reorg_watcher` keeps its sync `Fn() -> Option<BlockHash> + Send + 'static` signature for API stability. Production wiring at `pool.rs` switches from a polling closure to one that reads the `IpcChain` actor's `AtomicTipSnapshot`: `move || tip_snapshot.load_tip()` — lock-free, no per-tick UDS round-trip, satisfies `Fn() -> Option<BlockHash> + Send`. The tip-height publisher similarly reads from `snapshot.load_height()` rather than awaiting `get_tip_height()` per tick.

## Consequences

### Positive

- AGPL boundary collapses to the pool crate as ADR 0010 promised; the engine crate becomes AGPL-clean immediately (`cargo tree -p sv2-p2pool-engine | grep p2poolv2_lib` is empty).
- Push-driven tip distribution (one capnp subscription, broadcast fan-out + atomic snapshot) is strictly cheaper than per-tick `get_chain_tip()` + `get_tip_height()` polling on the watcher / publisher hot paths.
- `ShareChainReader` is a clean injection point for Track B's upcoming `validate_block_proposal` call.
- Native `BoxFuture` returns avoid `async-trait` macro overhead on the 100-hop reorg ancestor walk.
- The discriminated-union schema lets the engine distinguish "genesis reached", "header not found", and "transport error" without overloading `capnp::Error`.

### Negative

- The reorg ancestor walk at `engine_impl.rs:830-877` becomes up to 100 sequential async UDS round-trips. Realistic capnp-rpc-over-UDS latency is 100-500 µs/call, so worst-case ~10-50 ms p99 per reorg. Acceptable for a path that fires at most once per ~minute. If profiling shows >50 ms p99, the documented escape hatch is a server-side `walkAncestry` helper.
- Adding new chain reads now requires a schema bump + submodule bump + IPC client edit + IpcChain actor edit instead of one in-process method. Accepted as the explicit cost of the AGPL boundary.
- The actor-wrapper pattern adds one dedicated thread to the pool process. A detached watchdog propagates actor panic to a `watch::Sender<bool>` so a dead chain connection isn't silently swallowed; the pool binary's shutdown logic monitors this signal.
- Sync `network()` returns the value captured at construction; if the daemon ever supports network hot-swap (unlikely), this would lie until reconnect. Documented in the trait doc-comment.

## Risks

- **Schema method numbers `@3`-`@6` are permanently reserved.** Mitigation: keep `ShareHeaderRead` minimal (one field); use discriminated unions for results so adding new outcome variants stays forward-compatible.
- **Upstream coordination of the schema diff.** Mitigation: fallback path — carry the diff as a vendored patch on a tracking branch in `vendor/p2poolv2` if upstream review exceeds 2 weeks, rebase when upstream lands.
- **Actor wrapper correctness.** Mitigations: bounded mpsc with capacity = `2 × REORG_ANCESTRY_DEPTH` so a full reorg walk never blocks; explicit panic-propagation watchdog flipping `watch::Sender<bool>`; integration test for shutdown signal (covered in `share_chain.rs` and `e2e_ipc_chain.rs`).
- **Tip-snapshot freshness.** The lock-free `AtomicTipSnapshot` is updated by the subscription task; if that task falls behind, the watcher sees stale tips. Mitigation: bounded broadcast capacity (64), single-flight height-refresh task, integration test for subscription-lag path.

## Implementation Notes

Landed across three commits on `phase-2b/track-a` and surfaced through one PR:

1. `feat(ipc): chain-read methods on Sv2P2poolIpcClient` — capnp schema additions, `Sv2P2poolIpcClient::{get_chain_tip, get_share_header, get_tip_height, get_network}`, and the `ShareHeaderLookup` / `ChainTipResult` / `TipHeightResult` enums.
2. `feat(engine): ShareChainReader trait + EngineHandles swap` — trait, `EngineHandles.chain: Arc<dyn ShareChainReader>` migration, all engine call-sites moved to async, `MockShareChain` test backend, removal of `p2poolv2_lib` from the engine crate's `Cargo.toml`.
3. `feat(pool): IpcChain actor + AGPL-clean engine` — `IpcChain` actor wrapping the `!Send` capnp client on a dedicated `LocalSet` thread, `AtomicTipSnapshot` for the sync reorg watcher, watchdog-driven shutdown signal, `bootstrap_share_chain` picking IPC vs. in-process based on `p2pool_config.ipc.socket_path`.
4. `tests(testenv): e2e_ipc_chain — boot real capnp daemon + drive IpcChain` — the integration test under `crates/sv2-p2pool-testenv/tests/e2e_ipc_chain.rs` covers `get_chain_tip`, a 100-hop ancestor walk including mid-walk truncation, and tip subscription under a 50-update burst.

## Links

- ADR 0010 — Cap'n Proto schema crate hosting (the upstream framing this ADR completes).
- Schema source-of-truth: `vendor/p2poolv2/p2poolv2-capnp-types/proto/p2poolv2.capnp`.
- Trait + backends: `crates/sv2-p2pool-engine/src/share_chain_reader.rs`, `crates/sv2-p2pool-pool/src/share_chain.rs`.
- End-to-end test: `crates/sv2-p2pool-testenv/tests/e2e_ipc_chain.rs`.
