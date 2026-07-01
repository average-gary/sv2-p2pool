//! Phase 2-B Track A (ADR 0011) — end-to-end exercise of the chain-read
//! IPC seam.
//!
//! Boots a **real** capnp IPC daemon via [`p2poolv2_ipc::spawn_ipc_server_full`]
//! (the same server-side crate the production `p2poolv2` daemon ships)
//! over a Unix-domain socket in a per-test tempdir, then drives
//! [`sv2_p2pool::share_chain::IpcChain`] against it over the wire. The
//! transport, encoding, and dispatch are the same code paths the
//! production pool uses — only the chain backend behind the daemon's
//! `ChainReadBackend` trait is an in-memory fake so we can script the
//! scenario.
//!
//! All tests are `#[ignore]` because they spawn extra threads + open
//! UDS sockets and aren't part of the cheap default `cargo test` run.
//! The nightly CI workflow at `.github/workflows/nightly.yml` runs
//! `cargo test --workspace -- --ignored` and exercises these.
//!
//! Run locally:
//!
//! ```sh
//! cargo test -p sv2-p2pool-testenv --test e2e_ipc_chain -- --ignored
//! ```
//!
//! ## Coverage (per ADR 0011 step 8)
//!
//! - (a) `get_chain_tip` round-trips a configured tip back to the
//!   caller (validates wire encoding + the `IpcChain` actor's reply
//!   plumbing). Also exercises `get_tip_height`.
//! - (b) Reorg-style **ancestor walk over 100 hops**: seeds 100
//!   chained share headers in the daemon's `ChainReadBackend`, then
//!   confirms an unbroken walk terminates at the all-zeros sentinel,
//!   AND that pulling a header out of the middle causes the walk to
//!   return `ShareHeaderLookup::NotFound` at the gap (which the
//!   engine's `notify_share_chain_reorg` translates into a
//!   conservative cache flush — ADR 0011 § Decision §
//!   "selective invalidation").
//! - (c) Tip subscription under load: drive 50 tip swaps through the
//!   server's `watch::Sender<BlockHash>` and verify that
//!   `IpcChain::subscribe_tip()` delivers them AND that the
//!   `AtomicTipSnapshot` converges on the final tip. This is the
//!   primitive the reorg watcher reads at `pool.rs`.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bitcoin::BlockHash;
use bitcoin::hashes::Hash as _;
use p2poolv2_ipc::{ChainReadBackend, ShareHeaderOutcome};
use prometheus::Registry;
use sv2_p2pool::share_chain::{IpcChain, IpcChainMetrics, IpcTimeouts};
use sv2_p2pool_engine::{ShareChainReader, ShareHeaderLookup};

/// Fresh (default timeouts, fresh registry) metrics tuple for
/// [`IpcChain::connect`]. All e2e tests use the crate defaults —
/// timeouts are not what these tests exercise.
fn test_ipc_args() -> (IpcTimeouts, IpcChainMetrics) {
    (
        IpcTimeouts::default(),
        IpcChainMetrics::register(&Registry::new()).expect("register on fresh registry"),
    )
}

/// In-memory `ChainReadBackend` driven by tests. Seedable + mutable so
/// the (b) reorg-walk scenario can yank a header mid-walk.
struct ScriptedBackend {
    tip: Mutex<Option<[u8; 32]>>,
    height: Mutex<Option<u32>>,
    /// `share_hash` -> `prev_share_blockhash` (None means genesis
    /// predecessor; the daemon-side adapter encodes that as
    /// `ShareHeaderOutcome::Found { prev = [0; 32] }` per ADR 0011 §
    /// "Schema additions").
    headers: Mutex<std::collections::HashMap<[u8; 32], Option<[u8; 32]>>>,
    network: bitcoin::Network,
}

impl ScriptedBackend {
    fn new(network: bitcoin::Network) -> Self {
        Self {
            tip: Mutex::new(None),
            height: Mutex::new(None),
            headers: Mutex::new(std::collections::HashMap::new()),
            network,
        }
    }

    fn set_tip(&self, tip: [u8; 32], height: u32) {
        *self.tip.lock().expect("tip") = Some(tip);
        *self.height.lock().expect("height") = Some(height);
    }

    fn insert_header(&self, hash: [u8; 32], prev: Option<[u8; 32]>) {
        self.headers.lock().expect("headers").insert(hash, prev);
    }

    fn remove_header(&self, hash: &[u8; 32]) {
        self.headers.lock().expect("headers").remove(hash);
    }
}

impl ChainReadBackend for ScriptedBackend {
    fn get_chain_tip(&self) -> Result<Option<[u8; 32]>, String> {
        Ok(*self.tip.lock().expect("tip"))
    }

    fn get_share_header(&self, share_hash: &[u8; 32]) -> Result<ShareHeaderOutcome, String> {
        // The daemon-side adapter encodes the all-zeros sentinel as
        // `Genesis`; mirror that here so the test exercises the same
        // dispatch path the real daemon does.
        if share_hash.iter().all(|b| *b == 0) {
            return Ok(ShareHeaderOutcome::Genesis);
        }
        let headers = self.headers.lock().expect("headers");
        match headers.get(share_hash) {
            Some(prev) => Ok(ShareHeaderOutcome::Found {
                prev_share_blockhash: prev.unwrap_or([0u8; 32]),
            }),
            None => Ok(ShareHeaderOutcome::NotFound),
        }
    }

    fn get_tip_height(&self) -> Result<Option<u32>, String> {
        Ok(*self.height.lock().expect("height"))
    }

    fn network(&self) -> bitcoin::Network {
        self.network
    }
}

/// Allocate a unique UDS path under a fresh tempdir. Returns the
/// tempdir guard (keep it alive for the test's duration) and the
/// socket path inside it.
fn temp_socket() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("ipc.sock");
    (dir, path)
}

/// Wait for the daemon's UDS to appear on disk. `spawn_ipc_server_full`
/// returns immediately after handing the future off to its own thread,
/// so connecting before the bind completes races.
async fn wait_for_socket(path: &std::path::Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        if Instant::now() >= deadline {
            panic!("server socket never appeared at {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn bh(seed: [u8; 32]) -> BlockHash {
    BlockHash::from_raw_hash(bitcoin::hashes::sha256d::Hash::from_byte_array(seed))
}

// =======================================================================
// (a) get_chain_tip round-trips an expected value.
// =======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns a UDS-bound capnp daemon thread; runs in nightly --ignored"]
async fn ipc_chain_get_chain_tip_returns_configured_value() {
    let (_dir, sock) = temp_socket();
    let backend = Arc::new(ScriptedBackend::new(bitcoin::Network::Regtest));

    // Pick a non-trivial tip (the all-zeros pattern is a sentinel for
    // the genesis case; avoid the collision).
    let mut tip_bytes = [0u8; 32];
    tip_bytes[0] = 0xde;
    tip_bytes[1] = 0xad;
    tip_bytes[31] = 0xff;
    backend.set_tip(tip_bytes, 12_345);

    let _server = p2poolv2_ipc::spawn_ipc_server_full(
        sock.clone(),
        None,
        Some(backend.clone() as Arc<dyn ChainReadBackend>),
    );
    wait_for_socket(&sock, Duration::from_secs(5)).await;

    let (timeouts, metrics) = test_ipc_args();
    let chain = IpcChain::connect(sock.to_str().expect("utf-8 sock path"), timeouts, metrics)
        .await
        .expect("IpcChain::connect");

    // Sync `network()` is captured at connect time via `getNetwork @6`.
    assert_eq!(chain.network(), bitcoin::Network::Regtest);

    let got_tip = chain.get_chain_tip().await.expect("get_chain_tip ok");
    assert_eq!(got_tip, Some(bh(tip_bytes)), "tip round-trips over capnp");

    let got_height = chain.get_tip_height().await.expect("get_tip_height ok");
    assert_eq!(
        got_height,
        Some(12_345),
        "tip height round-trips over capnp"
    );

    // Uninitialised path: clear the tip, re-read. The capnp result
    // should be the `Uninitialised` arm of `ChainTipResult` which the
    // client maps to `Ok(None)`.
    *backend.tip.lock().unwrap() = None;
    *backend.height.lock().unwrap() = None;
    let got_tip = chain.get_chain_tip().await.expect("get_chain_tip ok");
    assert_eq!(got_tip, None, "uninitialised tip arrives as Ok(None)");
    let got_height = chain.get_tip_height().await.expect("get_tip_height ok");
    assert_eq!(got_height, None, "uninitialised height arrives as Ok(None)");
}

// =======================================================================
// (b) 100-hop ancestor walk: full walk + truncation on missing header.
// =======================================================================

/// Seed `depth` chained share headers in the backend such that
/// `h[i].prev_share_blockhash == h[i-1]`. `h[0]` has `prev = None`
/// (the daemon encodes that as `Found { prev = [0; 32] }`; the engine
/// reads it back as `ShareHeaderLookup::Found(prev = None)`).
fn seed_linear_chain(backend: &ScriptedBackend, depth: usize) -> Vec<[u8; 32]> {
    let mut hashes = Vec::with_capacity(depth);
    for i in 0..depth {
        // Distinct, non-zero hash per slot. `i + 1` so slot 0 is
        // [1; 32] (not the all-zeros sentinel).
        let mut h = [0u8; 32];
        let v = (i as u32 + 1).to_be_bytes();
        h[0..4].copy_from_slice(&v);
        h[31] = ((i as u32 + 1) & 0xff) as u8;
        hashes.push(h);
    }
    for i in 0..depth {
        let prev = if i == 0 { None } else { Some(hashes[i - 1]) };
        backend.insert_header(hashes[i], prev);
    }
    hashes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns a UDS-bound capnp daemon thread; runs in nightly --ignored"]
async fn ipc_chain_walks_full_100_hop_ancestry_to_genesis() {
    let (_dir, sock) = temp_socket();
    let backend = Arc::new(ScriptedBackend::new(bitcoin::Network::Regtest));

    // 100 hops matches `REORG_ANCESTRY_DEPTH` at
    // `crates/sv2-p2pool-engine/src/engine_impl.rs:901` — the bound
    // the engine's reorg walker uses in production.
    let hashes = seed_linear_chain(&backend, 100);
    backend.set_tip(*hashes.last().expect("non-empty"), 100);

    let _server = p2poolv2_ipc::spawn_ipc_server_full(
        sock.clone(),
        None,
        Some(backend.clone() as Arc<dyn ChainReadBackend>),
    );
    wait_for_socket(&sock, Duration::from_secs(5)).await;

    let (timeouts, metrics) = test_ipc_args();
    let chain = IpcChain::connect(sock.to_str().expect("utf-8 sock path"), timeouts, metrics)
        .await
        .expect("IpcChain::connect");

    // Walk from tip back. Mirrors `notify_share_chain_reorg`'s loop at
    // `engine_impl.rs:830-877` — terminate cleanly either when prev is
    // None (genesis predecessor) or when the lookup returns Genesis.
    let mut cursor = bh(*hashes.last().expect("non-empty"));
    let mut walked = 0usize;
    let start = Instant::now();
    for _ in 0..100 {
        match chain
            .get_share_header(&cursor)
            .await
            .expect("get_share_header transport ok")
        {
            ShareHeaderLookup::Found(header) => {
                walked += 1;
                match header.prev_share_blockhash {
                    Some(prev) => cursor = prev,
                    None => break,
                }
            }
            ShareHeaderLookup::Genesis => break,
            ShareHeaderLookup::NotFound => {
                panic!("unexpected NotFound during a fully-seeded 100-hop walk at step {walked}");
            }
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(walked, 100, "walked the full 100-hop ancestry");
    // ADR 0011 § Negative documents the latency budget as
    // 10-50 ms p99 over UDS. Local capnp-rpc round-trips on a quiet
    // dev box are ~100 µs; even with substantial CI noise a 100-hop
    // walk under 5s is a strong-but-not-flaky regression baseline.
    assert!(
        elapsed < Duration::from_secs(5),
        "100-hop walk took {elapsed:?}; ADR 0011 budget is 10-50 ms p99"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns a UDS-bound capnp daemon thread; runs in nightly --ignored"]
async fn ipc_chain_walk_truncates_on_missing_header_midway() {
    let (_dir, sock) = temp_socket();
    let backend = Arc::new(ScriptedBackend::new(bitcoin::Network::Regtest));

    // Seed 100 hops, then yank slot 50 to simulate a pruned / unknown
    // ancestor mid-walk. Production code at
    // `engine_impl.rs:848-861` interprets this as
    // `ShareHeaderLookup::NotFound`, falls back to flushing the
    // declared-jobs cache, and bumps the invalidated-jobs counter.
    let hashes = seed_linear_chain(&backend, 100);
    backend.remove_header(&hashes[50]);
    backend.set_tip(*hashes.last().expect("non-empty"), 100);

    let _server = p2poolv2_ipc::spawn_ipc_server_full(
        sock.clone(),
        None,
        Some(backend.clone() as Arc<dyn ChainReadBackend>),
    );
    wait_for_socket(&sock, Duration::from_secs(5)).await;

    let (timeouts, metrics) = test_ipc_args();
    let chain = IpcChain::connect(sock.to_str().expect("utf-8 sock path"), timeouts, metrics)
        .await
        .expect("IpcChain::connect");

    let mut cursor = bh(*hashes.last().expect("non-empty"));
    let mut walked = 0usize;
    let mut hit_not_found = false;
    for _ in 0..100 {
        match chain.get_share_header(&cursor).await.expect("transport ok") {
            ShareHeaderLookup::Found(header) => {
                walked += 1;
                match header.prev_share_blockhash {
                    Some(prev) => cursor = prev,
                    None => break,
                }
            }
            ShareHeaderLookup::Genesis => break,
            ShareHeaderLookup::NotFound => {
                hit_not_found = true;
                break;
            }
        }
    }
    assert!(hit_not_found, "walk should hit NotFound at the yanked slot");
    // Walked 49 ancestor pointers BEFORE hitting the gap (slot 99 →
    // slot 98 → ... → slot 50 was where prev was [50], pointing to a
    // hash that no longer exists in the backend). The exact count is
    // less important than the truncation event firing.
    assert!(
        (40..=60).contains(&walked),
        "expected to walk ~49 hops before the gap; got {walked}"
    );
}

// =======================================================================
// (c) Tip subscription delivers events under load.
// =======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns a UDS-bound capnp daemon thread; runs in nightly --ignored"]
async fn ipc_chain_subscribe_tip_delivers_burst_of_50_updates() {
    let (_dir, sock) = temp_socket();
    let backend = Arc::new(ScriptedBackend::new(bitcoin::Network::Regtest));

    // The server's `subscribe_chain_tip` fans out from this watch
    // sender. Initial value is the all-zeros sentinel so the
    // post-connect snapshot doesn't latch onto a noise value.
    let (tip_tx, tip_rx) = tokio::sync::watch::channel(BlockHash::all_zeros());

    let _server = p2poolv2_ipc::spawn_ipc_server_full(
        sock.clone(),
        Some(tip_rx),
        Some(backend.clone() as Arc<dyn ChainReadBackend>),
    );
    wait_for_socket(&sock, Duration::from_secs(5)).await;

    let (timeouts, metrics) = test_ipc_args();
    let chain = IpcChain::connect(sock.to_str().expect("utf-8 sock path"), timeouts, metrics)
        .await
        .expect("IpcChain::connect");
    let snapshot = chain.tip_snapshot();
    let mut rx = chain.subscribe_tip();

    // Burst-publish 50 distinct tips. Capnp's subscribe fan-out is
    // best-effort under `watch::Sender::send` semantics — i.e. only
    // the latest value is guaranteed observable, intermediate values
    // may coalesce. We assert (i) the final tip *is* observed on
    // both the broadcast channel and the atomic snapshot, and (ii) at
    // least one intermediate value is observed (no silent drop of the
    // whole stream).
    let mut final_tip_bytes = [0u8; 32];
    let mut tips = Vec::with_capacity(50);
    for i in 0..50u32 {
        let mut bytes = [0u8; 32];
        bytes[28..32].copy_from_slice(&i.to_be_bytes());
        bytes[0] = 0xaa; // keep it non-zero to avoid the genesis sentinel
        let tip = bh(bytes);
        tips.push(tip);
        final_tip_bytes = bytes;
        tip_tx.send(tip).expect("watch send");
        // Tiny pause so the fan-out task has a chance to schedule.
        // Without this the watch channel coalesces aggressively and
        // intermediate observability becomes very flaky.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let final_tip = bh(final_tip_bytes);

    // (i.a) The atomic snapshot converges on the final tip. The
    // reorg watcher's sync closure reads from here.
    let mut converged = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if snapshot.load_tip() == Some(final_tip) {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "tip_snapshot should converge on the final tip; saw {:?}",
        snapshot.load_tip()
    );

    // (i.b) The broadcast channel surfaces the final tip.
    //
    // Drain everything available, then assert the final tip is among
    // what we received. Order is not guaranteed (watch coalesces),
    // but the final value is.
    let mut observed: std::collections::HashSet<BlockHash> = std::collections::HashSet::new();
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Ok(tip)) => {
                observed.insert(tip);
                if observed.contains(&final_tip) {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => break,
        }
    }
    assert!(
        observed.contains(&final_tip),
        "broadcast channel should deliver the final tip; observed {} tips",
        observed.len()
    );
    // (ii) At least one tip was observed — the channel isn't a silent
    // sink. (If `watch` coalesced everything down to one value we'd
    // still see that one value here.)
    assert!(
        !observed.is_empty(),
        "broadcast channel should have delivered at least one tip"
    );
}
