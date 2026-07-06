//! Prometheus counters owned by the IPC client crate.
//!
//! Registered lazily against the crate-owned default `Registry`; the
//! pool binary re-uses [`registry`] to expose them from its
//! monitoring server without having to plumb a `Registry` into every
//! [`super::Sv2P2poolIpcClient::connect`] call.
//!
//! ## Why here, not in the engine crate
//!
//! The engine crate already has an `EngineMetrics` struct threaded
//! through `P2poolV2Engine`. Adding IPC-driver observability there
//! would force the engine to know when the IPC client's `RpcSystem`
//! driver exits — but engines don't own IPC clients; the pool crate
//! does, and it wires them into the `IpcChain` actor. Owning these
//! collectors here (in the crate that emits the events) keeps the
//! coupling correct.
//!
//! ## Item #4 (Phase 3 hardening)
//!
//! The `ipc_client_driver_exit_total{result}` counter is bumped by
//! [`super::Sv2P2poolIpcClient::connect`]'s spawned `RpcSystem`
//! driver whenever it terminates. `result="error"` indicates a
//! silent-disconnect condition the caller SHOULD have surfaced
//! (before this change it only produced a `warn!()` line); `result="clean"`
//! is the normal-shutdown path.

use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts, Registry};

/// Crate-owned default registry. The pool binary reads collectors
/// off this registry via [`registry`] and gathers them alongside its
/// own for the `/metrics` endpoint.
///
/// A crate-local registry (rather than requiring the caller to plumb
/// one in) keeps the `Sv2P2poolIpcClient::connect` signature free of
/// prometheus types, which is important because the client's public
/// API is the AGPL-boundary seam: keeping it typed only in
/// bitcoin/tokio/tracing/thiserror avoids leaking prometheus into
/// callers that don't want it.
static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

/// `ipc_client_driver_exit_total{result}` — bumped by the
/// capnp-rpc `RpcSystem` driver task when it exits. `result="error"`
/// means the driver observed a fatal transport error (server
/// crashed, peer closed the UDS, half-open connection detected via
/// keepalive); `result="clean"` means the client itself shut the
/// driver down (typical during pool teardown).
///
/// Registered on the crate's [`REGISTRY`] lazily so importing the
/// crate never fails on registry-full errors from downstream code.
pub static IPC_CLIENT_DRIVER_EXIT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let vec = IntCounterVec::new(
        Opts::new(
            "ipc_client_driver_exit_total",
            "Number of times the capnp-rpc RpcSystem driver task exited, labeled by result",
        ),
        &["result"],
    )
    .expect("valid IntCounterVec metadata");
    // Pre-create both label children so /metrics shows them at zero
    // from boot — dashboards don't have to special-case "label not
    // yet present" and alerting rules that key on `result="error"`
    // can be written once.
    let _ = vec.with_label_values(&["error"]);
    let _ = vec.with_label_values(&["clean"]);
    REGISTRY
        .register(Box::new(vec.clone()))
        .expect("crate-owned registry cannot fail on first register");
    vec
});

/// Access the crate-owned [`Registry`]. Callers that expose a
/// `/metrics` endpoint gather from this registry in addition to their
/// own.
///
/// Idempotent — repeated calls return the same registry. Touching
/// this from a test forces [`IPC_CLIENT_DRIVER_EXIT_TOTAL`] to be
/// registered (via `Lazy::force`) so the counter shows up in
/// `registry().gather()` even if the driver-exit path hasn't run yet.
pub fn registry() -> &'static Registry {
    Lazy::force(&IPC_CLIENT_DRIVER_EXIT_TOTAL);
    &REGISTRY
}

/// Stable label values for [`IPC_CLIENT_DRIVER_EXIT_TOTAL`].
///
/// Not an enum — kept as `&'static str` to avoid re-typing the same
/// `.as_str()` boilerplate in one call site and to make it obvious
/// at the increment sites what value shows up on `/metrics`.
pub const DRIVER_EXIT_RESULT_ERROR: &str = "error";
pub const DRIVER_EXIT_RESULT_CLEAN: &str = "clean";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_returns_stable_pointer() {
        // Two calls must return the same registry instance — the
        // Sender that owns collectors depends on this invariant to
        // avoid double-registration errors.
        let r1 = registry() as *const Registry;
        let r2 = registry() as *const Registry;
        assert_eq!(r1, r2);
    }

    #[test]
    fn driver_exit_counter_registered_at_zero_with_both_labels() {
        let names: Vec<String> = registry()
            .gather()
            .iter()
            .map(|mf| mf.get_name().to_string())
            .collect();
        assert!(
            names.contains(&"ipc_client_driver_exit_total".to_string()),
            "IPC driver-exit counter must be registered against crate registry"
        );
        // Both label children pre-created — dashboards can query them
        // without observing "label not yet present" errors. We assert
        // by pattern: `with_label_values` on a pre-registered child
        // returns the same child every time, and the counter can be
        // read (even if it's zero). The registry snapshot below
        // exposes both labels' rows in the text export.
        let text_encoded = {
            use prometheus::Encoder;
            let mut buf = Vec::new();
            let encoder = prometheus::TextEncoder::new();
            encoder
                .encode(&registry().gather(), &mut buf)
                .expect("encode ok");
            String::from_utf8(buf).expect("utf-8")
        };
        assert!(
            text_encoded.contains("result=\"error\""),
            "text export must expose the error label:\n{text_encoded}"
        );
        assert!(
            text_encoded.contains("result=\"clean\""),
            "text export must expose the clean label:\n{text_encoded}"
        );
    }
}
