//! Per-miner payout-script resolver — the pluggable seam that lets the
//! engine's `handle_allocate_mining_job_token` bind a custom coinbase
//! output for each miner instead of falling back to the pool-wide
//! `coinbase_reward_script`.
//!
//! # Design
//!
//! [`PayoutScriptResolver`] is the trait callers implement. It is
//! deliberately narrow — a single sync `resolve(&str) -> Option<ScriptBuf>`
//! — because the caller (the JDS Tokio worker inside
//! `handle_allocate_mining_job_token`) holds no yield point across the
//! call. Blocking here stalls the worker; implementors that need to
//! consult external state MUST maintain an in-memory cache refreshed
//! out-of-band. The sync-only contract is observable via the
//! `sv2_p2pool_engine_payout_resolver_resolve_duration_seconds`
//! histogram — a future implementor reaching for `block_on(reqwest::get)`
//! is visible in metrics rather than a silent throughput regression.
//!
//! Two stock implementations ship in-crate:
//!
//! - [`NullResolver`] — returns `None` for every input. Default used
//!   when the operator omits the `[payout.static]` config section.
//!   Preserves today's byte-for-byte pool-wide-fallback semantics.
//! - [`StaticMapResolver`] — table-driven from an in-memory
//!   `DashMap<String, ScriptBuf>`. Keys are stored in the same
//!   normalised form (`trim → nfkc`) that the engine's
//!   `handle_allocate_mining_job_token` uses. Fed by the
//!   `[payout.static]` TOML section (see the pool crate's
//!   `payout_config` module).
//!
//! Under `#[cfg(test)]` a blanket impl lets test sites pass raw
//! `Fn(&str) -> Option<ScriptBuf>` closures wrapped in `Arc::new`
//! without needing a named shim.

use std::collections::HashMap;

use bitcoin::ScriptBuf;
use dashmap::DashMap;
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

/// Normalise a raw `user_identifier` for use as a resolver key.
///
/// Mirrors the trim → NFKC dance the engine's
/// `handle_allocate_mining_job_token` performs on inbound
/// `DeclareMiningJob.user_identifier` values so keys match byte-for-byte
/// across the trait boundary.
pub fn normalize_user_identifier(user_identifier: &str) -> String {
    user_identifier
        .trim_matches(|c: char| c.is_ascii_whitespace())
        .nfkc()
        .collect()
}

/// Pluggable per-miner payout-script resolver.
///
/// Implementors receive an already-normalised `user_identifier` (the
/// engine trims + NFKC-folds before calling), so simple table lookups
/// don't need to renormalise. Callers that build a resolver from raw
/// input SHOULD renormalise defensively — [`StaticMapResolver::resolve`]
/// does.
///
/// # Sync-only contract
///
/// `resolve` MUST NOT perform I/O. It is called on the JDS Tokio
/// worker inside `handle_allocate_mining_job_token` with no yield
/// point around it — a blocking implementation stalls the worker.
/// Implementors requiring external data MUST maintain an in-memory
/// cache refreshed out-of-band (e.g. a background task swapping an
/// `ArcSwap` view). The engine records call latency in the
/// `sv2_p2pool_engine_payout_resolver_resolve_duration_seconds`
/// histogram so operators can detect regressions.
pub trait PayoutScriptResolver: Send + Sync + 'static {
    /// Short, static discriminant used for INFO logs at Pool startup.
    /// No cardinality risk (a single log line at boot), so a `&'static
    /// str` is fine. Example values: `"null"`, `"static-map"`.
    fn name(&self) -> &'static str;

    /// Resolve a payout script for a normalised `user_identifier`.
    /// `None` means "no per-miner binding for this user — fall back to
    /// the pool-wide `coinbase_reward_script`".
    fn resolve(&self, user_identifier: &str) -> Option<ScriptBuf>;
}

/// Null resolver: returns `None` for every input.
///
/// Preserves today's byte-for-byte pool-wide-fallback semantics on
/// production deployments that omit the `[payout.static]` config
/// section.
pub struct NullResolver;

impl PayoutScriptResolver for NullResolver {
    fn name(&self) -> &'static str {
        "null"
    }

    fn resolve(&self, _user_identifier: &str) -> Option<ScriptBuf> {
        None
    }
}

/// Errors returned by [`StaticMapResolver::new`].
#[derive(Debug, Error)]
pub enum StaticMapResolverError {
    /// A raw `user_identifier` was empty (or became empty after
    /// `trim → nfkc`). The engine's `handle_allocate_mining_job_token`
    /// rejects the same shape, so it would never match anyway.
    #[error("payout.static entry: empty user_identifier after trim+nfkc")]
    EmptyUserIdentifier,
    /// Two raw keys normalise to the same value. Silently keeping one
    /// would surprise operators; error out with both raw keys instead.
    #[error(
        "payout.static entry: duplicate user_identifier after trim+nfkc — {first:?} and {second:?} both normalise to {normalized:?}"
    )]
    DuplicateNormalizedKey {
        first: String,
        second: String,
        normalized: String,
    },
}

/// Table-driven [`PayoutScriptResolver`] backed by a `DashMap`.
///
/// Keys are stored in normalised form (`trim → nfkc`) matching the
/// engine's `handle_allocate_mining_job_token`. Constructor rejects
/// empty and duplicate-normalised keys so misconfiguration is caught
/// at boot rather than silently mapping two raw IDs to a single script.
///
/// The `Debug` impl only reports entry-count — payout scripts are
/// sensitive data (they credit real Bitcoin payouts) so a
/// derive-Debug that dumps the table would leak them into logs.
pub struct StaticMapResolver {
    table: DashMap<String, ScriptBuf>,
}

impl std::fmt::Debug for StaticMapResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticMapResolver")
            .field("entries", &self.table.len())
            .finish_non_exhaustive()
    }
}

impl StaticMapResolver {
    /// Build a resolver from a set of raw `(user_identifier,
    /// script_pubkey)` pairs.
    ///
    /// Keys are normalised with `trim → nfkc`. Duplicate normalised
    /// keys produce a [`StaticMapResolverError::DuplicateNormalizedKey`]
    /// error carrying both raw keys — the operator's config file has
    /// two entries that collide.
    pub fn new<I, K>(entries: I) -> Result<Self, StaticMapResolverError>
    where
        I: IntoIterator<Item = (K, ScriptBuf)>,
        K: Into<String>,
    {
        let table = DashMap::new();
        // Track raw keys per normalised form to build a helpful error
        // message on collision. `HashMap` (not `DashMap`) is fine — this
        // is single-threaded constructor code.
        let mut raw_by_normalized: HashMap<String, String> = HashMap::new();
        for (raw_key, script) in entries {
            let raw: String = raw_key.into();
            let normalized = normalize_user_identifier(&raw);
            if normalized.is_empty() {
                return Err(StaticMapResolverError::EmptyUserIdentifier);
            }
            if let Some(first) = raw_by_normalized.get(&normalized) {
                return Err(StaticMapResolverError::DuplicateNormalizedKey {
                    first: first.clone(),
                    second: raw,
                    normalized,
                });
            }
            raw_by_normalized.insert(normalized.clone(), raw);
            table.insert(normalized, script);
        }
        Ok(Self { table })
    }

    /// Current entry count. Cheap; exposed for tests + operator debug.
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// `true` iff the underlying table is empty.
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }
}

impl PayoutScriptResolver for StaticMapResolver {
    fn name(&self) -> &'static str {
        "static-map"
    }

    fn resolve(&self, user_identifier: &str) -> Option<ScriptBuf> {
        // Defensively renormalise: the engine calls with an
        // already-normalised value in production, but callers reaching
        // in from tests may not.
        let normalized = normalize_user_identifier(user_identifier);
        self.table.get(&normalized).map(|e| e.value().clone())
    }
}

/// Test-only blanket impl so unit tests can `Arc::new(closure)` a bare
/// `Fn(&str) -> Option<ScriptBuf>` without a named shim.
///
/// Kept `#[cfg(test)]` because a production impl for arbitrary `Fn`
/// closures would collide with implementors that also happen to be
/// callable — narrow the surface to what tests actually need.
#[cfg(test)]
impl<F> PayoutScriptResolver for F
where
    F: Fn(&str) -> Option<ScriptBuf> + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "test-closure"
    }

    fn resolve(&self, user_identifier: &str) -> Option<ScriptBuf> {
        self(user_identifier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_script(tag: u8) -> ScriptBuf {
        ScriptBuf::from_bytes(vec![tag; 22])
    }

    #[test]
    fn payout_resolver_null_returns_none() {
        let null = NullResolver;
        assert_eq!(null.name(), "null");
        assert!(null.resolve("miner-1").is_none());
        assert!(null.resolve("").is_none());
        assert!(null.resolve("\t  miner-1  \n").is_none());
    }

    #[test]
    fn static_map_resolver_normalises_keys() {
        // Constructor is fed a raw key with a NFKC-fold-able character
        // (ligature ﬁ U+FB01 → "fi") and leading/trailing whitespace.
        // The resolver must look the entry up with either the raw or
        // the already-normalised form.
        let script = dummy_script(1);
        let raw_key = "  \tﬁnal\n";
        let resolver =
            StaticMapResolver::new([(raw_key.to_string(), script.clone())]).expect("build");

        assert_eq!(resolver.name(), "static-map");
        assert_eq!(resolver.len(), 1);

        // Look up with the *raw* form — resolver renormalises input.
        assert_eq!(resolver.resolve(raw_key), Some(script.clone()));
        // Look up with the already-normalised form.
        assert_eq!(resolver.resolve("final"), Some(script.clone()));
        // Unrelated key: None.
        assert!(resolver.resolve("miner-other").is_none());
    }

    #[test]
    fn static_map_resolver_rejects_duplicate_normalised_keys() {
        let script_a = dummy_script(1);
        let script_b = dummy_script(2);
        let err = StaticMapResolver::new([
            ("  miner-alice ".to_string(), script_a),
            ("miner-alice".to_string(), script_b),
        ])
        .expect_err("duplicate normalised keys must error");

        match err {
            StaticMapResolverError::DuplicateNormalizedKey {
                first,
                second,
                normalized,
            } => {
                assert_eq!(first, "  miner-alice ");
                assert_eq!(second, "miner-alice");
                assert_eq!(normalized, "miner-alice");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn static_map_resolver_rejects_empty_user_identifier() {
        let err = StaticMapResolver::new([("   \t \n".to_string(), dummy_script(3))])
            .expect_err("empty-after-normalize must error");
        assert!(matches!(err, StaticMapResolverError::EmptyUserIdentifier));
    }

    #[test]
    fn cfg_test_closure_blanket_impl_satisfies_trait() {
        // The blanket impl exists so tests can wrap a raw closure via
        // Arc::new. Just prove the shape works.
        let closure = |uid: &str| -> Option<ScriptBuf> {
            if uid == "miner-1" {
                Some(dummy_script(9))
            } else {
                None
            }
        };
        // Explicit trait-object cast forces the impl to be picked up.
        let resolver: std::sync::Arc<dyn PayoutScriptResolver> = std::sync::Arc::new(closure);
        assert_eq!(resolver.name(), "test-closure");
        assert!(resolver.resolve("miner-1").is_some());
        assert!(resolver.resolve("miner-2").is_none());
    }
}
