//! Optional `[payout]` TOML config section for the sv2-p2pool binary.
//!
//! Layered ADDITIVELY on top of the upstream `pool_sv2::config::PoolConfig`
//! — the upstream type is unmodified. When the operator's TOML omits the
//! section entirely, [`PayoutConfig::None`] is returned and the pool
//! defaults to the [`sv2_p2pool_engine::NullResolver`], preserving
//! byte-for-byte pool-wide-fallback semantics.
//!
//! ## Shape
//!
//! ```toml
//! [payout.static]
//! entries = [
//!   { user_identifier = "miner-alice", script_hex = "0014ab..." },
//!   { user_identifier = "miner-bob",   script_hex = "0014cd..." },
//! ]
//! ```
//!
//! Validation rejects invalid hex, empty `user_identifier`, and any two
//! keys that normalise (trim + NFKC) to the same value.
//!
//! ## Why a separate config module
//!
//! The upstream `PoolConfig` lives in `vendor/sv2-apps` and is shared
//! across every downstream pool binary. Adding fields there would fork
//! the upstream type. Instead we parse the same TOML file twice: once
//! with `pool_sv2::config::PoolConfig::deserialize` (upstream fields,
//! `[payout]` ignored), once with [`RawPayoutSection::from_toml_file`]
//! (only `[payout]`, everything else ignored). Both parses succeed on
//! either config shape.

use std::path::Path;
use std::sync::Arc;

use bitcoin::ScriptBuf;
use serde::Deserialize;
use sv2_p2pool_engine::{
    NullResolver, PayoutScriptResolver, StaticMapResolver, StaticMapResolverError,
};
use thiserror::Error;

/// Errors from parsing / building the payout config.
#[derive(Debug, Error)]
pub enum PayoutConfigError {
    /// The config file could not be read from disk.
    #[error("failed to read payout config from {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    /// The TOML parse itself failed (before any semantic checks).
    #[error("failed to parse payout config: {0}")]
    Toml(#[from] toml::de::Error),
    /// An entry carried an empty `user_identifier` (before trim/NFKC).
    #[error("payout.static entry has empty user_identifier")]
    EmptyUserIdentifier,
    /// An entry's `script_hex` was not valid hex or produced empty bytes.
    #[error("payout.static entry {user_identifier:?} has invalid script_hex: {reason}")]
    InvalidHex {
        user_identifier: String,
        reason: String,
    },
    /// Two entries collide after `trim → nfkc`.
    #[error(transparent)]
    Resolver(#[from] StaticMapResolverError),
}

/// Raw `[payout]` TOML section.
///
/// Only exists for deserialisation; the caller normally goes straight
/// to [`build_resolver`] which returns a ready-to-install
/// `Arc<dyn PayoutScriptResolver>`.
#[derive(Debug, Default, Deserialize)]
pub struct RawPayoutSection {
    #[serde(default)]
    pub payout: Option<RawPayoutTable>,
}

#[derive(Debug, Default, Deserialize)]
pub struct RawPayoutTable {
    #[serde(default)]
    pub static_: Option<RawPayoutStatic>,
    // serde's rename lets us keep the field name `static` in TOML while
    // avoiding the Rust keyword collision.
    #[serde(rename = "static", default)]
    pub static_rename: Option<RawPayoutStatic>,
}

#[derive(Debug, Default, Deserialize)]
pub struct RawPayoutStatic {
    #[serde(default)]
    pub entries: Vec<RawPayoutEntry>,
}

/// One `[[payout.static.entries]]` row.
#[derive(Debug, Deserialize)]
pub struct RawPayoutEntry {
    pub user_identifier: String,
    pub script_hex: String,
}

impl RawPayoutSection {
    /// Read the given TOML file and extract just the `[payout]`
    /// section. Every non-payout key is ignored, so the same file the
    /// upstream `PoolConfig::deserialize` consumed is safe to pass
    /// here.
    pub fn from_toml_file(path: &Path) -> Result<Self, PayoutConfigError> {
        let bytes = std::fs::read(path).map_err(|e| PayoutConfigError::Read {
            path: path.display().to_string(),
            source: e,
        })?;
        let s = String::from_utf8_lossy(&bytes);
        Ok(toml::from_str::<Self>(&s)?)
    }

    /// Parse a TOML string directly (useful for tests + programmatic
    /// callers).
    pub fn from_toml_str(s: &str) -> Result<Self, PayoutConfigError> {
        Ok(toml::from_str::<Self>(s)?)
    }

    /// Extract the `[payout.static]` sub-table, tolerating either
    /// underscore-suffixed or bare `static` field names.
    pub fn payout_static(&self) -> Option<&RawPayoutStatic> {
        self.payout
            .as_ref()
            .and_then(|t| t.static_rename.as_ref().or(t.static_.as_ref()))
    }
}

/// Build a resolver from a parsed `[payout]` section.
///
/// Returns `Ok(Arc::new(NullResolver))` when the section is absent OR
/// present-but-empty. Returns `Ok(Arc::new(StaticMapResolver { ... }))`
/// otherwise. Any per-entry validation error surfaces as
/// [`PayoutConfigError`].
pub fn build_resolver(
    section: &RawPayoutSection,
) -> Result<Arc<dyn PayoutScriptResolver>, PayoutConfigError> {
    let Some(static_table) = section.payout_static() else {
        return Ok(Arc::new(NullResolver));
    };
    if static_table.entries.is_empty() {
        return Ok(Arc::new(NullResolver));
    }
    let mut pairs = Vec::with_capacity(static_table.entries.len());
    for entry in &static_table.entries {
        if entry.user_identifier.trim().is_empty() {
            return Err(PayoutConfigError::EmptyUserIdentifier);
        }
        let hex_str = entry.script_hex.trim();
        // `hex_string_to_bytes` here — use bitcoin::hex parsing.
        let bytes = decode_hex(hex_str).map_err(|e| PayoutConfigError::InvalidHex {
            user_identifier: entry.user_identifier.clone(),
            reason: e,
        })?;
        if bytes.is_empty() {
            return Err(PayoutConfigError::InvalidHex {
                user_identifier: entry.user_identifier.clone(),
                reason: "empty script".to_string(),
            });
        }
        let script = ScriptBuf::from_bytes(bytes);
        pairs.push((entry.user_identifier.clone(), script));
    }
    let resolver = StaticMapResolver::new(pairs)?;
    Ok(Arc::new(resolver))
}

/// Minimal even-length hex decoder — avoids pulling in the `hex` crate
/// just to parse a few tens of bytes at boot time. Case-insensitive.
fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err(format!("odd-length hex ({} chars)", s.len()));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        other => Err(format!("non-hex char {:?}", other as char)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn script_hex_p2wpkh(tag: u8) -> String {
        let mut bytes = vec![0x00, 0x14];
        bytes.extend(std::iter::repeat_n(tag, 20));
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    }

    #[test]
    fn static_map_resolver_config_deser_absent_section_yields_null() {
        // No [payout] section at all — resolver is Null.
        let section = RawPayoutSection::from_toml_str(
            r#"
authority_public_key = "irrelevant"
"#,
        )
        .expect("parse");
        let resolver = build_resolver(&section).expect("build");
        assert_eq!(resolver.name(), "null");
    }

    #[test]
    fn static_map_resolver_config_deser_empty_entries_yields_null() {
        let section = RawPayoutSection::from_toml_str(
            r#"
[payout.static]
entries = []
"#,
        )
        .expect("parse");
        let resolver = build_resolver(&section).expect("build");
        assert_eq!(resolver.name(), "null");
    }

    #[test]
    fn static_map_resolver_config_deser_valid_entries_build_static_map() {
        let alice = script_hex_p2wpkh(0x11);
        let bob = script_hex_p2wpkh(0x22);
        let toml = format!(
            r#"
[[payout.static.entries]]
user_identifier = "miner-alice"
script_hex = "{alice}"

[[payout.static.entries]]
user_identifier = "miner-bob"
script_hex = "{bob}"
"#
        );
        let section = RawPayoutSection::from_toml_str(&toml).expect("parse");
        let resolver = build_resolver(&section).expect("build");
        assert_eq!(resolver.name(), "static-map");
        assert!(resolver.resolve("miner-alice").is_some());
        assert!(resolver.resolve("miner-bob").is_some());
        assert!(resolver.resolve("miner-eve").is_none());
        assert_ne!(
            resolver.resolve("miner-alice"),
            resolver.resolve("miner-bob"),
            "distinct entries must yield distinct scripts"
        );
    }

    #[test]
    fn static_map_resolver_config_deser_rejects_empty_user_identifier() {
        let bob = script_hex_p2wpkh(0x22);
        let toml = format!(
            r#"
[[payout.static.entries]]
user_identifier = "   "
script_hex = "{bob}"
"#
        );
        let section = RawPayoutSection::from_toml_str(&toml).expect("parse");
        // `Arc<dyn PayoutScriptResolver>` deliberately has no Debug,
        // so we can't `expect_err()`. Manual match.
        match build_resolver(&section) {
            Err(PayoutConfigError::EmptyUserIdentifier) => {}
            Err(other) => panic!("expected EmptyUserIdentifier, got {other:?}"),
            Ok(_) => panic!("expected error for empty user_identifier, got Ok"),
        }
    }

    #[test]
    fn static_map_resolver_config_deser_rejects_invalid_hex() {
        let toml = r#"
[[payout.static.entries]]
user_identifier = "miner-alice"
script_hex = "zzznotvalidhex"
"#;
        let section = RawPayoutSection::from_toml_str(toml).expect("parse");
        match build_resolver(&section) {
            Err(PayoutConfigError::InvalidHex {
                user_identifier, ..
            }) => {
                assert_eq!(user_identifier, "miner-alice");
            }
            Err(other) => panic!("expected InvalidHex, got {other:?}"),
            Ok(_) => panic!("expected error for invalid hex, got Ok"),
        }
    }

    #[test]
    fn static_map_resolver_config_deser_rejects_duplicate_normalized_keys() {
        let a = script_hex_p2wpkh(0x11);
        let b = script_hex_p2wpkh(0x22);
        let toml = format!(
            r#"
[[payout.static.entries]]
user_identifier = "  miner-alice"
script_hex = "{a}"

[[payout.static.entries]]
user_identifier = "miner-alice "
script_hex = "{b}"
"#
        );
        let section = RawPayoutSection::from_toml_str(&toml).expect("parse");
        match build_resolver(&section) {
            Err(PayoutConfigError::Resolver(_)) => {}
            Err(other) => panic!("expected Resolver error, got {other:?}"),
            Ok(_) => panic!("expected error for duplicate normalised keys, got Ok"),
        }
    }
}
