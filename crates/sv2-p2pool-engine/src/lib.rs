//! `JobValidationEngine` implementation backed by p2poolv2's share chain.
//!
//! See the project's design doc for the SV2 ↔ p2poolv2 message-by-message mapping.
//! This crate is a Phase 1 stub.

#![forbid(unsafe_code)]

/// Placeholder so the crate compiles before the Phase 1 implementation lands.
pub struct P2poolV2Engine;

impl P2poolV2Engine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for P2poolV2Engine {
    fn default() -> Self {
        Self::new()
    }
}
