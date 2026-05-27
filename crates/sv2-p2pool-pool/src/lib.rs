//! sv2-p2pool — SV2 mining pool library backed by p2poolv2's share chain.
//!
//! Phase 1 assembles `JobDeclarator` + `ChannelManager` directly via
//! [`PoolBuilder`], bypassing `PoolSv2::start`. The engine
//! ([`sv2_p2pool_engine::P2poolV2Engine`]) is the `JobValidationEngine`
//! implementation.
//!
//! See [the Phase 1 wiring plan][1] for the full execution roadmap.
//!
//! [1]: ~/wiki/topics/sv2-p2pool-integration/output/plan-phase-1-wiring-2026-05-26.md

#![forbid(unsafe_code)]

pub mod args;
pub mod builder;
pub mod pool;
pub mod share_chain;
pub mod tdp_demux;

pub use args::process_cli_args;
pub use builder::PoolBuilder;
pub use pool::Pool;
