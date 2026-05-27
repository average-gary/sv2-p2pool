//! Regtest test harness for sv2-p2pool.
//!
//! See the full design in
//! `~/wiki/topics/sv2-p2pool-integration/wiki/topics/regtest-harness-design.md`.
//!
//! # Phase 1.8 scope
//!
//! - [`TestEnv`] composes `corepc-node::Node` (bitcoind regtest) with a
//!   small vocabulary inspired by `bdk_testenv` (`mine_blocks`,
//!   `invalidate_block`, `wait_until_*`).
//! - Spawner stubs for `p2poolv2_node`, `sv2-p2pool`, and `jd-client`
//!   exist as `TODO` — Phase 1.9 / Phase 2 will fill them once we have
//!   reliable binary discovery + matching config-file generation.
//!
//! # Why corepc-node?
//!
//! Confirmed by 4 of 5 research agents as the foundation: three-tier
//! discovery (auto-download / `BITCOIND_EXE` env / system PATH),
//! OS-assigned free ports with retry, Drop = SIGKILL + tempdir cleanup.
//! Replicating any of this in our crate is wasted effort.

#![forbid(unsafe_code)]

use std::time::Duration;

use bitcoin::{BlockHash, hashes::Hash};
pub use corepc_node::Node as BitcoinD;
use thiserror::Error;
use tracing::{debug, info};

pub mod p2poolv2d;
pub use p2poolv2d::{
    DEFAULT_READY_TIMEOUT as P2POOLV2_READY_TIMEOUT, P2poolV2D, P2poolV2DBuilder, P2poolV2DError,
};

/// Regtest test harness — at Phase 1.8, just a thin wrapper over
/// `corepc-node::Node` with a vocabulary mirroring `bdk_testenv`.
///
/// Phase 2 will compose this with `P2poolV2D`, `Sv2P2poolD`, `JdClientD`.
pub struct TestEnv {
    /// The bitcoind regtest node. Drop = process kill + tempdir cleanup.
    pub bitcoind: BitcoinD,
}

/// Builder for [`TestEnv`].
pub struct TestEnvBuilder {
    /// Override for the bitcoind binary path. Falls back to `BITCOIND_EXE`
    /// env var or auto-download.
    bitcoind_exe: Option<String>,
}

impl Default for TestEnvBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestEnvBuilder {
    pub fn new() -> Self {
        Self { bitcoind_exe: None }
    }

    /// Override the bitcoind binary path. If unset, `corepc-node` uses
    /// `BITCOIND_EXE` env var; if that's unset, it auto-downloads.
    pub fn with_bitcoind_exe(mut self, path: impl Into<String>) -> Self {
        self.bitcoind_exe = Some(path.into());
        self
    }

    /// Build and start the harness. Bitcoind regtest is up by the time
    /// this returns; cold-start is typically 500ms-1.5s.
    pub fn build(self) -> Result<TestEnv, TestEnvError> {
        let exe = match self.bitcoind_exe {
            Some(path) => path,
            None => {
                corepc_node::exe_path().map_err(|e| TestEnvError::BitcoindStart(e.to_string()))?
            }
        };
        info!(exe = %exe, "starting bitcoind regtest");
        let bitcoind =
            BitcoinD::new(&exe).map_err(|e| TestEnvError::BitcoindStart(e.to_string()))?;
        debug!(rpc_url = %bitcoind.rpc_url(), "bitcoind regtest ready");
        Ok(TestEnv { bitcoind })
    }
}

impl TestEnv {
    /// Convenience constructor: equivalent to
    /// `TestEnvBuilder::default().build()`.
    pub fn new() -> Result<Self, TestEnvError> {
        TestEnvBuilder::default().build()
    }

    /// Spawn a `p2poolv2` child process against this `TestEnv`'s
    /// bitcoind. Returns the spawner; hold it across the lifetime of
    /// the test (Drop kills the process).
    ///
    /// Requires `P2POOLV2_EXE` env var or `p2poolv2` on `$PATH`.
    pub fn with_p2poolv2(&self) -> Result<P2poolV2D, TestEnvError> {
        P2poolV2DBuilder::new(&self.bitcoind)
            .build()
            .map_err(|e| TestEnvError::P2poolV2(e.to_string()))
    }

    /// Mine `n` blocks to a freshly-generated regtest address.
    ///
    /// Returns the block hashes mined (parsed from corepc's `Vec<String>`
    /// hex output into typed `BlockHash`).
    pub fn mine_blocks(&self, n: usize) -> Result<Vec<BlockHash>, TestEnvError> {
        let addr = self
            .bitcoind
            .client
            .new_address()
            .map_err(|e| TestEnvError::Rpc(e.to_string()))?;
        let result = self
            .bitcoind
            .client
            .generate_to_address(n, &addr)
            .map_err(|e| TestEnvError::Rpc(e.to_string()))?;
        result
            .0
            .into_iter()
            .map(|hex| BlockHash::from_byte_array(parse_hash_hex(&hex)?).pipe(Ok))
            .collect()
    }

    /// Mark a block as invalid, forcing the chain to reorg if it was on
    /// the active chain. This is the regtest reorg primitive.
    pub fn invalidate_block(&self, hash: BlockHash) -> Result<(), TestEnvError> {
        self.bitcoind
            .client
            .invalidate_block(hash)
            .map_err(|e| TestEnvError::Rpc(e.to_string()))?;
        Ok(())
    }

    /// Wait until bitcoind reports a tip at or above `expected_height`,
    /// up to `timeout`. Useful after `mine_blocks` when downstream
    /// services need to catch up.
    pub fn wait_until_height(
        &self,
        expected_height: i64,
        timeout: Duration,
    ) -> Result<(), TestEnvError> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let info = self
                .bitcoind
                .client
                .get_blockchain_info()
                .map_err(|e| TestEnvError::Rpc(e.to_string()))?;
            if info.blocks >= expected_height {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Err(TestEnvError::Timeout(format!(
            "tip never reached height {expected_height} within {timeout:?}"
        )))
    }
}

/// Parse a 64-char hex string into a 32-byte array.
fn parse_hash_hex(hex: &str) -> Result<[u8; 32], TestEnvError> {
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TestEnvError::Rpc(format!("bad hash hex: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| TestEnvError::Rpc(format!("hash hex wrong length: {hex}")))
}

trait Pipe {
    fn pipe<F, R>(self, f: F) -> R
    where
        Self: Sized,
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}
impl<T> Pipe for T {}

#[derive(Debug, Error)]
pub enum TestEnvError {
    #[error("failed to start bitcoind: {0}")]
    BitcoindStart(String),
    #[error("bitcoind RPC error: {0}")]
    Rpc(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("failed to spawn p2poolv2: {0}")]
    P2poolV2(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: bitcoind boots, accepts RPC, mines 101 blocks, reorgs
    /// the tip. Marked `#[ignore]` because it requires `BITCOIND_EXE` env
    /// var or a downloaded binary — CI shouldn't run this by default.
    /// Run locally with `cargo test -- --ignored` once a binary is
    /// available.
    #[test]
    #[ignore = "requires BITCOIND_EXE or auto-download — run locally"]
    fn smoke_bitcoind_boots_mines_reorgs() {
        let env = TestEnv::new().expect("bitcoind starts");
        let hashes = env.mine_blocks(101).expect("mine 101");
        assert_eq!(hashes.len(), 101);
        env.wait_until_height(101, Duration::from_secs(5))
            .expect("tip at 101");

        // Reorg: invalidate the tip, mine 2 more, expect new tip.
        let tip = *hashes.last().expect("non-empty");
        env.invalidate_block(tip).expect("invalidate");
        let new_hashes = env.mine_blocks(2).expect("mine 2");
        assert_ne!(new_hashes.last(), Some(&tip));
    }
}
