# Running sv2-p2pool

Operator-oriented quickstart for booting the pool against a Bitcoin Core node + p2poolv2 share-chain config.

## Prerequisites

- Rust toolchain matching `rust-toolchain.toml` (currently 1.88).
- A reachable Bitcoin Core node with RPC + IPC enabled. Phase 2-A defaults to **testnet4**, which is the deployment target with a [live p2poolv2 dashboard](https://testnet4.p2poolv2.org/dashboard).
- `capnproto` + `libcapnp-dev` (Ubuntu) or equivalent — `bitcoin-core-sv2`'s build script needs them.

## Build

```sh
cargo build --release --bin sv2-p2pool
```

The binary lands at `target/release/sv2-p2pool`.

## Two configs

Phase 2-A separates the sv2-apps `PoolConfig` (authority keys, listen addresses, JDS, TP type) from the p2poolv2 share-chain config (rocksdb store path, bitcoinrpc creds, stratum.network). Both are required for full operation.

### `pool.toml` — sv2-apps PoolConfig

Reuses the upstream sv2-apps shape. Start from one of the example configs in `vendor/sv2-apps/pool-apps/pool/config-examples/<network>/`. Minimum fields:

```toml
authority_public_key = "..."     # Generate with sv2-apps utilities; matches your JDC's expected key
authority_secret_key = "..."
cert_validity_sec = 3600
listen_address = "0.0.0.0:34254" # Mining-protocol downstream listener

coinbase_reward_script = "addr(<your-address>)"

server_id = 1
pool_signature = "your-pool-name"
shares_per_minute = 6.0
share_batch_size = 10

supported_extensions = []
required_extensions = []

monitoring_address = "127.0.0.1:9090"
monitoring_cache_refresh_secs = 15

[template_provider_type.BitcoinCoreIpc]
network = "testnet4"
fee_threshold = 100
min_interval = 5

[jds]
listen_address = "0.0.0.0:34264"  # JDP listener for JDC connections
```

### `p2pool.toml` — p2poolv2 share-chain config

```toml
[network]
listen_address = "/ip4/0.0.0.0/tcp/6884"
dial_peers = []           # Add p2poolv2 peer multiaddrs here for share-chain participation
max_pending_incoming = 10
max_pending_outgoing = 10
max_established_incoming = 50
max_established_outgoing = 50
max_established_per_peer = 1
max_workbase_per_second = 10
max_userworkbase_per_second = 10
max_miningshare_per_second = 100
max_inventory_per_second = 100
max_transaction_per_second = 100
max_requests_per_second = 100
dial_timeout_secs = 30

[store]
path = "/var/lib/sv2-p2pool/store.db"
background_task_frequency_hours = 24
pplns_ttl_days = 7

[stratum]
hostname = "127.0.0.1"
port = 3333
start_difficulty = 10000
minimum_difficulty = 100
solo_address = "<your-address>"
bootstrap_address = "<your-address>"
zmqpubhashblock = "tcp://127.0.0.1:28332"
network = "testnet4"
version_mask = "1fffe000"
difficulty_multiplier = 1.0
pool_signature = "your-pool-name"

[bitcoinrpc]
url = "http://127.0.0.1:48332"
username = "rpcuser"
password = "rpcpass"

[logging]
console = true
level = "info"
stats_dir = "/var/lib/sv2-p2pool/stats"

[api]
hostname = "127.0.0.1"
port = 46884
```

## Run

```sh
sv2-p2pool \
    --config /etc/sv2-p2pool/pool.toml \
    --p2pool-config /etc/sv2-p2pool/p2pool.toml
```

Defaults:
- `--config` → `sv2-p2pool.toml` (in CWD)
- `--p2pool-config` → `p2poolv2.toml` (in CWD)

Logs go to stdout; configure verbosity via the standard `RUST_LOG` env var.

## Bitcoin Core IPC

Phase 2-A's binary uses **Bitcoin Core IPC** as the Template Provider (configured via `[template_provider_type.BitcoinCoreIpc]` in `pool.toml`). Bitcoin Core needs `-ipcbind=unix:...` set — see the upstream sv2-apps bitcoin-core docs. The default IPC socket path is derived from `network` and `data_dir`; override with `data_dir = "/custom/path"` under `[template_provider_type.BitcoinCoreIpc]`.

## Networks

p2poolv2's genesis builder supports **Bitcoin / Testnet4 / Signet**. **Regtest is not supported** — share-chain genesis would need an upstream PR to add `RegtestGenesisData` and a match arm in `genesis_data()`. Tests target Testnet4 by default.

## Operational notes

- **Persistence**: the rocksdb store at `[store].path` carries the share chain across restarts. Delete it to start fresh; the binary will init genesis on startup.
- **Reorg behaviour**: when the share-chain detects a tip swap, the engine drops every cached `DeclaredJob` (see ADR 0001 for the uncle-weighting decision that drives this).
- **Block submission**: when `handle_push_solution` matches a cached job and the TDP fetch succeeds, the reconstructed block is submitted to bitcoind fire-and-forget. Failures log a `submit_block failed` warning but don't crash the pool.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `share-chain bootstrap: failed to open rocksdb store` | rocksdb lock held by another process | stop the other p2poolv2 / sv2-p2pool, or move `[store].path` to a free location |
| `share-chain bootstrap: failed to construct bitcoind RPC client` | wrong creds or bitcoind not reachable | verify `[bitcoinrpc] url/username/password` against `bitcoin-cli getrpcinfo` |
| `[jds] config is required for sv2-p2pool` | missing `[jds]` section in `pool.toml` | add `[jds]` with at least `listen_address` |
| `Network Testnet and Regtest not yet supported` | tried `network = "regtest"` in `p2pool.toml` | switch to `testnet4` or `signet` (see "Networks" above) |
