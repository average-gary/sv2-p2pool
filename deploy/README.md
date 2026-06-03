# Deployment recipes

Operator-oriented templates for installing `sv2-p2pool` as a long-running service. Adjust paths + addresses to your environment before applying.

## Files

| Path | What it is |
|---|---|
| [`systemd/sv2-p2pool.service`](systemd/sv2-p2pool.service) | systemd unit for Linux hosts |
| [`docker-compose.yml`](docker-compose.yml) | docker-compose stack: bitcoind + sv2-p2pool |
| [`../Dockerfile`](../Dockerfile) | multi-stage build for the binary container |
| [`config/pool.example.toml`](config/pool.example.toml) | sv2-apps `PoolConfig` example |
| [`config/p2pool.example.toml`](config/p2pool.example.toml) | p2poolv2 share-chain config example |

## Quickstart (systemd)

Assumes:
- Bitcoin Core 28.0+ already running with `-ipcbind=unix:/var/lib/bitcoin/testnet4/node.sock`
- `sv2-p2pool` built (`cargo build --release --bin sv2-p2pool`)
- A `sv2-p2pool` system user

```sh
# 1. Install the binary.
sudo install -o root -g root -m 0755 \
    target/release/sv2-p2pool /usr/local/bin/sv2-p2pool

# 2. Create the system user + config dir. The state and log dirs are
#    auto-created by systemd on first start (StateDirectory= +
#    LogsDirectory= in the unit file).
sudo useradd --system --home-dir /var/lib/sv2-p2pool --shell /usr/sbin/nologin sv2-p2pool
sudo mkdir -p /etc/sv2-p2pool

# 3. Drop in the configs (edit auth keys + payout address first!).
sudo install -o root -g root -m 0644 \
    deploy/config/pool.example.toml /etc/sv2-p2pool/pool.toml
sudo install -o root -g root -m 0640 -g sv2-p2pool \
    deploy/config/p2pool.example.toml /etc/sv2-p2pool/p2pool.toml

# 4. Install + enable the unit.
sudo install -o root -g root -m 0644 \
    deploy/systemd/sv2-p2pool.service /etc/systemd/system/sv2-p2pool.service
sudo systemctl daemon-reload
sudo systemctl enable --now sv2-p2pool

# 5. Verify.
sudo systemctl status sv2-p2pool
sudo journalctl -u sv2-p2pool -f
curl http://127.0.0.1:9000/metrics
```

## What the unit does

- Runs as the unprivileged `sv2-p2pool:sv2-p2pool` user.
- Restarts on crash (`Restart=on-failure`); a graceful Ctrl-C / SIGTERM exits 0 and is left alone.
- `ProtectSystem=strict` + `StateDirectory=sv2-p2pool` + `LogsDirectory=sv2-p2pool`. systemd creates `/var/lib/sv2-p2pool` and `/var/log/sv2-p2pool` on first start (mode 0750, owned by `sv2-p2pool:sv2-p2pool`) and these are the only writable paths.
- Bumps `LimitNOFILE` to 65536 — rocksdb opens many small files at startup.
- Logs to a file (`/var/log/sv2-p2pool/sv2-p2pool.log`) AND the journal.
- Mounts `/metrics` at `127.0.0.1:9000` — change to `0.0.0.0:9000` if your Prometheus scraper is on another host (and put it behind a private network).

## Config-file editing notes

Both `pool.toml` and `p2pool.toml` ship with placeholder values. Before going live:

- **`pool.toml`**:
  - `authority_public_key` / `authority_secret_key` — generate fresh with sv2-apps's authority key util. Don't reuse the example keys.
  - `coinbase_reward_script` — your payout address, wrapped in `addr(...)`.
  - `listen_address` / `[jds].listen_address` — adjust ports if you run multiple pools or have firewall constraints.
- **`p2pool.toml`**:
  - `[stratum].solo_address` / `bootstrap_address` — your payout address.
  - `[bitcoinrpc].username` / `password` — your bitcoind RPC creds.
  - `[network].dial_peers` — at least one peer multiaddr is required for share-chain participation (the empty list is for isolated test runs).

## Quickstart (docker-compose)

For an isolated all-in-one stack on a single host:

```sh
# 1. Edit deploy/config/{pool,p2pool}.example.toml in place
#    (auth keys, payout addresses, bitcoinrpc creds — see notes below).

# 2. Bring up the stack from the repo root.
docker compose -f deploy/docker-compose.yml up -d

# 3. Watch the logs while bitcoind syncs.
docker compose -f deploy/docker-compose.yml logs -f sv2-p2pool

# 4. Confirm metrics.
curl http://127.0.0.1:9000/metrics
```

See [`docker-compose.yml`](docker-compose.yml) for the bitcoind multiprocess caveat — the upstream `bitcoin/bitcoin` image doesn't ship multiprocess support, so for IPC-mode operation you need to build bitcoind with `--enable-multiprocess` or use a community image that does.

## Prometheus scrape config

```yaml
scrape_configs:
  - job_name: sv2-p2pool
    static_configs:
      - targets: ['127.0.0.1:9000']
    scrape_interval: 15s
```

See [`docs/running.md`](../docs/running.md#observability) for the counter glossary.
