## binance-aggregator

Small pet‑project that aggregates historical Binance futures data (trades or orders)
for several accounts and stores it in ClickHouse.

The core pieces are:
- `Config` – parses `config.json` and exposes typed settings for Binance and ClickHouse.
- `Collector` – orchestrates data fetching from Binance and passes it to the writer.
- `EventWriter` – performs batch inserts of trades and orders into ClickHouse.
- `DatabaseChecker` – validates ClickHouse connectivity and ensures tables exist.

### Configuration

The application expects a `config.json` file in the `src` directory (next to `main.py`).
Example shape (simplified):

```json
{
  "credentials_accounts": [
    {
      "api_key": "your_api_key",
      "api_secret": "your_api_secret",
      "name": "main-account"
    }
  ],
  "clickhouse": {
    "host": "localhost",
    "port": 8123,
    "user": "default",
    "password": "password123"
  }
}
```

`credentials_accounts` is a list, so multiple Binance accounts can be configured.
`clickhouse` describes a single ClickHouse cluster used as a sink for all data.

### Running locally

From the project root:

```bash
python -m src.main trades   # collect futures trades
python -m src.main orders   # collect futures orders
```

The process is long‑running and will iterate over all active futures symbols discovered
from the Binance API.

### Docker / Taskfile helpers

The repo contains a `Taskfile.yaml` with a few helper tasks:

- `task build-trades` / `task build-orders` – build Docker images.
- `task up-trades` / `task up-orders` – start collectors in the background.
- `task logs-trades` / `task logs-orders` – follow container logs.

See the `Taskfile.yaml` for the full list and exact commands.

### Logrotation and cron (legacy setup)

For a bare‑metal deployment the project originally used log files and cron jobs.
The rough idea:

- Create log files and helper scripts:
  - `/var/log/trades_collect.log`
  - `/var/log/trades_collect.error.log`
  - `/usr/local/bin/trades_collect.sh`
  - `/etc/logrotate.d/trades_collect`
  - `/var/log/orders_collect.log`
  - `/var/log/orders_collect.error.log`
  - `/usr/local/bin/orders_collect.sh`
  - `/etc/logrotate.d/orders_collect`

- Point logrotate and helper scripts to this repo:

```bash
ln -s /root/repos/binance-aggregator/misc/trades/trades_collect /etc/logrotate.d/trades_collect
ln -s /root/repos/binance-aggregator/misc/trades/trades_collect.sh /usr/local/bin/trades_collect.sh
ln -s /root/repos/binance-aggregator/misc/orders/orders_collect /etc/logrotate.d/orders_collect
ln -s /root/repos/binance-aggregator/misc/orders/orders_collect.sh /usr/local/bin/orders_collect.sh
```

- Configure crontab (example):

```bash
sudo crontab -e

0 3 * * * REPO_DIR=/root/repos/binance-aggregator /usr/local/bin/trades_collect.sh
0 15 * * * REPO_DIR=/root/repos/binance-aggregator /usr/local/bin/orders_collect.sh
```

This section is mostly kept for historical reasons; Docker / Taskfile is usually easier for experiments.

