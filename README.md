## Kapital Demo

This repository contains a minimal websocket demo that streams OKX `HYPE-USDT` trades and order book updates using the low-latency [`picows`](https://github.com/quixdb/picows) client.

### Prerequisites

- Python 3.12 (managed automatically via `uv`)
- `uv` package manager

### Setup

Install dependencies into the local virtual environment:

```bash
uv sync
```

### Run the demo

```bash
uv run python main.py
```

The script subscribes to the OKX public websocket and prints trades and book snapshots with an inline latency estimate (in milliseconds). Press `Ctrl+C` to stop the process.
