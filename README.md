## Kapital

Low-latency OKX streaming demo powered by `picows`, `orjson`, and a Rust ring buffer exposed via PyO3.

### Quickstart

1. `uv sync` (installs Python deps and builds the Rust extension)
2. `uv run python main.py`

`uv run python main.py` is the single entrypoint; it subscribes to HYPE-USDT trades/books and prints latency plus short-window signals. Stop with Ctrl+C.
