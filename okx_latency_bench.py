#!/usr/bin/env python3
"""
OKX WebSocket latency/parse benchmark (no l2-tbt).

Features
- Subscribe to trades & books5 for given symbols
- Per-channel live stats: p50/p95/p99 latency (us), frame size (bytes), JSON parse time (us)
- Final 5-minute Markdown report with recommendations
- Config via env:
    OKX_SYMBOLS=HYPE-USDT,BTC-USDT
    OKX_BENCH_DURATION_SEC=300
    SUMMARY_EVERY_SEC=10
"""

import asyncio
import os
import time
import zlib
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import numpy as np
import orjson
from picows import WSMsgType, WSListener, ws_connect

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

SYMBOLS = [s.strip() for s in os.getenv("OKX_SYMBOLS", "HYPE-USDT").split(",") if s.strip()]
BENCH_DURATION_SEC = int(os.getenv("OKX_BENCH_DURATION_SEC", "300"))
SUMMARY_EVERY_SEC = int(os.getenv("SUMMARY_EVERY_SEC", "10"))

CHANNELS = ("trades", "books5")  # no l2-tbt per request

# 放在 import 之后、任何函数体之外
EPOCH_OFFSET_US = None  # 单调时钟 -> UNIX 时间的偏移

def init_epoch_offset():
    global EPOCH_OFFSET_US
    # 计算：UNIX(微秒) - 单调(微秒)
    EPOCH_OFFSET_US = (time.time_ns() // 1_000) - (time.perf_counter_ns() // 1_000)

def mono_us_to_epoch_us(mono_us: int) -> int:
    if EPOCH_OFFSET_US is None:
        raise RuntimeError('init_epoch_offset() must run before converting monotonic timestamps')
    # 单调时钟微秒 + 偏移 = UNIX 微秒
    return mono_us + EPOCH_OFFSET_US

def now_us_monotonic() -> int:
    return time.perf_counter_ns() // 1_000


@dataclass
class StreamStats:
    # ring buffers (fixed capacity for low jitter)
    cap: int = 8192
    lat_us: np.ndarray = field(default_factory=lambda: np.zeros(8192, dtype=np.int64))
    parse_us: np.ndarray = field(default_factory=lambda: np.zeros(8192, dtype=np.int64))
    frame_bytes: np.ndarray = field(default_factory=lambda: np.zeros(8192, dtype=np.int32))
    i: int = 0
    full: bool = False
    msgs: int = 0

    def add(self, latency_us: int, parse_us: int, frame_len: int) -> None:
        idx = self.i
        self.lat_us[idx] = latency_us
        self.parse_us[idx] = parse_us
        self.frame_bytes[idx] = frame_len
        self.i = (idx + 1) % self.cap
        self.full = self.full or self.i == 0
        self.msgs += 1

    def _view(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        if not self.full:
            return (
                self.lat_us[: self.i],
                self.parse_us[: self.i],
                self.frame_bytes[: self.i],
            )
        # time-ordered view by concatenation of two slices
        return (
            np.concatenate((self.lat_us[self.i :], self.lat_us[: self.i])),
            np.concatenate((self.parse_us[self.i :], self.parse_us[: self.i])),
            np.concatenate((self.frame_bytes[self.i :], self.frame_bytes[: self.i])),
        )

    def summary(self) -> Dict[str, float]:
        lat, pars, sz = self._view()
        if lat.size == 0:
            return {
                "count": 0,
                "lat_p50": np.nan,
                "lat_p95": np.nan,
                "lat_p99": np.nan,
                "parse_p50": np.nan,
                "parse_p95": np.nan,
                "parse_p99": np.nan,
                "frame_avg": np.nan,
                "frame_p95": np.nan,
            }
        return {
            "count": int(lat.size),
            "lat_p50": float(np.percentile(lat, 50)),
            "lat_p95": float(np.percentile(lat, 95)),
            "lat_p99": float(np.percentile(lat, 99)),
            "parse_p50": float(np.percentile(pars, 50)),
            "parse_p95": float(np.percentile(pars, 95)),
            "parse_p99": float(np.percentile(pars, 99)),
            "frame_avg": float(np.mean(sz)),
            "frame_p95": float(np.percentile(sz, 95)),
        }


class OkxLatencyListener(WSListener):
    def __init__(self, subs: List[Dict[str, str]], stats: Dict[str, StreamStats]) -> None:
        super().__init__()
        self._subs = subs
        self._stats = stats

    def on_ws_connected(self, transport) -> None:  # type: ignore[override]
        msg = orjson.dumps({"op": "subscribe", "args": self._subs})
        transport.send(WSMsgType.TEXT, msg)
        chs = ", ".join(f"{x['channel']}:{x['instId']}" for x in self._subs)
        print(f"[startup] Subscribed to {chs}")

    def on_ws_frame(self, transport, frame) -> None:  # type: ignore[override]
        if frame.msg_type not in (WSMsgType.TEXT, WSMsgType.BINARY):
            return

        recv_us_mono = now_us_monotonic()
        recv_us_epoch = mono_us_to_epoch_us(recv_us_mono)
        raw = self._payload_bytes(frame)
        if raw is None:
            return
        frame_len = len(raw)

        t0 = now_us_monotonic()
        try:
            msg = orjson.loads(raw)
        except orjson.JSONDecodeError:
            return
        parse_us = max(0, now_us_monotonic() - t0)

        arg = msg.get("arg", {})
        channel = arg.get("channel", "unknown")
        inst = arg.get("instId", "?")
        if not channel or channel == "unknown":
            # event or error
            return

        # Support both TEXT and BINARY (gzip) formats; process list entries
        data = msg.get("data", [])
        if not isinstance(data, list):
            return

        # latency computed per-entry (server ts vs our recv)
        key = f"{channel}:{inst}"
        st = self._stats[key]
        for entry in data:
            ts_ms = entry.get("ts")
            if ts_ms is None:
                continue
            try:
                msg_ts_us = int(ts_ms) * 1000
            except Exception:
                continue
            lat_us = max(0, recv_us_epoch - msg_ts_us)   # 同一时钟域
            st.add(lat_us, parse_us, frame_len)

    @staticmethod
    def _payload_bytes(frame) -> bytes | None:
        # TEXT: bytes available; BINARY: likely gzip raw (per OKX)
        if frame.msg_type == WSMsgType.TEXT:
            try:
                return frame.get_payload_as_bytes()
            except AttributeError:
                s = frame.get_payload_as_utf8_text()
                return s.encode("utf-8")
        # BINARY
        try:
            b = frame.get_payload_as_bytes()
        except AttributeError:
            return None
        try:
            return zlib.decompress(b, -zlib.MAX_WBITS)
        except zlib.error:
            return b


def build_subs(symbols: List[str]) -> List[Dict[str, str]]:
    subs: List[Dict[str, str]] = []
    for s in symbols:
        subs.append({"channel": "trades", "instId": s})
        subs.append({"channel": "books5", "instId": s})
    return subs


def print_live(stats: Dict[str, StreamStats]) -> None:
    lines = []
    for k in sorted(stats.keys()):
        s = stats[k].summary()
        if s["count"] == 0:
            continue
        lines.append(
            f"[{k:<18}] n={s['count']:>4}  "
            f"lat p50/p95/p99 = {s['lat_p50']:.0f}/{s['lat_p95']:.0f}/{s['lat_p99']:.0f} us  "
            f"parse p50/p95/p99 = {s['parse_p50']:.0f}/{s['parse_p95']:.0f}/{s['parse_p99']:.0f} us  "
            f"frame avg/p95 = {s['frame_avg']:.0f}/{s['frame_p95']:.0f} B"
        )
    if lines:
        print("\n".join(lines))


def make_markdown_report(stats: Dict[str, StreamStats], duration_sec: int) -> str:
    header = (
        "# OKX WebSocket Latency Report\n\n"
        f"- Duration: **{duration_sec} s**\n"
        f"- Symbols: `{', '.join(SYMBOLS)}`\n"
        f"- Channels: `{', '.join(CHANNELS)}`\n"
        f"- Endpoint: `{OKX_WS_URL}`\n\n"
        "## Summary (per channel)\n\n"
        "| channel | samples | latency p50 (us) | p95 (us) | p99 (us) | parse p50 (us) | parse p95 (us) | parse p99 (us) | frame avg (B) | frame p95 (B) |\n"
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"
    )
    rows = []
    for k in sorted(stats.keys()):
        s = stats[k].summary()
        if s["count"] == 0:
            continue
        rows.append(
            f"| `{k}` | {s['count']} | {s['lat_p50']:.0f} | {s['lat_p95']:.0f} | {s['lat_p99']:.0f} | "
            f"{s['parse_p50']:.0f} | {s['parse_p95']:.0f} | {s['parse_p99']:.0f} | {s['frame_avg']:.0f} | {s['frame_p95']:.0f} |"
        )
    if not rows:
        rows.append("| (no data) | 0 | - | - | - | - | - | - | - | - |")

    advice = (
        "\n\n## Recommendations\n"
        "- **Region/Peering**: 将 VM 放在 **ap-southeast-1（新加坡）/ap-northeast-1（东京）**，并确保 `ws.okx.com:8443` 直连（不走代理/VPN/TUN）。\n"
        "- **Machine Type**: 高频节点选 **c4-highcpu-2** 或 **c4-standard-2**；研究/采集可用 n4/c4a。\n"
        "- **OS**: 首选 **Ubuntu 24.04 LTS (kernel 6.8/EEVDF)**；关闭不必要的后台进程，固定 CPU 频率。\n"
        "- **Socket/Loop**: 已使用 `picows` 和 `orjson`；避免在热路径做 I/O 日志，采用 micro-batch（5–10ms）汇总计算因子。\n"
        "- **Target**: 若做市/低延迟执行，建议把 **latency p50 降到 ≤ 20 ms**；若仅中频/风控，**≤ 50 ms 且 p95 接近 p50** 即可。\n"
    )

    return header + "\n".join(rows) + advice


async def run() -> None:
    init_epoch_offset()  # <--- 新增
    # prepare stats map for all channel:symbol combinations
    stats: Dict[str, StreamStats] = {}
    for sym in SYMBOLS:
        for ch in CHANNELS:
            stats[f"{ch}:{sym}"] = StreamStats(cap=8192)

    subs = build_subs(SYMBOLS)
    transport, _listener = await ws_connect(
        lambda: OkxLatencyListener(subs, stats),
        OKX_WS_URL,
        enable_auto_ping=True,
    )

    async def live_printer():
        while True:
            await asyncio.sleep(SUMMARY_EVERY_SEC)
            print_live(stats)

    live_task = asyncio.create_task(live_printer())

    # run for BENCH_DURATION_SEC seconds
    try:
        await asyncio.sleep(BENCH_DURATION_SEC)
    finally:
        live_task.cancel()
        try:
            await live_task
        except asyncio.CancelledError:
            pass
        report = make_markdown_report(stats, BENCH_DURATION_SEC)
        print("\n" + report)
        transport.disconnect()

def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("[shutdown] Interrupted by user")

if __name__ == "__main__":
    main()
