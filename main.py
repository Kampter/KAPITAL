"""Minimal OKX streaming demo using picows with latency monitoring."""

import asyncio
import time
import zlib
from typing import Dict, List

import numpy as np
import orjson
from picows import WSListener, WSMsgType, ws_connect

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
LATENCY_WINDOW_CAPACITY = 2048
SUBSCRIPTIONS: List[Dict[str, str]] = [
    {"channel": "trades", "instId": "HYPE-USDT"},
    {"channel": "books5", "instId": "HYPE-USDT"},
]


def _now_us() -> int:
    """Current wall-clock time in microseconds."""

    return time.time_ns() // 1000


class LatencyRing:
    """Ring buffer backed by numpy for percentile latency metrics."""

    __slots__ = ("_buffer", "_capacity", "_count", "_cursor")

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._buffer = np.zeros(capacity, dtype=np.int64)
        self._count = 0
        self._cursor = 0

    def add(self, value_us: int) -> None:
        self._buffer[self._cursor] = value_us
        self._cursor = (self._cursor + 1) % self._capacity
        if self._count < self._capacity:
            self._count += 1

    def percentiles(self, values: tuple[int, int]) -> tuple[float, float] | None:
        if not self._count:
            return None

        if self._count < self._capacity:
            data = self._buffer[: self._count]
        else:
            data = np.concatenate(
                (self._buffer[self._cursor :], self._buffer[: self._cursor])
            )

        result = np.percentile(data, values)
        return float(result[0]), float(result[1])


class OkxListener(WSListener):
    """Receive OKX data frames and emit lightweight latency metrics."""

    def __init__(self, subscriptions: List[Dict[str, str]]) -> None:
        super().__init__()
        self._subscriptions = subscriptions
        self._latency_stats: Dict[str, LatencyRing] = {}

    def on_ws_connected(self, transport) -> None:  # type: ignore[override]
        message = orjson.dumps({"op": "subscribe", "args": self._subscriptions})
        transport.send(WSMsgType.TEXT, message)
        channels = ", ".join(f"{item['channel']}:{item['instId']}" for item in self._subscriptions)
        print(f"[startup] Subscribed to {channels}")

    def on_ws_frame(self, transport, frame) -> None:  # type: ignore[override]
        if frame.msg_type not in (WSMsgType.TEXT, WSMsgType.BINARY):
            return

        payload = self._extract_payload(frame)
        if payload is None:
            return

        try:
            message = orjson.loads(payload)
        except orjson.JSONDecodeError:
            preview = payload[:80]
            try:
                preview_text = preview.decode("utf-8", errors="replace")
            except AttributeError:
                preview_text = str(preview)
            print("[warn] Unable to decode message", preview_text)
            return

        if event := message.get("event"):
            arg = message.get("arg", {})
            print(f"[event] {event} {arg}")
            return

        arg = message.get("arg", {})
        channel = arg.get("channel", "unknown")
        inst_id = arg.get("instId", "?")

        for entry in message.get("data", []):
            self._process_entry(channel, inst_id, entry)

    def on_ws_disconnected(self, transport) -> None:  # type: ignore[override]
        print("[shutdown] Websocket disconnected")

    @staticmethod
    def _extract_payload(frame) -> bytes | None:
        try:
            raw = frame.get_payload_as_bytes()
        except AttributeError:
            text = frame.get_payload_as_utf8_text()
            return text.encode("utf-8")

        if frame.msg_type == WSMsgType.TEXT:
            return raw

        try:
            return zlib.decompress(raw, -zlib.MAX_WBITS)
        except zlib.error:
            return raw

    def _process_entry(self, channel: str, inst_id: str, entry: Dict[str, str]) -> None:
        ts_value = entry.get("ts")
        if ts_value is None:
            return

        try:
            message_ts_ms = int(ts_value)
        except (TypeError, ValueError):
            return

        message_ts_us = message_ts_ms * 1000
        latency_us = max(0, _now_us() - message_ts_us)
        entry_inst_id = entry.get("instId", inst_id)
        key = f"{channel}:{entry_inst_id}"
        ring = self._latency_stats.get(key)
        if ring is None:
            ring = LatencyRing(LATENCY_WINDOW_CAPACITY)
            self._latency_stats[key] = ring
        ring.add(latency_us)
        percentile_stats = ring.percentiles((50, 95))
        stats_text = ""
        if percentile_stats is not None:
            p50, p95 = percentile_stats
            stats_text = f" Δlatency p50={p50:.0f}us p95={p95:.0f}us"

        if channel == "trades":
            side = entry.get("side", "?")
            price = entry.get("px", "?")
            size = entry.get("sz", "?")
            print(
                f"[trade] {entry_inst_id} {side} px={price} sz={size} latency={latency_us}us{stats_text}"
            )
        elif channel.startswith("books"):
            bids = entry.get("bids", [])
            asks = entry.get("asks", [])
            best_bid = bids[0] if bids else ["-", "-"]
            best_ask = asks[0] if asks else ["-", "-"]
            action = entry.get("action", "update")
            print(
                f"[book] {entry_inst_id} {action} bid={best_bid[0]}@{best_bid[1]} "
                f"ask={best_ask[0]}@{best_ask[1]} latency={latency_us}us{stats_text}"
            )


async def _run() -> None:
    transport, _listener = await ws_connect(
        lambda: OkxListener(SUBSCRIPTIONS),
        OKX_WS_URL,
        enable_auto_ping=True,
    )

    try:
        await transport.wait_disconnected()
    except asyncio.CancelledError:
        transport.disconnect()
        raise


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        print("[shutdown] Interrupted by user")


if __name__ == "__main__":
    main()
