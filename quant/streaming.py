"""OKX streaming pipeline with latency metrics and short-window signals."""

from __future__ import annotations

import asyncio
import math
import time
import zlib
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import kapital_rust
import orjson
from picows import WSListener, WSMsgType, ws_connect
from quant.config import STREAMING_CONFIG, FilterThresholds

LATENCY_WINDOW_CAPACITY = STREAMING_CONFIG.latency_window_capacity
TRADE_BUFFER_CAPACITY = STREAMING_CONFIG.trade_buffer_capacity
WINDOWS_US = np.asarray(STREAMING_CONFIG.windows_us, dtype=np.int64)
SUBSCRIPTIONS: List[Dict[str, str]] = [
    subscription.to_dict() for subscription in STREAMING_CONFIG.subscriptions
]
PERCENTILE_TARGETS = STREAMING_CONFIG.percentile_targets


def _now_us() -> int:
    """Current wall-clock time in microseconds."""

    return time.time_ns() // 1000


def _stable_sigmoid(z: float) -> float:
    if z >= 0:
        exp_term = math.exp(-z)
        return 1.0 / (1.0 + exp_term)
    exp_term = math.exp(z)
    return exp_term / (1.0 + exp_term)


class LatencyRing:
    """Ring buffer backed by Rust for percentile latency metrics."""

    __slots__ = ("_buffer",)

    def __init__(self, capacity: int) -> None:
        self._buffer = kapital_rust.RingBufferI64(capacity)

    def add(self, value_us: int) -> None:
        self._buffer.push(int(value_us))

    def percentiles(self, values: tuple[int, ...]) -> Optional[tuple[float, float]]:
        if len(self._buffer) == 0:
            return None

        data = np.asarray(self._buffer.to_numpy(), dtype=np.int64)
        result = np.percentile(data, values)
        return float(result[0]), float(result[1])


class TradeBuffer:
    """Rust-backed ring buffers for recent trades with numpy exports."""

    __slots__ = ("_timestamps", "_volumes")

    def __init__(self, capacity: int) -> None:
        self._timestamps = kapital_rust.RingBufferI64(capacity)
        self._volumes = kapital_rust.RingBufferF64(capacity)

    def add_trade(self, ts_us: int, volume: float) -> None:
        self._timestamps.push(int(ts_us))
        self._volumes.push(float(volume))

    def window_volumes(self, ts_us: int, windows_us: np.ndarray, out: np.ndarray) -> None:
        timestamps = np.asarray(self._timestamps.to_numpy(), dtype=np.int64)
        volumes = np.asarray(self._volumes.to_numpy(), dtype=np.float64)
        if timestamps.size == 0:
            out.fill(0.0)
            return

        valid_end = np.searchsorted(timestamps, ts_us, side="right")
        if valid_end == 0:
            out.fill(0.0)
            return

        timestamps = timestamps[:valid_end]
        volumes = volumes[:valid_end]

        abs_volumes = np.abs(volumes)
        prefix = abs_volumes.cumsum()
        for idx, window in enumerate(np.asarray(windows_us, dtype=np.int64)):
            cutoff = ts_us - int(window)
            left = int(np.searchsorted(timestamps, cutoff, side="left"))
            if left <= 0:
                total = float(prefix[-1])
            else:
                total = float(prefix[-1] - prefix[left - 1])
            out[idx] = total


class LogisticRegressionModel:
    """Minimal online logistic regression with pure numpy operations."""

    __slots__ = ("_weights", "_bias", "_learning_rate", "_decay", "_min_lr")

    def __init__(
        self,
        n_features: int,
        learning_rate: float = 5e-2,
        decay: float = 0.999,
        min_lr: float = 1e-4,
    ) -> None:
        self._weights = np.zeros(n_features, dtype=np.float64)
        self._bias = 0.0
        self._learning_rate = learning_rate
        self._decay = decay
        self._min_lr = min_lr

    def predict(self, features: np.ndarray) -> float:
        z = float(np.dot(self._weights, features) + self._bias)
        return float(_stable_sigmoid(z))

    def update(self, features: np.ndarray, label: float) -> float:
        lr = max(self._learning_rate, self._min_lr)
        z = float(np.dot(self._weights, features) + self._bias)
        prob = float(_stable_sigmoid(z))
        error = prob - float(label)
        self._weights -= lr * error * features
        self._bias -= lr * error
        self._learning_rate = max(self._min_lr, self._learning_rate * self._decay)
        return float(prob)


@dataclass(slots=True)
class SignalSnapshot:
    mid_price: float
    spread: float
    imbalance_top1: float
    volume_10ms: float
    volume_50ms: float
    volume_100ms: float
    probability: float
    confidence: float
    direction: str


@dataclass(slots=True)
class LatencyBreakdown:
    """Latency decomposition for a single message."""

    network_us: int
    parse_us: int
    factor_us: int
    dispatch_us: int

    @property
    def total_us(self) -> int:
        return self.network_us + self.parse_us + self.factor_us + self.dispatch_us

    def format(self) -> str:
        return (
            f"lat={self.total_us}us net={self.network_us}us parse={self.parse_us}us "
            f"factor={self.factor_us}us comm={self.dispatch_us}us"
        )


class SignalFilter:
    """Centralized filtering logic for low-quality signals."""

    __slots__ = ("_thresholds",)

    def __init__(self, thresholds: FilterThresholds) -> None:
        self._thresholds = thresholds

    def allow(self, snapshot: SignalSnapshot) -> bool:
        if snapshot.direction == "long":
            return (
                snapshot.probability >= self._thresholds.min_probability_long
                and snapshot.confidence >= self._thresholds.min_confidence
            )
        return (
            snapshot.probability <= self._thresholds.max_probability_short
            and snapshot.confidence >= self._thresholds.min_confidence
        )


class FeaturePipeline:
    """Lightweight feature/alpha pipeline for a single instrument."""

    __slots__ = (
        "instrument",
        "_trade_buffer",
        "_volumes",
        "_features",
        "_model",
        "_pending",
        "_latest_snapshot",
    )

    def __init__(self, instrument: str) -> None:
        self.instrument = instrument
        self._trade_buffer = TradeBuffer(TRADE_BUFFER_CAPACITY)
        self._volumes = np.zeros(WINDOWS_US.shape[0], dtype=np.float64)
        self._features = np.zeros(5, dtype=np.float64)
        self._model = LogisticRegressionModel(n_features=5)
        self._pending: Optional[tuple[np.ndarray, float]] = None
        self._latest_snapshot: Optional[SignalSnapshot] = None

    def record_trade(self, entry: Dict[str, str]) -> Optional[Dict[str, float]]:
        ts_value = entry.get("ts")
        size_value = entry.get("sz")
        if ts_value is None or size_value is None:
            return None

        try:
            ts_us = int(ts_value) * 1000
            size = float(size_value)
        except (TypeError, ValueError):
            return None

        side = entry.get("side", "").lower()
        signed_volume = size if side != "sell" else -size
        self._trade_buffer.add_trade(ts_us, signed_volume)

        price_value = entry.get("px")
        price = float(price_value) if price_value is not None else float("nan")
        return {
            "ts_us": float(ts_us),
            "side": side if side else "?",
            "price": price,
            "signed_volume": signed_volume,
            "abs_volume": abs(signed_volume),
        }

    def process_book(self, entry: Dict[str, str]) -> Optional[SignalSnapshot]:
        ts_value = entry.get("ts")
        bids = entry.get("bids", [])
        asks = entry.get("asks", [])
        if ts_value is None or not bids or not asks:
            return None

        try:
            ts_us = int(ts_value) * 1000
            best_bid_px = float(bids[0][0])
            best_bid_sz = float(bids[0][1])
            best_ask_px = float(asks[0][0])
            best_ask_sz = float(asks[0][1])
        except (TypeError, ValueError, IndexError):
            return None

        spread = max(0.0, best_ask_px - best_bid_px)
        denom = best_bid_sz + best_ask_sz
        imbalance = (best_bid_sz - best_ask_sz) / denom if denom > 0 else 0.0
        mid_price = 0.5 * (best_bid_px + best_ask_px)

        self._trade_buffer.window_volumes(ts_us, WINDOWS_US, self._volumes)
        self._features[0] = imbalance
        self._features[1] = spread
        self._features[2] = self._volumes[0]
        self._features[3] = self._volumes[1]
        self._features[4] = self._volumes[2]

        if self._pending is not None:
            prev_features, prev_mid = self._pending
            if mid_price > prev_mid:
                self._model.update(prev_features, 1.0)
            elif mid_price < prev_mid:
                self._model.update(prev_features, 0.0)

        probability = self._model.predict(self._features)
        confidence = min(1.0, abs(probability - 0.5) * 2.0)
        direction = "long" if probability >= 0.5 else "short"

        self._pending = (self._features.copy(), mid_price)

        snapshot = SignalSnapshot(
            mid_price=mid_price,
            spread=spread,
            imbalance_top1=imbalance,
            volume_10ms=self._volumes[0],
            volume_50ms=self._volumes[1],
            volume_100ms=self._volumes[2],
            probability=probability,
            confidence=confidence,
            direction=direction,
        )
        self._latest_snapshot = snapshot
        return snapshot

    @property
    def latest_snapshot(self) -> Optional[SignalSnapshot]:
        return self._latest_snapshot


class OkxListener(WSListener):
    """Receive OKX data frames and emit latency metrics plus trading signals."""

    def __init__(self, subscriptions: List[Dict[str, str]]) -> None:
        super().__init__()
        self._subscriptions = subscriptions
        self._latency_stats: Dict[str, LatencyRing] = {}
        self._pipelines: Dict[str, FeaturePipeline] = {}
        self._signal_filter = SignalFilter(STREAMING_CONFIG.filter_thresholds)

    def on_ws_connected(self, transport) -> None:  # type: ignore[override]
        message = orjson.dumps({"op": "subscribe", "args": self._subscriptions})
        transport.send(WSMsgType.TEXT, message)
        channels = ", ".join(
            f"{item['channel']}:{item['instId']}" for item in self._subscriptions
        )
        print(f"[startup] Subscribed to {channels}")

    def on_ws_frame(self, transport, frame) -> None:  # type: ignore[override]
        frame_received_us = _now_us()
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
        message_parsed_us = _now_us()

        if event := message.get("event"):
            arg = message.get("arg", {})
            print(f"[event] {event} {arg}")
            return

        arg = message.get("arg", {})
        channel = arg.get("channel", "unknown")
        inst_id = arg.get("instId", "?")

        for entry in message.get("data", []):
            self._process_entry(
                channel,
                inst_id,
                entry,
                frame_received_us,
                message_parsed_us,
            )

    def on_ws_disconnected(self, transport) -> None:  # type: ignore[override]
        print("[shutdown] Websocket disconnected")

    @staticmethod
    def _extract_payload(frame) -> Optional[bytes]:
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

    def _process_entry(
        self,
        channel: str,
        inst_id: str,
        entry: Dict[str, str],
        frame_received_us: int,
        message_parsed_us: int,
    ) -> None:
        ts_value = entry.get("ts")
        if ts_value is None:
            return

        try:
            message_ts_us = int(ts_value) * 1000
        except (TypeError, ValueError):
            return

        network_latency_us = max(0, frame_received_us - message_ts_us)
        parse_latency_us = max(0, message_parsed_us - frame_received_us)
        entry_inst_id = entry.get("instId", inst_id)
        key = f"{channel}:{entry_inst_id}"
        ring = self._latency_stats.get(key)
        if ring is None:
            ring = LatencyRing(LATENCY_WINDOW_CAPACITY)
            self._latency_stats[key] = ring
        pipeline = self._pipelines.get(entry_inst_id)
        if pipeline is None:
            pipeline = FeaturePipeline(entry_inst_id)
            self._pipelines[entry_inst_id] = pipeline

        factor_start_us = _now_us()

        if channel == "trades":
            trade_summary = pipeline.record_trade(entry)
            factor_end_us = _now_us()
            factor_latency_us = max(0, factor_end_us - factor_start_us)
            if trade_summary is None:
                return
            side = trade_summary["side"]
            price = trade_summary["price"]
            abs_volume = trade_summary["abs_volume"]
            snapshot = pipeline.latest_snapshot
            signal_text = ""
            if snapshot is not None and self._signal_filter.allow(snapshot):
                signal_text = (
                    f" signal={snapshot.direction} p={snapshot.probability:.3f}"
                    f" conf={snapshot.confidence:.3f}"
                )
            price_text = f"{price:.6f}" if not math.isnan(price) else "nan"
            dispatch_ready_us = _now_us()
            dispatch_latency_us = max(0, dispatch_ready_us - factor_end_us)
            breakdown = LatencyBreakdown(
                network_us=network_latency_us,
                parse_us=parse_latency_us,
                factor_us=factor_latency_us,
                dispatch_us=dispatch_latency_us,
            )
            ring.add(breakdown.total_us)
            percentile_stats = ring.percentiles(PERCENTILE_TARGETS)
            stats_text = ""
            if percentile_stats is not None:
                p50, p95 = percentile_stats
                stats_text = f" Δlatency p50={p50:.0f}us p95={p95:.0f}us"
            print(
                f"[trade] {entry_inst_id} {side:>4} px={price_text} "
                f"vol={abs_volume:.4f} {breakdown.format()}{stats_text}{signal_text}"
            )
        elif channel.startswith("books"):
            snapshot = pipeline.process_book(entry)
            factor_end_us = _now_us()
            factor_latency_us = max(0, factor_end_us - factor_start_us)
            if snapshot is None:
                return
            if not self._signal_filter.allow(snapshot):
                return
            dispatch_ready_us = _now_us()
            dispatch_latency_us = max(0, dispatch_ready_us - factor_end_us)
            breakdown = LatencyBreakdown(
                network_us=network_latency_us,
                parse_us=parse_latency_us,
                factor_us=factor_latency_us,
                dispatch_us=dispatch_latency_us,
            )
            ring.add(breakdown.total_us)
            percentile_stats = ring.percentiles(PERCENTILE_TARGETS)
            stats_text = ""
            if percentile_stats is not None:
                p50, p95 = percentile_stats
                stats_text = f" Δlatency p50={p50:.0f}us p95={p95:.0f}us"
            print(
                f"[signal] {entry_inst_id} mid={snapshot.mid_price:.6f} "
                f"spread={snapshot.spread:.6f} imbalance={snapshot.imbalance_top1:.3f} "
                f"vol10={snapshot.volume_10ms:.4f} vol50={snapshot.volume_50ms:.4f} "
                f"vol100={snapshot.volume_100ms:.4f} prob={snapshot.probability:.3f} "
                f"conf={snapshot.confidence:.3f} dir={snapshot.direction} "
                f"{breakdown.format()}{stats_text}"
            )


async def _run() -> None:
    transport, _listener = await ws_connect(
        lambda: OkxListener(SUBSCRIPTIONS),
        STREAMING_CONFIG.okx_ws_url,
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
