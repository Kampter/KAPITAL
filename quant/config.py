"""Centralized configuration for the streaming pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Tuple


@dataclass(frozen=True)
class Subscription:
    """Websocket subscription descriptor."""

    channel: str
    inst_id: str

    def to_dict(self) -> dict[str, str]:
        return {"channel": self.channel, "instId": self.inst_id}


@dataclass(frozen=True)
class FilterThresholds:
    """Thresholds used to filter low-quality trading signals."""

    min_confidence: float = 0.1
    min_probability_long: float = 0.55
    max_probability_short: float = 0.45


@dataclass(frozen=True)
class StreamingConfig:
    """Runtime configuration for the streaming agent."""

    okx_ws_url: str = "wss://ws.okx.com:8443/ws/v5/public"
    subscriptions: Tuple[Subscription, ...] = (
        Subscription(channel="trades", inst_id="HYPE-USDT"),
        Subscription(channel="books5", inst_id="HYPE-USDT"),
    )
    latency_window_capacity: int = 2048
    trade_buffer_capacity: int = 4096
    windows_us: Tuple[int, ...] = (10_000, 50_000, 100_000)
    percentile_targets: Tuple[int, int] = (50, 95)
    filter_thresholds: FilterThresholds = field(default_factory=FilterThresholds)


STREAMING_CONFIG = StreamingConfig()

__all__ = [
    "FilterThresholds",
    "STREAMING_CONFIG",
    "StreamingConfig",
    "Subscription",
]
