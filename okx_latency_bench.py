#!/usr/bin/env python3
# okx_latency_bench.py
"""
OKX WebSocket latency/parse benchmark with parallel dial & best-connection selection.

新增：
- 并行拨号到多个端点（:8443 与默认 :443），每条连接热身 3s 收集延迟，择优保留最低 p50。
- 可选择多路“候选连接”并发拨号（env: DIAL_CANDIDATES=N）。
- 支持 CPU 亲和（env: CPU_AFFINITY="0" 或 "0,1"）。
- 其余保持你原脚本的统计/报告/输出格式。

ENV:
  OKX_SYMBOLS=HYPE-USDT,BTC-USDT
  OKX_BENCH_DURATION_SEC=300
  SUMMARY_EVERY_SEC=10
  WARMUP_SEC=3
  DIAL_CANDIDATES=2       # 每个端点拨多少条（总连接数=端点数*此值）
"""

import asyncio
import os
import time
import zlib
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import numpy as np
import orjson
from picows import WSMsgType, WSListener, ws_connect

# 端点集：8443 与默认 443
OKX_ENDPOINTS = [
    "wss://ws.okx.com:8443/ws/v5/public",
    "wss://ws.okx.com/ws/v5/public",
]

SYMBOLS = [s.strip() for s in os.getenv("OKX_SYMBOLS", "HYPE-USDT").split(",") if s.strip()]
BENCH_DURATION_SEC = int(os.getenv("OKX_BENCH_DURATION_SEC", "300"))
SUMMARY_EVERY_SEC = int(os.getenv("SUMMARY_EVERY_SEC", "10"))
WARMUP_SEC = int(os.getenv("WARMUP_SEC", "3"))
DIAL_CANDIDATES = int(os.getenv("DIAL_CANDIDATES", "2"))

CHANNELS = ("trades", "books5")  # 仍按你要求：无 l2-tbt

# 单调时钟 → UNIX 偏移
EPOCH_OFFSET_US: Optional[int] = None

def init_epoch_offset():
  global EPOCH_OFFSET_US
  EPOCH_OFFSET_US = (time.time_ns() // 1_000) - (time.perf_counter_ns() // 1_000)

def mono_us_to_epoch_us(mono_us: int) -> int:
  if EPOCH_OFFSET_US is None:
    raise RuntimeError('init_epoch_offset() must run before converting monotonic timestamps')
  return mono_us + EPOCH_OFFSET_US

def now_us_monotonic() -> int:
  return time.perf_counter_ns() // 1_000


@dataclass
class StreamStats:
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
      return (self.lat_us[: self.i], self.parse_us[: self.i], self.frame_bytes[: self.i])
    return (
      np.concatenate((self.lat_us[self.i :], self.lat_us[: self.i])),
      np.concatenate((self.parse_us[self.i :], self.parse_us[: self.i])),
      np.concatenate((self.frame_bytes[self.i :], self.frame_bytes[: self.i])),
    )

  def summary(self) -> Dict[str, float]:
    lat, pars, sz = self._view()
    if lat.size == 0:
      return {"count": 0, "lat_p50": np.nan, "lat_p95": np.nan, "lat_p99": np.nan,
              "parse_p50": np.nan, "parse_p95": np.nan, "parse_p99": np.nan,
              "frame_avg": np.nan, "frame_p95": np.nan}
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
      return

    data = msg.get("data", [])
    if not isinstance(data, list):
      return

    key = f"{channel}:{inst}"
    st = self._stats.get(key)
    if st is None:
      return

    for entry in data:
      ts_ms = entry.get("ts")
      if ts_ms is None:
        continue
      try:
        msg_ts_us = int(ts_ms) * 1000
      except Exception:
        continue
      lat_us = max(0, recv_us_epoch - msg_ts_us)
      st.add(lat_us, parse_us, frame_len)

  @staticmethod
  def _payload_bytes(frame) -> Optional[bytes]:
    if frame.msg_type == WSMsgType.TEXT:
      try:
        return frame.get_payload_as_bytes()
      except AttributeError:
        s = frame.get_payload_as_utf8_text()
        return s.encode("utf-8")
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


def print_live(stats_map: Dict[str, StreamStats]) -> None:
  lines = []
  for k in sorted(stats_map.keys()):
    s = stats_map[k].summary()
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


def make_markdown_report(stats: Dict[str, StreamStats], duration_sec: int, endpoint: str) -> str:
  header = (
    "# OKX WebSocket Latency Report\n\n"
    f"- Duration: **{duration_sec} s**\n"
    f"- Symbols: `{', '.join(SYMBOLS)}`\n"
    f"- Channels: `{', '.join(CHANNELS)}`\n"
    f"- Endpoint: `{endpoint}`\n\n"
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
    "- **Region/Peering**: 首选 GCP `asia-northeast1`（东京），对比 `:8443` 与 `:443` 的 Anycast 命中差异。\n"
    "- **CPU/IRQ**: 进程与 IRQ 绑同核（`taskset -c N` + 脚本自动绑 IRQ）。\n"
    "- **订阅隔离**: 符号/频道增加后，拆分为多进程、各自独立连接与 CPU。\n"
    "- **A/B**: busy_poll=0/50μs、是否压缩、端点选择、并行拨号候选数（`DIAL_CANDIDATES`）。\n"
  )
  return header + "\n".join(rows) + advice


class CandidateConn:
  """单个候选连接：用于热身收集延迟分布，随后择优保留或关闭。"""
  def __init__(self, endpoint: str, subs: List[Dict[str, str]], warmup_cap: int = 1024):
    self.endpoint = endpoint
    self.subs = subs
    self.warm_stats: Dict[str, StreamStats] = {}
    for s in SYMBOLS:
      for ch in CHANNELS:
        self.warm_stats[f"{ch}:{s}"] = StreamStats(cap=warmup_cap)
    self.transport = None
    self.listener = None

  def p50_overall_us(self) -> float:
    # 聚合 channels 的 p50 再取中位
    vals = []
    for st in self.warm_stats.values():
      s = st.summary()
      if s["count"] > 0 and not np.isnan(s["lat_p50"]):
        vals.append(s["lat_p50"])
    return float(np.median(vals)) if vals else float("inf")

  async def connect(self):
    self.transport, self.listener = await ws_connect(
      lambda: OkxLatencyListener(self.subs, self.warm_stats),
      self.endpoint,
      enable_auto_ping=True,
    )

  async def close(self):
    try:
      if self.transport:
        self.transport.disconnect()
    except Exception:
      pass


async def parallel_dial_and_select() -> Tuple[str, object]:
  """并行拨号：对多个端点 * 多候选，热身 WARMUP_SEC 秒，择优保留一条连接并返回 (endpoint, transport)"""
  subs = build_subs(SYMBOLS)
  cand: List[CandidateConn] = []
  for ep in OKX_ENDPOINTS:
    for _ in range(DIAL_CANDIDATES):
      cand.append(CandidateConn(ep, subs))

  # 并行建立连接
  await asyncio.gather(*[c.connect() for c in cand])

  # 热身
  t_end = time.time() + WARMUP_SEC
  while time.time() < t_end:
    await asyncio.sleep(0.2)

  # 选最优（总体 p50 最低）
  best = min(cand, key=lambda c: c.p50_overall_us())
  best_ep = best.endpoint
  best_transport = best.transport

  # 关掉其他
  for c in cand:
    if c is not best:
      await c.close()

  print(f"[select] Best endpoint: {best_ep}  (warmup median p50 ≈ {best.p50_overall_us():.0f} us)")
  return best_ep, best_transport


def set_cpu_affinity_from_env():
  # 可选：CPU_AFFINITY="0" 或 "0,1"
  cpu_str = os.getenv("CPU_AFFINITY", "").strip()
  if not cpu_str or not hasattr(os, "sched_setaffinity"):
    return
  try:
    cpus = {int(x) for x in cpu_str.split(",") if x.strip().isdigit()}
    if cpus:
      os.sched_setaffinity(0, cpus)
      print(f"[affinity] Set CPU affinity to {sorted(cpus)}")
  except Exception as e:
    print(f"[affinity] Set failed: {e}")


async def run() -> None:
  init_epoch_offset()
  set_cpu_affinity_from_env()

  # 1) 并行拨号，择优保留连接
  endpoint, transport = await parallel_dial_and_select()

  # 2) 正式统计：新的全量 stats
  stats: Dict[str, StreamStats] = {}
  for sym in SYMBOLS:
    for ch in CHANNELS:
      stats[f"{ch}:{sym}"] = StreamStats(cap=8192)

  # 将最优连接的 listener 替换为新的 stats（重新订阅）
  subs = build_subs(SYMBOLS)

  # 断开并用同端点重连，避免 warmup 数据混入正式统计
  try:
    transport.disconnect()
  except Exception:
    pass

  transport, _listener = await ws_connect(
    lambda: OkxLatencyListener(subs, stats),
    endpoint,
    enable_auto_ping=True,
  )

  async def live_printer():
    while True:
      await asyncio.sleep(SUMMARY_EVERY_SEC)
      print_live(stats)

  live_task = asyncio.create_task(live_printer())

  # 3) 运行基准
  try:
    await asyncio.sleep(BENCH_DURATION_SEC)
  finally:
    live_task.cancel()
    try:
      await live_task
    except asyncio.CancelledError:
      pass
    report = make_markdown_report(stats, BENCH_DURATION_SEC, endpoint)
    print("\n" + report)
    transport.disconnect()


def main() -> None:
  try:
    asyncio.run(run())
  except KeyboardInterrupt:
    print("[shutdown] Interrupted by user")

if __name__ == "__main__":
  main()
