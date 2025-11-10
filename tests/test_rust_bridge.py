import unittest

import numpy as np

from quant.streaming import LatencyRing, TradeBuffer, WINDOWS_US


class LatencyRingTests(unittest.TestCase):
    def test_percentiles(self) -> None:
        ring = LatencyRing(8)
        for value in range(1, 9):
            ring.add(value)
        p50, p95 = ring.percentiles((50, 95)) or (None, None)
        self.assertAlmostEqual(p50, 4.5, places=6)
        self.assertAlmostEqual(p95, 7.65, places=6)


class TradeBufferTests(unittest.TestCase):
    def test_window_volumes(self) -> None:
        buf = TradeBuffer(4)
        now = 1_000_000
        buf.add_trade(now - 80_000, 3.0)
        buf.add_trade(now - 30_000, -2.0)
        buf.add_trade(now - 5_000, 1.0)

        out = np.zeros(WINDOWS_US.shape[0], dtype=np.float64)
        buf.window_volumes(now, WINDOWS_US, out)
        self.assertTrue(np.allclose(out, np.array([1.0, 3.0, 6.0])))


if __name__ == "__main__":
    unittest.main()
