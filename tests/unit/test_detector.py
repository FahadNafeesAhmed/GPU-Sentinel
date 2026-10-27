"""Latency outlier detection, leak-trend detection and hard resource limits."""

from __future__ import annotations

import math

import pytest

from sentinel.config import LatencyPolicy, ResourceLimits
from sentinel.controller.detector import (
    FleetLatencyDetector,
    LatencyWindow,
    LeakDetector,
    TrendWindow,
    percentile,
    resource_verdict,
)
from sentinel.controller.state import Severity


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_percentile_nearest_rank():
    vals = sorted(float(i) for i in range(1, 101))
    assert percentile(vals, 50) == 50.0
    assert percentile(vals, 95) == 95.0
    assert percentile(vals, 99) == 99.0
    assert percentile(vals, 100) == 100.0
    assert percentile([7.0], 95) == 7.0
    assert math.isnan(percentile([], 50))


def test_latency_window_prunes_by_time():
    clk = FakeClock()
    w = LatencyWindow(10.0, clk)
    for i in range(5):
        clk.t = i
        w.add(10.0 * i)
    assert w.count() == 5
    clk.t = 13.0  # cutoff = 3 -> samples at t=0,1,2 drop
    assert w.values() == [30.0, 40.0]
    s = w.summary()
    assert s["n"] == 2 and s["p50"] == 30.0 and s["max"] == 40.0


def feed(det: FleetLatencyDetector, wid: str, values: list[float]) -> None:
    for v in values:
        det.record(wid, v)


def test_single_slow_worker_flagged_relative_to_peers():
    pol = LatencyPolicy(window_s=60, min_samples=10, p95_abs_ms=100, p95_fleet_ratio=3.0)
    det = FleetLatencyDetector(pol, FakeClock())
    feed(det, "w1", [20.0] * 50)
    feed(det, "w2", [25.0] * 50)
    feed(det, "w3", [900.0] * 50)
    v = det.evaluate(["w1", "w2", "w3"])
    assert v["w1"].severity == Severity.OK
    assert v["w2"].severity == Severity.OK
    assert v["w3"].severity == Severity.DEGRADE
    assert v["w3"].reason == "latency_p95"
    assert v["w3"].detail["threshold_ms"] == 100.0  # max(abs=100, 3 * median(20,25)=67.5)


def test_fleet_wide_slowdown_does_not_flag_everyone():
    pol = LatencyPolicy(window_s=60, min_samples=10, p95_abs_ms=100, p95_fleet_ratio=3.0)
    det = FleetLatencyDetector(pol, FakeClock())
    for wid in ("w1", "w2", "w3"):
        feed(det, wid, [500.0] * 30)  # all above the absolute floor, but equal
    v = det.evaluate(["w1", "w2", "w3"])
    assert all(x.severity == Severity.OK for x in v.values())


def test_lone_worker_uses_absolute_floor_only():
    pol = LatencyPolicy(window_s=60, min_samples=10, p95_abs_ms=100, p95_fleet_ratio=3.0)
    det = FleetLatencyDetector(pol, FakeClock())
    feed(det, "w1", [150.0] * 30)
    v = det.evaluate(["w1"])
    assert v["w1"].severity == Severity.DEGRADE
    assert v["w1"].detail["peer_median_ms"] is None


def test_min_samples_gate():
    pol = LatencyPolicy(window_s=60, min_samples=20, p95_abs_ms=10, p95_fleet_ratio=1.0)
    det = FleetLatencyDetector(pol, FakeClock())
    feed(det, "w1", [999.0] * 19)
    assert det.evaluate(["w1"])["w1"].severity == Severity.OK
    det.record("w1", 999.0)
    assert det.evaluate(["w1"])["w1"].severity == Severity.DEGRADE


def test_reset_clears_window():
    det = FleetLatencyDetector(LatencyPolicy(min_samples=1), FakeClock())
    feed(det, "w1", [5.0, 6.0])
    det.reset("w1")
    assert det.window("w1").count() == 0


def test_trend_slope_is_least_squares_per_minute():
    clk = FakeClock()
    w = TrendWindow(120.0, clk)
    for i in range(11):
        clk.t = float(i)  # 1 sample per second, +2 MB per second = +120 MB/min
        w.add(100.0 + 2.0 * i)
    assert w.slope_per_min() == pytest.approx(120.0)
    assert w.span_s() == 10.0 and w.last() == 120.0


def test_trend_flat_is_zero_and_window_prunes():
    clk = FakeClock()
    w = TrendWindow(5.0, clk)
    for i in range(20):
        clk.t = float(i)
        w.add(50.0)
    assert w.slope_per_min() == 0.0
    assert len(w) == 6  # t = 14..19


def test_leak_detector_fires_on_sustained_growth_only():
    clk = FakeClock()
    limits = ResourceLimits(leak_slope_mb_per_min=60.0, leak_window_s=10.0, leak_min_samples=5)
    det = LeakDetector(limits, clk)
    for i in range(4):
        clk.t = float(i)
        det.record("w1", 100.0 + 10.0 * i, None)
    assert det.evaluate("w1").severity == Severity.OK  # too few samples
    for i in range(4, 9):
        clk.t = float(i)
        det.record("w1", 100.0 + 10.0 * i, None)
    v = det.evaluate("w1")  # 10 MB/s = 600 MB/min over 8 s of a 10 s window
    assert v.severity == Severity.FATAL and v.reason == "rss_leak_trend"
    assert v.detail["slope_mb_per_min"] == pytest.approx(600.0)


def test_leak_detector_ignores_a_step_then_plateau():
    """The CUDA caching allocator reserves its pool in one step after start-up: a step
    followed by a flat line has a positive full-window slope but is not a leak."""
    clk = FakeClock()
    limits = ResourceLimits(leak_slope_mb_per_min=60.0, leak_window_s=10.0, leak_min_samples=5)
    det = LeakDetector(limits, clk)
    for i in range(10):
        clk.t = float(i)
        det.record("w1", 100.0, 54.0 if i < 3 else 202.0, uptime_s=999.0)
    v = det.evaluate("w1")
    assert v.severity == Severity.OK, v
    # ...whereas steady growth over the same window still fires.
    for i in range(10, 20):
        clk.t = float(i)
        det.record("w1", 100.0, 202.0 + 20.0 * (i - 9), uptime_s=999.0)
    v = det.evaluate("w1")
    assert v.reason == "gpu_leak_trend" and v.detail["recent_slope_mb_per_min"] > 30.0


def test_leak_detector_skips_warmup_samples():
    clk = FakeClock()
    limits = ResourceLimits(leak_slope_mb_per_min=60.0, leak_window_s=10.0, leak_min_samples=5,
                            leak_warmup_s=30.0)
    det = LeakDetector(limits, clk)
    for i in range(10):
        clk.t = float(i)
        det.record("w1", 100.0 + 50.0 * i, None, uptime_s=float(i))  # young worker
    assert det.evaluate("w1").severity == Severity.OK and len(det.rss.get("w1", [])) == 0
    for i in range(10, 20):
        clk.t = float(i)
        det.record("w1", 100.0 + 50.0 * i, None, uptime_s=30.0 + i)  # past warm-up
    assert det.evaluate("w1").reason == "rss_leak_trend"


def test_leak_detector_needs_window_coverage():
    clk = FakeClock()
    limits = ResourceLimits(leak_slope_mb_per_min=60.0, leak_window_s=60.0, leak_min_samples=5)
    det = LeakDetector(limits, clk)
    for i in range(10):
        clk.t = float(i)  # only 9 s of a 60 s window
        det.record("w1", 100.0 + 50.0 * i, None)
    assert det.evaluate("w1").severity == Severity.OK


def test_leak_detector_gpu_and_disabled():
    clk = FakeClock()
    det = LeakDetector(ResourceLimits(leak_slope_mb_per_min=1.0, leak_window_s=5.0,
                                      leak_min_samples=3), clk)
    for i in range(6):
        clk.t = float(i)
        det.record("w1", 10.0, 100.0 + 5.0 * i)
    assert det.evaluate("w1").reason == "gpu_leak_trend"
    off = LeakDetector(ResourceLimits(leak_slope_mb_per_min=None), clk)
    off.record("w1", 1e9, 1e9)
    assert off.evaluate("w1").severity == Severity.OK


def test_resource_verdict_limits():
    lim = ResourceLimits(rss_mb=100.0, gpu_mem_mb=500.0)
    assert resource_verdict(lim, {"rss_mb": 99.0}).severity == Severity.OK
    v = resource_verdict(lim, {"rss_mb": 101.0})
    assert v.severity == Severity.FATAL and v.reason == "rss_limit"
    v = resource_verdict(lim, {"rss_mb": 10.0, "gpu": {"proc_mem_mb": 600.0}})
    assert v.reason == "gpu_mem_limit"
    none = ResourceLimits(rss_mb=None, gpu_mem_mb=None)
    assert resource_verdict(none, {"rss_mb": 1e9, "gpu": {"proc_mem_mb": 1e9}}).severity == 0
    assert resource_verdict(lim, {"rss_mb": "garbage"}).severity == Severity.OK
