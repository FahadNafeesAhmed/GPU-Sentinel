"""Passive degradation detectors fed by gateway observations and probe bodies.

FleetLatencyDetector  flags a worker whose p95 latency is high *relative to its
                      peers* (so fleet-wide load does not trip every worker at once)
                      and above an absolute floor.
LeakDetector          least-squares slope of RSS / GPU memory over a window; fires
                      before a hard limit is hit so the restart happens off-peak.
"""

from __future__ import annotations

import math
import time
from collections import deque
from collections.abc import Callable, Iterable

from sentinel.config import LatencyPolicy, ResourceLimits
from sentinel.controller.state import Verdict


def percentile(values: list[float], q: float) -> float:
    """Nearest-rank percentile on a pre-sorted list, q in [0, 100]."""
    if not values:
        return math.nan
    k = max(0, min(len(values) - 1, math.ceil(q / 100.0 * len(values)) - 1))
    return values[k]


class LatencyWindow:
    def __init__(self, window_s: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.window_s = window_s
        self.clock = clock
        self._samples: deque[tuple[float, float]] = deque()

    def add(self, latency_ms: float) -> None:
        self._samples.append((self.clock(), latency_ms))

    def prune(self) -> None:
        cutoff = self.clock() - self.window_s
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def values(self) -> list[float]:
        self.prune()
        return sorted(v for _, v in self._samples)

    def count(self) -> int:
        self.prune()
        return len(self._samples)

    def summary(self) -> dict[str, float | int]:
        vals = self.values()
        if not vals:
            return {"n": 0}
        return {
            "n": len(vals),
            "p50": percentile(vals, 50),
            "p95": percentile(vals, 95),
            "p99": percentile(vals, 99),
            "max": vals[-1],
        }


class FleetLatencyDetector:
    def __init__(self, policy: LatencyPolicy, clock: Callable[[], float] = time.monotonic) -> None:
        self.policy = policy
        self.clock = clock
        self.windows: dict[str, LatencyWindow] = {}

    def window(self, worker_id: str) -> LatencyWindow:
        w = self.windows.get(worker_id)
        if w is None:
            w = self.windows[worker_id] = LatencyWindow(self.policy.window_s, self.clock)
        return w

    def record(self, worker_id: str, latency_ms: float) -> None:
        self.window(worker_id).add(latency_ms)

    def reset(self, worker_id: str) -> None:
        self.windows.pop(worker_id, None)

    def evaluate(
        self, worker_ids: Iterable[str], sticky: Iterable[str] = ()
    ) -> dict[str, Verdict]:
        """Verdict per worker. ``sticky`` = workers already DEGRADED: without fresh
        samples (they are out of rotation) there is no evidence of recovery, so the
        DEGRADE verdict is held until the supervisor recycles them."""
        ids = list(worker_ids)
        hold = set(sticky)
        p95: dict[str, float] = {}
        for wid in ids:
            vals = self.window(wid).values()
            if len(vals) >= self.policy.min_samples:
                p95[wid] = percentile(vals, 95)
        out: dict[str, Verdict] = {}
        for wid in ids:
            if wid not in p95:
                n = self.window(wid).count()
                if wid in hold:
                    out[wid] = Verdict.degrade("latency_no_recovery_evidence", samples=n)
                else:
                    out[wid] = Verdict.ok("latency", samples=n)
                continue
            peers = sorted(v for k, v in p95.items() if k != wid)
            threshold = self.policy.p95_abs_ms
            peer_median = percentile(peers, 50) if peers else math.nan
            if peers:
                threshold = max(threshold, self.policy.p95_fleet_ratio * peer_median)
            if p95[wid] > threshold:
                out[wid] = Verdict.degrade(
                    "latency_p95", p95_ms=round(p95[wid], 1), threshold_ms=round(threshold, 1),
                    peer_median_ms=None if math.isnan(peer_median) else round(peer_median, 1),
                )
            else:
                out[wid] = Verdict.ok("latency", p95_ms=round(p95[wid], 1))
        return out


class TrendWindow:
    """Fixed-time window of (t, value) with a least-squares slope in value/minute."""

    def __init__(self, window_s: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.window_s = window_s
        self.clock = clock
        self._pts: deque[tuple[float, float]] = deque()

    def add(self, value: float) -> None:
        now = self.clock()
        self._pts.append((now, value))
        cutoff = now - self.window_s
        while self._pts and self._pts[0][0] < cutoff:
            self._pts.popleft()

    def clear(self) -> None:
        self._pts.clear()

    def __len__(self) -> int:
        return len(self._pts)

    def span_s(self) -> float:
        return self._pts[-1][0] - self._pts[0][0] if len(self._pts) >= 2 else 0.0

    def slope_per_min(self) -> float:
        n = len(self._pts)
        if n < 2:
            return 0.0
        t0 = self._pts[0][0]
        xs = [t - t0 for t, _ in self._pts]
        ys = [v for _, v in self._pts]
        mx = sum(xs) / n
        my = sum(ys) / n
        sxx = sum((x - mx) ** 2 for x in xs)
        if sxx == 0:
            return 0.0
        sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True))
        return (sxy / sxx) * 60.0

    def last(self) -> float | None:
        return self._pts[-1][1] if self._pts else None


class LeakDetector:
    def __init__(self, limits: ResourceLimits, clock: Callable[[], float] = time.monotonic) -> None:
        self.limits = limits
        self.clock = clock
        self.rss: dict[str, TrendWindow] = {}
        self.gpu: dict[str, TrendWindow] = {}

    def _win(self, store: dict[str, TrendWindow], wid: str) -> TrendWindow:
        w = store.get(wid)
        if w is None:
            w = store[wid] = TrendWindow(self.limits.leak_window_s, self.clock)
        return w

    def record(self, worker_id: str, rss_mb: float | None, gpu_mb: float | None) -> None:
        if rss_mb is not None:
            self._win(self.rss, worker_id).add(rss_mb)
        if gpu_mb is not None:
            self._win(self.gpu, worker_id).add(gpu_mb)

    def reset(self, worker_id: str) -> None:
        self.rss.pop(worker_id, None)
        self.gpu.pop(worker_id, None)

    def evaluate(self, worker_id: str) -> Verdict:
        thr = self.limits.leak_slope_mb_per_min
        if thr is None:
            return Verdict.ok("resource")
        for kind, store in (("rss", self.rss), ("gpu", self.gpu)):
            w = store.get(worker_id)
            if w is None or len(w) < self.limits.leak_min_samples:
                continue
            if w.span_s() < 0.8 * self.limits.leak_window_s:
                continue
            slope = w.slope_per_min()
            if slope > thr:
                return Verdict.fatal(
                    f"{kind}_leak_trend", "resource",
                    slope_mb_per_min=round(slope, 1), threshold_mb_per_min=thr,
                    current_mb=round(w.last() or 0.0, 1), window_s=round(w.span_s(), 1),
                )
        return Verdict.ok("resource")


def resource_verdict(limits: ResourceLimits, status: dict) -> Verdict:
    """Hard-limit check on a worker's self-reported /health body."""
    rss = status.get("rss_mb")
    if limits.rss_mb is not None and isinstance(rss, int | float) and rss > limits.rss_mb:
        return Verdict.fatal("rss_limit", "resource", rss_mb=round(rss, 1), limit_mb=limits.rss_mb)
    gpu = status.get("gpu") or {}
    gpu_mb = gpu.get("proc_mem_mb")
    if (
        limits.gpu_mem_mb is not None
        and isinstance(gpu_mb, int | float)
        and gpu_mb > limits.gpu_mem_mb
    ):
        return Verdict.fatal(
            "gpu_mem_limit", "resource", gpu_mem_mb=round(gpu_mb, 1), limit_mb=limits.gpu_mem_mb
        )
    return Verdict.ok("resource")
