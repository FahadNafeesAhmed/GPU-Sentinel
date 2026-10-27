"""Load generators and result summarisation.

OpenLoopLoad sends requests on a fixed schedule regardless of responses (an
"open" system, like real users), which is what makes availability-during-failure
measurable: a slow or failed request does not reduce the arrival rate.
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from sentinel.controller.detector import percentile


@dataclass
class Sample:
    t: float  # wall clock at send
    latency_ms: float
    status: int  # HTTP status; 0 = client timeout; -1 = connection error
    worker: str | None = None
    attempts: int = 1


@dataclass
class LoadResult:
    samples: list[Sample] = field(default_factory=list)
    started_at: float = 0.0
    finished_at: float = 0.0
    target_rps: float = 0.0

    @property
    def achieved_rps(self) -> float:
        d = self.finished_at - self.started_at
        return len(self.samples) / d if d > 0 else 0.0


async def _one(session: aiohttp.ClientSession, url: str, payload: dict[str, Any],
               timeout_s: float, out: list[Sample]) -> None:
    t0_wall = time.time()
    t0 = time.perf_counter()
    status, worker, attempts = -1, None, 1
    try:
        async with session.post(url, json=payload,
                                timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
            await r.read()
            status = r.status
            worker = r.headers.get("X-Sentinel-Worker")
            attempts = int(r.headers.get("X-Sentinel-Attempts", "1") or 1)
    except TimeoutError:
        status = 0
    except aiohttp.ClientError:
        status = -1
    out.append(Sample(t0_wall, (time.perf_counter() - t0) * 1000.0, status, worker, attempts))


class OpenLoopLoad:
    def __init__(self, session: aiohttp.ClientSession, url: str, rps: float, duration_s: float,
                 tokens: int = 16, timeout_s: float = 5.0) -> None:
        self.session, self.url, self.rps = session, url, rps
        self.duration_s, self.tokens, self.timeout_s = duration_s, tokens, timeout_s

    async def run(self) -> LoadResult:
        res = LoadResult(target_rps=self.rps, started_at=time.time())
        payload = {"tokens": self.tokens}
        tasks: list[asyncio.Task[None]] = []
        start = time.perf_counter()
        i = 0
        while True:
            due = start + i / self.rps
            if due - start >= self.duration_s:
                break
            now = time.perf_counter()
            if due > now:
                await asyncio.sleep(due - now)
            tasks.append(asyncio.create_task(
                _one(self.session, self.url, payload, self.timeout_s, res.samples)))
            i += 1
        if tasks:
            await asyncio.gather(*tasks)
        res.finished_at = time.time()
        res.samples.sort(key=lambda s: s.t)
        return res


class ClosedLoopLoad:
    """``concurrency`` clients each sending back-to-back: measures saturated throughput."""

    def __init__(self, session: aiohttp.ClientSession, url: str, concurrency: int,
                 duration_s: float, tokens: int = 16, timeout_s: float = 5.0) -> None:
        self.session, self.url, self.concurrency = session, url, concurrency
        self.duration_s, self.tokens, self.timeout_s = duration_s, tokens, timeout_s

    async def run(self) -> LoadResult:
        res = LoadResult(target_rps=math.nan, started_at=time.time())
        payload = {"tokens": self.tokens}
        stop = time.perf_counter() + self.duration_s

        async def client() -> None:
            while time.perf_counter() < stop:
                await _one(self.session, self.url, payload, self.timeout_s, res.samples)

        await asyncio.gather(*(client() for _ in range(self.concurrency)))
        res.finished_at = time.time()
        res.samples.sort(key=lambda s: s.t)
        return res


def summarize(samples: list[Sample], t_from: float | None = None,
              t_to: float | None = None) -> dict[str, Any]:
    sel = [s for s in samples
           if (t_from is None or s.t >= t_from) and (t_to is None or s.t <= t_to)]
    n = len(sel)
    if n == 0:
        return {"n": 0}
    ok = [s for s in sel if s.status == 200]
    lat = sorted(s.latency_ms for s in ok)
    by_status: dict[str, int] = {}
    for s in sel:
        key = {0: "timeout", -1: "conn_error"}.get(s.status, str(s.status))
        by_status[key] = by_status.get(key, 0) + 1
    retried = sum(1 for s in sel if s.attempts > 1)
    out: dict[str, Any] = {
        "n": n, "ok": len(ok), "failed": n - len(ok),
        "success_pct": round(100.0 * len(ok) / n, 3),
        "retried": retried, "retried_ok": sum(1 for s in ok if s.attempts > 1),
        "by_status": by_status,
    }
    if lat:
        out.update({
            "p50_ms": round(percentile(lat, 50), 2), "p95_ms": round(percentile(lat, 95), 2),
            "p99_ms": round(percentile(lat, 99), 2), "max_ms": round(lat[-1], 2),
            "mean_ms": round(sum(lat) / len(lat), 2),
        })
    if t_from is not None and t_to is not None and t_to > t_from:
        out["window_s"] = round(t_to - t_from, 3)
    return out
