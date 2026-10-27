"""Integration fixtures: a real controller with real worker subprocesses on free ports."""

from __future__ import annotations

import asyncio
import os
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp
import psutil
import pytest
import pytest_asyncio

from sentinel.config import (
    GatewayConfig,
    LatencyPolicy,
    ProbeConfig,
    RecoveryConfig,
    ResourceLimits,
    SentinelConfig,
    WorkersConfig,
    WorkerSpec,
)
from sentinel.controller.main import Controller
from sentinel.logging_setup import setup_logging

pytestmark = pytest.mark.integration


def free_ports(n: int) -> list[int]:
    socks, ports = [], []
    for _ in range(n):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


def fast_config(tmp_path, workers: int = 3, **overrides: Any) -> SentinelConfig:
    """Tight timings so fault tests finish in seconds, same policy shape as production."""
    ports = free_ports(workers + 2)
    cfg = SentinelConfig(
        api_host="127.0.0.1", api_port=ports[0], log_level="INFO", log_format="text",
        data_dir=str(tmp_path / "data"),
        probe=ProbeConfig(interval_s=0.25, timeout_s=0.5, unhealthy_threshold=3,
                          healthy_threshold=2, startup_grace_s=10.0, startup_interval_s=0.1),
        limits=ResourceLimits(rss_mb=1536.0, leak_slope_mb_per_min=60.0, leak_window_s=3.0,
                              leak_min_samples=6, leak_warmup_s=0.0),
        latency=LatencyPolicy(window_s=5.0, min_samples=10, p95_abs_ms=150.0,
                              p95_fleet_ratio=3.0, degraded_consecutive=2,
                              recycle_degraded_after_s=1.5, stuck_request_s=3.0),
        recovery=RecoveryConfig(backoff_base_s=0.1, backoff_cap_s=0.5, jitter="full",
                                max_restarts=5, restart_window_s=60.0, drain_grace_s=1.0,
                                term_grace_s=1.0, stable_reset_s=30.0),
        gateway=GatewayConfig(host="127.0.0.1", port=ports[1], max_retries=2,
                              request_timeout_s=3.0),
        workers=WorkersConfig(
            count=workers, backend="subprocess", engine="cpu", concurrency=2,
            specs=[WorkerSpec(id=f"w{i + 1}", port=ports[2 + i]) for i in range(workers)],
        ),
    )
    for key, val in overrides.items():
        section, _, attr = key.partition("__")
        if attr:
            setattr(getattr(cfg, section), attr, val)
        else:
            setattr(cfg, section, val)
    return cfg


@dataclass
class Harness:
    ctl: Controller
    cfg: SentinelConfig
    http: aiohttp.ClientSession

    @property
    def api(self) -> str:
        return self.ctl.api_url

    @property
    def gateway(self) -> str:
        return self.ctl.gateway_url

    def worker_url(self, wid: str) -> str:
        return next(s.base_url for s in self.cfg.workers.resolved_specs() if s.id == wid)

    async def state(self) -> dict[str, Any]:
        async with self.http.get(f"{self.api}/api/state") as r:
            return await r.json()

    async def worker(self, wid: str) -> dict[str, Any]:
        return next(w for w in (await self.state())["workers"] if w["id"] == wid)

    async def incidents(self) -> list[dict[str, Any]]:
        async with self.http.get(f"{self.api}/api/incidents?limit=100") as r:
            return (await r.json())["incidents"]

    async def events(self) -> list[dict[str, Any]]:
        async with self.http.get(f"{self.api}/api/events?limit=2000") as r:
            return (await r.json())["events"]

    async def chaos(self, wid: str, cmd: str, **params: Any) -> dict[str, Any]:
        async with self.http.post(f"{self.worker_url(wid)}/chaos/{cmd}", json=params) as r:
            return await r.json()

    async def infer(self, tokens: int = 8) -> tuple[int, dict[str, str]]:
        try:
            async with self.http.post(f"{self.gateway}/infer", json={"tokens": tokens},
                                      timeout=aiohttp.ClientTimeout(total=5)) as r:
                await r.read()
                return r.status, dict(r.headers)
        except Exception as exc:
            return 599, {"error": type(exc).__name__}

    async def wait_for(
        self, pred: Callable[[], Awaitable[bool]], timeout: float, what: str = "condition"
    ) -> float:
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            if await pred():
                return time.monotonic() - t0
            await asyncio.sleep(0.02)
        raise AssertionError(f"timed out after {timeout}s waiting for {what}")

    async def wait_state(self, wid: str, state: str, timeout: float = 15.0) -> float:
        async def _p() -> bool:
            return (await self.worker(wid))["state"] == state

        return await self.wait_for(_p, timeout, f"{wid} -> {state}")

    async def kill_and_wait_replaced(self, wid: str, timeout: float = 15.0) -> int:
        """Kill the worker's process and wait until a *new* pid is healthy."""
        old = (await self.worker(wid))["pid"]
        assert old, f"{wid} has no live pid"
        psutil.Process(old).kill()

        async def _p() -> bool:
            w = await self.worker(wid)
            return w["state"] == "healthy" and w["pid"] not in (None, old)

        await self.wait_for(_p, timeout, f"{wid} replaced after kill")
        return old

    async def wait_incident(self, wid: str, status: str | None = None, timeout: float = 15.0,
                            after: float = 0.0) -> dict[str, Any]:
        found: dict[str, Any] = {}

        async def _p() -> bool:
            for inc in await self.incidents():
                if inc["worker"] == wid and inc["opened_at"] >= after and (
                    status is None or inc["status"] == status
                ):
                    found.update(inc)
                    return True
            return False

        await self.wait_for(_p, timeout, f"incident for {wid} status={status}")
        return found

    async def load(self, seconds: float, concurrency: int = 8, tokens: int = 8) -> list[int]:
        """Closed-loop load: ``concurrency`` clients hammering the gateway. Returns statuses."""
        stop = time.monotonic() + seconds
        statuses: list[int] = []

        async def client() -> None:
            while time.monotonic() < stop:
                st, _ = await self.infer(tokens)
                statuses.append(st)

        await asyncio.gather(*(client() for _ in range(concurrency)))
        return statuses


@pytest.fixture(scope="session", autouse=True)
def _logging() -> None:
    setup_logging(os.environ.get("SENTINEL_TEST_LOG", "WARNING"), "text")


@pytest_asyncio.fixture
async def make_harness(tmp_path) -> AsyncIterator[Callable[..., Awaitable[Harness]]]:
    created: list[Harness] = []

    async def _make(workers: int = 3, ready_timeout: float = 30.0, **overrides: Any) -> Harness:
        cfg = fast_config(tmp_path, workers, **overrides)
        ctl = Controller(cfg)
        await ctl.start()
        http = aiohttp.ClientSession()
        h = Harness(ctl, cfg, http)
        created.append(h)
        if ready_timeout:
            assert await ctl.wait_ready(ready_timeout), "workers did not become healthy"
        return h

    yield _make
    for h in created:
        await h.http.close()
        await h.ctl.stop()


@pytest_asyncio.fixture
async def harness(make_harness) -> Harness:
    return await make_harness()
