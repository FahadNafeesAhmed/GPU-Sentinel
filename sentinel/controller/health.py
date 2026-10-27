"""Active HTTP health probing with a hard timeout per probe."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from sentinel.config import WorkerSpec
from sentinel.controller.state import Verdict


@dataclass
class ProbeResult:
    verdict: Verdict
    latency_ms: float
    status: dict[str, Any] | None = field(default=None)


class Prober:
    def __init__(self, session: aiohttp.ClientSession, timeout_s: float) -> None:
        self.session = session
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)

    async def probe(self, spec: WorkerSpec) -> ProbeResult:
        t0 = time.perf_counter()
        try:
            async with self.session.get(f"{spec.base_url}/health", timeout=self.timeout) as resp:
                ms = (time.perf_counter() - t0) * 1000.0
                if resp.status == 200:
                    try:
                        body = await resp.json()
                    except Exception:
                        return ProbeResult(Verdict.fail("bad_body"), ms)
                    if not isinstance(body, dict):
                        return ProbeResult(Verdict.fail("bad_body"), ms)
                    return ProbeResult(Verdict.ok("active", latency_ms=round(ms, 1)), ms, body)
                return ProbeResult(Verdict.fail(f"http_{resp.status}"), ms)
        except TimeoutError:
            return ProbeResult(Verdict.fail("timeout"), (time.perf_counter() - t0) * 1000.0)
        except aiohttp.ClientConnectorError:
            return ProbeResult(Verdict.fail("conn_refused"), (time.perf_counter() - t0) * 1000.0)
        except (aiohttp.ClientError, OSError) as exc:
            return ProbeResult(
                Verdict.fail("conn_error", error=type(exc).__name__),
                (time.perf_counter() - t0) * 1000.0,
            )

    async def drain(self, spec: WorkerSpec, grace_s: float) -> bool:
        try:
            async with self.session.post(
                f"{spec.base_url}/drain", json={"grace_s": grace_s},
                timeout=aiohttp.ClientTimeout(total=1.0),
            ) as resp:
                return resp.status == 200
        except Exception:
            return False
