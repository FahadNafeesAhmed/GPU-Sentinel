"""Request gateway: routes /infer to healthy workers, retries failures elsewhere,
and reports what it sees back to the supervisor (passive health checking).

Only HEALTHY workers (optionally DEGRADED) receive traffic. A connection error,
timeout or 5xx from a worker is retried on a *different* worker within the
request's remaining deadline. 503 (load shedding) is retried but not counted
against the worker's health; 4xx is passed through untouched.
"""

from __future__ import annotations

import random
import time

import aiohttp
from aiohttp import web

from sentinel.config import GatewayConfig
from sentinel.controller.supervisor import ManagedWorker, Supervisor
from sentinel.logging_setup import get_logger
from sentinel.metrics import Registry

log = get_logger("sentinel.gateway")
_RETRYABLE_CLIENT_ERRORS = (
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientOSError,
    aiohttp.ClientPayloadError,
    aiohttp.ClientResponseError,
)


class Gateway:
    def __init__(
        self,
        cfg: GatewayConfig,
        supervisor: Supervisor,
        session: aiohttp.ClientSession,
        registry: Registry,
    ) -> None:
        self.cfg = cfg
        self.sup = supervisor
        self.session = session
        self._rr = 0
        self.requests_total = 0
        self.requests_ok = 0
        self.m_requests = registry.counter(
            "sentinel_gateway_requests_total", "Gateway requests by final outcome", ["outcome"]
        )
        self.m_latency = registry.histogram(
            "sentinel_gateway_request_seconds", "Gateway end-to-end latency (incl. retries)"
        )
        self.m_attempts = registry.counter(
            "sentinel_gateway_attempts_total", "Upstream attempts by worker and result",
            ["worker", "result"],
        )
        self.m_inflight = registry.gauge(
            "sentinel_gateway_inflight", "In-flight upstream requests", ["worker"]
        )

    # ------------------------------------------------------------------ routing

    def pick(self, candidates: list[ManagedWorker]) -> ManagedWorker:
        if self.cfg.strategy == "round_robin":
            self._rr += 1
            return candidates[self._rr % len(candidates)]
        lo = min(len(c.inflight) for c in candidates)
        best = [c for c in candidates if len(c.inflight) == lo]
        return best[0] if len(best) == 1 else random.choice(best)

    # ------------------------------------------------------------------ handler

    async def infer(self, request: web.Request) -> web.Response:
        body = await request.read()
        t_start = time.perf_counter()
        deadline = t_start + self.cfg.request_timeout_s
        tried: set[str] = set()
        attempts = 0
        last_error = "no_routable_workers"
        self.requests_total += 1
        try:
            while attempts <= self.cfg.max_retries:
                candidates = [mw for mw in self.sup.routable() if mw.id not in tried]
                if not candidates:
                    break
                remaining = deadline - time.perf_counter()
                if remaining <= 0.01:
                    last_error = "deadline_exceeded"
                    break
                mw = self.pick(candidates)
                tried.add(mw.id)
                attempts += 1
                rid = mw.begin_request()
                self.m_inflight.labels(worker=mw.id).set(len(mw.inflight))
                t0 = time.perf_counter()
                try:
                    async with self.session.post(
                        f"{mw.spec.base_url}/infer", data=body,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=remaining),
                    ) as resp:
                        payload = await resp.read()
                        ms = (time.perf_counter() - t0) * 1000.0
                        if resp.status == 200:
                            self.sup.record_request(mw.id, ms, "ok")
                            self.m_attempts.labels(worker=mw.id, result="ok").inc()
                            self.requests_ok += 1
                            self.m_requests.labels(outcome="ok").inc()
                            return web.Response(
                                body=payload, content_type="application/json",
                                headers={"X-Sentinel-Worker": mw.id,
                                         "X-Sentinel-Attempts": str(attempts)},
                            )
                        if resp.status in (503, 429):
                            self.sup.record_request(mw.id, ms, "shed")
                            self.m_attempts.labels(worker=mw.id, result="shed").inc()
                            last_error = f"http_{resp.status}"
                            continue
                        if resp.status >= 500:
                            self.sup.record_request(mw.id, ms, "error", f"http_{resp.status}")
                            self.m_attempts.labels(worker=mw.id, result="5xx").inc()
                            last_error = f"http_{resp.status}"
                            continue
                        self.m_attempts.labels(worker=mw.id, result="4xx").inc()
                        self.m_requests.labels(outcome="client_error").inc()
                        return web.Response(body=payload, status=resp.status,
                                            content_type="application/json",
                                            headers={"X-Sentinel-Worker": mw.id})
                except TimeoutError:
                    ms = (time.perf_counter() - t0) * 1000.0
                    self.sup.record_request(mw.id, ms, "error", "timeout")
                    self.m_attempts.labels(worker=mw.id, result="timeout").inc()
                    last_error = "timeout"
                except _RETRYABLE_CLIENT_ERRORS as exc:
                    ms = (time.perf_counter() - t0) * 1000.0
                    self.sup.record_request(mw.id, ms, "error", "conn_error")
                    self.m_attempts.labels(worker=mw.id, result="conn_error").inc()
                    last_error = f"conn_error:{type(exc).__name__}"
                finally:
                    mw.end_request(rid)
                    self.m_inflight.labels(worker=mw.id).set(len(mw.inflight))
            outcome = "no_workers" if attempts == 0 else "failed"
            self.m_requests.labels(outcome=outcome).inc()
            return web.json_response(
                {"error": outcome, "last_error": last_error, "attempts": attempts},
                status=503, headers={"Retry-After": "0", "X-Sentinel-Attempts": str(attempts)},
            )
        finally:
            self.m_latency.observe(time.perf_counter() - t_start)

    async def healthz(self, _: web.Request) -> web.Response:
        n = len(self.sup.routable())
        return web.json_response({"ok": n > 0, "routable": n}, status=200 if n > 0 else 503)

    def build_app(self) -> web.Application:
        app = web.Application(client_max_size=2 * 1024 * 1024)
        app.add_routes([web.post("/infer", self.infer), web.get("/healthz", self.healthz)])
        return app
