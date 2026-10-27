"""Inference worker HTTP service.

Endpoints
  GET  /health         liveness + self-reported resource stats (always 200 unless draining)
  GET  /ready          readiness (503 while draining/unready/overloaded)
  POST /infer          {"tokens": int} -> runs the engine on a thread pool
  GET  /metrics        Prometheus text exposition
  POST /chaos/{cmd}    fault injection (see chaos.py)
  POST /drain          stop accepting work, finish in-flight requests, exit 0
  GET  /info           static identity
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from aiohttp import web

from sentinel import __version__
from sentinel.logging_setup import get_logger, setup_logging
from sentinel.metrics import CONTENT_TYPE, Registry
from sentinel.worker.chaos import ChaosController
from sentinel.worker.engine import Engine, make_engine
from sentinel.worker.resources import ResourceSampler

MAX_TOKENS = 4096


class WorkerService:
    def __init__(
        self,
        worker_id: str,
        engine: Engine,
        *,
        concurrency: int = 4,
        max_inflight: int | None = None,
    ) -> None:
        self.id = worker_id
        self.engine = engine
        self.concurrency = max(1, concurrency)
        self.max_inflight = max_inflight or self.concurrency * 4
        self.started_at = time.time()
        self.inflight = 0
        self.requests_total = 0
        self.errors_total = 0
        self.shed_total = 0
        self.last_error: str | None = None
        self.draining = False
        self.chaos = ChaosController(engine)
        self.sampler = ResourceSampler(engine_memory_mb=getattr(engine, "memory_mb", None))
        self.executor = ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix="infer")
        self.log = get_logger("sentinel.worker", worker=worker_id)

        self.registry = Registry()
        self.m_requests = self.registry.counter(
            "worker_requests_total", "Inference requests by outcome", ["worker", "outcome"]
        )
        self.m_latency = self.registry.histogram(
            "worker_inference_seconds", "End-to-end handler latency", ["worker"]
        )
        self.m_inflight = self.registry.gauge("worker_inflight", "In-flight requests", ["worker"])
        self.m_rss = self.registry.gauge("worker_rss_bytes", "Resident set size", ["worker"])

    # ------------------------------------------------------------------ handlers

    def status(self) -> dict[str, Any]:
        res = self.sampler.sample()
        self.m_rss.labels(worker=self.id).set(res["rss_mb"] * 1024 * 1024)
        return {
            "status": "draining" if self.draining else "ok",
            "worker": self.id,
            "engine": self.engine.name,
            "device": self.engine.device,
            "version": __version__,
            "uptime_s": round(time.time() - self.started_at, 3),
            "inflight": self.inflight,
            "max_inflight": self.max_inflight,
            "requests_total": self.requests_total,
            "errors_total": self.errors_total,
            "shed_total": self.shed_total,
            "last_error": self.last_error,
            "chaos": self.chaos.state.snapshot(),
            **res,
        }

    async def health(self, _: web.Request) -> web.Response:
        body = self.status()
        return web.json_response(body, status=503 if self.draining else 200)

    async def ready(self, _: web.Request) -> web.Response:
        if self.draining:
            return web.json_response({"ready": False, "reason": "draining"}, status=503)
        if self.chaos.state.unready:
            return web.json_response({"ready": False, "reason": "chaos:unready"}, status=503)
        if self.inflight >= self.max_inflight:
            return web.json_response({"ready": False, "reason": "overloaded"}, status=503)
        return web.json_response({"ready": True, "worker": self.id})

    async def info(self, _: web.Request) -> web.Response:
        return web.json_response(
            {"worker": self.id, "engine": self.engine.name, "device": self.engine.device,
             "pid": os.getpid(), "version": __version__}
        )

    async def metrics(self, _: web.Request) -> web.Response:
        self.m_inflight.labels(worker=self.id).set(self.inflight)
        return web.Response(body=self.registry.render().encode("utf-8"),
                            headers={"Content-Type": CONTENT_TYPE})

    async def infer(self, request: web.Request) -> web.Response:
        t0 = time.perf_counter()
        if self.draining:
            return web.json_response({"error": "draining"}, status=503,
                                     headers={"Retry-After": "0"})
        if self.inflight >= self.max_inflight:
            self.shed_total += 1
            self.m_requests.labels(worker=self.id, outcome="shed").inc()
            return web.json_response({"error": "overloaded"}, status=503,
                                     headers={"Retry-After": "0"})
        self.inflight += 1
        self.requests_total += 1
        outcome = "ok"
        try:
            try:
                body = await request.json() if request.can_read_body else {}
            except ValueError:
                return web.json_response({"error": "invalid json"}, status=400)
            tokens = int(body.get("tokens", 32)) if isinstance(body, dict) else 32
            tokens = min(MAX_TOKENS, max(1, tokens))

            st = self.chaos.state
            if st.slow_ms > 0:
                await asyncio.sleep(st.slow_ms / 1000.0)
            if st.error_rate > 0 and random.random() < st.error_rate:
                raise RuntimeError("chaos: injected failure")
            self.chaos.on_request()

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(self.executor, self.engine.infer, tokens)
            total_ms = (time.perf_counter() - t0) * 1000.0
            return web.json_response(
                {"worker": self.id, "tokens": tokens, "latency_ms": round(total_ms, 3),
                 "compute_ms": round(result["compute_ms"], 3), "checksum": result["checksum"]}
            )
        except Exception as exc:
            outcome = "error"
            self.errors_total += 1
            self.last_error = f"{type(exc).__name__}: {exc}"[:200]
            self.log.warning("inference_failed", error=self.last_error)
            return web.json_response({"error": self.last_error, "worker": self.id}, status=500)
        finally:
            self.inflight -= 1
            self.m_requests.labels(worker=self.id, outcome=outcome).inc()
            self.m_latency.labels(worker=self.id).observe(time.perf_counter() - t0)

    async def chaos_cmd(self, request: web.Request) -> web.Response:
        cmd = request.match_info["cmd"]
        try:
            params = await request.json() if request.can_read_body else {}
        except ValueError:
            params = {}
        try:
            state = self.chaos.apply(cmd, params or {})
        except ValueError as exc:
            return web.json_response({"error": str(exc)}, status=400)
        self.log.warning("chaos_applied", command=cmd, params=params)
        return web.json_response({"applied": cmd, "chaos": state})

    async def drain(self, request: web.Request) -> web.Response:
        try:
            params = await request.json() if request.can_read_body else {}
        except ValueError:
            params = {}
        grace = float((params or {}).get("grace_s", 5.0))
        if not self.draining:
            self.draining = True
            self.log.info("drain_started", inflight=self.inflight, grace_s=grace)
            asyncio.get_running_loop().create_task(self._exit_when_drained(grace))
        return web.json_response({"draining": True, "inflight": self.inflight})

    async def _exit_when_drained(self, grace: float) -> None:
        deadline = time.monotonic() + grace
        while self.inflight > 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        self.log.info("drain_complete", inflight_abandoned=self.inflight)
        await asyncio.sleep(0.05)  # let the /drain response flush
        os._exit(0)

    # ------------------------------------------------------------------ wiring

    def build_app(self) -> web.Application:
        app = web.Application(client_max_size=1024 * 1024)
        app.add_routes(
            [
                web.get("/health", self.health),
                web.get("/ready", self.ready),
                web.get("/info", self.info),
                web.get("/metrics", self.metrics),
                web.post("/infer", self.infer),
                web.post("/chaos/{cmd}", self.chaos_cmd),
                web.post("/drain", self.drain),
            ]
        )
        return app


def add_worker_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--id", required=True, help="worker id (e.g. w1)")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--engine", default="cpu", choices=["cpu", "torch"])
    p.add_argument("--dim", type=int, default=None, help="matrix size (cpu=128, torch=1024)")
    p.add_argument("--concurrency", type=int, default=4, help="inference threads")
    p.add_argument("--max-inflight", type=int, default=None, help="queue bound (default 4x)")
    p.add_argument("--startup-delay-s", type=float, default=0.0,
                   help="sleep before serving (simulates model load time)")
    p.add_argument("--log-format", default="text", choices=["json", "text"])
    p.add_argument("--log-level", default="INFO")


def run_worker(args: argparse.Namespace) -> None:
    setup_logging(args.log_level, args.log_format)
    log = get_logger("sentinel.worker", worker=args.id)
    t0 = time.perf_counter()
    if args.startup_delay_s > 0:
        time.sleep(args.startup_delay_s)
    engine = make_engine(args.engine, args.dim)
    svc = WorkerService(args.id, engine, concurrency=args.concurrency,
                        max_inflight=args.max_inflight)
    log.info("worker_starting", port=args.port, engine=engine.name, device=engine.device,
             pid=os.getpid(), engine_init_ms=round((time.perf_counter() - t0) * 1000, 1))
    web.run_app(svc.build_app(), host=args.host, port=args.port, print=None, access_log=None,
                shutdown_timeout=2.0)
