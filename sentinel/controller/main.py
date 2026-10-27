"""Controller runtime: wires config -> backend -> supervisor -> gateway -> API."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys
import time
from pathlib import Path

import aiohttp
from aiohttp import web

from sentinel.config import SentinelConfig, load_config
from sentinel.controller.api import ControlApi
from sentinel.controller.backends import make_backend
from sentinel.controller.gateway import Gateway
from sentinel.controller.incidents import IncidentLog
from sentinel.controller.state import WorkerState
from sentinel.controller.supervisor import Supervisor
from sentinel.logging_setup import get_logger, setup_logging
from sentinel.metrics import Registry

log = get_logger("sentinel.controller")


class Controller:
    """In-process runtime. Used by the CLI, the tests and the benchmark harness."""

    def __init__(self, cfg: SentinelConfig) -> None:
        self.cfg = cfg
        self.session: aiohttp.ClientSession | None = None
        self.registry = Registry()
        self.incidents: IncidentLog | None = None
        self.supervisor: Supervisor | None = None
        self.gateway: Gateway | None = None
        self.api: ControlApi | None = None
        self._runners: list[web.AppRunner] = []

    @property
    def gateway_url(self) -> str:
        return f"http://{self.cfg.gateway.host}:{self.cfg.gateway.port}"

    @property
    def api_url(self) -> str:
        return f"http://{self.cfg.api_host}:{self.cfg.api_port}"

    async def start(self) -> None:
        Path(self.cfg.data_dir).mkdir(parents=True, exist_ok=True)
        connector = aiohttp.TCPConnector(limit=0, enable_cleanup_closed=True)
        self.session = aiohttp.ClientSession(connector=connector)
        self.incidents = IncidentLog(self.cfg.incident_log_path)
        backend = make_backend(self.cfg.workers, self.cfg.data_dir)
        self.supervisor = Supervisor(self.cfg, backend, self.incidents, self.registry, self.session)
        self.gateway = Gateway(self.cfg.gateway, self.supervisor, self.session, self.registry)
        self.api = ControlApi(
            self.cfg, self.supervisor, self.gateway, self.incidents, self.registry
        )
        await self._serve(self.gateway.build_app(), self.cfg.gateway.host, self.cfg.gateway.port)
        await self._serve(self.api.build_app(), self.cfg.api_host, self.cfg.api_port)
        self.incidents.emit("controller_started", gateway=self.gateway_url, api=self.api_url,
                            workers=[s.id for s in self.cfg.workers.resolved_specs()])
        await self.supervisor.start()
        log.info("controller_started", gateway=self.gateway_url, api=self.api_url,
                 dashboard=self.api_url + "/")

    async def _serve(self, app: web.Application, host: str, port: int) -> None:
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        await web.TCPSite(runner, host, port, reuse_address=True).start()
        self._runners.append(runner)

    async def wait_ready(self, timeout_s: float = 30.0, min_healthy: int | None = None) -> bool:
        assert self.supervisor is not None
        need = min_healthy if min_healthy is not None else len(self.supervisor.workers)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            healthy = sum(
                1 for mw in self.supervisor.workers.values() if mw.state == WorkerState.HEALTHY
            )
            if healthy >= need:
                return True
            await asyncio.sleep(0.05)
        return False

    async def stop(self) -> None:
        if self.supervisor is not None:
            await self.supervisor.stop()
        for runner in self._runners:
            with contextlib.suppress(Exception):
                await runner.cleanup()
        self._runners.clear()
        if self.session is not None:
            await self.session.close()
        if self.incidents is not None:
            self.incidents.emit("controller_stopped")
            self.incidents.close_file()


async def _run_until_signal(cfg: SentinelConfig) -> int:
    ctl = Controller(cfg)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
    await ctl.start()
    try:
        if sys.platform == "win32":
            while not stop.is_set():  # KeyboardInterrupt surfaces through asyncio.run on Windows
                await asyncio.sleep(0.5)
        else:
            await stop.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        log.info("controller_stopping")
        await ctl.stop()
    return 0


def run_controller(args: argparse.Namespace) -> int:
    cfg = load_config(args.config, overrides=args.overrides)
    if args.log_level:
        cfg.log_level = args.log_level
    if args.log_format:
        cfg.log_format = args.log_format
    setup_logging(cfg.log_level, cfg.log_format)
    try:
        return asyncio.run(_run_until_signal(cfg))
    except KeyboardInterrupt:
        return 0
