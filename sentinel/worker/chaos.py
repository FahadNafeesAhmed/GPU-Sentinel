"""Fault injection switches for a worker. Driven over HTTP by tests and the benchmark.

Faults are deliberately realistic:
  crash   - os._exit after an optional delay (a segfault, not a graceful shutdown)
  hang    - wedge the event loop with a blocking sleep: no HTTP response of any kind,
            not even /health, and no signal handler runs (only SIGKILL/TerminateProcess works)
  slow    - add fixed latency to every request (a thermally throttled or contended GPU)
  errors  - return HTTP 500 for a fraction of requests
  leak    - retain N MB per request forever (a reference-cycle leak)
  oom     - allocate until the device is out of memory (or until a cap, on CPU)
  unready - readiness goes 503 while liveness stays 200
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from sentinel.worker.engine import Engine


@dataclass
class ChaosState:
    slow_ms: float = 0.0
    error_rate: float = 0.0
    hang: bool = False
    unready: bool = False
    leak_mb_per_request: float = 0.0
    leaked_mb: float = 0.0
    oom_allocated_mb: float = 0.0
    crash_scheduled: bool = False
    _held: list[Any] = field(default_factory=list, repr=False)

    def snapshot(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("_held", None)
        return d


class ChaosController:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.state = ChaosState()

    def apply(self, command: str, params: dict[str, Any]) -> dict[str, Any]:
        handler = getattr(self, f"_cmd_{command}", None)
        if handler is None:
            raise ValueError(f"unknown chaos command {command!r}")
        handler(params)
        return self.state.snapshot()

    # --- commands -----------------------------------------------------------------

    def _cmd_reset(self, _: dict[str, Any]) -> None:
        self.state = ChaosState()

    def _cmd_slow(self, p: dict[str, Any]) -> None:
        self.state.slow_ms = max(0.0, float(p.get("ms", 500)))

    def _cmd_errors(self, p: dict[str, Any]) -> None:
        self.state.error_rate = min(1.0, max(0.0, float(p.get("rate", 0.5))))

    def _cmd_unready(self, p: dict[str, Any]) -> None:
        self.state.unready = bool(p.get("on", True))

    def _cmd_leak(self, p: dict[str, Any]) -> None:
        self.state.leak_mb_per_request = max(0.0, float(p.get("mb_per_request", 8)))

    def _cmd_oom(self, p: dict[str, Any]) -> None:
        cap = float(p.get("max_mb", 2048))
        step = float(p.get("step_mb", 128))
        self.state.oom_allocated_mb += self.engine.allocate_until_oom(step_mb=step, max_mb=cap)

    def _cmd_crash(self, p: dict[str, Any]) -> None:
        delay = max(0.0, float(p.get("delay_ms", 50))) / 1000.0
        code = int(p.get("exit_code", 137))
        self.state.crash_scheduled = True

        def _die() -> None:
            time.sleep(delay)
            os._exit(code)

        threading.Thread(target=_die, daemon=True, name="chaos-crash").start()

    def _cmd_hang(self, p: dict[str, Any]) -> None:
        delay = max(0.0, float(p.get("delay_ms", 50))) / 1000.0
        self.state.hang = True
        loop = asyncio.get_running_loop()

        def _wedge() -> None:
            # Block the event loop thread indefinitely: simulates a deadlocked process.
            while True:
                time.sleep(3600)

        loop.call_later(delay, _wedge)

    # --- per-request hooks ----------------------------------------------------------

    def on_request(self) -> None:
        """Called once per inference request to apply leak behaviour."""
        mb = self.state.leak_mb_per_request
        if mb > 0:
            self.state._held.append(self.engine.allocate_mb(mb))
            self.state.leaked_mb += mb
