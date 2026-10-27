"""Recovery policy: backoff schedule, restart budget, and escalated stop.

Backoff follows "Exponential Backoff And Jitter" (AWS Architecture Blog, 2015):
``full``  sleep = U(0, min(cap, base * 2**attempt))      -- default, best spread
``equal`` sleep = half fixed + half uniform random
``none``  sleep = min(cap, base * 2**attempt)

The restart budget is a sliding window (``max_restarts`` in ``restart_window_s``).
When it is exhausted the worker is quarantined instead of restarted forever
(a crash-looping worker that is never allowed to rest only adds load).

Stopping a worker escalates: HTTP /drain -> SIGTERM -> SIGKILL. The rung that
worked is recorded in the incident so operators can see which workers die gracefully.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from sentinel.config import RecoveryConfig, WorkerSpec
from sentinel.controller.backends import WorkerBackend


def backoff_delay(
    attempt: int,
    base_s: float,
    cap_s: float,
    jitter: str = "full",
    rng: random.Random | None = None,
) -> float:
    uniform = (rng or random).uniform
    exp = min(cap_s, base_s * (2.0 ** max(0, attempt)))
    if jitter == "none":
        return exp
    if jitter == "equal":
        return exp / 2.0 + uniform(0.0, exp / 2.0)
    return uniform(0.0, exp)


class RestartBudget:
    """Sliding-window counter: allow at most ``max_restarts`` per ``window_s``."""

    def __init__(
        self, max_restarts: int, window_s: float, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self.max_restarts = max_restarts
        self.window_s = window_s
        self.clock = clock
        self._events: deque[float] = deque()

    def _prune(self) -> None:
        cutoff = self.clock() - self.window_s
        while self._events and self._events[0] < cutoff:
            self._events.popleft()

    def count(self) -> int:
        self._prune()
        return len(self._events)

    def allow(self) -> bool:
        return self.count() < self.max_restarts

    def record(self) -> None:
        self._events.append(self.clock())

    def reset(self) -> None:
        self._events.clear()


@dataclass
class StopOutcome:
    rung: str  # drain | terminate | kill | already_dead
    duration_ms: float
    exit_code: int | None


async def stop_with_escalation(
    backend: WorkerBackend,
    spec: WorkerSpec,
    cfg: RecoveryConfig,
    *,
    drain: Callable[[WorkerSpec], Awaitable[bool]] | None = None,
) -> StopOutcome:
    """Try graceful drain, then terminate, then kill. Returns which rung succeeded."""
    t0 = time.perf_counter()

    def done(rung: str) -> StopOutcome:
        return StopOutcome(rung, (time.perf_counter() - t0) * 1000.0, backend.exit_code(spec))

    if not await backend.is_alive(spec):
        return done("already_dead")

    if drain is not None and cfg.drain_grace_s > 0:
        try:
            accepted = await asyncio.wait_for(drain(spec), timeout=min(1.0, cfg.drain_grace_s))
        except Exception:
            accepted = False
        if accepted and await backend.wait_exit(spec, cfg.drain_grace_s):
            return done("drain")

    await backend.terminate(spec)
    if await backend.wait_exit(spec, cfg.term_grace_s):
        return done("terminate")

    await backend.kill(spec)
    await backend.wait_exit(spec, max(2.0, cfg.term_grace_s))
    return done("kill")
