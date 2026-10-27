"""Per-worker health state machine. Pure logic, no I/O, fully unit-testable.

    STARTING --(healthy_threshold passes)--> HEALTHY
    HEALTHY  --(1 fail)--> SUSPECT --(1 pass)--> HEALTHY
    SUSPECT  --(unhealthy_threshold consecutive fails)--> UNHEALTHY
    HEALTHY  --(degraded_consecutive slow ticks)--> DEGRADED
    DEGRADED --(recycle_degraded_after_s)--> UNHEALTHY
    any      --(FATAL: process exit / resource limit / leak trend)--> UNHEALTHY
    UNHEALTHY -> RECOVERING -> STARTING   (driven by the supervisor)
    RECOVERING -> QUARANTINED             (restart budget exhausted)

Hysteresis (``unhealthy_threshold``/``healthy_threshold``) is what keeps a single
dropped probe from triggering a restart, i.e. it is the false-positive control.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from typing import Any

from sentinel.config import LatencyPolicy, ProbeConfig


class WorkerState(StrEnum):
    STARTING = "starting"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    SUSPECT = "suspect"
    UNHEALTHY = "unhealthy"
    RECOVERING = "recovering"
    QUARANTINED = "quarantined"
    STOPPED = "stopped"


PROBED_STATES = frozenset(
    {WorkerState.STARTING, WorkerState.HEALTHY, WorkerState.DEGRADED, WorkerState.SUSPECT}
)


class Severity(int, Enum):
    OK = 0
    DEGRADE = 1
    FAIL = 2
    FATAL = 3


@dataclass(frozen=True)
class Verdict:
    severity: Severity
    reason: str = ""
    source: str = "active"  # active | passive | resource | latency | process
    detail: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def ok(source: str = "active", **detail: Any) -> Verdict:
        return Verdict(Severity.OK, "", source, detail)

    @staticmethod
    def fail(reason: str, source: str = "active", **detail: Any) -> Verdict:
        return Verdict(Severity.FAIL, reason, source, detail)

    @staticmethod
    def fatal(reason: str, source: str = "resource", **detail: Any) -> Verdict:
        return Verdict(Severity.FATAL, reason, source, detail)

    @staticmethod
    def degrade(reason: str, source: str = "latency", **detail: Any) -> Verdict:
        return Verdict(Severity.DEGRADE, reason, source, detail)


def worst(verdicts: list[Verdict]) -> Verdict:
    """Aggregate several signals for one tick: the most severe wins."""
    if not verdicts:
        return Verdict.ok()
    return max(verdicts, key=lambda v: v.severity)


@dataclass(frozen=True)
class Transition:
    worker: str
    old: WorkerState
    new: WorkerState
    reason: str
    source: str
    at: float  # wall clock
    streak_started_at: float | None = None  # wall clock of first symptom in this streak


class HealthTracker:
    def __init__(
        self,
        worker_id: str,
        probe: ProbeConfig,
        latency: LatencyPolicy,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.worker_id = worker_id
        self.probe = probe
        self.latency = latency
        self.clock = clock
        self.state = WorkerState.STOPPED
        self.state_since = clock()
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.degraded_ticks = 0
        self.last_reason = ""
        self.last_verdict: Verdict | None = None
        self.streak_started_at: float | None = None  # first non-OK since last OK

    # ------------------------------------------------------------------ helpers

    def _enter(self, new: WorkerState, reason: str, source: str) -> Transition:
        now = self.clock()
        t = Transition(self.worker_id, self.state, new, reason, source, now, self.streak_started_at)
        self.state = new
        self.state_since = now
        self.last_reason = reason
        return t

    def set_state(self, new: WorkerState, reason: str, source: str = "supervisor") -> Transition:
        """Supervisor-driven transitions (RECOVERING, STARTING, QUARANTINED, STOPPED)."""
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.degraded_ticks = 0
        self.streak_started_at = None
        return self._enter(new, reason, source)

    def time_in_state(self) -> float:
        return self.clock() - self.state_since

    # ------------------------------------------------------------------ core

    def observe(self, v: Verdict) -> Transition | None:
        """Feed one aggregated verdict. Returns a Transition if the state changed."""
        self.last_verdict = v
        if self.state not in PROBED_STATES:
            return None
        now = self.clock()

        if v.severity == Severity.OK:
            self.consecutive_failures = 0
            self.consecutive_successes += 1
            self.degraded_ticks = 0
            self.streak_started_at = None
            if self.state == WorkerState.SUSPECT:
                return self._enter(WorkerState.HEALTHY, "probe_recovered", v.source)
            if (
                self.state in (WorkerState.STARTING, WorkerState.DEGRADED)
                and self.consecutive_successes >= self.probe.healthy_threshold
            ):
                return self._enter(WorkerState.HEALTHY, "healthy_threshold", v.source)
            return self._startup_timeout(now)

        if self.streak_started_at is None:
            self.streak_started_at = now

        if v.severity == Severity.FATAL:
            self.consecutive_successes = 0
            return self._enter(WorkerState.UNHEALTHY, v.reason, v.source)

        if v.severity == Severity.FAIL:
            self.consecutive_successes = 0
            if self.state == WorkerState.STARTING:
                return self._startup_timeout(now)
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.probe.unhealthy_threshold:
                return self._enter(WorkerState.UNHEALTHY, v.reason, v.source)
            if self.state in (WorkerState.HEALTHY, WorkerState.DEGRADED):
                return self._enter(WorkerState.SUSPECT, v.reason, v.source)
            return self._startup_timeout(now)

        # DEGRADE: the worker answers, but slowly.
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.degraded_ticks += 1
        if self.state == WorkerState.DEGRADED:
            if self.time_in_state() >= self.latency.recycle_degraded_after_s:
                return self._enter(WorkerState.UNHEALTHY, "degraded_too_long", v.source)
            return None
        if (
            self.state in (WorkerState.HEALTHY, WorkerState.SUSPECT)
            and self.degraded_ticks >= self.latency.degraded_consecutive
        ):
            return self._enter(WorkerState.DEGRADED, v.reason, v.source)
        return None

    def _startup_timeout(self, now: float) -> Transition | None:
        if (
            self.state == WorkerState.STARTING
            and now - self.state_since >= self.probe.startup_grace_s
        ):
            return self._enter(WorkerState.UNHEALTHY, "startup_timeout", "supervisor")
        return None
