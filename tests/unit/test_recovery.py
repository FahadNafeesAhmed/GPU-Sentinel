"""Backoff math, restart budget, and the drain -> terminate -> kill escalation ladder."""

from __future__ import annotations

import random

import pytest

from sentinel.config import RecoveryConfig, WorkerSpec
from sentinel.controller.backends import WorkerBackend
from sentinel.controller.recovery import RestartBudget, backoff_delay, stop_with_escalation


def test_backoff_none_doubles_and_caps():
    assert [backoff_delay(i, 0.5, 30.0, "none") for i in range(8)] == [
        0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0,
    ]


def test_backoff_full_jitter_within_envelope():
    rng = random.Random(1)
    for attempt in range(10):
        for _ in range(50):
            d = backoff_delay(attempt, 0.5, 30.0, "full", rng)
            assert 0.0 <= d <= min(30.0, 0.5 * 2**attempt)


def test_backoff_equal_jitter_has_floor():
    rng = random.Random(2)
    for attempt in range(8):
        exp = min(30.0, 0.5 * 2**attempt)
        for _ in range(50):
            d = backoff_delay(attempt, 0.5, 30.0, "equal", rng)
            assert exp / 2 <= d <= exp


def test_full_jitter_spreads_retries():
    rng = random.Random(3)
    ds = sorted(backoff_delay(4, 0.5, 30.0, "full", rng) for _ in range(200))
    assert ds[-1] - ds[0] > 6.0  # envelope is 8 s; samples should actually use it


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_restart_budget_sliding_window():
    clk = Clock()
    b = RestartBudget(max_restarts=3, window_s=100.0, clock=clk)
    for _ in range(3):
        assert b.allow()
        b.record()
        clk.t += 10
    assert not b.allow() and b.count() == 3
    clk.t = 100.5  # first event (t=0) now outside the window
    assert b.count() == 2 and b.allow()
    b.reset()
    assert b.count() == 0


class FakeBackend(WorkerBackend):
    """Scriptable backend: which rung actually stops the worker."""

    def __init__(self, dies_on: str | None, alive: bool = True) -> None:
        self.dies_on = dies_on
        self.alive = alive
        self.calls: list[str] = []

    async def start(self, spec: WorkerSpec) -> None:
        self.alive = True

    async def is_alive(self, spec: WorkerSpec) -> bool:
        return self.alive

    def exit_code(self, spec: WorkerSpec) -> int | None:
        return None if self.alive else 0

    def pid(self, spec: WorkerSpec) -> int | None:
        return 1234 if self.alive else None

    async def terminate(self, spec: WorkerSpec) -> None:
        self.calls.append("terminate")
        if self.dies_on == "terminate":
            self.alive = False

    async def kill(self, spec: WorkerSpec) -> None:
        self.calls.append("kill")
        if self.dies_on == "kill":
            self.alive = False

    async def drain_hook(self, spec: WorkerSpec) -> bool:
        self.calls.append("drain")
        if self.dies_on == "drain":
            self.alive = False
            return True
        return self.dies_on != "drain_refused"


SPEC = WorkerSpec(id="w1", port=9999)
CFG = RecoveryConfig(drain_grace_s=0.2, term_grace_s=0.2)


@pytest.mark.asyncio
async def test_escalation_stops_at_drain():
    be = FakeBackend(dies_on="drain")
    out = await stop_with_escalation(be, SPEC, CFG, drain=be.drain_hook)
    assert out.rung == "drain" and be.calls == ["drain"]


@pytest.mark.asyncio
async def test_escalation_falls_through_to_terminate():
    be = FakeBackend(dies_on="terminate")
    out = await stop_with_escalation(be, SPEC, CFG, drain=be.drain_hook)
    assert out.rung == "terminate" and be.calls == ["drain", "terminate"]


@pytest.mark.asyncio
async def test_escalation_kills_a_wedged_worker():
    be = FakeBackend(dies_on="kill")
    out = await stop_with_escalation(be, SPEC, CFG, drain=be.drain_hook)
    assert out.rung == "kill" and be.calls == ["drain", "terminate", "kill"]
    assert out.duration_ms >= 400  # waited both grace periods


@pytest.mark.asyncio
async def test_escalation_skips_drain_when_disabled_or_dead():
    be = FakeBackend(dies_on="terminate")
    out = await stop_with_escalation(be, SPEC, RecoveryConfig(drain_grace_s=0.0, term_grace_s=0.2),
                                     drain=be.drain_hook)
    assert out.rung == "terminate" and be.calls == ["terminate"]
    dead = FakeBackend(dies_on=None, alive=False)
    out = await stop_with_escalation(dead, SPEC, CFG, drain=dead.drain_hook)
    assert out.rung == "already_dead" and dead.calls == []
