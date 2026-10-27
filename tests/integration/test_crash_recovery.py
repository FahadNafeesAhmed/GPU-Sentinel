"""A worker process dies (SIGKILL from outside, or a crash from inside): detect, restart, heal."""

from __future__ import annotations

import time

import psutil
import pytest

pytestmark = pytest.mark.integration


async def test_external_kill_is_detected_and_recovered(harness):
    before = await harness.worker("w2")
    old_pid = before["pid"]
    t_kill = time.time()
    psutil.Process(old_pid).kill()

    inc = await harness.wait_incident("w2", timeout=5.0)
    assert inc["reason"] == "process_exited" and inc["source"] == "process"
    detect_ms = (inc["opened_at"] - t_kill) * 1000
    assert detect_ms < 1500, f"detection took {detect_ms:.0f} ms"

    inc = await harness.wait_incident("w2", status="recovered", timeout=15.0)
    assert inc["attempts"] == 1 and inc["escalation"] == ["already_dead"]
    after = await harness.worker("w2")
    assert after["state"] == "healthy" and after["pid"] != old_pid and after["restarts"] == 1
    assert after["pid"] is not None and psutil.pid_exists(after["pid"])

    kinds = [e["event"] for e in await harness.events() if e.get("incident") == inc["id"]]
    assert kinds[:1] == ["incident_opened"]
    assert "recovery_backoff" in kinds and "worker_restarted" in kinds
    assert kinds[-1] == "incident_closed"


async def test_chaos_crash_from_inside(harness):
    await harness.chaos("w1", "crash", delay_ms=10, exit_code=3)
    inc = await harness.wait_incident("w1", status="recovered", timeout=15.0)
    assert inc["reason"] == "process_exited"
    stopped = next(n for n in inc["notes"] if n["event"] == "worker_stopped")
    assert stopped["exit_code"] == 3
    assert (await harness.worker("w1"))["state"] == "healthy"


async def test_fleet_keeps_serving_while_one_worker_is_dead(harness):
    statuses = await harness.load(0.5, concurrency=4)
    assert statuses and all(s == 200 for s in statuses)
    pid = (await harness.worker("w3"))["pid"]
    psutil.Process(pid).kill()
    statuses = await harness.load(2.0, concurrency=4)
    ok = sum(1 for s in statuses if s == 200)
    assert ok / len(statuses) >= 0.995, f"success {ok}/{len(statuses)} during failure"
    await harness.wait_state("w3", "healthy", timeout=15.0)
