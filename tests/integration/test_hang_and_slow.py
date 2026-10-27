"""Wedged event loop (no HTTP at all) and slow-but-alive workers."""

from __future__ import annotations

import time

import pytest

pytestmark = pytest.mark.integration


async def test_wedged_worker_is_killed_and_replaced(harness):
    old_pid = (await harness.worker("w1"))["pid"]
    t0 = time.time()
    await harness.chaos("w1", "hang", delay_ms=20)

    inc = await harness.wait_incident("w1", timeout=10.0)
    assert inc["reason"] == "timeout" and inc["source"] == "active"
    # 3 consecutive 0.5 s probe timeouts on a 0.25 s cadence: detection is bounded.
    assert inc["opened_at"] - t0 < 4.0
    assert inc["symptom_to_detect_ms"] is not None and inc["symptom_to_detect_ms"] > 0

    inc = await harness.wait_incident("w1", status="recovered", timeout=20.0)
    # /drain cannot be answered by a wedged loop, so the ladder has to escalate.
    assert inc["escalation"][0] in ("terminate", "kill")
    after = await harness.worker("w1")
    assert after["pid"] != old_pid and after["state"] == "healthy"


async def test_slow_worker_is_drained_of_traffic_then_recycled(harness):
    await harness.chaos("w2", "slow", ms=700)
    statuses = await harness.load(4.0, concurrency=6, tokens=4)
    assert sum(1 for s in statuses if s == 200) / len(statuses) > 0.95

    events = await harness.events()
    degraded = [e for e in events if e["event"] == "state_transition"
                and e["worker"] == "w2" and e["new"] == "degraded"]
    assert degraded, "w2 never entered DEGRADED"
    assert degraded[0]["reason"] == "latency_p95"
    assert degraded[0]["detail"]["p95_ms"] > degraded[0]["detail"]["threshold_ms"]

    inc = await harness.wait_incident("w2", status="recovered", timeout=20.0)
    assert inc["reason"] == "degraded_too_long"
    # The healthy peers never tripped: fleet-relative thresholds, not absolute ones.
    assert not [i for i in await harness.incidents() if i["worker"] != "w2"]


async def test_stuck_requests_are_a_failure_signal(make_harness):
    h = await make_harness(latency__stuck_request_s=0.8, gateway__request_timeout_s=6.0)
    # Make every request to w1 take far longer than stuck_request_s but keep /health alive.
    await h.chaos("w1", "slow", ms=4000)
    statuses = await h.load(3.0, concurrency=3, tokens=2)
    assert statuses
    events = await h.events()
    stuck = [e for e in events if e.get("worker") == "w1" and e.get("reason") == "stuck_request"]
    assert stuck, "stuck_request verdict never fired"
