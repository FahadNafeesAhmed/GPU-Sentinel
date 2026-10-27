"""False positives under load, restart budget / quarantine, startup timeout, operator API."""

from __future__ import annotations

import asyncio

import psutil
import pytest

pytestmark = pytest.mark.integration


async def test_no_false_positives_under_sustained_load(harness):
    statuses = await harness.load(5.0, concurrency=12, tokens=8)
    assert len(statuses) > 200
    assert all(s == 200 for s in statuses)
    assert await harness.incidents() == []
    st = await harness.state()
    assert all(w["restarts"] == 0 and w["state"] == "healthy" for w in st["workers"])
    suspects = [e for e in await harness.events()
                if e["event"] == "state_transition" and e["new"] == "suspect"]
    assert suspects == [], f"transient probe failures under load: {suspects}"


async def test_restart_budget_quarantines_a_crash_loop(make_harness):
    h = await make_harness(recovery__max_restarts=2, recovery__restart_window_s=60.0)
    for _ in range(2):
        await h.kill_and_wait_replaced("w1")
    pid = (await h.worker("w1"))["pid"]
    psutil.Process(pid).kill()
    await h.wait_state("w1", "quarantined", timeout=10.0)
    inc = next(i for i in await h.incidents() if i["worker"] == "w1")
    assert inc["status"] == "quarantined"
    w = await h.worker("w1")
    assert w["budget_used"] == 2 and w["restarts"] == 2
    assert w["id"] not in {x["id"] for x in (await h.state())["workers"] if x["state"] == "healthy"}

    # Operator releases the quarantine: budget resets and the worker is relaunched.
    async with h.http.post(f"{h.api}/api/workers/w1/unquarantine") as r:
        assert r.status == 200
    await h.wait_state("w1", "healthy", timeout=15.0)
    assert (await h.worker("w1"))["budget_used"] == 0


async def test_startup_timeout_opens_incident_without_counting_probe_failures(make_harness):
    from sentinel.config import WorkerSpec

    h = await make_harness(workers=1, probe__startup_grace_s=2.5)
    # Replace the spec's args so every *re*start sleeps longer than the grace period.
    spec = h.cfg.workers.specs[0]
    h.cfg.workers.specs[0] = WorkerSpec(id=spec.id, port=spec.port, extra_args=[
        "--startup-delay-s", "4",
    ])
    h.ctl.supervisor.workers["w1"].spec = h.cfg.workers.specs[0]
    pid = (await h.worker("w1"))["pid"]
    psutil.Process(pid).kill()  # forces a restart that will now be too slow
    inc = await h.wait_incident("w1", timeout=5.0)
    assert inc["reason"] == "process_exited"

    async def _timed_out() -> bool:
        evs = await h.events()
        return any(e.get("reason") == "startup_timeout" for e in evs)

    await h.wait_for(_timed_out, 8.0, "startup_timeout transition")
    evs = await h.events()
    assert not any(e.get("reason") == "conn_refused" and e.get("new") == "unhealthy" for e in evs)


async def test_operator_restart_uses_graceful_drain(harness):
    async with harness.http.post(f"{harness.api}/api/workers/w2/restart") as r:
        assert r.status == 200 and (await r.json())["accepted"] is True
    inc = await harness.wait_incident("w2", status="recovered", timeout=15.0)
    assert inc["reason"] == "manual_restart" and inc["source"] == "operator"
    assert inc["escalation"] == ["drain"]
    async with harness.http.post(f"{harness.api}/api/workers/nope/restart") as r:
        assert r.status == 404


async def test_api_surface(harness):
    st, headers = await harness.infer()
    assert st == 200 and headers["X-Sentinel-Worker"] in {"w1", "w2", "w3"}
    assert headers["X-Sentinel-Attempts"] == "1"
    await asyncio.sleep(0.6)  # at least one regular probe tick
    async with harness.http.get(f"{harness.api}/metrics") as r:
        text = await r.text()
        assert r.headers["Content-Type"].startswith("text/plain; version=0.0.4")
    assert 'sentinel_worker_state{worker="w1",state="healthy"} 1' in text
    assert "sentinel_gateway_requests_total" in text and "sentinel_probe_seconds_bucket" in text
    async with harness.http.get(f"{harness.api}/healthz") as r:
        assert r.status == 200
    async with harness.http.get(f"{harness.gateway}/healthz") as r:
        assert (await r.json())["routable"] == 3
    async with harness.http.get(f"{harness.api}/") as r:
        assert r.status == 200 and "GPU Sentinel" in await r.text()
    async with harness.http.get(f"{harness.api}/api/config") as r:
        assert (await r.json())["probe"]["interval_s"] == 0.25


async def test_gateway_returns_503_when_nothing_is_routable(make_harness):
    h = await make_harness(workers=1)
    async with h.http.post(f"{h.api}/api/workers/w1/quarantine") as r:
        assert r.status == 200
    await h.wait_state("w1", "quarantined", timeout=5.0)
    st, headers = await h.infer()
    assert st == 503 and headers["X-Sentinel-Attempts"] == "0"
