"""Memory growth: proactive leak-trend recycle and the hard RSS limit."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


async def test_leak_trend_recycles_before_the_hard_limit(harness):
    await harness.chaos("w3", "leak", mb_per_request=0.5)
    # ~ +0.5 MB per request routed to w3 (tens of MB/s); the 3 s trend window
    # needs >= 6 samples before the slope is trusted.
    await harness.load(4.0, concurrency=3, tokens=2)
    inc = await harness.wait_incident("w3", timeout=8.0)
    assert inc["reason"] == "rss_leak_trend"
    opened = next(e for e in await harness.events()
                  if e["event"] == "incident_opened" and e["incident"] == inc["id"])
    assert opened["detail"]["slope_mb_per_min"] > 60.0
    assert opened["detail"]["current_mb"] < 1536  # recycled long before the hard limit
    await harness.wait_incident("w3", status="recovered", timeout=15.0)
    assert (await harness.worker("w3"))["rss_mb"] < 300


async def test_hard_rss_limit_is_immediate(make_harness):
    h = await make_harness(limits__rss_mb=200.0, limits__leak_slope_mb_per_min=None)
    await h.chaos("w1", "oom", max_mb=256, step_mb=64)
    inc = await h.wait_incident("w1", timeout=5.0)
    assert inc["reason"] == "rss_limit"
    # fatal verdicts do not wait for a streak: the incident opens at the transition timestamp
    assert inc["symptom_to_detect_ms"] == 0.0
    await h.wait_incident("w1", status="recovered", timeout=15.0)
