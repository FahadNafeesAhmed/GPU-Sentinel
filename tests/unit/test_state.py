"""State-machine semantics: hysteresis, startup grace, degradation, fatal paths."""

from __future__ import annotations

from sentinel.config import LatencyPolicy, ProbeConfig
from sentinel.controller.state import (
    HealthTracker,
    Severity,
    Verdict,
    WorkerState,
    worst,
)


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def make(**kw) -> tuple[HealthTracker, FakeClock]:
    clock = FakeClock()
    probe = ProbeConfig(unhealthy_threshold=3, healthy_threshold=2, startup_grace_s=5.0)
    lat = LatencyPolicy(degraded_consecutive=3, recycle_degraded_after_s=10.0)
    for k, v in kw.items():
        if hasattr(probe, k):
            setattr(probe, k, v)
        else:
            setattr(lat, k, v)
    tr = HealthTracker("w1", probe, lat, clock=clock)
    tr.set_state(WorkerState.STARTING, "test")
    return tr, clock


def to_healthy(tr: HealthTracker) -> None:
    for _ in range(2):
        tr.observe(Verdict.ok())
    assert tr.state == WorkerState.HEALTHY


def test_starting_requires_healthy_threshold_passes():
    tr, _ = make()
    assert tr.observe(Verdict.ok()) is None
    t = tr.observe(Verdict.ok())
    assert t is not None and t.new == WorkerState.HEALTHY and t.reason == "healthy_threshold"


def test_single_failure_is_suspect_not_unhealthy():
    tr, _ = make()
    to_healthy(tr)
    t = tr.observe(Verdict.fail("timeout"))
    assert t is not None and t.new == WorkerState.SUSPECT
    assert tr.streak_started_at is not None
    t2 = tr.observe(Verdict.ok())
    assert t2 is not None and t2.new == WorkerState.HEALTHY and t2.reason == "probe_recovered"
    assert tr.consecutive_failures == 0 and tr.streak_started_at is None


def test_consecutive_failures_trip_unhealthy_and_record_first_symptom():
    tr, clock = make()
    to_healthy(tr)
    t_first = clock()
    assert tr.observe(Verdict.fail("timeout")).new == WorkerState.SUSPECT
    clock.advance(1.0)
    assert tr.observe(Verdict.fail("timeout")) is None
    clock.advance(1.0)
    t = tr.observe(Verdict.fail("conn_refused"))
    assert t is not None and t.new == WorkerState.UNHEALTHY
    assert t.reason == "conn_refused"
    assert t.streak_started_at == t_first


def test_interleaved_success_resets_failure_streak():
    tr, _ = make()
    to_healthy(tr)
    tr.observe(Verdict.fail("timeout"))
    tr.observe(Verdict.fail("timeout"))
    tr.observe(Verdict.ok())
    tr.observe(Verdict.fail("timeout"))
    tr.observe(Verdict.fail("timeout"))
    assert tr.state == WorkerState.SUSPECT  # never reached 3 in a row


def test_fatal_is_immediate():
    tr, _ = make()
    to_healthy(tr)
    t = tr.observe(Verdict.fatal("process_exited", "process", exit_code=137))
    assert t is not None and t.new == WorkerState.UNHEALTHY and t.reason == "process_exited"


def test_passive_failures_count_toward_threshold():
    tr, _ = make()
    to_healthy(tr)
    tr.observe(Verdict.fail("conn_error", "passive"))
    tr.observe(Verdict.fail("timeout", "active"))
    t = tr.observe(Verdict.fail("conn_error", "passive"))
    assert t is not None and t.new == WorkerState.UNHEALTHY and t.source == "passive"


def test_starting_ignores_probe_failures_until_grace_expires():
    tr, clock = make(startup_grace_s=5.0)
    for _ in range(10):  # far more than unhealthy_threshold
        assert tr.observe(Verdict.fail("conn_refused")) is None
    assert tr.state == WorkerState.STARTING
    clock.advance(5.0)
    t = tr.observe(Verdict.fail("conn_refused"))
    assert t is not None and t.new == WorkerState.UNHEALTHY and t.reason == "startup_timeout"


def test_starting_fatal_still_immediate():
    tr, _ = make()
    t = tr.observe(Verdict.fatal("process_exited", "process"))
    assert t is not None and t.new == WorkerState.UNHEALTHY


def test_degrade_requires_consecutive_ticks_then_recycles_after_timeout():
    tr, clock = make(degraded_consecutive=3, recycle_degraded_after_s=10.0)
    to_healthy(tr)
    assert tr.observe(Verdict.degrade("latency_p95")) is None
    assert tr.observe(Verdict.degrade("latency_p95")) is None
    t = tr.observe(Verdict.degrade("latency_p95"))
    assert t is not None and t.new == WorkerState.DEGRADED
    clock.advance(9.0)
    assert tr.observe(Verdict.degrade("latency_p95")) is None
    clock.advance(1.0)
    t2 = tr.observe(Verdict.degrade("latency_p95"))
    assert t2 is not None and t2.new == WorkerState.UNHEALTHY
    assert t2.reason == "degraded_too_long"


def test_degraded_recovers_after_healthy_threshold():
    tr, _ = make()
    to_healthy(tr)
    for _ in range(3):
        tr.observe(Verdict.degrade("latency_p95"))
    assert tr.state == WorkerState.DEGRADED
    assert tr.observe(Verdict.ok()) is None
    t = tr.observe(Verdict.ok())
    assert t is not None and t.new == WorkerState.HEALTHY


def test_ok_tick_resets_degraded_streak():
    tr, _ = make()
    to_healthy(tr)
    tr.observe(Verdict.degrade("latency_p95"))
    tr.observe(Verdict.degrade("latency_p95"))
    tr.observe(Verdict.ok())
    tr.observe(Verdict.degrade("latency_p95"))
    tr.observe(Verdict.degrade("latency_p95"))
    assert tr.state == WorkerState.HEALTHY


def test_non_probed_states_ignore_verdicts():
    tr, _ = make()
    to_healthy(tr)
    tr.set_state(WorkerState.RECOVERING, "x")
    assert tr.observe(Verdict.fatal("process_exited")) is None
    tr.set_state(WorkerState.QUARANTINED, "x")
    assert tr.observe(Verdict.ok()) is None
    assert tr.state == WorkerState.QUARANTINED


def test_set_state_resets_counters():
    tr, _ = make()
    to_healthy(tr)
    tr.observe(Verdict.fail("timeout"))
    tr.set_state(WorkerState.STARTING, "restart")
    assert tr.consecutive_failures == 0 and tr.streak_started_at is None


def test_worst_picks_highest_severity():
    v = worst([Verdict.ok(), Verdict.degrade("slow"), Verdict.fail("timeout"), Verdict.ok()])
    assert v.severity == Severity.FAIL and v.reason == "timeout"
    assert worst([]).severity == Severity.OK
    assert worst([Verdict.fail("a"), Verdict.fatal("b")]).reason == "b"
