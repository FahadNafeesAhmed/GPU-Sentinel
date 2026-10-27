"""The supervisor: owns worker lifecycles, runs the probe tick, drives recovery.

One tick (every ``probe.interval_s``):
  1. process liveness via the backend
  2. HTTP /health probe (concurrently for all workers, each with a hard timeout)
  3. resource limits + leak trend from the probe body
  4. fleet-relative latency verdict from gateway observations
  5. stuck in-flight request check
  -> worst verdict -> HealthTracker.observe -> Transition -> incident / recovery
"""

from __future__ import annotations

import asyncio
import itertools
import time
from typing import Any

import aiohttp

from sentinel.config import SentinelConfig, WorkerSpec
from sentinel.controller.backends import WorkerBackend
from sentinel.controller.detector import (
    FleetLatencyDetector,
    LeakDetector,
    resource_verdict,
)
from sentinel.controller.health import Prober
from sentinel.controller.incidents import Incident, IncidentLog
from sentinel.controller.recovery import RestartBudget, backoff_delay, stop_with_escalation
from sentinel.controller.state import (
    PROBED_STATES,
    HealthTracker,
    Transition,
    Verdict,
    WorkerState,
    worst,
)
from sentinel.logging_setup import get_logger
from sentinel.metrics import Registry

log = get_logger("sentinel.supervisor")
ROUTABLE_BASE = frozenset({WorkerState.HEALTHY})


class ManagedWorker:
    _ids = itertools.count(1)

    def __init__(self, spec: WorkerSpec, cfg: SentinelConfig) -> None:
        self.spec = spec
        self.tracker = HealthTracker(spec.id, cfg.probe, cfg.latency)
        self.budget = RestartBudget(cfg.recovery.max_restarts, cfg.recovery.restart_window_s)
        self.attempt = 0
        self.restarts_total = 0
        self.recovery_task: asyncio.Task[None] | None = None
        self.incident: Incident | None = None
        self.last_status: dict[str, Any] | None = None
        self.last_probe_ms: float | None = None
        self.started_at: float | None = None
        self.healthy_since: float | None = None
        self.inflight: dict[int, float] = {}
        self.requests_ok = 0
        self.requests_err = 0
        self.requests_shed = 0
        self.generation = 0  # bumps on every (re)start; stale watcher tasks check it
        self.watchers: list[asyncio.Task[None]] = []

    @property
    def id(self) -> str:
        return self.spec.id

    @property
    def state(self) -> WorkerState:
        return self.tracker.state

    def begin_request(self) -> int:
        rid = next(ManagedWorker._ids)
        self.inflight[rid] = time.monotonic()
        return rid

    def end_request(self, rid: int) -> None:
        self.inflight.pop(rid, None)

    def oldest_inflight_age(self) -> float:
        if not self.inflight:
            return 0.0
        return time.monotonic() - min(self.inflight.values())


class Supervisor:
    def __init__(
        self,
        cfg: SentinelConfig,
        backend: WorkerBackend,
        incidents: IncidentLog,
        registry: Registry,
        session: aiohttp.ClientSession,
    ) -> None:
        self.cfg = cfg
        self.backend = backend
        self.incidents = incidents
        self.registry = registry
        self.prober = Prober(session, cfg.probe.timeout_s)
        self.latency = FleetLatencyDetector(cfg.latency)
        self.leaks = LeakDetector(cfg.limits)
        self.workers: dict[str, ManagedWorker] = {
            s.id: ManagedWorker(s, cfg) for s in cfg.workers.resolved_specs()
        }
        self.started_at = time.time()
        self.tick_count = 0
        self._tick_task: asyncio.Task[None] | None = None
        self._routable_states = set(ROUTABLE_BASE)
        if cfg.gateway.route_to_degraded:
            self._routable_states.add(WorkerState.DEGRADED)
        self._init_metrics()

    def _init_metrics(self) -> None:
        r = self.registry
        self.m_state = r.gauge("sentinel_worker_state", "1 for the worker's current state",
                               ["worker", "state"])
        self.m_probe = r.histogram("sentinel_probe_seconds", "Active probe round trip",
                                   ["worker", "outcome"])
        self.m_transitions = r.counter("sentinel_state_transitions_total", "State transitions",
                                       ["worker", "from", "to"])
        self.m_incidents = r.counter("sentinel_incidents_total", "Incidents opened by reason",
                                     ["reason"])
        self.m_recoveries = r.counter("sentinel_recoveries_total", "Recovery outcomes", ["outcome"])
        self.m_recovery_s = r.histogram(
            "sentinel_recovery_seconds", "Detection to healthy again",
            buckets=(0.5, 1, 2, 3, 5, 8, 13, 21, 34, 60),
        )
        self.m_restarts = r.counter("sentinel_restarts_total", "Worker restarts", ["worker"])
        self.m_rss = r.gauge("sentinel_worker_rss_mb", "Worker RSS (MB)", ["worker"])
        self.m_gpu = r.gauge("sentinel_worker_gpu_mem_mb", "Worker GPU memory (MB)", ["worker"])
        self.m_p95 = r.gauge("sentinel_worker_request_p95_ms", "Gateway-observed p95 (MB)",
                             ["worker"])
        self.m_ticks = r.counter("sentinel_ticks_total", "Supervisor ticks")

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        for mw in self.workers.values():
            await self._launch(mw, "initial_start")
        self._tick_task = asyncio.create_task(self._run_ticks(), name="sentinel-ticks")
        log.info("supervisor_started", workers=list(self.workers),
                 interval_s=self.cfg.probe.interval_s)

    async def stop(self) -> None:
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except (asyncio.CancelledError, Exception):
                pass
        for mw in self.workers.values():
            if mw.recovery_task and not mw.recovery_task.done():
                mw.recovery_task.cancel()
            for t in mw.watchers:
                t.cancel()
        for mw in self.workers.values():
            if mw.state != WorkerState.STOPPED:
                self._apply(mw, mw.tracker.set_state(WorkerState.STOPPED, "controller_shutdown"))
        await self.backend.close()
        log.info("supervisor_stopped")

    async def _launch(self, mw: ManagedWorker, reason: str) -> None:
        await self.backend.start(mw.spec)
        mw.started_at = time.time()
        mw.last_status = None
        mw.inflight.clear()
        mw.generation += 1
        self.latency.reset(mw.id)
        self.leaks.reset(mw.id)
        self._apply(mw, mw.tracker.set_state(WorkerState.STARTING, reason))
        gen = mw.generation
        mw.watchers = [t for t in mw.watchers if not t.done()]
        mw.watchers.append(asyncio.create_task(self._watch_exit(mw, gen), name=f"exit-{mw.id}"))
        mw.watchers.append(
            asyncio.create_task(self._watch_startup(mw, gen), name=f"startup-{mw.id}")
        )

    async def _watch_exit(self, mw: ManagedWorker, gen: int) -> None:
        """Event-driven crash detection: fires the moment the process exits."""
        code = await self.backend.wait_for_exit(mw.spec)
        if code is None or mw.generation != gen or mw.state not in PROBED_STATES:
            return
        self._observe(
            mw,
            Verdict.fatal("process_exited", "process", exit_code=code, detected_by="exit_watcher"),
        )

    async def _watch_startup(self, mw: ManagedWorker, gen: int) -> None:
        """Probe a STARTING worker at ``startup_interval_s`` so it takes traffic sooner."""
        interval = self.cfg.probe.startup_interval_s
        while mw.generation == gen and mw.state == WorkerState.STARTING:
            res = await self.prober.probe(mw.spec)
            if mw.generation != gen or mw.state != WorkerState.STARTING:
                return
            mw.last_probe_ms = res.latency_ms
            self.m_probe.labels(
                worker=mw.id, outcome="ok" if res.verdict.severity == 0 else res.verdict.reason
            ).observe(res.latency_ms / 1000.0)
            if res.status is not None:
                mw.last_status = res.status
            self._observe(mw, res.verdict)
            await asyncio.sleep(interval)

    # ------------------------------------------------------------------ tick loop

    async def _run_ticks(self) -> None:
        interval = self.cfg.probe.interval_s
        while True:
            t0 = time.monotonic()
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.error("tick_failed", exc_info=True)
            await asyncio.sleep(max(0.0, interval - (time.monotonic() - t0)))

    async def _tick(self) -> None:
        self.tick_count += 1
        self.m_ticks.inc()
        # STARTING workers are probed by their startup watcher; everything else here.
        active = [
            mw for mw in self.workers.values()
            if mw.state in PROBED_STATES and mw.state != WorkerState.STARTING
        ]
        if not active:
            return
        results = await asyncio.gather(*(self.prober.probe(mw.spec) for mw in active))
        latency_verdicts = self.latency.evaluate(
            [mw.id for mw in active],
            sticky=[mw.id for mw in active if mw.state == WorkerState.DEGRADED],
        )
        now_m = time.monotonic()
        for mw, res in zip(active, results, strict=True):
            verdicts: list[Verdict] = []
            if not await self.backend.is_alive(mw.spec):
                verdicts.append(
                    Verdict.fatal("process_exited", "process",
                                  exit_code=self.backend.exit_code(mw.spec))
                )
            verdicts.append(res.verdict)
            mw.last_probe_ms = res.latency_ms
            self.m_probe.labels(
                worker=mw.id, outcome="ok" if res.verdict.severity == 0 else res.verdict.reason
            ).observe(res.latency_ms / 1000.0)
            if res.status is not None:
                mw.last_status = res.status
                verdicts.append(resource_verdict(self.cfg.limits, res.status))
                rss = res.status.get("rss_mb")
                gpu_mb = (res.status.get("gpu") or {}).get("proc_mem_mb")
                self.leaks.record(mw.id, rss, gpu_mb)
                verdicts.append(self.leaks.evaluate(mw.id))
                if isinstance(rss, int | float):
                    self.m_rss.labels(worker=mw.id).set(rss)
                if isinstance(gpu_mb, int | float):
                    self.m_gpu.labels(worker=mw.id).set(gpu_mb)
            if mw.id in latency_verdicts:
                verdicts.append(latency_verdicts[mw.id])
            age = mw.oldest_inflight_age()
            if age > self.cfg.latency.stuck_request_s:
                verdicts.append(Verdict.fail("stuck_request", "latency", age_s=round(age, 1)))
            summary = self.latency.window(mw.id).summary()
            if summary.get("n"):
                self.m_p95.labels(worker=mw.id).set(float(summary["p95"]))
            self._observe(mw, worst(verdicts))
            if (
                mw.state == WorkerState.HEALTHY
                and mw.attempt > 0
                and mw.healthy_since is not None
                and now_m - mw.healthy_since >= self.cfg.recovery.stable_reset_s
            ):
                log.info("backoff_reset", worker=mw.id, after_s=self.cfg.recovery.stable_reset_s)
                mw.attempt = 0

    # ------------------------------------------------------------------ transitions

    def _observe(self, mw: ManagedWorker, v: Verdict) -> None:
        t = mw.tracker.observe(v)
        if t is not None:
            self._apply(mw, t)

    def _apply(self, mw: ManagedWorker, t: Transition) -> None:
        detail = mw.tracker.last_verdict.detail if mw.tracker.last_verdict else {}
        log.info("state_transition", worker=mw.id, old=t.old.value, new=t.new.value,
                 reason=t.reason, source=t.source, **detail)
        self.m_transitions.labels(worker=mw.id, **{"from": t.old.value, "to": t.new.value}).inc()
        self.incidents.emit("state_transition", worker=mw.id, old=t.old.value, new=t.new.value,
                            reason=t.reason, source=t.source, detail=detail,
                            incident=mw.incident.id if mw.incident else None)
        for s in WorkerState:
            self.m_state.labels(worker=mw.id, state=s.value).set(1.0 if s == t.new else 0.0)

        if t.new == WorkerState.HEALTHY:
            mw.healthy_since = time.monotonic()
            if mw.incident is not None and mw.incident.status == "open":
                self.incidents.close(mw.incident, "recovered")
                self.m_recoveries.labels(outcome="recovered").inc()
                self.m_recovery_s.observe((mw.incident.detect_to_recover_ms or 0.0) / 1000.0)
                mw.incident = None
        elif t.new == WorkerState.UNHEALTHY:
            if mw.incident is None or mw.incident.status != "open":
                mw.incident = self.incidents.open(mw.id, t.reason, t.source, t.streak_started_at,
                                                  detail=detail, previous_state=t.old.value)
                self.m_incidents.labels(reason=t.reason).inc()
            else:
                self.incidents.note(mw.incident, "recovery_attempt_failed", reason=t.reason,
                                    attempt=mw.attempt)
            self._schedule_recovery(mw)

    def _schedule_recovery(self, mw: ManagedWorker) -> None:
        cur = asyncio.current_task()
        if mw.recovery_task and not mw.recovery_task.done() and mw.recovery_task is not cur:
            return
        mw.recovery_task = asyncio.create_task(self._recover(mw), name=f"recover-{mw.id}")

    async def _recover(self, mw: ManagedWorker) -> None:
        inc = mw.incident
        rc = self.cfg.recovery
        self._apply(mw, mw.tracker.set_state(WorkerState.RECOVERING, "recovery_started"))
        if not mw.budget.allow():
            self._apply(
                mw, mw.tracker.set_state(WorkerState.QUARANTINED, "restart_budget_exhausted")
            )
            self.m_recoveries.labels(outcome="quarantined").inc()
            if inc is not None:
                self.incidents.close(inc, "quarantined", restarts_in_window=mw.budget.count(),
                                     window_s=rc.restart_window_s)
            log.error("worker_quarantined", worker=mw.id, restarts=mw.budget.count(),
                      window_s=rc.restart_window_s)
            return
        delay = backoff_delay(mw.attempt, rc.backoff_base_s, rc.backoff_cap_s, rc.jitter)
        if inc is not None:
            self.incidents.note(inc, "recovery_backoff", attempt=mw.attempt,
                                delay_s=round(delay, 3))
        await asyncio.sleep(delay)
        stop = await stop_with_escalation(
            self.backend, mw.spec, rc,
            drain=lambda s: self.prober.drain(s, rc.drain_grace_s),
        )
        if inc is not None:
            inc.attempts += 1
            inc.escalation.append(stop.rung)
            self.incidents.note(inc, "worker_stopped", rung=stop.rung,
                                duration_ms=round(stop.duration_ms, 1), exit_code=stop.exit_code)
        mw.budget.record()
        mw.attempt += 1
        mw.restarts_total += 1
        self.m_restarts.labels(worker=mw.id).inc()
        try:
            await self._launch(mw, "restart")
        except Exception as exc:
            log.error("worker_start_failed", worker=mw.id, error=str(exc))
            if inc is not None:
                self.incidents.note(inc, "worker_start_failed", error=str(exc))
            self._apply(mw, mw.tracker.set_state(WorkerState.UNHEALTHY, "start_failed"))
            return
        if inc is not None:
            self.incidents.note(inc, "worker_restarted", attempt=mw.attempt,
                                pid=self.backend.pid(mw.spec))

    # ------------------------------------------------------------------ gateway hooks

    def routable(self) -> list[ManagedWorker]:
        return [mw for mw in self.workers.values() if mw.state in self._routable_states]

    def record_request(
        self, worker_id: str, latency_ms: float, outcome: str, reason: str | None = None
    ) -> None:
        mw = self.workers.get(worker_id)
        if mw is None:
            return
        if outcome == "ok":
            mw.requests_ok += 1
            self.latency.record(worker_id, latency_ms)
        elif outcome == "shed":
            mw.requests_shed += 1
        else:
            mw.requests_err += 1
            if self.cfg.probe.passive_checks and reason:
                self._observe(mw, Verdict.fail(reason, "passive", latency_ms=round(latency_ms, 1)))

    # ------------------------------------------------------------------ operator actions

    async def restart_worker(self, worker_id: str, reason: str = "manual_restart") -> bool:
        mw = self.workers.get(worker_id)
        if mw is None or mw.state not in PROBED_STATES:
            return False
        self._apply(mw, mw.tracker.set_state(WorkerState.UNHEALTHY, reason, "operator"))
        return True

    async def quarantine(self, worker_id: str) -> bool:
        mw = self.workers.get(worker_id)
        if mw is None or mw.state == WorkerState.QUARANTINED:
            return False
        if mw.recovery_task and not mw.recovery_task.done():
            mw.recovery_task.cancel()
        self._apply(
            mw, mw.tracker.set_state(WorkerState.QUARANTINED, "manual_quarantine", "operator")
        )
        return True

    async def unquarantine(self, worker_id: str) -> bool:
        mw = self.workers.get(worker_id)
        if mw is None or mw.state != WorkerState.QUARANTINED:
            return False
        mw.budget.reset()
        mw.attempt = 0
        if await self.backend.is_alive(mw.spec):
            self._apply(mw, mw.tracker.set_state(WorkerState.STARTING, "unquarantine", "operator"))
        else:
            await self._launch(mw, "unquarantine")
        return True

    # ------------------------------------------------------------------ snapshot

    def snapshot(self) -> dict[str, Any]:
        workers = []
        for mw in self.workers.values():
            st = mw.last_status or {}
            workers.append({
                "id": mw.id, "port": mw.spec.port, "state": mw.state.value,
                "state_since": round(mw.tracker.state_since, 3),
                "time_in_state_s": round(mw.tracker.time_in_state(), 1),
                "reason": mw.tracker.last_reason, "pid": self.backend.pid(mw.spec),
                "uptime_s": round(time.time() - mw.started_at, 1) if mw.started_at else None,
                "restarts": mw.restarts_total, "backoff_attempt": mw.attempt,
                "budget_used": mw.budget.count(), "budget_max": mw.budget.max_restarts,
                "consecutive_failures": mw.tracker.consecutive_failures,
                "inflight": len(mw.inflight), "requests_ok": mw.requests_ok,
                "requests_err": mw.requests_err, "requests_shed": mw.requests_shed,
                "latency": self.latency.window(mw.id).summary(),
                "probe_ms": round(mw.last_probe_ms, 1) if mw.last_probe_ms is not None else None,
                "rss_mb": (round(st["rss_mb"], 1)
                           if isinstance(st.get("rss_mb"), int | float) else None),
                "cpu_pct": st.get("cpu_pct"), "gpu": st.get("gpu"), "engine": st.get("engine"),
                "chaos": {k: v for k, v in (st.get("chaos") or {}).items() if v},
                "incident": mw.incident.id if mw.incident else None,
            })
        states = [w["state"] for w in workers]
        return {
            "now": round(time.time(), 3),
            "uptime_s": round(time.time() - self.started_at, 1),
            "tick": self.tick_count,
            "fleet": {
                "total": len(workers),
                "healthy": states.count("healthy"),
                "routable": len(self.routable()),
                "degraded": states.count("degraded"),
                "recovering": states.count("recovering") + states.count("unhealthy"),
                "quarantined": states.count("quarantined"),
            },
            "workers": workers,
        }
