"""Benchmark scenarios. Each injects one fault under open-loop load and reports timings
measured against the injection timestamp taken by the *injector*, not the controller."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp
import psutil

from bench.load import OpenLoopLoad, summarize
from sentinel.config import SentinelConfig
from sentinel.controller.main import Controller
from sentinel.controller.state import WorkerState


@dataclass
class BenchContext:
    ctl: Controller
    cfg: SentinelConfig
    session: aiohttp.ClientSession
    rps: float
    tokens: int
    log: Callable[[str], None]

    @property
    def gateway(self) -> str:
        return self.ctl.gateway_url + "/infer"

    def worker_url(self, wid: str) -> str:
        return next(s.base_url for s in self.cfg.workers.resolved_specs() if s.id == wid)

    async def chaos(self, wid: str, cmd: str, **params: Any) -> float:
        t = time.time()
        try:
            async with self.session.post(f"{self.worker_url(wid)}/chaos/{cmd}", json=params,
                                         timeout=aiohttp.ClientTimeout(total=2.0)) as r:
                r.raise_for_status()
        except (TimeoutError, aiohttp.ClientOSError, aiohttp.ServerDisconnectedError):
            pass  # crash/hang faults may take effect before the reply is flushed
        return t

    async def settle(self, timeout_s: float = 60.0, reset_policy: bool = True) -> None:
        """Wait until the fleet is fully healthy, then a short quiet period.

        ``reset_policy`` clears each worker's backoff attempt counter and restart budget so
        every repeat measures the first-restart path (the crash_loop scenario measures the
        growth explicitly)."""
        assert self.ctl.supervisor is not None
        if reset_policy:
            for mw in self.ctl.supervisor.workers.values():
                mw.attempt = 0
                mw.budget.reset()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            states = {mw.state for mw in self.ctl.supervisor.workers.values()}
            if states == {WorkerState.HEALTHY}:
                break
            await asyncio.sleep(0.1)
        else:
            raise RuntimeError("fleet did not settle")
        await asyncio.sleep(1.0)

    def events(self) -> list[dict[str, Any]]:
        assert self.ctl.incidents is not None
        return list(self.ctl.incidents.events)

    def incidents(self) -> list[dict[str, Any]]:
        assert self.ctl.incidents is not None
        return self.ctl.incidents.list(limit=1000)


async def _wait_incident(ctx: BenchContext, wid: str, t_fault: float,
                         closed: bool, timeout_s: float) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for inc in ctx.incidents():
            if inc["worker"] == wid and inc["opened_at"] >= t_fault - 0.2 and (
                not closed or inc["status"] != "open"
            ):
                return inc
        await asyncio.sleep(0.01)
    return None


async def measure_fault(
    ctx: BenchContext,
    name: str,
    wid: str,
    inject: Callable[[], Awaitable[float]],
    *,
    fault_at_s: float = 5.0,
    duration_s: float = 25.0,
    recover_timeout_s: float = 90.0,
) -> dict[str, Any]:
    await ctx.settle()
    seq_before = ctx.events()[-1]["seq"] if ctx.events() else 0
    load = asyncio.create_task(
        OpenLoopLoad(ctx.session, ctx.gateway, ctx.rps, duration_s, ctx.tokens).run()
    )
    await asyncio.sleep(fault_at_s)
    try:
        t_fault = await inject()
    except Exception as exc:
        await load
        return {"scenario": name, "worker": wid, "error": f"inject failed: {exc!r}"}
    inc = await _wait_incident(ctx, wid, t_fault, closed=True, timeout_s=recover_timeout_s)
    result = await load
    out: dict[str, Any] = {
        "scenario": name, "worker": wid, "t_fault": t_fault,
        "load": summarize(result.samples), "achieved_rps": round(result.achieved_rps, 1),
    }
    if inc is None:
        out["error"] = "incident not observed/closed in time"
        return out
    t_detect, t_rec = inc["opened_at"], inc["recovered_at"]
    first_degraded = next(
        (e for e in ctx.events() if e["seq"] > seq_before and e["event"] == "state_transition"
         and e.get("worker") == wid and e.get("new") in ("degraded", "suspect", "unhealthy")),
        None,
    )
    out.update({
        "incident": inc["id"], "reason": inc["reason"], "source": inc["source"],
        "status": inc["status"], "attempts": inc["attempts"], "escalation": inc["escalation"],
        "detect_ms": round((t_detect - t_fault) * 1000, 1),
        "recover_ms": round((t_rec - t_fault) * 1000, 1) if t_rec else None,
        "first_reaction_ms": (round((first_degraded["ts"] - t_fault) * 1000, 1)
                              if first_degraded else None),
        "symptom_to_detect_ms": inc["symptom_to_detect_ms"],
        "detect_to_recover_ms": inc["detect_to_recover_ms"],
        "fault_window": summarize(result.samples, t_fault, t_rec),
        "pre_fault": summarize(result.samples, None, t_fault),
        "post_recovery": summarize(result.samples, t_rec, None),
        "failed_total": sum(1 for s in result.samples if s.status != 200),
        "failed_in_window": sum(1 for s in result.samples
                                if s.status != 200 and t_fault <= s.t <= (t_rec or 1e18)),
    })
    return out


# --------------------------------------------------------------------------- scenarios

async def crash_sigkill(ctx: BenchContext, wid: str = "w2") -> dict[str, Any]:
    async def inject() -> float:
        assert ctx.ctl.supervisor is not None
        pid = ctx.ctl.supervisor.backend.pid(ctx.ctl.supervisor.workers[wid].spec)
        assert pid
        t = time.time()
        psutil.Process(pid).kill()
        return t
    return await measure_fault(ctx, "crash_sigkill", wid, inject)


async def crash_internal(ctx: BenchContext, wid: str = "w3") -> dict[str, Any]:
    async def inject() -> float:
        return await ctx.chaos(wid, "crash", delay_ms=30, exit_code=139)
    return await measure_fault(ctx, "crash_internal", wid, inject)


async def hang(ctx: BenchContext, wid: str = "w1") -> dict[str, Any]:
    async def inject() -> float:
        return await ctx.chaos(wid, "hang", delay_ms=0)
    return await measure_fault(ctx, "hang", wid, inject)


async def slow(ctx: BenchContext, wid: str = "w2", extra_ms: float = 800.0) -> dict[str, Any]:
    async def inject() -> float:
        return await ctx.chaos(wid, "slow", ms=extra_ms)
    out = await measure_fault(ctx, "slow", wid, inject, duration_s=45.0, recover_timeout_s=120.0)
    out["extra_latency_ms"] = extra_ms
    return out


async def leak(ctx: BenchContext, wid: str = "w3", mb_per_request: float = 0.25) -> dict[str, Any]:
    async def inject() -> float:
        return await ctx.chaos(wid, "leak", mb_per_request=mb_per_request)
    window = ctx.cfg.limits.leak_window_s
    out = await measure_fault(ctx, "leak", wid, inject, duration_s=window + 30.0,
                              recover_timeout_s=window + 90.0)
    out["mb_per_request"] = mb_per_request
    return out


async def oom(ctx: BenchContext, wid: str = "w1") -> dict[str, Any]:
    """CPU engine: allocate past the RSS limit. Torch engine: allocate until CUDA OOM, after
    which inference fails with 500s and passive checks recycle the worker."""
    async def inject() -> float:
        if ctx.cfg.workers.engine == "torch":
            cap = 65536.0  # allocate until CUDA actually reports OOM
        else:
            cap = (ctx.cfg.limits.rss_mb or 1536.0) + 512.0  # just past the RSS limit
        return await ctx.chaos(wid, "oom", max_mb=cap, step_mb=128)
    return await measure_fault(ctx, "oom", wid, inject)


async def crash_loop(ctx: BenchContext, wid: str = "w3") -> dict[str, Any]:
    """Kill the same worker again every time it comes back, without resetting policy state:
    shows backoff growth per attempt and the restart budget ending in quarantine."""
    await ctx.settle()
    assert ctx.ctl.supervisor is not None
    sup = ctx.ctl.supervisor
    mw = sup.workers[wid]
    max_restarts = ctx.cfg.recovery.max_restarts
    rounds: list[dict[str, Any]] = []
    for i in range(max_restarts + 1):
        pid = sup.backend.pid(mw.spec)
        assert pid
        t_kill = time.time()
        psutil.Process(pid).kill()
        deadline = time.monotonic() + 90.0
        inc = None
        while time.monotonic() < deadline:
            inc = next((x for x in ctx.incidents()
                        if x["worker"] == wid and x["opened_at"] >= t_kill - 0.2
                        and x["status"] != "open"), None)
            if inc is not None:
                break
            await asyncio.sleep(0.01)
        if inc is None:
            rounds.append({"round": i + 1, "error": "incident did not close"})
            break
        backoff = next((n["delay_s"] for n in inc["notes"] if n["event"] == "recovery_backoff"),
                       None)
        rounds.append({
            "round": i + 1, "status": inc["status"], "backoff_s": backoff,
            "detect_ms": round((inc["opened_at"] - t_kill) * 1000, 1),
            "recover_ms": (round((inc["recovered_at"] - t_kill) * 1000, 1)
                           if inc["status"] == "recovered" else None),
            "budget_used": mw.budget.count(),
        })
        if inc["status"] != "recovered":
            break
        # wait until the replacement is HEALTHY before killing it again
        while mw.state != WorkerState.HEALTHY:
            await asyncio.sleep(0.01)
    final_state = mw.state.value
    await sup.unquarantine(wid)
    return {"scenario": "crash_loop", "worker": wid, "rounds": rounds,
            "final_state": final_state, "max_restarts": max_restarts,
            "restart_window_s": ctx.cfg.recovery.restart_window_s}


async def baseline(ctx: BenchContext, duration_s: float = 60.0) -> dict[str, Any]:
    """Healthy fleet under load: counts anything the detector got wrong."""
    await ctx.settle()
    seq_before = ctx.events()[-1]["seq"] if ctx.events() else 0
    inc_before = len(ctx.incidents())
    res = await OpenLoopLoad(ctx.session, ctx.gateway, ctx.rps, duration_s, ctx.tokens).run()
    evs = [e for e in ctx.events() if e["seq"] > seq_before]
    suspects = [e for e in evs if e["event"] == "state_transition" and e.get("new") == "suspect"]
    degraded = [e for e in evs if e["event"] == "state_transition" and e.get("new") == "degraded"]
    return {
        "scenario": "baseline", "duration_s": duration_s, "rps": ctx.rps,
        "achieved_rps": round(res.achieved_rps, 1), "load": summarize(res.samples),
        "incidents_opened": len(ctx.incidents()) - inc_before,
        "suspect_transitions": len(suspects), "degraded_transitions": len(degraded),
        "probe_ticks": ctx.ctl.supervisor.tick_count if ctx.ctl.supervisor else None,
    }


async def load_sweep(ctx: BenchContext, levels: list[float], duration_s: float = 15.0
                     ) -> dict[str, Any]:
    rows = []
    for rps in levels:
        await ctx.settle()
        res = await OpenLoopLoad(ctx.session, ctx.gateway, rps, duration_s, ctx.tokens).run()
        s = summarize(res.samples)
        s.update({"target_rps": rps, "achieved_rps": round(res.achieved_rps, 1)})
        rows.append(s)
        ctx.log(f"  sweep {rps:>5} rps -> p50 {s.get('p50_ms')} p99 {s.get('p99_ms')} "
                f"ok {s['success_pct']}%")
    return {"scenario": "load_sweep", "duration_s": duration_s, "levels": rows}


FAULT_SCENARIOS: dict[str, Callable[[BenchContext], Awaitable[dict[str, Any]]]] = {
    "crash_sigkill": crash_sigkill,
    "crash_internal": crash_internal,
    "hang": hang,
    "slow": slow,
    "leak": leak,
    "oom": oom,
}
