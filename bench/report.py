"""Render a benchmark result JSON to Markdown."""
# ruff: noqa: RUF001  (typographic characters are intentional in the rendered Markdown)

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path
from typing import Any


def _med_range(vals: list[float | None], unit: str = "", nd: int = 0) -> str:
    xs = [v for v in vals if v is not None]
    if not xs:
        return "—"
    med = statistics.median(xs)
    fmt = f"{{:.{nd}f}}"
    if len(xs) == 1:
        return fmt.format(med) + unit
    return f"{fmt.format(med)}{unit} ({fmt.format(min(xs))}-{fmt.format(max(xs))})"


def _pct(vals: list[float]) -> str:
    return "—" if not vals else f"{statistics.mean(vals):.2f} %"


def render_markdown(result: dict[str, Any]) -> str:
    sysinfo = result["system"]
    cfg = result["config"]
    lines: list[str] = []
    lines.append(f"# GPU Sentinel benchmark — {result['run_id']}")
    lines.append("")
    lines.append(f"- **Engine**: `{result['engine']}` · **workers**: {cfg['workers']['count']} × "
                 f"concurrency {cfg['workers']['concurrency']} · **load**: {result['rps']} req/s "
                 f"open-loop, {result['tokens']} tokens/request")
    lines.append(f"- **Host**: {sysinfo['platform']} · {sysinfo['cpu']} "
                 f"({sysinfo['cpu_count']} logical CPUs) · GPU: {sysinfo.get('gpu') or 'n/a'} · "
                 f"Python {sysinfo['python']}")
    p = cfg["probe"]
    lines.append(f"- **Policy**: probe every {p['interval_s']} s (timeout {p['timeout_s']} s), "
                 f"{p['unhealthy_threshold']} consecutive failures → unhealthy, "
                 f"{p['healthy_threshold']} passes → healthy, startup probe "
                 f"{p['startup_interval_s']} s; backoff base {cfg['recovery']['backoff_base_s']} s "
                 f"cap {cfg['recovery']['backoff_cap_s']} s ({cfg['recovery']['jitter']} jitter); "
                 f"budget {cfg['recovery']['max_restarts']}/"
                 f"{cfg['recovery']['restart_window_s']} s")
    lines.append(f"- **Repeats per fault**: {result['repeats']} · total wall time "
                 f"{result['wall_s']:.0f} s")
    lines.append("")

    faults = result.get("faults", {})
    if faults:
        lines.append("## Fault injection (timings measured from the injection timestamp)")
        lines.append("")
        lines.append("| Scenario | n | Detect (ms) | Removed from routing (ms) "
                     "| Service restored (ms) | Stop rung | Requests in fault window | Failed "
                     "| Success in window | Retried OK |")
        lines.append("|---|---:|---:|---:|---:|---|---:|---:|---:|---:|")
        for name, runs in faults.items():
            ok_runs = [r for r in runs if "error" not in r]
            if not ok_runs:
                lines.append(f"| {name} | 0 | failed: {runs[0].get('error')} | | | | | | | |")
                continue
            rungs = sorted({"→".join(r["escalation"]) for r in ok_runs})
            win = [r["fault_window"] for r in ok_runs]
            lines.append(
                f"| {name} (`{ok_runs[0]['reason']}`) | {len(ok_runs)} | "
                f"{_med_range([r['detect_ms'] for r in ok_runs])} | "
                f"{_med_range([r['first_reaction_ms'] for r in ok_runs])} | "
                f"{_med_range([r['recover_ms'] for r in ok_runs])} | {', '.join(rungs)} | "
                f"{_med_range([w.get('n', 0) for w in win])} | "
                f"{_med_range([w.get('failed', 0) for w in win])} | "
                f"{_pct([w.get('success_pct', 0.0) for w in win if w.get('n')])} | "
                f"{_med_range([w.get('retried_ok', 0) for w in win])} |"
            )
        lines.append("")
        lines.append("*Detect* = incident opened. *Removed from routing* = first transition out of "
                     "HEALTHY (suspect/degraded/unhealthy), after which the gateway stops sending "
                     "new requests to the worker. *Service restored* = replacement worker passed "
                     "its readiness checks and is routable again. Ranges are min–max over repeats; "
                     "the headline figure is the median.")
        lines.append("")
        lines.append("| Scenario | p50 before fault | p99 before fault | p50 in window | "
                     "p99 in window | p99 after recovery | Total failed / total requests |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for name, runs in faults.items():
            ok_runs = [r for r in runs if "error" not in r]
            if not ok_runs:
                continue
            def g(key: str, sub: str, runs: list[dict[str, Any]] = ok_runs) -> list[Any]:
                return [r[sub].get(key) for r in runs]

            lines.append(
                f"| {name} | {_med_range(g('p50_ms', 'pre_fault'), ' ms', 1)} | "
                f"{_med_range(g('p99_ms', 'pre_fault'), ' ms', 1)} | "
                f"{_med_range(g('p50_ms', 'fault_window'), ' ms', 1)} | "
                f"{_med_range(g('p99_ms', 'fault_window'), ' ms', 1)} | "
                f"{_med_range(g('p99_ms', 'post_recovery'), ' ms', 1)} | "
                f"{sum(r['failed_total'] for r in ok_runs)} / "
                f"{sum(r['load']['n'] for r in ok_runs)} |"
            )
        lines.append("")

    loop = result.get("crash_loop")
    if loop:
        lines.append(f"## Crash loop (same worker killed again after every recovery; budget "
                     f"{loop['max_restarts']} restarts / {loop['restart_window_s']:.0f} s)")
        lines.append("")
        lines.append("| Kill # | Backoff slept (s) | Detect (ms) | Service restored (ms) "
                     "| Outcome |")
        lines.append("|---:|---:|---:|---:|---|")
        for r in loop["rounds"]:
            rec = r.get('recover_ms')
            lines.append(f"| {r['round']} | {r.get('backoff_s', '—')} | "
                         f"{r.get('detect_ms', '—')} | {rec if rec is not None else '—'} | "
                         f"{r.get('status', r.get('error'))} |")
        lines.append("")
        lines.append(f"Final state: `{loop['final_state']}`. Backoff is full-jitter, so slept "
                     "time is a uniform sample from a doubling envelope, not a smooth curve.")
        lines.append("")

    base = result.get("baseline")
    if base:
        lines.append("## False positives (healthy fleet under load)")
        lines.append("")
        lo = base["load"]
        lines.append(f"- {base['duration_s']:.0f} s at {base['achieved_rps']} req/s achieved "
                     f"({lo['n']} requests, {lo['success_pct']} % success, "
                     f"p50 {lo.get('p50_ms')} ms, p99 {lo.get('p99_ms')} ms)")
        lines.append(f"- **Incidents opened: {base['incidents_opened']}** · "
                     f"suspect transitions (single failed probe): {base['suspect_transitions']} · "
                     f"degraded transitions: {base['degraded_transitions']} · "
                     f"probe ticks: {base['probe_ticks']}")
        lines.append("")

    sweep = result.get("load_sweep")
    if sweep:
        lines.append(f"## Latency vs load ({sweep['duration_s']:.0f} s per level, healthy fleet)")
        lines.append("")
        lines.append("| Target req/s | Achieved | Success | p50 ms | p95 ms | p99 ms | max ms "
                     "| Shed (503) |")
        lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|")
        for lv in sweep["levels"]:
            shed = lv["by_status"].get("503", 0)
            lines.append(f"| {lv['target_rps']:.0f} | {lv['achieved_rps']} | "
                         f"{lv['success_pct']} % | {lv.get('p50_ms', '—')} | "
                         f"{lv.get('p95_ms', '—')} | {lv.get('p99_ms', '—')} | "
                         f"{lv.get('max_ms', '—')} | {shed} |")
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    for arg in argv:
        path = Path(arg)
        print(render_markdown(json.loads(path.read_text(encoding="utf-8"))))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
