"""Benchmark driver: ``python -m sentinel bench [options]`` or ``python -m bench.run``."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import aiohttp

from bench.report import render_markdown
from bench.scenarios import FAULT_SCENARIOS, BenchContext, baseline, crash_loop, load_sweep
from sentinel import __version__
from sentinel.config import load_config
from sentinel.controller.main import Controller
from sentinel.logging_setup import setup_logging


def system_info() -> dict[str, Any]:
    cpu = platform.processor() or ""
    if sys.platform.startswith("linux"):
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.lower().startswith("model name"):
                    cpu = line.split(":", 1)[1].strip()
                    break
        except OSError:
            pass
    gpu = None
    try:
        out = subprocess.run(["nvidia-smi", "--query-gpu=name,memory.total,driver_version",
                              "--format=csv,noheader"], capture_output=True, text=True, timeout=5)
        if out.returncode == 0 and out.stdout.strip():
            gpu = out.stdout.strip().splitlines()[0]
    except (OSError, subprocess.TimeoutExpired):
        pass
    return {"platform": platform.platform(), "cpu": cpu, "cpu_count": os.cpu_count(),
            "python": platform.python_version(), "gpu": gpu, "sentinel": __version__}


def parse(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="sentinel bench")
    p.add_argument("--config", default="config/sentinel.toml")
    p.add_argument("--engine", default=None, choices=["cpu", "torch"])
    p.add_argument("--workers", type=int, default=None)
    p.add_argument("--rps", type=float, default=100.0, help="open-loop load during fault runs")
    p.add_argument("--tokens", type=int, default=16)
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--scenarios", default="all",
                   help="comma list of " + ",".join(FAULT_SCENARIOS) + ",baseline,sweep | all")
    p.add_argument("--baseline-s", type=float, default=60.0)
    p.add_argument("--sweep", default="50,100,200,400,800", help="req/s levels")
    p.add_argument("--sweep-s", type=float, default=15.0)
    p.add_argument("--out", default="bench/results")
    p.add_argument("--quick", action="store_true", help="1 repeat, short durations")
    p.add_argument("--set", dest="overrides", action="append", default=[])
    return p.parse_args(argv)


async def run_bench(args: argparse.Namespace) -> dict[str, Any]:
    cfg = load_config(args.config, overrides=args.overrides)
    if args.engine:
        cfg.workers.engine = args.engine
    if args.workers:
        cfg.workers.count = args.workers
    cfg.log_format = "text"
    cfg.log_level = "WARNING"
    run_id = time.strftime("%Y%m%d-%H%M%S") + f"-{cfg.workers.engine}"
    out_dir = Path(args.out) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    cfg.data_dir = str(out_dir / "data")
    setup_logging(cfg.log_level, cfg.log_format)

    wanted = ([*FAULT_SCENARIOS, "crash_loop", "baseline", "sweep"] if args.scenarios == "all"
              else [s.strip() for s in args.scenarios.split(",") if s.strip()])
    repeats = 1 if args.quick else args.repeats
    baseline_s = 15.0 if args.quick else args.baseline_s
    sweep_s = 6.0 if args.quick else args.sweep_s
    levels = [float(x) for x in args.sweep.split(",") if x]

    t_start = time.time()

    def log(msg: str) -> None:
        print(f"[{time.time() - t_start:7.1f}s] {msg}", flush=True)

    log(f"run {run_id}: engine={cfg.workers.engine} workers={cfg.workers.count} "
        f"rps={args.rps} tokens={args.tokens} repeats={repeats} scenarios={wanted}")
    ctl = Controller(cfg)
    await ctl.start()
    result: dict[str, Any] = {
        "run_id": run_id, "engine": cfg.workers.engine, "rps": args.rps, "tokens": args.tokens,
        "repeats": repeats, "system": system_info(), "config": cfg.to_dict(),
        "faults": {}, "crash_loop": None, "baseline": None, "load_sweep": None,
    }
    try:
        if not await ctl.wait_ready(120.0):
            raise RuntimeError("fleet did not become healthy")
        log(f"fleet healthy; dashboard at {ctl.api_url}/")
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
            ctx = BenchContext(ctl, cfg, session, args.rps, args.tokens, log)
            # warm-up: JIT-free, but fills connection pools and OS caches
            from bench.load import OpenLoopLoad
            await OpenLoopLoad(session, ctx.gateway, args.rps, 3.0, args.tokens).run()
            for name in wanted:
                if name in FAULT_SCENARIOS:
                    runs = []
                    for i in range(repeats):
                        try:
                            r = await FAULT_SCENARIOS[name](ctx)
                        except Exception as exc:  # keep going; the report shows the failure
                            r = {"scenario": name, "error": f"{type(exc).__name__}: {exc}"}
                        runs.append(r)
                        if "error" in r:
                            log(f"  {name} #{i + 1}: ERROR {r['error']}")
                        else:
                            fw = r["fault_window"]
                            log(f"  {name} #{i + 1}: detect {r['detect_ms']} ms, routed-around "
                                f"{r['first_reaction_ms']} ms, restored {r['recover_ms']} ms, "
                                f"window {fw.get('failed', 0)}/{fw.get('n', 0)} failed, "
                                f"rung {r['escalation']}")
                    result["faults"][name] = runs
                elif name == "crash_loop":
                    log("crash loop ...")
                    result["crash_loop"] = await crash_loop(ctx)
                    for r in result["crash_loop"]["rounds"]:
                        log(f"  round {r['round']}: {r.get('status')} backoff "
                            f"{r.get('backoff_s')} s recover {r.get('recover_ms')} ms "
                            f"budget {r.get('budget_used')}")
                elif name == "baseline":
                    log(f"baseline {baseline_s:.0f}s ...")
                    result["baseline"] = await baseline(ctx, baseline_s)
                    b = result["baseline"]
                    log(f"  baseline: incidents {b['incidents_opened']}, suspects "
                        f"{b['suspect_transitions']}, p99 {b['load'].get('p99_ms')} ms")
                elif name == "sweep":
                    log("load sweep ...")
                    result["load_sweep"] = await load_sweep(ctx, levels, sweep_s)
                else:
                    log(f"unknown scenario {name!r}")
    finally:
        await ctl.stop()
    result["wall_s"] = time.time() - t_start
    (out_dir / "result.json").write_text(json.dumps(result, indent=2, default=str),
                                         encoding="utf-8")
    md = render_markdown(result)
    (out_dir / "RESULTS.md").write_text(md, encoding="utf-8")
    latest = Path(args.out) / f"latest-{cfg.workers.engine}.md"
    latest.write_text(md, encoding="utf-8")
    log(f"wrote {out_dir / 'result.json'} and {latest}")
    return result


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # Windows consoles
    args = parse(argv if argv is not None else sys.argv[1:])
    result = asyncio.run(run_bench(args))
    print()
    print(render_markdown(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
