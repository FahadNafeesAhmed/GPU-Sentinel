"""Command-line entry point: ``python -m sentinel {controller|worker|bench}``."""

from __future__ import annotations

import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sentinel", description="GPU Sentinel")
    sub = parser.add_subparsers(dest="command", required=True)

    w = sub.add_parser("worker", help="run one inference worker")
    from sentinel.worker.app import add_worker_args

    add_worker_args(w)

    c = sub.add_parser("controller", help="run the monitoring controller + gateway")
    c.add_argument("--config", default=None, help="TOML config path")
    c.add_argument("--set", dest="overrides", action="append", default=[],
                   metavar="SECTION.KEY=VALUE", help="override a config value")
    c.add_argument("--log-format", default=None, choices=["json", "text"])
    c.add_argument("--log-level", default=None)

    sub.add_parser("bench", help="run the fault-injection benchmark suite "
                                 "(all further options are passed through; see bench --help)")
    return parser


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if raw and raw[0] == "bench":
        from bench.run import main as bench_main

        return bench_main(raw[1:])
    args = build_parser().parse_args(raw)
    if args.command == "worker":
        from sentinel.worker.app import run_worker

        run_worker(args)
        return 0
    if args.command == "controller":
        from sentinel.controller.main import run_controller

        return run_controller(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
