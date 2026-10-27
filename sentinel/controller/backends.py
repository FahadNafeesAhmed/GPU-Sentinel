"""Worker lifecycle backends: local subprocesses or Docker containers.

The supervisor only talks to this interface, so swapping the deployment target
does not touch detection or recovery logic.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from pathlib import Path

from sentinel.config import WorkersConfig, WorkerSpec
from sentinel.logging_setup import get_logger

log = get_logger("sentinel.backend")


class WorkerBackend(ABC):
    @abstractmethod
    async def start(self, spec: WorkerSpec) -> None: ...

    @abstractmethod
    async def is_alive(self, spec: WorkerSpec) -> bool: ...

    @abstractmethod
    def exit_code(self, spec: WorkerSpec) -> int | None: ...

    @abstractmethod
    def pid(self, spec: WorkerSpec) -> int | None: ...

    @abstractmethod
    async def terminate(self, spec: WorkerSpec) -> None: ...

    @abstractmethod
    async def kill(self, spec: WorkerSpec) -> None: ...

    async def wait_exit(self, spec: WorkerSpec, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if not await self.is_alive(spec):
                return True
            await asyncio.sleep(0.02)
        return not await self.is_alive(spec)

    async def wait_for_exit(self, spec: WorkerSpec) -> int | None:
        """Block until the worker exits and return its exit code.

        Backends that cannot wait event-style return None immediately; the supervisor
        then relies on the per-tick ``is_alive`` poll alone.
        """
        return None

    async def close(self) -> None:
        return None


class SubprocessBackend(WorkerBackend):
    """Spawns ``python -m sentinel worker`` children; stdout/stderr go to per-worker logs."""

    def __init__(self, workers: WorkersConfig, log_dir: str | Path = "data/logs") -> None:
        self.workers = workers
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._procs: dict[str, subprocess.Popen[bytes]] = {}
        self._logs: dict[str, object] = {}

    def command(self, spec: WorkerSpec) -> list[str]:
        engine = spec.engine or self.workers.engine
        return [
            sys.executable, "-u", "-m", "sentinel", "worker",
            "--id", spec.id, "--port", str(spec.port), "--host", spec.host,
            "--engine", engine, "--concurrency", str(self.workers.concurrency),
            "--log-format", "json", *spec.extra_args,
        ]

    async def start(self, spec: WorkerSpec) -> None:
        old = self._procs.get(spec.id)
        if old is not None and old.poll() is None:
            raise RuntimeError(f"{spec.id} is already running (pid {old.pid})")
        logf = open(self.log_dir / f"{spec.id}.log", "ab", buffering=0)  # noqa: SIM115
        self._logs[spec.id] = logf
        env = dict(os.environ)
        env.setdefault("PYTHONUNBUFFERED", "1")
        proc = subprocess.Popen(
            self.command(spec), stdout=logf, stderr=subprocess.STDOUT, env=env,
            cwd=os.getcwd(),
        )
        self._procs[spec.id] = proc
        log.info("worker_spawned", worker=spec.id, pid=proc.pid, port=spec.port)

    async def is_alive(self, spec: WorkerSpec) -> bool:
        proc = self._procs.get(spec.id)
        return proc is not None and proc.poll() is None

    def exit_code(self, spec: WorkerSpec) -> int | None:
        proc = self._procs.get(spec.id)
        return None if proc is None else proc.poll()

    def pid(self, spec: WorkerSpec) -> int | None:
        proc = self._procs.get(spec.id)
        return None if proc is None or proc.poll() is not None else proc.pid

    async def wait_for_exit(self, spec: WorkerSpec) -> int | None:
        proc = self._procs.get(spec.id)
        if proc is None:
            return None
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, proc.wait)

    async def terminate(self, spec: WorkerSpec) -> None:
        proc = self._procs.get(spec.id)
        if proc is not None and proc.poll() is None:
            proc.terminate()  # SIGTERM on POSIX, TerminateProcess on Windows

    async def kill(self, spec: WorkerSpec) -> None:
        proc = self._procs.get(spec.id)
        if proc is not None and proc.poll() is None:
            proc.kill()

    async def close(self) -> None:
        for proc in self._procs.values():
            if proc.poll() is None:
                proc.terminate()
        deadline = time.monotonic() + 3.0
        for proc in self._procs.values():
            remaining = max(0.0, deadline - time.monotonic())
            try:
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda p=proc, r=remaining: p.wait(timeout=r)  # type: ignore[misc]
                )
            except subprocess.TimeoutExpired:
                proc.kill()
        for f in self._logs.values():
            try:
                f.close()  # type: ignore[attr-defined]
            except Exception:
                pass


class DockerBackend(WorkerBackend):
    """Controls pre-created containers (e.g. from docker compose) through the docker CLI."""

    def __init__(self, workers: WorkersConfig, docker_bin: str = "docker") -> None:
        self.workers = workers
        self.docker = docker_bin
        self._last_exit: dict[str, int | None] = {}

    def container(self, spec: WorkerSpec) -> str:
        return spec.container or f"sentinel-{spec.id}"

    async def _run(self, *args: str, timeout: float = 15.0) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            self.docker, *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except TimeoutError:
            proc.kill()
            return 124, "timeout"
        return proc.returncode or 0, out.decode(errors="replace").strip()

    async def start(self, spec: WorkerSpec) -> None:
        rc, out = await self._run("start", self.container(spec))
        if rc != 0:
            raise RuntimeError(f"docker start {self.container(spec)} failed: {out}")
        log.info("container_started", worker=spec.id, container=self.container(spec))

    async def is_alive(self, spec: WorkerSpec) -> bool:
        rc, out = await self._run(
            "inspect", "-f", "{{.State.Running}} {{.State.ExitCode}}", self.container(spec),
            timeout=5.0,
        )
        if rc != 0:
            return False
        running, _, code = out.partition(" ")
        self._last_exit[spec.id] = int(code) if code.strip().lstrip("-").isdigit() else None
        return running.strip() == "true"

    def exit_code(self, spec: WorkerSpec) -> int | None:
        return self._last_exit.get(spec.id)

    def pid(self, spec: WorkerSpec) -> int | None:
        return None

    async def wait_for_exit(self, spec: WorkerSpec) -> int | None:
        rc, out = await self._run("wait", self.container(spec), timeout=86400.0)
        if rc != 0 or not out.strip().lstrip("-").isdigit():
            return None
        code = int(out.strip())
        self._last_exit[spec.id] = code
        return code

    async def terminate(self, spec: WorkerSpec) -> None:
        await self._run("stop", "-t", "2", self.container(spec), timeout=10.0)

    async def kill(self, spec: WorkerSpec) -> None:
        await self._run("kill", self.container(spec), timeout=10.0)


def make_backend(workers: WorkersConfig, data_dir: str) -> WorkerBackend:
    if workers.backend == "docker":
        return DockerBackend(workers)
    return SubprocessBackend(workers, Path(data_dir) / "logs")
