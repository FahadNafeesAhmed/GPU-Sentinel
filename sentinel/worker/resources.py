"""Process and GPU resource sampling for a worker (psutil + optional NVML)."""

from __future__ import annotations

import os
from typing import Any

import psutil


class ResourceSampler:
    def __init__(self, engine_memory_mb: Any = None) -> None:
        self._proc = psutil.Process(os.getpid())
        self._proc.cpu_percent(interval=None)  # prime the counter
        self._engine_memory_mb = engine_memory_mb
        self._nvml: Any = None
        self._nvml_handle: Any = None
        self._nvml_error: str | None = None
        self._init_nvml()

    def _init_nvml(self) -> None:
        try:
            import pynvml

            pynvml.nvmlInit()
            visible = os.environ.get("CUDA_VISIBLE_DEVICES", "0").split(",")[0].strip()
            index = int(visible) if visible.isdigit() else 0
            self._nvml_handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            self._nvml = pynvml
        except Exception as exc:  # pragma: no cover - depends on host
            self._nvml_error = f"{type(exc).__name__}: {exc}"

    def sample(self) -> dict[str, Any]:
        mem = self._proc.memory_info()
        out: dict[str, Any] = {
            "pid": self._proc.pid,
            "rss_mb": mem.rss / (1024 * 1024),
            "cpu_pct": self._proc.cpu_percent(interval=None),
            "threads": self._proc.num_threads(),
        }
        gpu = self._gpu_sample()
        if gpu:
            out["gpu"] = gpu
        return out

    def _gpu_sample(self) -> dict[str, Any] | None:
        proc_mb: float | None = None
        if self._engine_memory_mb is not None:
            try:
                proc_mb = self._engine_memory_mb()
            except Exception:
                proc_mb = None
        if self._nvml is None:
            if proc_mb is None:
                return None
            return {"proc_mem_mb": proc_mb, "source": "engine"}
        nv = self._nvml
        info: dict[str, Any] = {"source": "nvml"}
        try:
            m = nv.nvmlDeviceGetMemoryInfo(self._nvml_handle)
            info["device_used_mb"] = m.used / (1024 * 1024)
            info["device_total_mb"] = m.total / (1024 * 1024)
            util = nv.nvmlDeviceGetUtilizationRates(self._nvml_handle)
            info["util_pct"] = util.gpu
            info["temp_c"] = nv.nvmlDeviceGetTemperature(self._nvml_handle, nv.NVML_TEMPERATURE_GPU)
        except Exception as exc:  # pragma: no cover
            info["error"] = f"{type(exc).__name__}: {exc}"
        # Per-process memory is unavailable under WDDM on Windows; fall back to the engine.
        try:
            for p in nv.nvmlDeviceGetComputeRunningProcesses(self._nvml_handle):
                if p.pid == self._proc.pid and p.usedGpuMemory:
                    proc_mb = p.usedGpuMemory / (1024 * 1024)
                    info["source"] = "nvml-process"
        except Exception:
            pass
        if proc_mb is not None:
            info["proc_mem_mb"] = proc_mb
        return info
