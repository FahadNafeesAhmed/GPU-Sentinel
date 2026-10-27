"""Inference engines. ``infer`` is blocking and is run on a thread pool by the server.

CpuEngine  - numpy matmul chain; runs anywhere, deterministic cost per token.
TorchEngine - the same chain on CUDA in fp16 (falls back to CPU torch if no GPU).
"""

from __future__ import annotations

import hashlib
import time
from typing import Any, Protocol

import numpy as np


class Engine(Protocol):
    name: str
    device: str

    def infer(self, tokens: int) -> dict[str, Any]: ...

    def memory_mb(self) -> float | None: ...

    def allocate_mb(self, mb: float) -> Any: ...

    def allocate_until_oom(self, step_mb: float = 256.0, max_mb: float = 65536.0) -> float: ...


class CpuEngine:
    name = "cpu"
    device = "cpu"

    def __init__(self, dim: int = 128, seed: int = 7) -> None:
        rng = np.random.default_rng(seed)
        self.dim = dim
        # Orthogonal-ish weight keeps activations bounded through the tanh chain.
        q, _ = np.linalg.qr(rng.standard_normal((dim, dim)))
        self._w = (q * 0.9).astype(np.float32)
        self._x0 = rng.standard_normal((dim, dim)).astype(np.float32)

    def infer(self, tokens: int) -> dict[str, Any]:
        t0 = time.perf_counter()
        x = self._x0
        for _ in range(tokens):
            x = np.tanh(x @ self._w)
        checksum = hashlib.blake2b(x[:4, :4].tobytes(), digest_size=8).hexdigest()
        return {"compute_ms": (time.perf_counter() - t0) * 1000.0, "checksum": checksum}

    def memory_mb(self) -> float | None:
        return None

    def allocate_mb(self, mb: float) -> Any:
        buf = np.ones(int(mb * 1024 * 1024 / 8), dtype=np.float64)
        return buf

    def allocate_until_oom(self, step_mb: float = 256.0, max_mb: float = 65536.0) -> float:
        # On CPU we never truly exhaust host RAM (that would take the box down).
        # ``max_mb`` bounds the allocation so the controller's RSS limit fires instead.
        held: list[Any] = []
        total = 0.0
        try:
            while total < max_mb:
                held.append(self.allocate_mb(step_mb))
                total += step_mb
        except MemoryError:
            pass
        self._oom_hold = held
        return total


class TorchEngine:
    name = "torch"

    def __init__(self, dim: int = 1024, seed: int = 7) -> None:
        import torch  # local import: torch is optional

        self._torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        dtype = torch.float16 if self.device == "cuda" else torch.float32
        g = torch.Generator(device="cpu").manual_seed(seed)
        w = torch.randn((dim, dim), generator=g)
        q, _ = torch.linalg.qr(w)
        self._w = (q * 0.9).to(self.device, dtype)
        self._x0 = torch.randn((dim, dim), generator=g).to(self.device, dtype)
        self._held: list[Any] = []
        if self.device == "cuda":
            self.infer(4)  # warm up kernels / cuBLAS handles
        self.name = f"torch-{self.device}"

    def infer(self, tokens: int) -> dict[str, Any]:
        torch = self._torch
        t0 = time.perf_counter()
        x = self._x0
        with torch.no_grad():
            for _ in range(tokens):
                x = torch.tanh(x @ self._w)
            if self.device == "cuda":
                torch.cuda.synchronize()
            sample = x[:4, :4].float().cpu().numpy()
        checksum = hashlib.blake2b(sample.tobytes(), digest_size=8).hexdigest()
        return {"compute_ms": (time.perf_counter() - t0) * 1000.0, "checksum": checksum}

    def memory_mb(self) -> float | None:
        if self.device != "cuda":
            return None
        return self._torch.cuda.memory_reserved() / (1024 * 1024)

    def allocate_mb(self, mb: float) -> Any:
        n = int(mb * 1024 * 1024 / 2)  # fp16
        return self._torch.empty(n, dtype=self._torch.float16, device=self.device)

    def allocate_until_oom(self, step_mb: float = 256.0, max_mb: float = 65536.0) -> float:
        total = 0.0
        try:
            while total < max_mb:
                self._held.append(self.allocate_mb(step_mb))
                total += step_mb
        except (RuntimeError, MemoryError):  # torch.cuda.OutOfMemoryError subclasses RuntimeError
            pass
        return total


def make_engine(kind: str, dim: int | None = None) -> Engine:
    if kind == "cpu":
        return CpuEngine(dim or 128)
    if kind == "torch":
        return TorchEngine(dim or 1024)
    raise ValueError(f"unknown engine {kind!r}")
