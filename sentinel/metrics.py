"""Minimal, dependency-free Prometheus text-exposition registry.

Supports Counter, Gauge and Histogram with labels. Not a full client library:
no multiprocess mode, no push gateway. Rendering follows the text format spec:
https://prometheus.io/docs/instrumenting/exposition_formats/
"""

from __future__ import annotations

import math
import threading
from collections.abc import Iterable, Sequence

DEFAULT_BUCKETS: tuple[float, ...] = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)


def _escape_label(v: str) -> str:
    return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _fmt_labels(names: Sequence[str], values: Sequence[str], extra: str = "") -> str:
    parts = [f'{n}="{_escape_label(str(v))}"' for n, v in zip(names, values, strict=True)]
    if extra:
        parts.append(extra)
    return "{" + ",".join(parts) + "}" if parts else ""


def _fmt_float(v: float) -> str:
    if math.isinf(v):
        return "+Inf" if v > 0 else "-Inf"
    if math.isnan(v):
        return "NaN"
    if float(v).is_integer():
        return str(int(v))
    return repr(float(v))


class _Metric:
    kind = "untyped"

    def __init__(self, name: str, help_text: str, labelnames: Sequence[str] = ()) -> None:
        self.name = name
        self.help = help_text
        self.labelnames = tuple(labelnames)
        self._lock = threading.Lock()
        self._children: dict[tuple[str, ...], object] = {}

    def _key(self, labels: dict[str, str]) -> tuple[str, ...]:
        if set(labels) != set(self.labelnames):
            raise ValueError(f"{self.name}: expected labels {self.labelnames}, got {tuple(labels)}")
        return tuple(str(labels[n]) for n in self.labelnames)

    def _header(self) -> list[str]:
        return [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} {self.kind}"]

    def render(self) -> list[str]:  # pragma: no cover - overridden
        raise NotImplementedError


class Counter(_Metric):
    kind = "counter"

    class _Child:
        __slots__ = ("value",)

        def __init__(self) -> None:
            self.value = 0.0

        def inc(self, n: float = 1.0) -> None:
            if n < 0:
                raise ValueError("counters only go up")
            self.value += n

    def labels(self, **labels: str) -> Counter._Child:
        key = self._key(labels)
        with self._lock:
            child = self._children.get(key)
            if child is None:
                child = self._children[key] = Counter._Child()
        return child  # type: ignore[return-value]

    def inc(self, n: float = 1.0) -> None:
        self.labels().inc(n)

    def render(self) -> list[str]:
        out = self._header()
        with self._lock:
            for key, child in sorted(self._children.items()):
                lbl = _fmt_labels(self.labelnames, key)
                out.append(f"{self.name}{lbl} {_fmt_float(child.value)}")  # type: ignore[attr-defined]
        return out


class Gauge(_Metric):
    kind = "gauge"

    class _Child:
        __slots__ = ("value",)

        def __init__(self) -> None:
            self.value = 0.0

        def set(self, v: float) -> None:
            self.value = float(v)

        def inc(self, n: float = 1.0) -> None:
            self.value += n

        def dec(self, n: float = 1.0) -> None:
            self.value -= n

    def labels(self, **labels: str) -> Gauge._Child:
        key = self._key(labels)
        with self._lock:
            child = self._children.get(key)
            if child is None:
                child = self._children[key] = Gauge._Child()
        return child  # type: ignore[return-value]

    def set(self, v: float) -> None:
        self.labels().set(v)

    def remove(self, **labels: str) -> None:
        with self._lock:
            self._children.pop(self._key(labels), None)

    def render(self) -> list[str]:
        out = self._header()
        with self._lock:
            for key, child in sorted(self._children.items()):
                lbl = _fmt_labels(self.labelnames, key)
                out.append(f"{self.name}{lbl} {_fmt_float(child.value)}")  # type: ignore[attr-defined]
        return out


class Histogram(_Metric):
    kind = "histogram"

    class _Child:
        __slots__ = ("buckets", "count", "counts", "sum")

        def __init__(self, buckets: tuple[float, ...]) -> None:
            self.buckets = buckets
            self.counts = [0] * len(buckets)
            self.sum = 0.0
            self.count = 0

        def observe(self, v: float) -> None:
            self.sum += v
            self.count += 1
            for i, b in enumerate(self.buckets):
                if v <= b:
                    self.counts[i] += 1

    def __init__(
        self,
        name: str,
        help_text: str,
        labelnames: Sequence[str] = (),
        buckets: Iterable[float] = DEFAULT_BUCKETS,
    ) -> None:
        super().__init__(name, help_text, labelnames)
        bs = sorted(set(float(b) for b in buckets))
        if not bs or bs[-1] != math.inf:
            bs.append(math.inf)
        self.buckets = tuple(bs)

    def labels(self, **labels: str) -> Histogram._Child:
        key = self._key(labels)
        with self._lock:
            child = self._children.get(key)
            if child is None:
                child = self._children[key] = Histogram._Child(self.buckets)
        return child  # type: ignore[return-value]

    def observe(self, v: float) -> None:
        self.labels().observe(v)

    def render(self) -> list[str]:
        out = self._header()
        with self._lock:
            for key, child in sorted(self._children.items()):
                c: Histogram._Child = child  # type: ignore[assignment]
                for b, n in zip(c.buckets, c.counts, strict=True):
                    le = _fmt_labels(self.labelnames, key, f'le="{_fmt_float(b)}"')
                    out.append(f"{self.name}_bucket{le} {n}")
                lbl = _fmt_labels(self.labelnames, key)
                out.append(f"{self.name}_sum{lbl} {_fmt_float(c.sum)}")
                out.append(f"{self.name}_count{lbl} {c.count}")
        return out


class Registry:
    def __init__(self) -> None:
        self._metrics: dict[str, _Metric] = {}
        self._lock = threading.Lock()

    def _register(self, m: _Metric) -> None:
        with self._lock:
            if m.name in self._metrics:
                raise ValueError(f"metric already registered: {m.name}")
            self._metrics[m.name] = m

    def counter(self, name: str, help_text: str, labelnames: Sequence[str] = ()) -> Counter:
        c = Counter(name, help_text, labelnames)
        self._register(c)
        return c

    def gauge(self, name: str, help_text: str, labelnames: Sequence[str] = ()) -> Gauge:
        g = Gauge(name, help_text, labelnames)
        self._register(g)
        return g

    def histogram(
        self,
        name: str,
        help_text: str,
        labelnames: Sequence[str] = (),
        buckets: Iterable[float] = DEFAULT_BUCKETS,
    ) -> Histogram:
        h = Histogram(name, help_text, labelnames, buckets)
        self._register(h)
        return h

    def render(self) -> str:
        lines: list[str] = []
        with self._lock:
            metrics = list(self._metrics.values())
        for m in metrics:
            lines.extend(m.render())
        return "\n".join(lines) + "\n"


CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
