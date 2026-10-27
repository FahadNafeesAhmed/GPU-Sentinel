"""Typed configuration for the controller, loaded from TOML with env/CLI overrides.

Precedence (lowest to highest): dataclass defaults -> TOML file -> environment
variables (``SENTINEL_<SECTION>__<KEY>``) -> ``--set section.key=value`` CLI flags.
"""

from __future__ import annotations

import dataclasses
import os
import tomllib
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, get_args, get_origin, get_type_hints

ENV_PREFIX = "SENTINEL_"


@dataclass
class ProbeConfig:
    """Active health probing. Hysteresis thresholds control false-positive rate."""

    interval_s: float = 1.0
    timeout_s: float = 1.0
    unhealthy_threshold: int = 3  # consecutive failed probes before acting
    healthy_threshold: int = 2  # consecutive passes before trusting a (re)started worker
    startup_grace_s: float = 15.0  # how long a STARTING worker may take to pass probes
    startup_interval_s: float = 0.2  # faster probing while STARTING (shortens recovery)
    passive_checks: bool = True  # gateway-observed errors count as probe failures


@dataclass
class ResourceLimits:
    """Hard limits (immediate recycle) and leak-trend detection (proactive recycle)."""

    rss_mb: float | None = 1536.0
    gpu_mem_mb: float | None = None
    leak_slope_mb_per_min: float | None = 60.0
    leak_window_s: float = 45.0
    leak_min_samples: int = 10


@dataclass
class LatencyPolicy:
    """Passive degradation detection from gateway-observed request latencies."""

    window_s: float = 15.0
    min_samples: int = 20
    p95_abs_ms: float = 400.0  # never flag below this absolute p95
    p95_fleet_ratio: float = 3.0  # ...or below this multiple of the peer median p95
    degraded_consecutive: int = 3  # consecutive ticks over threshold -> DEGRADED
    recycle_degraded_after_s: float = 15.0  # DEGRADED for this long -> UNHEALTHY
    stuck_request_s: float = 8.0  # an in-flight request older than this = hang


@dataclass
class RecoveryConfig:
    """Restart policy: capped exponential backoff with full jitter and a restart budget."""

    backoff_base_s: float = 0.5
    backoff_cap_s: float = 30.0
    jitter: str = "full"  # full | equal | none
    max_restarts: int = 5
    restart_window_s: float = 300.0
    drain_grace_s: float = 2.0  # wait for HTTP /drain before SIGTERM
    term_grace_s: float = 3.0  # wait after SIGTERM before SIGKILL
    stable_reset_s: float = 60.0  # healthy this long -> backoff attempt counter resets


@dataclass
class GatewayConfig:
    """Request router sitting in front of the workers."""

    host: str = "127.0.0.1"
    port: int = 8080
    max_retries: int = 2
    request_timeout_s: float = 5.0
    route_to_degraded: bool = False
    strategy: str = "least_inflight"  # least_inflight | round_robin


@dataclass
class WorkerSpec:
    """One managed worker. ``container`` is used by the docker backend only."""

    id: str
    port: int
    host: str = "127.0.0.1"
    container: str | None = None
    engine: str | None = None  # overrides WorkersConfig.engine
    extra_args: list[str] = field(default_factory=list)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


@dataclass
class WorkersConfig:
    count: int = 3
    base_port: int = 9001
    backend: str = "subprocess"  # subprocess | docker
    engine: str = "cpu"  # cpu | torch
    concurrency: int = 4
    specs: list[WorkerSpec] = field(default_factory=list)

    def resolved_specs(self) -> list[WorkerSpec]:
        if self.specs:
            return list(self.specs)
        return [WorkerSpec(id=f"w{i + 1}", port=self.base_port + i) for i in range(self.count)]


@dataclass
class SentinelConfig:
    api_host: str = "127.0.0.1"
    api_port: int = 8090
    log_level: str = "INFO"
    log_format: str = "json"  # json | text
    data_dir: str = "data"
    incident_log: str = "incidents.jsonl"
    probe: ProbeConfig = field(default_factory=ProbeConfig)
    limits: ResourceLimits = field(default_factory=ResourceLimits)
    latency: LatencyPolicy = field(default_factory=LatencyPolicy)
    recovery: RecoveryConfig = field(default_factory=RecoveryConfig)
    gateway: GatewayConfig = field(default_factory=GatewayConfig)
    workers: WorkersConfig = field(default_factory=WorkersConfig)

    @property
    def incident_log_path(self) -> Path:
        return Path(self.data_dir) / self.incident_log

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


# --------------------------------------------------------------------------- loading


class ConfigError(ValueError):
    pass


def _coerce(value: Any, annotation: Any) -> Any:
    """Coerce a raw TOML/env/CLI value to the annotated type (handles Optional/list)."""
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is None and annotation in (int, float, str, bool):
        return _coerce_scalar(value, annotation)
    if annotation is Any:
        return value
    # Optional[X] / X | None
    if origin is type(int | None) or ((origin is None and args == ()) and "|" in str(annotation)):
        pass
    if args and type(None) in args:
        if value is None or (isinstance(value, str) and value.lower() in ("none", "null", "")):
            return None
        inner = next(a for a in args if a is not type(None))
        return _coerce(value, inner)
    if origin is list:
        if isinstance(value, str):
            value = [v for v in value.split(",") if v]
        inner = args[0] if args else Any
        return [_coerce(v, inner) for v in value]
    if origin is dict:
        return dict(value)
    if is_dataclass(annotation) and isinstance(annotation, type):
        return _build(annotation, value)
    return value


def _coerce_scalar(value: Any, typ: type) -> Any:
    if typ is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.lower() in ("1", "true", "yes", "on"):
                return True
            if value.lower() in ("0", "false", "no", "off"):
                return False
        raise ConfigError(f"cannot parse boolean from {value!r}")
    if typ is int:
        if isinstance(value, bool):
            raise ConfigError(f"expected int, got bool {value!r}")
        return int(value)
    if typ is float:
        return float(value)
    return str(value)


def _build(cls: type, data: Any) -> Any:
    if not isinstance(data, dict):
        raise ConfigError(f"section for {cls.__name__} must be a table, got {type(data).__name__}")
    hints = get_type_hints(cls)
    known = {f.name for f in fields(cls)}
    unknown = set(data) - known
    if unknown:
        raise ConfigError(f"unknown key(s) in {cls.__name__}: {sorted(unknown)}")
    kwargs = {k: _coerce(v, hints[k]) for k, v in data.items()}
    return cls(**kwargs)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _env_overrides(environ: dict[str, str]) -> dict[str, Any]:
    """``SENTINEL_PROBE__INTERVAL_S=0.5`` -> {"probe": {"interval_s": "0.5"}}."""
    out: dict[str, Any] = {}
    for key, val in environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        path = key[len(ENV_PREFIX) :].lower().split("__")
        cur = out
        for part in path[:-1]:
            cur = cur.setdefault(part, {})
        cur[path[-1]] = val
    return out


def _dotted_overrides(pairs: list[str]) -> dict[str, Any]:
    """``probe.interval_s=0.5`` -> nested dict."""
    out: dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ConfigError(f"--set expects key=value, got {pair!r}")
        key, val = pair.split("=", 1)
        path = key.strip().split(".")
        cur = out
        for part in path[:-1]:
            cur = cur.setdefault(part, {})
        cur[path[-1]] = val.strip()
    return out


def load_config(
    path: str | Path | None = None,
    *,
    overrides: list[str] | None = None,
    environ: dict[str, str] | None = None,
) -> SentinelConfig:
    """Load config with precedence defaults < TOML < env < --set."""
    raw: dict[str, Any] = {}
    if path is not None:
        p = Path(path)
        if not p.exists():
            raise ConfigError(f"config file not found: {p}")
        with p.open("rb") as fh:
            raw = tomllib.load(fh)
    raw = _deep_merge(raw, _env_overrides(environ if environ is not None else dict(os.environ)))
    raw = _deep_merge(raw, _dotted_overrides(overrides or []))
    cfg: SentinelConfig = _build(SentinelConfig, raw)
    validate(cfg)
    return cfg


def validate(cfg: SentinelConfig) -> None:
    p, r, g, w, lat = cfg.probe, cfg.recovery, cfg.gateway, cfg.workers, cfg.latency
    if p.interval_s <= 0 or p.timeout_s <= 0 or p.startup_interval_s <= 0:
        raise ConfigError("probe intervals and timeout must be > 0")
    if p.unhealthy_threshold < 1 or p.healthy_threshold < 1:
        raise ConfigError("probe thresholds must be >= 1")
    if r.backoff_base_s <= 0 or r.backoff_cap_s < r.backoff_base_s:
        raise ConfigError("recovery.backoff_cap_s must be >= backoff_base_s > 0")
    if r.jitter not in ("full", "equal", "none"):
        raise ConfigError("recovery.jitter must be full|equal|none")
    if g.strategy not in ("least_inflight", "round_robin"):
        raise ConfigError("gateway.strategy must be least_inflight|round_robin")
    if w.backend not in ("subprocess", "docker"):
        raise ConfigError("workers.backend must be subprocess|docker")
    if w.engine not in ("cpu", "torch"):
        raise ConfigError("workers.engine must be cpu|torch")
    if not w.resolved_specs():
        raise ConfigError("at least one worker is required")
    ids = [s.id for s in w.resolved_specs()]
    if len(ids) != len(set(ids)):
        raise ConfigError(f"duplicate worker ids: {ids}")
    if lat.p95_fleet_ratio < 1.0:
        raise ConfigError("latency.p95_fleet_ratio must be >= 1.0")
