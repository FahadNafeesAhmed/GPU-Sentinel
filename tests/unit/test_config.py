"""Config loading precedence and validation."""

from __future__ import annotations

import pytest

from sentinel.config import ConfigError, SentinelConfig, WorkerSpec, load_config


def test_defaults_resolve_workers():
    cfg = SentinelConfig()
    specs = cfg.workers.resolved_specs()
    assert [s.id for s in specs] == ["w1", "w2", "w3"]
    assert [s.port for s in specs] == [9001, 9002, 9003]
    assert specs[0].base_url == "http://127.0.0.1:9001"


def test_toml_env_and_cli_precedence(tmp_path):
    p = tmp_path / "s.toml"
    p.write_text(
        '[probe]\ninterval_s = 2.0\ntimeout_s = 0.7\n[workers]\ncount = 2\nbase_port = 9100\n',
        encoding="utf-8",
    )
    cfg = load_config(
        p,
        environ={"SENTINEL_PROBE__INTERVAL_S": "0.3", "SENTINEL_WORKERS__COUNT": "4",
                 "UNRELATED": "x"},
        overrides=["workers.count=5", "recovery.jitter=equal"],
    )
    assert cfg.probe.timeout_s == 0.7  # toml
    assert cfg.probe.interval_s == 0.3  # env beats toml
    assert cfg.workers.count == 5  # cli beats env
    assert cfg.workers.base_port == 9100
    assert cfg.recovery.jitter == "equal"


def test_explicit_worker_specs(tmp_path):
    p = tmp_path / "s.toml"
    p.write_text(
        '[workers]\nbackend = "docker"\n'
        '[[workers.specs]]\nid = "a"\nport = 9001\nhost = "worker-a"\ncontainer = "sentinel-a"\n'
        '[[workers.specs]]\nid = "b"\nport = 9002\nhost = "worker-b"\n',
        encoding="utf-8",
    )
    cfg = load_config(p, environ={})
    specs = cfg.workers.resolved_specs()
    assert isinstance(specs[0], WorkerSpec)
    assert specs[0].container == "sentinel-a" and specs[1].container is None
    assert specs[1].base_url == "http://worker-b:9002"


def test_optional_and_bool_coercion():
    cfg = load_config(None, environ={"SENTINEL_LIMITS__RSS_MB": "none",
                                     "SENTINEL_PROBE__PASSIVE_CHECKS": "false",
                                     "SENTINEL_LIMITS__GPU_MEM_MB": "4096"})
    assert cfg.limits.rss_mb is None
    assert cfg.probe.passive_checks is False
    assert cfg.limits.gpu_mem_mb == 4096.0


@pytest.mark.parametrize(
    "override",
    [
        "probe.interval_s=0",
        "probe.unhealthy_threshold=0",
        "recovery.jitter=random",
        "recovery.backoff_cap_s=0.1",
        "gateway.strategy=hash",
        "workers.backend=k8s",
        "workers.engine=tpu",
        "workers.count=0",
        "latency.p95_fleet_ratio=0.5",
        "probe.passive_checks=maybe",
    ],
)
def test_validation_rejects(override):
    with pytest.raises(ConfigError):
        load_config(None, environ={}, overrides=[override])


def test_unknown_key_and_missing_file(tmp_path):
    p = tmp_path / "bad.toml"
    p.write_text("[probe]\ninterval_seconds = 1\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="unknown key"):
        load_config(p, environ={})
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "missing.toml", environ={})
    with pytest.raises(ConfigError, match="key=value"):
        load_config(None, environ={}, overrides=["nonsense"])


def test_to_dict_roundtrip_keys():
    d = SentinelConfig().to_dict()
    assert set(d) >= {"probe", "limits", "latency", "recovery", "gateway", "workers"}
    assert d["gateway"]["port"] == 8080
