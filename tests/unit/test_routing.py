"""Gateway worker selection."""

from __future__ import annotations

from types import SimpleNamespace

from sentinel.config import GatewayConfig
from sentinel.controller.gateway import Gateway
from sentinel.metrics import Registry


def mk(wid: str, inflight: int) -> SimpleNamespace:
    return SimpleNamespace(id=wid, inflight={i: 0.0 for i in range(inflight)})


def gateway(strategy: str) -> Gateway:
    return Gateway(GatewayConfig(strategy=strategy), supervisor=None, session=None,  # type: ignore[arg-type]
                   registry=Registry())


def test_least_inflight_prefers_idle_worker():
    gw = gateway("least_inflight")
    picks = {gw.pick([mk("a", 3), mk("b", 0), mk("c", 2)]).id for _ in range(20)}
    assert picks == {"b"}


def test_least_inflight_breaks_ties_randomly():
    gw = gateway("least_inflight")
    picks = {gw.pick([mk("a", 1), mk("b", 1), mk("c", 5)]).id for _ in range(100)}
    assert picks == {"a", "b"}


def test_round_robin_cycles():
    gw = gateway("round_robin")
    cands = [mk("a", 0), mk("b", 0), mk("c", 0)]
    seq = [gw.pick(cands).id for _ in range(6)]
    assert seq[:3] == seq[3:] and set(seq) == {"a", "b", "c"}
