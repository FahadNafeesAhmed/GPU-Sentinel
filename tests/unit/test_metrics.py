"""Prometheus text exposition correctness."""

from __future__ import annotations

import pytest

from sentinel.metrics import Registry


def test_counter_gauge_render():
    r = Registry()
    c = r.counter("req_total", "Requests", ["worker", "outcome"])
    c.labels(worker="w1", outcome="ok").inc()
    c.labels(worker="w1", outcome="ok").inc(2)
    c.labels(worker="w2", outcome='we"ird\n').inc()
    g = r.gauge("temp", "Temperature")
    g.set(21.5)
    text = r.render()
    assert "# HELP req_total Requests\n# TYPE req_total counter\n" in text
    assert 'req_total{worker="w1",outcome="ok"} 3\n' in text
    assert 'req_total{worker="w2",outcome="we\\"ird\\n"} 1\n' in text
    assert "# TYPE temp gauge\ntemp 21.5\n" in text
    assert text.endswith("\n")


def test_histogram_buckets_are_cumulative_with_inf():
    r = Registry()
    h = r.histogram("lat", "Latency", ["w"], buckets=(0.1, 0.5, 1.0))
    for v in (0.05, 0.3, 0.7, 5.0):
        h.labels(w="a").observe(v)
    text = r.render()
    assert 'lat_bucket{w="a",le="0.1"} 1' in text
    assert 'lat_bucket{w="a",le="0.5"} 2' in text
    assert 'lat_bucket{w="a",le="1"} 3' in text
    assert 'lat_bucket{w="a",le="+Inf"} 4' in text
    assert 'lat_sum{w="a"} 6.05' in text
    assert 'lat_count{w="a"} 4' in text


def test_errors():
    r = Registry()
    c = r.counter("x", "x", ["a"])
    with pytest.raises(ValueError):
        r.counter("x", "dup")
    with pytest.raises(ValueError):
        c.labels(b="1")
    with pytest.raises(ValueError):
        c.labels(a="1").inc(-1)


def test_gauge_remove_and_unlabelled_shortcuts():
    r = Registry()
    g = r.gauge("g", "g", ["k"])
    g.labels(k="1").set(5)
    g.labels(k="2").set(6)
    g.remove(k="1")
    assert 'g{k="1"}' not in r.render() and 'g{k="2"} 6' in r.render()
    c = r.counter("c", "c")
    c.inc()
    assert "\nc 1\n" in r.render()
