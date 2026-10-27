"""Audit log: JSONL persistence, incident timing fields, ring buffer queries."""

from __future__ import annotations

import json

from sentinel.controller.incidents import IncidentLog


class Clock:
    def __init__(self) -> None:
        self.t = 1_700_000_000.0

    def __call__(self) -> float:
        return self.t


def test_incident_lifecycle_and_jsonl(tmp_path):
    clk = Clock()
    path = tmp_path / "inc.jsonl"
    log = IncidentLog(path, clock=clk)
    first_symptom = clk.t
    clk.t += 2.5
    inc = log.open("w1", "timeout", "active", first_symptom, previous_state="healthy")
    assert inc.symptom_to_detect_ms == 2500.0
    assert log.open_for("w1") is inc and log.open_for("w2") is None
    log.note(inc, "recovery_backoff", attempt=0, delay_s=0.3)
    clk.t += 4.0
    inc.attempts += 1
    inc.escalation.append("terminate")
    log.close(inc, "recovered")
    assert inc.status == "recovered" and inc.detect_to_recover_ms == 4000.0
    assert log.open_for("w1") is None

    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    events = [line["event"] for line in lines]
    assert events == ["incident_opened", "recovery_backoff", "incident_closed"]
    assert lines[0]["symptom_to_detect_ms"] == 2500.0
    assert lines[2]["escalation"] == ["terminate"] and lines[2]["attempts"] == 1
    assert all("iso" in line and "seq" in line for line in lines)
    log.close_file()


def test_listing_and_event_queries(tmp_path):
    clk = Clock()
    log = IncidentLog(None, clock=clk)  # memory-only
    a = log.open("w1", "x", "active", None)
    clk.t += 1
    b = log.open("w2", "y", "process", None)
    assert [i["id"] for i in log.list()] == [b.id, a.id]  # newest first
    assert [i["id"] for i in log.list(worker="w1")] == [a.id]
    assert len(log.list(limit=1)) == 1
    log.emit("custom", k=1)
    evs = log.recent_events(since_seq=0)
    assert [e["event"] for e in evs] == ["incident_opened", "incident_opened", "custom"]
    assert log.recent_events(since_seq=evs[1]["seq"]) == [evs[2]]
    assert log.recent_events(limit=1) == [evs[2]]
