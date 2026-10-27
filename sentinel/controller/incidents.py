"""Append-only incident / audit log (JSONL on disk + in-memory ring for the API).

Every state transition, recovery decision and action is one JSON line with a
timestamp, so the sequence detection -> decision -> action -> outcome can be
reconstructed after the fact.
"""

from __future__ import annotations

import json
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Incident:
    id: str
    worker: str
    opened_at: float
    reason: str
    source: str
    first_symptom_at: float | None = None
    status: str = "open"  # open | recovered | quarantined | stopped
    attempts: int = 0
    escalation: list[str] = field(default_factory=list)
    recovered_at: float | None = None
    symptom_to_detect_ms: float | None = None
    detect_to_recover_ms: float | None = None
    notes: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class IncidentLog:
    def __init__(
        self,
        path: str | Path | None,
        *,
        ring_size: int = 2000,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.path = Path(path) if path else None
        self.clock = clock
        self.events: deque[dict[str, Any]] = deque(maxlen=ring_size)
        self.incidents: dict[str, Incident] = {}
        self._order: deque[str] = deque(maxlen=ring_size)
        self._fh: Any = None
        self._seq = 0
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = self.path.open("a", encoding="utf-8")

    # ------------------------------------------------------------------ events

    def emit(self, event: str, **fields: Any) -> dict[str, Any]:
        self._seq += 1
        now = self.clock()
        rec: dict[str, Any] = {
            "seq": self._seq,
            "ts": round(now, 3),
            "iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now))
            + f".{int((now % 1) * 1000):03d}Z",
            "event": event,
            **fields,
        }
        self.events.append(rec)
        if self._fh is not None:
            self._fh.write(json.dumps(rec, default=str, separators=(",", ":")) + "\n")
            self._fh.flush()
        return rec

    def recent_events(self, since_seq: int = 0, limit: int = 200) -> list[dict[str, Any]]:
        out = [e for e in self.events if e["seq"] > since_seq]
        return out[-limit:]

    # ------------------------------------------------------------------ incidents

    def open(
        self, worker: str, reason: str, source: str, first_symptom_at: float | None, **detail: Any
    ) -> Incident:
        now = self.clock()
        inc = Incident(
            id=uuid.uuid4().hex[:10], worker=worker, opened_at=now, reason=reason, source=source,
            first_symptom_at=first_symptom_at,
        )
        if first_symptom_at is not None:
            inc.symptom_to_detect_ms = round((now - first_symptom_at) * 1000.0, 1)
        self.incidents[inc.id] = inc
        self._order.append(inc.id)
        self.emit("incident_opened", incident=inc.id, worker=worker, reason=reason, source=source,
                  symptom_to_detect_ms=inc.symptom_to_detect_ms, **detail)
        return inc

    def note(self, inc: Incident, event: str, **fields: Any) -> None:
        inc.notes.append({"ts": round(self.clock(), 3), "event": event, **fields})
        self.emit(event, incident=inc.id, worker=inc.worker, **fields)

    def close(self, inc: Incident, status: str, **fields: Any) -> None:
        now = self.clock()
        inc.status = status
        inc.recovered_at = now
        inc.detect_to_recover_ms = round((now - inc.opened_at) * 1000.0, 1)
        self.emit(
            "incident_closed", incident=inc.id, worker=inc.worker, status=status,
            attempts=inc.attempts, escalation=inc.escalation,
            detect_to_recover_ms=inc.detect_to_recover_ms, **fields,
        )

    def list(self, limit: int = 50, worker: str | None = None) -> list[dict[str, Any]]:
        ids = list(self._order)[::-1]
        out: list[dict[str, Any]] = []
        for iid in ids:
            inc = self.incidents[iid]
            if worker and inc.worker != worker:
                continue
            out.append(inc.to_dict())
            if len(out) >= limit:
                break
        return out

    def open_for(self, worker: str) -> Incident | None:
        for iid in reversed(self._order):
            inc = self.incidents[iid]
            if inc.worker == worker and inc.status == "open":
                return inc
        return None

    def close_file(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
