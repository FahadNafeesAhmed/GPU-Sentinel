"""Control-plane HTTP API: dashboard, JSON state, incidents, events, metrics, operator actions."""

from __future__ import annotations

from pathlib import Path

from aiohttp import web

from sentinel import __version__
from sentinel.config import SentinelConfig
from sentinel.controller.gateway import Gateway
from sentinel.controller.incidents import IncidentLog
from sentinel.controller.supervisor import Supervisor
from sentinel.metrics import CONTENT_TYPE, Registry

_DASHBOARD = Path(__file__).with_name("dashboard.html")


class ControlApi:
    def __init__(
        self,
        cfg: SentinelConfig,
        supervisor: Supervisor,
        gateway: Gateway,
        incidents: IncidentLog,
        registry: Registry,
    ) -> None:
        self.cfg = cfg
        self.sup = supervisor
        self.gw = gateway
        self.incidents = incidents
        self.registry = registry

    async def dashboard(self, _: web.Request) -> web.Response:
        return web.Response(text=_DASHBOARD.read_text(encoding="utf-8"), content_type="text/html")

    async def state(self, _: web.Request) -> web.Response:
        snap = self.sup.snapshot()
        snap["gateway"] = {
            "requests_total": self.gw.requests_total,
            "requests_ok": self.gw.requests_ok,
            "port": self.cfg.gateway.port,
        }
        snap["version"] = __version__
        return web.json_response(snap)

    async def list_incidents(self, request: web.Request) -> web.Response:
        limit = int(request.query.get("limit", "50"))
        worker = request.query.get("worker")
        return web.json_response({"incidents": self.incidents.list(limit=limit, worker=worker)})

    async def events(self, request: web.Request) -> web.Response:
        since = int(request.query.get("since", "0"))
        limit = int(request.query.get("limit", "200"))
        return web.json_response({"events": self.incidents.recent_events(since, limit)})

    async def config(self, _: web.Request) -> web.Response:
        return web.json_response(self.cfg.to_dict())

    async def metrics(self, _: web.Request) -> web.Response:
        return web.Response(body=self.registry.render().encode("utf-8"),
                            headers={"Content-Type": CONTENT_TYPE})

    async def healthz(self, _: web.Request) -> web.Response:
        return web.json_response({"ok": True, "tick": self.sup.tick_count})

    async def action(self, request: web.Request) -> web.Response:
        wid = request.match_info["worker"]
        act = request.match_info["action"]
        if wid not in self.sup.workers:
            return web.json_response({"error": "unknown worker"}, status=404)
        if act == "restart":
            ok = await self.sup.restart_worker(wid)
        elif act == "quarantine":
            ok = await self.sup.quarantine(wid)
        elif act == "unquarantine":
            ok = await self.sup.unquarantine(wid)
        else:
            return web.json_response({"error": "unknown action"}, status=404)
        self.incidents.emit("operator_action", worker=wid, action=act, accepted=ok)
        return web.json_response({"worker": wid, "action": act, "accepted": ok},
                                 status=200 if ok else 409)

    def build_app(self) -> web.Application:
        app = web.Application()
        app.add_routes([
            web.get("/", self.dashboard),
            web.get("/api/state", self.state),
            web.get("/api/incidents", self.list_incidents),
            web.get("/api/events", self.events),
            web.get("/api/config", self.config),
            web.get("/metrics", self.metrics),
            web.get("/healthz", self.healthz),
            web.post("/api/workers/{worker}/{action}", self.action),
        ])
        return app
