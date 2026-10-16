"""REST surface of the Tasks framework (/api/tasks/*).

Behind the app wide session guard (admin/auth.py PasswordGuardMiddleware covers
every /api/ path) and the CSRF middleware like every other /api route. Every id
on the wire is a string; ids in paths are accepted as strings."""

from __future__ import annotations

import functools
import json
import re
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from . import registry
from .service import ACTIVE, RunFinished, TaskService, _ids_to_str

# The statuses the runs list counts for its tabs.
COUNTED = ("queued", "running", "success", "failed", "cancelled")

router = APIRouter(prefix="/api/tasks", tags=["tasks"])

# What a start may send. A run stored with an older trigger still reads and lists as stored.
TRIGGERS = ("manual", "api")
MAX_PER_PAGE = 200

BOTS_DOWN = "Start the bots first"

_RUN_ID = re.compile(r"[0-9a-f]{32}")

_service: Optional[TaskService] = None
# Answers whether the server and client bots are both running; app.py wires it to /api/status.
_bots_running: Optional[Callable[[], Awaitable[bool]]] = None


def set_service(service: Optional[TaskService]) -> None:
    global _service
    _service = service


def set_bots_check(check: Optional[Callable[[], Awaitable[bool]]]) -> None:
    global _bots_running
    _bots_running = check


def get_service() -> Optional[TaskService]:
    return _service


def _svc() -> TaskService:
    if _service is None:
        raise _Unavailable()
    return _service


class _Unavailable(Exception):
    pass


def _error(status: int, detail: Any, **extra) -> JSONResponse:
    return JSONResponse({"detail": detail, **extra}, status_code=status)


def _run_id(raw: str) -> Optional[str]:
    """A run id is a uuid4 hex string; anything else is no run (404)."""
    value = str(raw).strip().lower()
    return value if _RUN_ID.fullmatch(value) else None


def _int(raw: Optional[str], default: int, lo: int, hi: Optional[int] = None) -> int:
    try:
        value = int(raw) if raw not in (None, "") else default
    except ValueError:
        value = default
    value = max(lo, value)
    return min(hi, value) if hi is not None else value


def _filters(request: Request) -> dict:
    qp = request.query_params
    filters: dict = {}
    for key in ("type", "actor", "context", "q"):
        value = (qp.get(key) or "").strip()
        if value:
            filters[key] = value
    statuses = [s.strip() for s in (qp.get("status") or "").split(",") if s.strip()]
    if len(statuses) == 1:
        filters["status"] = statuses[0]
    elif statuses:
        filters["status"] = statuses
    return filters


def _distinct(svc: TaskService, type_key: Optional[str]) -> tuple[list[str], list[str]]:
    """Actors and context labels for the list filters."""
    repo = svc.repo
    if hasattr(repo, "actors") and hasattr(repo, "contexts"):
        return list(repo.actors()), list(repo.contexts(type_key))
    runs, _ = repo.list_runs({}, 1, 10_000)
    actors = sorted({r.actor for r in runs})
    contexts = sorted({str((r.context or {}).get("label")) for r in runs
                       if (r.context or {}).get("label") and (not type_key or r.type == type_key)})
    return actors, contexts


def _guard(handler):
    """503 while the service is not built yet (startup)."""

    @functools.wraps(handler)
    async def wrapper(*args, **kwargs):
        try:
            return await handler(*args, **kwargs)
        except _Unavailable:
            return _error(503, "Task service is not ready")

    return wrapper


@router.get("/types")
@_guard
async def list_types():
    svc = _svc()
    types = []
    for t in registry.all():
        _, total = svc.list({"type": t.key}, 1, 1)
        types.append({
            "key": t.key,
            "title": t.title,
            "description": t.description,
            "inputs_schema": t.inputs_schema(),
            "requires_bots": t.requires_bots,
            "active": len(svc.active_runs(t.key)),
            "total": total,
        })
    return {"types": types}


@router.get("/types/{key}/options")
@_guard
async def type_options(key: str, request: Request):
    _svc()
    t = registry.get(key)
    if t is None:
        return _error(404, f"Unknown task type {key!r}")
    if t.options is None:
        return _error(404, f"Task type {key!r} has no options")
    return _ids_to_str(await t.options(dict(request.query_params)))


@router.get("/runs")
@_guard
async def list_runs(request: Request):
    svc = _svc()
    qp = request.query_params
    page = _int(qp.get("page"), 1, 1)
    per_page = _int(qp.get("per_page"), 25, 1, MAX_PER_PAGE)
    filters = _filters(request)
    runs, total = svc.list(filters, page, per_page)
    actors, contexts = _distinct(svc, filters.get("type"))
    # The quick status tabs: runs per status under every filter but the status itself.
    rest = {k: v for k, v in filters.items() if k != "status"}
    counts = {s: svc.list({**rest, "status": s}, 1, 1)[1] for s in COUNTED}
    counts["all"] = svc.list(rest, 1, 1)[1]
    return {
        "runs": [r.to_wire(include_steps=False) for r in runs],
        "total": total,
        "counts": counts,
        "page": page,
        "actors": actors,
        "contexts": contexts,
    }


@router.post("/runs", status_code=201)
@_guard
async def start_run(request: Request):
    svc = _svc()
    try:
        body = await request.json()
    except ValueError:
        return JSONResponse({"errors": {"body": "Invalid JSON"}}, status_code=422)
    if not isinstance(body, dict):
        return JSONResponse({"errors": {"body": "Expected an object"}}, status_code=422)
    type_key = str(body.get("type") or "")
    task_type = registry.get(type_key)
    if task_type is None:
        return _error(404, f"Unknown task type {type_key!r}")
    if task_type.requires_bots and _bots_running is not None and not await _bots_running():
        return _error(409, BOTS_DOWN)
    inputs = body.get("inputs")
    if inputs is None:
        inputs = {}
    if not isinstance(inputs, dict):
        return JSONResponse({"errors": {"inputs": "Expected an object"}}, status_code=422)
    trigger = str(body.get("trigger") or "manual")
    if trigger not in TRIGGERS:
        return JSONResponse({"errors": {"trigger": f"One of {', '.join(TRIGGERS)}"}},
                            status_code=422)
    try:
        run = await svc.start(type_key, inputs, actor="admin", trigger=trigger)
    except ValueError as exc:
        errors = exc.args[0] if exc.args and isinstance(exc.args[0], dict) else {"inputs": str(exc)}
        return JSONResponse({"errors": errors}, status_code=422)
    return JSONResponse({"run": run.to_wire()}, status_code=201)


@router.get("/runs/{run_id}")
@_guard
async def get_run(run_id: str):
    svc = _svc()
    rid = _run_id(run_id)
    run = svc.get(rid) if rid is not None else None
    if run is None:
        return _error(404, "Unknown run")
    return {"run": run.to_wire(detail=True)}


@router.delete("/runs/{run_id}")
@_guard
async def delete_run(run_id: str):
    svc = _svc()
    rid = _run_id(run_id)
    run = svc.get(rid) if rid is not None else None
    if run is None:
        return _error(404, "Unknown run")
    if run.status in ACTIVE or any(r.id == rid for r in svc.active_runs()):
        return _error(409, f"Run is {run.status}; cancel it first", run=run.to_wire())
    svc.repo.delete_run(rid)
    return Response(status_code=204)


@router.get("/runs/{run_id}/report.json")
@_guard
async def run_report(run_id: str):
    svc = _svc()
    rid = _run_id(run_id)
    run = svc.get(rid) if rid is not None else None
    if run is None:
        return _error(404, "Unknown run")
    if run.report is None:
        return _error(404, "No report yet")
    filename = f"{run.type.replace('_', '-')}-{run.number}.json"
    return Response(
        json.dumps(_ids_to_str(run.report), indent=2, ensure_ascii=False) + "\n",
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/runs/{run_id}/cancel")
@_guard
async def cancel_run(run_id: str):
    svc = _svc()
    rid = _run_id(run_id)
    if rid is None:
        return _error(404, "Unknown run")
    try:
        run = await svc.cancel(rid)
    except KeyError:
        return _error(404, "Unknown run")
    except RunFinished as exc:
        return _error(409, f"Run already {exc.run.status}", run=exc.run.to_wire())
    return {"run": run.to_wire()}
