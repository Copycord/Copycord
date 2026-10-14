"""REST surface of the Tasks framework (/api/tasks/*).

Behind the app wide session guard (admin/auth.py PasswordGuardMiddleware covers
every /api/ path) and the CSRF middleware like every other /api route. Every id
on the wire is a string; ids in paths are accepted as strings."""

from __future__ import annotations

import functools
from typing import Any, Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from . import registry
from .service import RunFinished, TaskService, _ids_to_str

router = APIRouter(prefix="/api/tasks", tags=["tasks"])

TRIGGERS = ("manual", "rerun", "api")
MAX_PER_PAGE = 200

_service: Optional[TaskService] = None


def set_service(service: Optional[TaskService]) -> None:
    global _service
    _service = service


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


def _run_id(raw: str) -> Optional[int]:
    try:
        value = int(str(raw).strip())
    except ValueError:
        return None
    return value if value > 0 else None


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
    return {
        "runs": [r.to_wire(include_steps=False) for r in runs],
        "total": total,
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
    if registry.get(type_key) is None:
        return _error(404, f"Unknown task type {type_key!r}")
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
    return {"run": run.to_wire()}


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


@router.get("/runs/{run_id}/log")
@_guard
async def run_log(run_id: str, request: Request):
    svc = _svc()
    rid = _run_id(run_id)
    if rid is None or svc.get(rid) is None:
        return _error(404, "Unknown run")
    reader = getattr(svc.log_sink, "read", None)
    if reader is None:
        return {"lines": [], "next": 0, "complete": True}
    qp = request.query_params
    after = _int(qp.get("after"), 0, 0)
    limit = _int(qp.get("limit"), 500, 1, 2000)
    step = (qp.get("step") or "").strip() or None
    lines, nxt, complete = reader(rid, after=after, step=step, limit=limit)
    return {
        "lines": [{"n": ln["seq"], "ts": ln["ts"], "level": ln["level"], "step": ln["step"],
                   "msg": ln["msg"]} for ln in lines],
        "next": nxt,
        "complete": complete,
    }
