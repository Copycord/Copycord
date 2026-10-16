"""/api/messages: the Messages page list, its filter facets and the live message preview.

The DB keeps ids and a few small fields per cloned message (author, counts, timestamp), never the
content; the preview reads the message through the server bot on demand and caches it briefly.
"""
from __future__ import annotations

import base64
import json
import time
from typing import Awaitable, Callable, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

router = APIRouter()

LIMIT_DEFAULT = 50
LIMIT_MAX = 200
CONTENT_TTL = 300.0
CONTENT_CACHE_MAX = 500
SORTS = ("created_at", "author", "channel", "username", "guild")

_state: dict = {"db": None, "send": None}
# (original_message_id, cloned_guild_id) -> (expires_at, payload)
_content_cache: dict[tuple[int, int], tuple[float, dict]] = {}


def configure(*, db=None, send: Optional[Callable[[dict, float], Awaitable[dict]]] = None) -> None:
    """db: the DBManager; send(payload, timeout): one request to the server agent, its reply dict."""
    if db is not None:
        _state["db"] = db
    if send is not None:
        _state["send"] = send
    _content_cache.clear()


def _db():
    db = _state["db"]
    if db is None:
        raise HTTPException(status_code=503, detail="db-unavailable")
    return db


def _sid(v) -> Optional[str]:
    return str(v) if v is not None else None


def _link(guild, channel, message) -> Optional[str]:
    if not (guild and channel and message):
        return None
    return f"https://discord.com/channels/{guild}/{channel}/{message}"


def _row_json(r: dict) -> dict:
    return {
        "original_message_id": _sid(r["original_message_id"]),
        "original_guild_id": _sid(r["original_guild_id"]),
        "original_guild_name": r.get("original_guild_name"),
        "original_channel_id": _sid(r["original_channel_id"]),
        "original_channel_name": r.get("original_channel_name"),
        "cloned_message_id": _sid(r["cloned_message_id"]),
        "cloned_guild_id": _sid(r["cloned_guild_id"]),
        "cloned_guild_name": r.get("cloned_guild_name"),
        "cloned_channel_id": _sid(r["cloned_channel_id"]),
        "cloned_channel_name": r.get("cloned_channel_name"),
        "mapping_id": r.get("mapping_id"),
        "mapping_name": r.get("mapping_name"),
        "author_id": _sid(r["author_id"]),
        "author_name": r["author_name"],
        "author_username": r["author_username"],
        "attachment_count": r["attachment_count"],
        "embed_count": r["embed_count"],
        "created_at": r["message_ts"],
        "cloned_at": r["created_at"],
        "original_url": _link(r["original_guild_id"], r["original_channel_id"], r["original_message_id"]),
        "cloned_url": _link(r["cloned_guild_id"], r["cloned_channel_id"], r["cloned_message_id"]),
    }


def _encode_cursor(sort: str, order: str, r: dict) -> str:
    raw = json.dumps([sort, order, r["sort_key"], r["original_message_id"], r["cloned_guild_id"] or 0])
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _decode_cursor(cursor: str, sort: str, order: str) -> tuple:
    try:
        pad = "=" * (-len(cursor) % 4)
        s, o, key, mid, gid = json.loads(base64.urlsafe_b64decode(cursor + pad).decode())
        if s != sort or o != order or not isinstance(key, (int, str)):
            raise ValueError
        return key, int(mid), int(gid)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid-cursor") from None


def _int_or_400(name: str, v: Optional[str]) -> Optional[int]:
    if v is None or str(v).strip() == "":
        return None
    try:
        return int(str(v).strip())
    except ValueError:
        raise HTTPException(status_code=400, detail=f"invalid-{name}") from None


@router.get("/api/messages", response_class=JSONResponse)
async def api_list_messages(
    q: Optional[str] = None,
    mapping_id: Optional[str] = None,
    original_guild_id: Optional[str] = None,
    cloned_guild_id: Optional[str] = None,
    channel_id: Optional[str] = None,
    author_id: Optional[str] = None,
    created_from: Optional[str] = None,
    created_to: Optional[str] = None,
    has_attachments: Optional[bool] = None,
    sort: str = "created_at",
    order: str = "desc",
    limit: int = Query(LIMIT_DEFAULT, ge=1, le=LIMIT_MAX),
    cursor: Optional[str] = None,
):
    if sort not in SORTS:
        raise HTTPException(status_code=400, detail="invalid-sort")
    order = order.lower()
    if order not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="invalid-order")
    after = _decode_cursor(cursor, sort, order) if cursor else None
    rows = _db().list_messages(
        sort=sort,
        order=order,
        limit=limit + 1,
        after=after,
        q=q,
        mapping_id=mapping_id or None,
        original_guild_id=_int_or_400("original_guild_id", original_guild_id),
        cloned_guild_id=_int_or_400("cloned_guild_id", cloned_guild_id),
        channel_id=_int_or_400("channel_id", channel_id),
        author_id=_int_or_400("author_id", author_id),
        created_from=_int_or_400("created_from", created_from),
        created_to=_int_or_400("created_to", created_to),
        has_attachments=has_attachments,
    )
    more = len(rows) > limit
    rows = rows[:limit]
    return JSONResponse({
        "ok": True,
        "items": [_row_json(r) for r in rows],
        "next_cursor": _encode_cursor(sort, order, rows[-1]) if more and rows else None,
    })


@router.get("/api/messages/facets", response_class=JSONResponse)
async def api_message_facets():
    return JSONResponse({"ok": True, **_db().message_facets()})


@router.get("/api/messages/{original_message_id}/content", response_class=JSONResponse)
async def api_message_content(original_message_id: str, cloned_guild_id: Optional[str] = None):
    mid = _int_or_400("original_message_id", original_message_id)
    gid = _int_or_400("cloned_guild_id", cloned_guild_id)
    row = _db().get_message_row(mid, gid)
    if row is None:
        raise HTTPException(status_code=404, detail="message-not-recorded")
    key = (int(row["original_message_id"]), int(row["cloned_guild_id"] or 0))
    now = time.monotonic()
    hit = _content_cache.get(key)
    if hit and hit[0] > now:
        return JSONResponse({**hit[1], "cached": True})

    targets = []
    if row["cloned_channel_id"] and row["cloned_message_id"]:
        targets.append({"channel_id": str(row["cloned_channel_id"]),
                        "message_id": str(row["cloned_message_id"]), "source": "clone"})
    if row["original_channel_id"]:
        targets.append({"channel_id": str(row["original_channel_id"]),
                        "message_id": str(row["original_message_id"]), "source": "original"})
    send = _state["send"]
    if send is None:
        raise HTTPException(status_code=503, detail="bot-not-running")
    res = await send({"type": "fetch_message", "data": {"targets": targets}}, 8.0)
    if not isinstance(res, dict) or ("ok" in res and "status" not in res and not res.get("ok")):
        # the agent socket did not answer: the server process is down or still starting
        raise HTTPException(status_code=503, detail="bot-not-running")
    if not res.get("ok"):
        err = str(res.get("error") or "discord_error")
        status = int(res.get("status") or 502)
        if err == "bot_not_ready":
            raise HTTPException(status_code=503, detail="bot-not-running")
        if err == "rate_limited":
            retry_after = float(res.get("retry_after") or 1)
            return JSONResponse(
                {"ok": False, "error": "rate-limited", "retry_after": retry_after},
                status_code=429,
                headers={"Retry-After": str(max(1, int(retry_after + 0.999)))},
            )
        detail = {"not_found": "message-deleted", "forbidden": "no-access"}.get(err, "discord-error")
        raise HTTPException(status_code=status if status in (403, 404) else 502, detail=detail)

    payload = {"ok": True, "source": res.get("source") or "", "message": res.get("message") or {},
               "row": _row_json(row)}
    if len(_content_cache) >= CONTENT_CACHE_MAX:
        for k in [k for k, (exp, _) in _content_cache.items() if exp <= now] or list(_content_cache)[:50]:
            _content_cache.pop(k, None)
    _content_cache[key] = (now + CONTENT_TTL, payload)
    return JSONResponse({**payload, "cached": False})
