# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""The scraper's debug report (dev mode only): what each token sent, recorded by the
StandaloneScraper at the point it builds each request. Per token: the REST headers of each
kind of request (token check, guild info, bio, guild list) with the curl_cffi session slot,
impersonate target and proxy, and per gateway session the upgrade headers and the IDENTIFY
exactly as sent, without its token. X-Super-Properties is decoded to JSON.

Secrets never reach the report: a token shows only its first 6 and last 4 characters, a
proxy only its scheme, host and port, and the finished report is scrubbed once more for any
token or proxy credentials still in it."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

from copycord.selfclient.gateway import mask_proxy

REPORT_KIND = "copycord-scrape-debug-report"
REPORT_VERSION = 1
# REST request kinds, by the scraper call that makes them.
TOKEN_CHECK = "token_check"
GUILD_INFO = "guild_info"
GUILD_LIST = "guild_list"
BIO = "bio"


def mask_token(token: Optional[str]) -> str:
    """The first 6 and last 4 characters; a token too short to show that much is all stars."""
    t = str(token or "")
    if len(t) <= 10:
        return "***"
    return f"{t[:6]}...{t[-4:]}"


def masked_proxy(proxy: Optional[str]) -> Optional[str]:
    """scheme://***@host:port for a proxy with credentials, None for no proxy."""
    if not proxy:
        return None
    return mask_proxy(proxy if "://" in proxy else f"http://{proxy}")


def decode_super_properties(value: Any) -> Any:
    """X-Super-Properties (base64 JSON) as JSON, or None when it does not decode."""
    if not value:
        return None
    try:
        return json.loads(base64.b64decode(str(value)))
    except Exception:
        return None


def _key(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


class DebugRecorder:
    """Collects what the scraper's tokens send. Every value is masked as it is recorded;
    snapshot() returns the report as it stands, so a failed or cancelled scrape still has
    what was captured so far."""

    def __init__(self, tokens: List[str], proxies: Optional[List[str]] = None,
                 guild_id: Any = None, impersonate: Optional[str] = None) -> None:
        self._tokens = [t for t in tokens if t]
        self._proxies = [p for p in (proxies or []) if p]
        self.guild_id = str(guild_id) if guild_id is not None else None
        self.impersonate = impersonate
        self.started_at = datetime.now().isoformat(timespec="seconds")
        self._entries: Dict[str, dict] = {}
        for i, token in enumerate(self._tokens, 1):
            self._entries.setdefault(token, {
                "index": i, "token": mask_token(token), "valid": None, "username": None,
                "user_agent": None, "super_properties": None, "rest": [], "gateway": [],
            })
        self._rest_seen: Dict[tuple, dict] = {}
        # session index -> (token entry, its latest connect, still waiting for IDENTIFY)
        self._pending: Dict[int, tuple] = {}

    def _entry(self, token: str) -> dict:
        if token not in self._entries:
            self._tokens.append(token)
            self._entries[token] = {
                "index": len(self._tokens), "token": mask_token(token), "valid": None,
                "username": None, "user_agent": None, "super_properties": None,
                "rest": [], "gateway": [],
            }
        return self._entries[token]

    def _headers(self, headers: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for name, value in (headers or {}).items():
            if str(name).lower() == "authorization":
                out[name] = mask_token(value)
            else:
                out[name] = self._scrub_text(str(value)) if isinstance(value, str) else value
        return out

    def note_token(self, token: str, valid: bool, username: Optional[str] = None) -> None:
        entry = self._entry(token)
        entry["valid"] = bool(valid)
        if username:
            entry["username"] = str(username)

    def rest(self, token: str, kind: str, method: str, url: str, headers: Dict[str, Any],
             *, slot: int = 0, proxy: Optional[str] = None) -> None:
        """One REST request as it is about to go out. Requests of the same kind with the same
        headers, slot and proxy are counted on one entry; any difference gets its own."""
        entry = self._entry(token)
        masked = self._headers(headers)
        sp = decode_super_properties(_header(headers, "X-Super-Properties"))
        ua = _header(headers, "User-Agent")
        key = (token, kind, method, slot, proxy, _key(masked))
        seen = self._rest_seen.get(key)
        if seen is not None:
            seen["requests"] += 1
            return
        item = {
            "kind": kind,
            "method": method,
            "url": self._scrub_text(url),
            "session_slot": slot,
            "impersonate": self.impersonate,
            "proxy": masked_proxy(proxy),
            "user_agent": ua,
            "requests": 1,
            "headers": masked,
            "super_properties": sp,
        }
        self._rest_seen[key] = item
        entry["rest"].append(item)
        if entry["user_agent"] is None and ua:
            entry["user_agent"] = ua
        if entry["super_properties"] is None and sp is not None:
            entry["super_properties"] = sp

    def gateway_connect(self, token: str, session_index: int, url: str, headers: Dict[str, Any],
                        *, slot: int = 0, proxy: Optional[str] = None) -> None:
        """The gateway socket's upgrade request, as it is about to be opened."""
        entry = self._entry(token)
        conn = {
            "session_index": session_index,
            "session_slot": slot,
            "url": self._scrub_text(url),
            "proxy": masked_proxy(proxy),
            "user_agent": _header(headers, "User-Agent"),
            "connects": 1,
            "headers": self._headers(headers),
            "identify": None,
        }
        # A connect that never got to IDENTIFY is final once the session connects again.
        prev = self._pending.pop(session_index, None)
        if prev is not None:
            _fold(*prev)
        entry["gateway"].append(conn)
        self._pending[session_index] = (entry, conn)
        if entry["user_agent"] is None and conn["user_agent"]:
            entry["user_agent"] = conn["user_agent"]

    def gateway_sent(self, token: str, session_index: int, text: Any) -> None:
        """A frame the token's gateway identity sent; an IDENTIFY is kept, without its token."""
        try:
            msg = json.loads(text)
        except Exception:
            return
        if not isinstance(msg, dict) or msg.get("op") != 2:
            return
        pending = self._pending.pop(session_index, None)
        if pending is None:
            return
        payload = dict(msg)
        d = dict(payload.get("d") or {})
        d.pop("token", None)
        payload["d"] = d
        entry, conn = pending
        conn["identify"] = json.loads(self._scrub_text(json.dumps(payload)))
        _fold(entry, conn)

    def _scrub_text(self, text: str) -> str:
        for token in self._tokens:
            if token and token in text:
                text = text.replace(token, mask_token(token))
        for proxy in self._proxies:
            parts = urlsplit(proxy if "://" in proxy else f"http://{proxy}")
            creds = parts.netloc.rpartition("@")[0]
            if creds and creds in text:
                text = text.replace(creds, "***")
            if parts.password and parts.password in text:
                text = text.replace(parts.password, "***")
        return text

    def snapshot(self, *, status: Optional[str] = None, error: Optional[str] = None,
                 run_id: Any = None) -> dict:
        tokens = []
        for token in self._tokens:
            entry = dict(self._entries[token])
            entry["rest"] = [dict(r) for r in entry["rest"]]
            entry["gateway"] = sorted((dict(g) for g in entry["gateway"]),
                                      key=lambda g: g["session_index"])
            tokens.append(entry)
        report = {
            "kind": REPORT_KIND,
            "version": REPORT_VERSION,
            "run_id": str(run_id) if run_id is not None else None,
            "guild_id": self.guild_id,
            "started_at": self.started_at,
            "written_at": datetime.now().isoformat(timespec="seconds"),
            "status": status,
            "error": self._scrub_text(error) if error else error,
            "impersonate": self.impersonate,
            "tokens": tokens,
        }
        # One last pass over the whole report: no token or proxy credentials anywhere.
        return json.loads(self._scrub_text(json.dumps(report)))


def _fold(entry: dict, conn: dict) -> None:
    """A reconnect of the same session that sent the same thing is counted on the earlier
    connection instead of listed again."""
    same = _key({**conn, "connects": 0})
    for prev in entry["gateway"]:
        if prev is not conn and _key({**prev, "connects": 0}) == same:
            prev["connects"] += conn["connects"]
            entry["gateway"] = [g for g in entry["gateway"] if g is not conn]
            return


def _header(headers: Dict[str, Any], name: str) -> Any:
    wanted = name.lower()
    for key, value in (headers or {}).items():
        if str(key).lower() == wanted:
            return value
    return None
