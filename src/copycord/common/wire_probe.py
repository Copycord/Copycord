# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""What Discord sees from a user token's requests, recorded on each connect.

build_headers() is only our half: curl_cffi's impersonation profile adds and
overrides headers, and sets the TLS and HTTP/2 fingerprints. The probe sends a
real request through the token's own session and proxy lease to an echo
service (tls.peet.ws) and stores what arrived in client_headers.json, under
the account's entry. The Authorization value is replaced by a placeholder
before it leaves, and nothing written carries the token or proxy credentials.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit

from copycord.common import selfbot_headers as sh
from copycord.common.proxy_pool import get_pool as _get_proxy_pool

logger = logging.getLogger("common.wire_probe")

PROBE_URL = "https://tls.peet.ws/api/all"
PROBE_TIMEOUT = 15

_TLS_FIELDS = (
    "ja3",
    "ja3_hash",
    "ja4",
    "ja4_r",
    "peetprint",
    "peetprint_hash",
    "tls_version_record",
    "tls_version_negotiated",
)

# Strong references, or a pending probe can be garbage collected mid-flight.
_TASKS: set = set()


def _redact_header(name: str) -> bool:
    return "authorization" in name.lower()


def _split_header_line(line: str) -> tuple[str, str]:
    """``"Name: value"`` or the HTTP/2 pseudo form ``":method: GET"``."""
    lead = ":" if line.startswith(":") else ""
    name, _, value = line[len(lead):].partition(": ")
    return lead + name.strip(), value


def _exit_ip(addr: Any) -> Optional[str]:
    """``"1.2.3.4:5678"`` or ``"[::1]:5678"`` without the port."""
    if not isinstance(addr, str) or not addr:
        return None
    if addr.startswith("["):
        return addr[1:].split("]", 1)[0]
    if addr.count(":") == 1:
        return addr.split(":", 1)[0]
    return addr


def _h2_header_lines(http2: dict) -> list:
    for frame in http2.get("sent_frames") or []:
        if isinstance(frame, dict) and frame.get("frame_type") == "HEADERS":
            return list(frame.get("headers") or [])
    return []


def parse_peet(body: dict) -> dict:
    """The parts of a tls.peet.ws /api/all response that describe our request."""
    body = body if isinstance(body, dict) else {}
    http1 = body.get("http1") if isinstance(body.get("http1"), dict) else {}
    http2 = body.get("http2") if isinstance(body.get("http2"), dict) else {}
    tls = body.get("tls") if isinstance(body.get("tls"), dict) else {}

    lines = _h2_header_lines(http2) or list(http1.get("headers") or [])
    headers: dict = {}
    order: list = []
    pseudo: list = []
    for line in lines:
        if not isinstance(line, str):
            continue
        name, value = _split_header_line(line)
        if name.startswith(":"):
            pseudo.append(name)
            continue
        key = name.lower()
        order.append(key)
        headers[key] = value

    h2: dict = {}
    if http2:
        h2 = {
            "akamai_fingerprint": http2.get("akamai_fingerprint"),
            "akamai_fingerprint_hash": http2.get("akamai_fingerprint_hash"),
            "pseudo_header_order": pseudo,
        }

    return {
        "exit_ip": _exit_ip(body.get("ip")),
        "http_version": body.get("http_version"),
        "headers": headers,
        "header_order": order,
        "tls": {k: tls[k] for k in _TLS_FIELDS if k in tls},
        "http2": h2,
    }


def _scrub(obj: Any, replacements: list) -> Any:
    """Replace every secret substring in ``obj``'s strings, longest first."""
    if isinstance(obj, str):
        for secret, mask in replacements:
            if secret in obj:
                obj = obj.replace(secret, mask)
        return obj
    if isinstance(obj, dict):
        return {_scrub(k, replacements): _scrub(v, replacements) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub(v, replacements) for v in obj]
    return obj


def _secrets(token: str, placeholder: str, proxy: Optional[str]) -> list:
    pairs = [(token, "[redacted]"), (placeholder, "[redacted]")]
    if proxy:
        pairs.append((proxy, sh._mask_proxy_for_log(proxy)))
        userinfo = urlsplit(proxy).netloc.rpartition("@")[0]
        if userinfo:
            pairs.append((userinfo, "***"))
    pairs = [(s, m) for s, m in pairs if s]
    return sorted(pairs, key=lambda p: len(p[0]), reverse=True)


def _placeholder(token: str) -> str:
    # Same length as the token, so content-length matches a real send.
    return "PROBE." + "x" * max(0, len(token) - 6)


async def _probe(token: str, placeholder: str) -> tuple[dict, Optional[str]]:
    session = await sh.get_tls_session(token, use_proxy=True)
    # Read after the session: opening it is what takes the lease.
    proxy = _get_proxy_pool().current(token)

    # Build the headers exactly as a real send does, or the probe reports
    # differences that only exist in the probe.
    headers = {**sh.build_headers(token), **sh.SUPPRESSED_PROFILE_HEADERS}
    headers["Authorization"] = placeholder
    headers["X-Context-Properties"] = sh.context_properties("chat_input")

    resp = await session.get(
        PROBE_URL, headers=headers, timeout=PROBE_TIMEOUT, proxy=proxy
    )
    if resp.status_code != 200:
        return {"ok": False, "error": f"HTTP {resp.status_code}"}, proxy

    parsed = parse_peet(resp.json() or {})
    wire = parsed["headers"]
    # Diff first, then drop the value: the header's presence is part of the
    # comparison, its contents are not ours to write down.
    diff = sh.diff_against_real_client(wire)
    for name in wire:
        if _redact_header(name):
            wire[name] = "[redacted]"

    return {
        "ok": True,
        "exit_ip": parsed["exit_ip"],
        "http_version": parsed["http_version"],
        "headers_on_wire": wire,
        "header_order": parsed["header_order"],
        "tls": parsed["tls"],
        "http2": parsed["http2"],
        "diff_vs_real_client": diff,
    }, proxy


async def probe_wire(token: str) -> dict:
    """Send one request as this token would and report what arrived. Never raises."""
    probed_at = int(time.time())
    placeholder = _placeholder(token)
    proxy = None
    try:
        result, proxy = await asyncio.wait_for(
            _probe(token, placeholder), timeout=PROBE_TIMEOUT
        )
    except asyncio.TimeoutError:
        result = {"ok": False, "error": f"timed out after {PROBE_TIMEOUT}s"}
    except Exception as e:
        result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    if proxy is None:
        try:
            proxy = _get_proxy_pool().current(token)
        except Exception:
            proxy = None
    result["probed_at"] = probed_at
    result["proxy"] = sh._mask_proxy_for_log(proxy) if proxy else None
    return _scrub(result, _secrets(token, placeholder, proxy))


def gateway_snapshot(gateway: Any, event: str) -> dict:
    """The IDENTIFY properties and URL this gateway last used; no token."""
    url = getattr(gateway, "connected_url", None)
    props = getattr(gateway, "identify_properties", None)
    query = {}
    if isinstance(url, str) and url:
        query = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(urlsplit(url).query).items()}
    return {
        "event": event,
        "url": url,
        "query": query,
        "capabilities": getattr(gateway, "capabilities", None),
        "properties": dict(props) if isinstance(props, dict) else None,
    }


def _store(token: str, wire: dict, gateway: dict) -> None:
    data = sh._read_cache()
    accounts = data.get("accounts") if isinstance(data.get("accounts"), dict) else {}
    key = sh._token_log_id(token)
    entry = accounts.get(key) if isinstance(accounts.get(key), dict) else {}
    entry["wire"] = wire
    entry["gateway"] = gateway
    accounts[key] = entry
    data["accounts"] = accounts
    sh._write_cache(data)


async def record_connect(token: str, gateway: dict) -> None:
    """Probe and store the result under the account's entry. Never raises."""
    try:
        wire = await probe_wire(token)
        _store(token, wire, _scrub(gateway, _secrets(token, _placeholder(token), None)))
        logger.debug(
            "wire record for %s: ok=%s exit_ip=%s",
            sh._token_log_id(token),
            wire.get("ok"),
            wire.get("exit_ip"),
        )
    except Exception as e:
        logger.debug("wire record failed for %s: %r", sh._token_log_id(token), e)


def schedule_connect_record(
    token: Optional[str], gateway: Any = None, *, event: str = "READY"
) -> Optional[asyncio.Task]:
    """Start the record in the background; the connect never waits on it."""
    if not token:
        return None
    try:
        snapshot = gateway_snapshot(gateway, event)
        task = asyncio.get_running_loop().create_task(record_connect(token, snapshot))
    except Exception as e:
        logger.debug("wire record not scheduled: %r", e)
        return None
    _TASKS.add(task)
    task.add_done_callback(_TASKS.discard)
    return task
