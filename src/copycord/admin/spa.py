# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Serves the React dashboard built from admin/web: index.html for every page
and hashed files under assets/."""

from __future__ import annotations

import html
from pathlib import Path
from typing import Mapping, Optional

from fastapi import HTTPException
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, Response

BASE_DIR = Path(__file__).resolve().parent
# web_static in a wheel, web/dist in a checkout.
SPA_DIRS = (BASE_DIR / "web_static", BASE_DIR / "web" / "dist")
SPA_ASSET_CACHE = "public, max-age=31536000, immutable"
# The login page reads a failed attempt's message from this meta tag.
LOGIN_ERROR_META = "copycord-login-error"
NOT_BUILT = "The dashboard is not built (run `npm ci && npm run build` in src/copycord/admin/web)."


def spa_dir() -> Optional[Path]:
    for d in SPA_DIRS:
        if (d / "index.html").is_file():
            return d
    return None


def index_response(
    status_code: int = 200,
    *,
    login_error: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
) -> Response:
    """index.html, uncached; login_error goes in an escaped meta tag."""
    root = spa_dir()
    if root is None:
        return PlainTextResponse(NOT_BUILT, status_code=404)
    hdrs = {"Cache-Control": "no-cache", **(headers or {})}
    index = root / "index.html"
    if not login_error:
        return FileResponse(index, status_code=status_code, headers=hdrs, media_type="text/html")
    page = index.read_text(encoding="utf-8")
    meta = f'<meta name="{LOGIN_ERROR_META}" content="{html.escape(login_error, quote=True)}" />'
    page = page.replace("</head>", meta + "</head>", 1) if "</head>" in page else meta + page
    return HTMLResponse(page, status_code=status_code, headers=hdrs)


def asset_response(name: str) -> Response:
    """A hashed file from the build's assets dir, cached for a year."""
    from copycord.admin.security import safe_child

    root = spa_dir()
    asset = safe_child(root / "assets", name) if root is not None else None
    if asset is None or not asset.is_file():
        raise HTTPException(status_code=404)
    return FileResponse(asset, headers={"Cache-Control": SPA_ASSET_CACHE})
