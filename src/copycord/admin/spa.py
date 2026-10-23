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
# /version sits behind the password, so the login page reads the footer version from this one.
VERSION_META = "copycord-version"
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
    version: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
) -> Response:
    """index.html, uncached; version and login_error go in escaped meta tags."""
    root = spa_dir()
    if root is None:
        return PlainTextResponse(NOT_BUILT, status_code=404)
    hdrs = {"Cache-Control": "no-cache", **(headers or {})}
    index = root / "index.html"
    tags = [(VERSION_META, version), (LOGIN_ERROR_META, login_error)]
    meta = "".join(
        f'<meta name="{name}" content="{html.escape(value, quote=True)}" />' for name, value in tags if value
    )
    if not meta:
        return FileResponse(index, status_code=status_code, headers=hdrs, media_type="text/html")
    page = index.read_text(encoding="utf-8")
    page = page.replace("</head>", meta + "</head>", 1) if "</head>" in page else meta + page
    return HTMLResponse(page, status_code=status_code, headers=hdrs)


def _asset_file(assets: Path, name: str) -> Optional[Path]:
    """assets/name, only when name is a regular file directly inside assets.

    Vite names files by a base64url hash, which can start with "-" or "_", so
    this checks the folder itself rather than a name pattern.
    """
    if not name or name in (".", "..") or any(c in name for c in "/\\:\x00"):
        return None
    try:
        folder = assets.resolve()
        path = (folder / name).resolve()
    except (OSError, ValueError):
        return None
    if path.parent != folder or not path.is_file():
        return None
    return path


def asset_response(name: str) -> Response:
    """A hashed file from the build's assets dir, cached for a year."""
    root = spa_dir()
    asset = _asset_file(root / "assets", name) if root is not None else None
    if asset is None:
        raise HTTPException(status_code=404)
    return FileResponse(asset, headers={"Cache-Control": SPA_ASSET_CACHE})
