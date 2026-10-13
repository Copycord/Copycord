# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Request hardening helpers shared by the admin routes."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

_SAFE_PART_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._ -]*$")


def _safe_part(part: str) -> bool:
    if not part or part in (".", ".."):
        return False
    if ".." in part:
        return False
    return bool(_SAFE_PART_RE.match(part))


def safe_child(base: Path, name: str, *, max_depth: int = 1) -> Optional[Path]:
    """Return base/name only if name is a plain relative name inside base.

    Rejects separators other than "/" (between up to max_depth parts),
    "..", drive letters, absolute paths and anything that resolves outside
    base. Returns None when the name is not acceptable.
    """
    if not isinstance(name, str) or not name:
        return None
    if "\\" in name or "\x00" in name or ":" in name:
        return None
    parts = name.split("/")
    if len(parts) > max_depth or not all(_safe_part(p) for p in parts):
        return None
    try:
        root = Path(base).resolve()
        p = (root / Path(*parts)).resolve()
    except (OSError, ValueError):
        return None
    if p == root or root not in p.parents:
        return None
    return p
