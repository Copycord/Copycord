# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
"""Write files that only the current user may read (keys, passwords)."""
from __future__ import annotations

import getpass
import logging
import os
import secrets
import subprocess
from pathlib import Path
from typing import Union

logger = logging.getLogger("common.private_file")


def _restrict_windows_acl(path: Path) -> None:
    """Best effort: drop inherited ACEs and grant only the current user full control."""
    try:
        user = os.environ.get("USERNAME") or getpass.getuser()
        result = subprocess.run(
            ["icacls", str(path), "/inheritance:r", "/grant:r", f"{user}:F"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            logger.warning(
                "Could not restrict access to %s: %s",
                path,
                (result.stderr or result.stdout).strip(),
            )
    except Exception:
        logger.warning("Could not restrict access to %s", path, exc_info=True)


def write_private_file(path: Union[str, Path], data: Union[str, bytes]) -> None:
    """Atomically replace path with data, readable only by the current user.

    The content goes to a new 0600 temp file in the same folder (on Windows its
    ACL is limited to the current user), is fsynced, then renamed over path, so
    readers never see a partial file and the old mode is never inherited.
    """
    path = Path(path)
    if isinstance(data, str):
        data = data.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    fd = os.open(tmp, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        if os.name == "nt":
            _restrict_windows_acl(tmp)
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
