# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
copycord update          upgrade with the tool that installed Copycord, then
                         restart the service if it is installed and running
copycord update --check  print the installed and the latest (PyPI) version
"""

from __future__ import annotations

import json
import re
import shutil
import sys
import urllib.request
from pathlib import Path
from typing import Optional, Tuple

from copycord import service

PYPI_URL = "https://pypi.org/pypi/copycord/json"
PYPI_TIMEOUT = 5.0

UV_TOOL, PIPX, UV_PIP, PIP, DOCKER, SOURCE = (
    "uv tool",
    "pipx",
    "uv pip",
    "pip",
    "docker",
    "source",
)


def installer_metadata() -> Optional[str]:
    """Contents of copycord's dist-info INSTALLER file; None when not installed."""
    from importlib import metadata

    try:
        dist = metadata.distribution("copycord")
    except metadata.PackageNotFoundError:
        return None
    return (dist.read_text("INSTALLER") or "").strip().lower()


def detect_installer(
    prefix: Optional[str] = None,
    installer: Optional[str] = "",
    docker: Optional[bool] = None,
) -> str:
    """How this Copycord was installed: UV_TOOL, PIPX, UV_PIP, PIP, DOCKER or SOURCE.

    installer is the dist-info INSTALLER text (None: copycord is not installed
    as a package, e.g. run from a checkout); "" reads it.
    """
    if docker is None:
        from common.paths import in_docker

        docker = in_docker()
    if docker:
        return DOCKER
    root = Path(sys.prefix if prefix is None else prefix)
    if installer == "":
        installer = installer_metadata()
    if installer is None:
        return SOURCE
    parts = [p.lower() for p in root.parts]
    pairs = list(zip(parts, parts[1:]))
    if (root / "uv-receipt.toml").exists() or ("uv", "tools") in pairs:
        return UV_TOOL
    if (root / "pipx_metadata.json").exists() or ("pipx", "venvs") in pairs:
        return PIPX
    if installer == "uv":
        return UV_PIP
    return PIP


def upgrade_command(kind: str) -> Tuple[Optional[list], str]:
    """(command to run, or None) and a human hint for the given install kind."""
    if kind == UV_TOOL:
        return ["uv", "tool", "upgrade", "copycord"], "uv tool upgrade copycord"
    if kind == PIPX:
        return ["pipx", "upgrade", "copycord"], "pipx upgrade copycord"
    py = sys.executable
    if kind == UV_PIP:
        return None, f'uv pip install --upgrade --python "{py}" copycord'
    if kind == PIP:
        return None, f'"{py}" -m pip install --upgrade copycord'
    if kind == DOCKER:
        return None, "pull the new image and recreate the container (docker compose pull && docker compose up -d)"
    return None, "update your checkout (git pull), or install the package: uv tool install copycord"


def current_version() -> str:
    from common.config import CURRENT_VERSION

    return CURRENT_VERSION.lstrip("v")


def fetch_latest_version(url: str = PYPI_URL, timeout: float = PYPI_TIMEOUT) -> str:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.load(resp)
    return str(data["info"]["version"])


def _version_key(v: str):
    return tuple(int(n) for n in re.findall(r"\d+", v.split("+")[0])[:4])


def is_newer(latest: str, current: str) -> bool:
    try:
        return _version_key(latest) > _version_key(current)
    except ValueError:
        return latest != current


def check() -> int:
    current = current_version()
    try:
        latest = fetch_latest_version()
    except Exception as e:
        print(f"current {current}")
        print(f"WARN  could not get the latest version from PyPI: {e}")
        return 0
    print(f"current {current}")
    print(f"latest  {latest}")
    if is_newer(latest, current):
        print("an update is available: run `copycord update`")
    else:
        print("Copycord is up to date")
    return 0


def update(check_only: bool = False) -> int:
    if check_only:
        return check()

    kind = detect_installer()
    cmd, hint = upgrade_command(kind)
    if cmd is None:
        print(f"Copycord was installed with {kind}; update it with:\n  {hint}")
        return 1
    if shutil.which(cmd[0]) is None:
        print(f"{cmd[0]} is not on PATH; update Copycord with:\n  {hint}", file=sys.stderr)
        return 1

    # The service state is read before the upgrade replaces the files.
    try:
        state = service.service_state()
    except Exception as e:
        print(f"WARN  could not read the service state: {e}", file=sys.stderr)
        state = None

    print(f"$ {hint}")
    result = service.run_cmd(cmd, capture=False)
    if result.returncode != 0:
        print(f"upgrade failed (exit {result.returncode})", file=sys.stderr)
        return result.returncode

    if state is not None and state.installed and state.running:
        try:
            service.restart_service()
        except service.ServiceError as e:
            print(f"upgraded, but restarting the service failed: {e}", file=sys.stderr)
            return 1
        print("restarted the copycord service")
    return 0
