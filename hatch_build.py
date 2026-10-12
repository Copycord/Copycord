# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Hatch build hook: ship the built frontend.

code/admin/frontend/dist (npm run build) lands in the wheel as admin/static,
next to the tracked logo, the same place the Docker image copies it to. When
dist is missing (a git install, `copycord update --channel dev`) and npm is on
PATH, the hook builds it first with `npm ci && npm run build`. Without npm,
or when that build fails, it only warns; `copycord doctor` reports the missing
assets at runtime.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

FRONTEND = Path("code") / "admin" / "frontend"
DIST = FRONTEND / "dist"


def _has_files(path: Path) -> bool:
    return path.is_dir() and any(path.iterdir())


class FrontendBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict) -> None:
        # Editable installs serve code/admin/static straight from the checkout.
        if self.target_name == "wheel" and version == "editable":
            return
        dist = Path(self.root) / DIST
        if not _has_files(dist):
            self.build_frontend()
        if not _has_files(dist):
            self.app.display_warning(
                f"{DIST} not found: building without the admin frontend "
                "(run `npm run build` in code/admin/frontend; `copycord doctor` will warn)"
            )
            return
        target = "admin/static" if self.target_name == "wheel" else DIST.as_posix()
        build_data["force_include"][str(dist)] = target

    def build_frontend(self) -> None:
        """npm ci && npm run build in code/admin/frontend; warns on any failure."""
        frontend = Path(self.root) / FRONTEND
        if not (frontend / "package.json").is_file():
            return
        npm = shutil.which("npm")
        if npm is None:
            self.app.display_warning("npm not found on PATH: cannot build the admin frontend")
            return
        self.app.display_info(f"building the admin frontend in {FRONTEND} (npm ci && npm run build)")
        for args in (["ci"], ["run", "build"]):
            try:
                subprocess.run([npm, *args], cwd=frontend, check=True)
            except (OSError, subprocess.CalledProcessError) as e:
                self.app.display_warning(f"npm {' '.join(args)} failed: {e}")
                return
