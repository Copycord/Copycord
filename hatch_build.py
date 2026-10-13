# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Hatch build hook: puts the built admin frontend in the wheel as
copycord/admin/static, building it with npm when dist is missing, and
writes copycord/_build.json with the source commit.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

FRONTEND = Path("src") / "copycord" / "admin" / "frontend"
DIST = FRONTEND / "dist"
SKIP_ENV = "COPYCORD_SKIP_FRONTEND"


def _has_files(path: Path) -> bool:
    return path.is_dir() and any(path.iterdir())


class FrontendBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict) -> None:
        # Editable installs serve src/copycord/admin/static straight from the checkout.
        if self.target_name == "wheel" and version == "editable":
            return
        if self.target_name == "wheel":
            self.stamp_commit(build_data)
        dist = Path(self.root) / DIST
        if not _has_files(dist):
            self.build_frontend()
        if not _has_files(dist):
            self.app.display_warning(
                f"{DIST} not found: building without the admin frontend "
                "(run `npm run build` in src/copycord/admin/frontend; `copycord doctor` will warn)"
            )
            return
        target = "copycord/admin/static" if self.target_name == "wheel" else DIST.as_posix()
        build_data["force_include"][str(dist)] = target

    def stamp_commit(self, build_data: dict) -> None:
        """Ship copycord/_build.json with the commit this wheel is built from."""
        commit = os.environ.get("COPYCORD_BUILD_COMMIT") or os.environ.get("GITHUB_SHA") or ""
        if not commit:
            try:
                res = subprocess.run(
                    ["git", "-C", self.root, "rev-parse", "HEAD"],
                    capture_output=True, text=True, timeout=10,
                )
                commit = res.stdout.strip() if res.returncode == 0 else ""
            except (OSError, subprocess.SubprocessError):
                commit = ""
        if not re.fullmatch(r"[0-9a-f]{40}", commit):
            return
        out = Path(tempfile.mkdtemp(prefix="copycord-build-")) / "_build.json"
        out.write_text(json.dumps({"commit": commit}), encoding="utf-8")
        build_data["force_include"][str(out)] = "copycord/_build.json"

    def build_frontend(self) -> None:
        """Run npm ci and npm run build; fails unless COPYCORD_SKIP_FRONTEND=1."""
        frontend = Path(self.root) / FRONTEND
        if not (frontend / "package.json").is_file():
            return
        skip = os.environ.get(SKIP_ENV) == "1"
        npm = shutil.which("npm")
        if npm is None:
            self.fail(
                f"npm not found on PATH, so the admin frontend in {FRONTEND} cannot be "
                "built. Install Node.js 20 or newer (it includes npm) and build again, "
                f"or set {SKIP_ENV}=1 to build without the admin UI.",
                skip,
            )
            return
        self.app.display_info(f"building the admin frontend in {FRONTEND} (npm ci && npm run build)")
        for args in (["ci"], ["run", "build"]):
            try:
                subprocess.run([npm, *args], cwd=frontend, check=True)
            except (OSError, subprocess.CalledProcessError) as e:
                self.fail(
                    f"npm {' '.join(args)} failed in {FRONTEND}: {e}. Fix the error above, "
                    f"or set {SKIP_ENV}=1 to build without the admin UI.",
                    skip,
                )
                return

    def fail(self, message: str, skip: bool) -> None:
        if skip:
            self.app.display_warning(message)
            return
        raise RuntimeError(f"copycord build: {message}")
