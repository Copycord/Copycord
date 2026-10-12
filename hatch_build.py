# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Hatch build hook: ship the built frontend when it exists.

code/admin/frontend/dist (npm run build) lands in the wheel as admin/static,
next to the tracked logo, the same place the Docker image copies it to. A
missing dist (local dev without npm) only warns; `copycord doctor` reports
the missing assets at runtime.
"""

from __future__ import annotations

from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

DIST = Path("code") / "admin" / "frontend" / "dist"


class FrontendBuildHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict) -> None:
        # Editable installs serve code/admin/static straight from the checkout.
        if self.target_name == "wheel" and version == "editable":
            return
        dist = Path(self.root) / DIST
        if not dist.is_dir() or not any(dist.iterdir()):
            self.app.display_warning(
                f"{DIST} not found: building without the admin frontend "
                "(run `npm run build` in code/admin/frontend; `copycord doctor` will warn)"
            )
            return
        target = "admin/static" if self.target_name == "wheel" else DIST.as_posix()
        build_data["force_include"][str(dist)] = target
