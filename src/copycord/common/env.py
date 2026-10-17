# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""The environment Copycord runs in. COPYCORD_ENV=dev (any case) turns on the developer
options, such as the scraper's debug report; anything else, or unset, is a normal install."""

from __future__ import annotations

import os

ENV_VAR = "COPYCORD_ENV"
DEV = "dev"


def is_dev() -> bool:
    """True when COPYCORD_ENV is dev, read on every call so a test or a restart can change it."""
    return (os.environ.get(ENV_VAR) or "").strip().casefold() == DEV
