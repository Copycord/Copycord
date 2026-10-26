# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
The modules a Copycord install must import, shared by copycord doctor and the
check copycord update runs on a freshly installed environment.
"""

# Module name -> distribution that must provide it. pydantic_core comes before
# what needs it, so a half-replaced environment is named by its broken package.
REQUIRED_IMPORTS = (
    ("curl_cffi", "curl_cffi"),
    ("discord", "py-cord"),
    ("pydantic_core", "pydantic-core"),
    ("pydantic", "pydantic"),
    ("fastapi", "fastapi"),
    ("uvicorn", "uvicorn"),
)
