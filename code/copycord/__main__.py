# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
python -m copycord run    admin, server and client in one process
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import List, Optional

logger = logging.getLogger("copycord")

ROLE_REMOVED_MESSAGE = (
    "ROLE is set, but Copycord v4 runs admin, server and client in one container "
    "and the ROLE option was removed. Delete ROLE from your compose file "
    "(and the extra server/client services), or stay on a v3 image tag."
)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="copycord")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="run Copycord")
    return parser.parse_args(argv)


def check_removed_options() -> bool:
    """False (after logging why) when the removed ROLE variable is still set."""
    if "ROLE" in os.environ:
        logger.error(ROLE_REMOVED_MESSAGE)
        return False
    return True


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if args.command == "run":
        from copycord.logging_setup import setup_logging

        # Console only here; supervisor.run adds the per-role files once DATA_DIR is set.
        setup_logging(roles=())
        if not check_removed_options():
            return 1

        from copycord import supervisor

        try:
            asyncio.run(supervisor.run())
        except KeyboardInterrupt:
            pass
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
