# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
python -m copycord run                 admin, server and client in one process
python -m copycord run --role server   one role, the split-mode equivalent
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import List, Optional

from copycord.supervisor import ROLES


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="copycord")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run Copycord")
    run.add_argument(
        "--role",
        choices=ROLES,
        default=None,
        help="run only this role (split mode); default runs all three",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if args.command == "run":
        from copycord import supervisor

        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            )
        try:
            asyncio.run(supervisor.run(args.role))
        except KeyboardInterrupt:
            pass
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
