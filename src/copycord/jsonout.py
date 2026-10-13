# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
--json output: stdout carries exactly one JSON document; everything a command
would print for people goes to stderr, without color.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
from typing import Iterator, Optional, TextIO


def emit(doc, stream: Optional[TextIO] = None) -> None:
    """Write DOC as the one JSON document on stdout."""
    stream = sys.stdout if stream is None else stream
    stream.write(json.dumps(doc, indent=2) + "\n")
    stream.flush()


@contextlib.contextmanager
def human_to_stderr() -> Iterator[None]:
    """Send what would print on stdout to stderr, with color off, until the block ends."""
    old = os.environ.get("NO_COLOR")
    os.environ["NO_COLOR"] = "1"
    try:
        with contextlib.redirect_stdout(sys.stderr):
            yield
    finally:
        if old is None:
            os.environ.pop("NO_COLOR", None)
        else:
            os.environ["NO_COLOR"] = old
