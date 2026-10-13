# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
What the copycord command prints when it hits an unexpected error: one plain
line, the log file with the full traceback, and where to report it.
"""

from __future__ import annotations

import os
import platform
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import List, Optional, Sequence, TextIO

ISSUES_URL = "https://github.com/Copycord/Copycord/issues"


def summary(exc: BaseException) -> str:
    """'<ExcType>: <first line of the message>', or the type alone."""
    lines = str(exc).strip().splitlines()
    name = type(exc).__name__
    return f"{name}: {lines[0]}" if lines else name


def log_dirs() -> List[Path]:
    """Where the crash log goes: the logs folder, else the temp folder."""
    dirs = []
    try:
        from copycord.common.paths import logs_dir

        dirs.append(logs_dir())
    except Exception:
        pass
    dirs.append(Path(tempfile.gettempdir()))
    return dirs


def _command(argv: Sequence[str]) -> str:
    """The command and subcommand only; values can be secrets."""
    words = []
    for arg in argv:
        if arg == "--" or len(words) == 2:
            break
        if not arg.startswith("-"):
            words.append(arg)
    return " ".join(["copycord", *words])


def write_log(exc: BaseException, argv: Sequence[str] = (),
              dirs: Optional[Sequence[Path]] = None) -> Optional[Path]:
    """Write the traceback to a new file; None when no folder is writable."""
    try:
        from copycord.common.config import CURRENT_VERSION as version
    except Exception:
        version = "unknown"
    text = (
        f"Copycord {version}\n"
        f"Python {platform.python_version()} on {platform.platform()}\n"
        f"Command: {_command(argv)}\n"
        f"Time: {time.strftime('%Y-%m-%d %H:%M:%S %z')}\n\n"
        + "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    )
    name = f"copycord-crash-{time.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}.log"
    for folder in log_dirs() if dirs is None else dirs:
        path = Path(folder) / name
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        except OSError:
            continue
        return path
    return None


def report(exc: BaseException, argv: Sequence[str] = (), verbose: bool = False,
           stream: Optional[TextIO] = None) -> int:
    """Print the crash report on stderr; returns the exit code, 1."""
    stream = sys.stderr if stream is None else stream
    path = write_log(exc, argv)
    if verbose:
        traceback.print_exception(type(exc), exc, exc.__traceback__, file=stream)
        print(file=stream)
    print(f"Copycord hit an unexpected error: {summary(exc)}", file=stream)
    if path is None:
        print("The details could not be saved to a log file.", file=stream)
    else:
        print(f"The details were saved to {path}", file=stream)
    print(f"Report it at {ISSUES_URL}", file=stream)
    stream.flush()
    return 1
