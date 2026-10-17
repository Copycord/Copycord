"""The scraper tokens: a plain text file (data/scraper_tokens.txt), one token per line.
Not validated and not in the database; a token that fails is skipped when it is used."""
import logging
from pathlib import Path
from typing import Any, Iterable, Optional

from copycord.common.private_file import write_private_file

log = logging.getLogger("scraper_tokens")

TOKEN_FILE = "scraper_tokens.txt"


def token_path(data_dir: Any) -> Path:
    return Path(data_dir) / TOKEN_FILE


def clean(values: Iterable[Any]) -> list[str]:
    """Trimmed, non-blank, de-duplicated (first one wins) strings."""
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        if not isinstance(v, str):
            continue
        v = v.strip()
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def read_tokens(data_dir: Any) -> list[str]:
    try:
        text = token_path(data_dir).read_text(encoding="utf-8")
    except OSError:
        return []
    return clean(text.splitlines())


def write_tokens(data_dir: Any, values: Iterable[Any]) -> list[str]:
    tokens = clean(values)
    write_private_file(token_path(data_dir), "\n".join(tokens) + ("\n" if tokens else ""))
    return tokens


def migrate_from_db(db: Any, data_dir: Any) -> Optional[int]:
    """Once: with no file yet, copy the scraper_tokens rows into it. Returns how many were
    written, or None when nothing was done."""
    if token_path(data_dir).exists():
        return None
    try:
        rows = db.list_scraper_tokens() or []
    except Exception:
        log.exception("scraper tokens: could not read the table to migrate")
        return None
    tokens = clean(r.get("token_value") for r in rows)
    if not tokens:
        return None
    write_tokens(data_dir, tokens)
    log.info("scraper tokens: moved %d from the database to %s", len(tokens), TOKEN_FILE)
    return len(tokens)
