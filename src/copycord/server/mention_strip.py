# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


"""STRIP_MENTIONS: remove user, role, @everyone and @here mentions from cloned text."""

from __future__ import annotations
import re
from typing import Iterable

# raw user and role tokens: <@id>, <@!id>, <@&id>
_RAW_MENTION = re.compile(r"<@[!&]?\d+>")

# @everyone / @here, also after DISABLE_EVERYONE_MENTIONS put a zero-width space in
_EVERYONE = re.compile(r"(?<![\w@.])@​?(?:everyone|here)(?!\w)")

_SPACES = re.compile(r"[ \t]{2,}")
_SPACE_BEFORE_PUNCT = re.compile(r"[ \t]+([,.!?;:])")


def _names_pattern(names: Iterable[str]) -> re.Pattern | None:
    """One pattern for '@Name' over all names, longest first so '@Bob Smith' beats '@Bob'."""
    clean = sorted({n.strip() for n in names if n and n.strip()}, key=len, reverse=True)
    if not clean:
        return None
    alts = "|".join(re.escape(n) for n in clean)
    return re.compile(rf"(?<![\w@.])@(?:{alts})(?!\w)")


def strip_mentions(
    text: str | None,
    *,
    user_names: Iterable[str] = (),
    role_names: Iterable[str] = (),
) -> str | None:
    """Remove mentions from text and tidy the whitespace they leave.

    user_names and role_names are display names without the '@'. Only lines that
    lost a mention are tidied, so code blocks and blank lines elsewhere stay as sent.
    """
    if not text:
        return text

    patterns = [_RAW_MENTION, _EVERYONE]
    names = _names_pattern([*user_names, *role_names])
    if names is not None:
        patterns.append(names)

    out: list[str] = []
    for line in text.split("\n"):
        new = line
        for p in patterns:
            new = p.sub("", new)
        if new == line:
            out.append(line)
            continue
        new = _SPACE_BEFORE_PUNCT.sub(r"\1", _SPACES.sub(" ", new)).strip()
        if new:
            out.append(new)

    return "\n".join(out).strip()


def strip_embed_mentions(
    embed: dict,
    *,
    user_names: Iterable[str] = (),
    role_names: Iterable[str] = (),
) -> dict:
    """strip_mentions over an embed dict's title, description, author name, footer text and fields."""
    user_names = list(user_names)
    role_names = list(role_names)

    def s(v):
        return strip_mentions(v, user_names=user_names, role_names=role_names)

    for key in ("title", "description"):
        if embed.get(key):
            embed[key] = s(embed[key])
    for block, key in (("author", "name"), ("footer", "text")):
        sub = embed.get(block)
        if isinstance(sub, dict) and sub.get(key):
            sub[key] = s(sub[key])
    for f in embed.get("fields") or []:
        if not isinstance(f, dict):
            continue
        for key in ("name", "value"):
            if f.get(key):
                # Discord rejects an empty field name or value
                f[key] = s(f[key]) or "​"
    return embed
