# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


"""Mention passes for cloned text.

STRIP_MENTIONS removes user, role, @everyone and @here mentions.
DISABLE_USER_MENTIONS keeps user mentions visible but stops them pinging.
"""

from __future__ import annotations
import re
from typing import Iterable

# raw user and role tokens: <@id>, <@!id>, <@&id>
_RAW_MENTION = re.compile(r"<@[!&]?\d+>")

# raw user tokens only: <@id>, <@!id>
_RAW_USER_MENTION = re.compile(r"<@(?=!?\d+>)")

_ZWSP = "​"

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


def disable_user_mentions(
    text: str | None, *, user_names: Iterable[str] = ()
) -> str | None:
    """Put a zero-width space after the '@' of user mentions so they show but don't ping.

    Covers '@Name' for the given display names and raw <@id> / <@!id> tokens.
    Emails, a bare '@' and unknown names are left alone.
    """
    if not text:
        return text
    text = _RAW_USER_MENTION.sub("<@" + _ZWSP, text)
    names = _names_pattern(user_names)
    if names is not None:
        text = names.sub(lambda m: "@" + _ZWSP + m.group(0)[1:], text)
    return text


def disable_embed_user_mentions(embed: dict, *, user_names: Iterable[str] = ()) -> dict:
    """disable_user_mentions over the same embed parts strip_embed_mentions covers."""
    user_names = list(user_names)
    return _map_embed_text(
        embed, lambda v: disable_user_mentions(v, user_names=user_names)
    )


def strip_embed_mentions(
    embed: dict,
    *,
    user_names: Iterable[str] = (),
    role_names: Iterable[str] = (),
) -> dict:
    """strip_mentions over an embed dict's title, description, author name, footer text and fields."""
    user_names = list(user_names)
    role_names = list(role_names)
    return _map_embed_text(
        embed,
        lambda v: strip_mentions(v, user_names=user_names, role_names=role_names),
    )


def _map_embed_text(embed: dict, s) -> dict:
    """Run s over an embed dict's title, description, author name, footer text and fields."""
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
