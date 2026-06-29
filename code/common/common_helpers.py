# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


import json
import re


def resolve_mapping_settings(
    db,
    config,
    *,
    original_guild_id: int | None = None,
    cloned_guild_id: int | None = None,
    mapping_id: int | None = None,
) -> dict:
    """
    Precedence:
      1) mapping_id
      2) (original_guild_id, cloned_guild_id) pair
      3) original_guild_id
      4) cloned_guild_id
      5) defaults

    If ENABLE_CLONING is False: drop ALL clone/sync/delete/edit/mirror features.
    """
    eff = dict(config.default_mapping_settings())

    row = None

    pair_requested = original_guild_id is not None and cloned_guild_id is not None
    force_disable = False

    if mapping_id is not None:
        try:
            row = db.get_mapping_by_id(int(mapping_id))
        except Exception:
            row = None

    if row is None and pair_requested:
        try:
            row = db.get_mapping_by_original_and_clone(
                int(original_guild_id), int(cloned_guild_id)
            )
        except Exception:
            row = None

        if row is None:
            force_disable = True

    if row is None and not pair_requested and original_guild_id is not None:
        try:
            row = db.get_mapping_by_original(int(original_guild_id))
        except Exception:
            row = None

    if row is None and not pair_requested and cloned_guild_id is not None:
        try:
            row = db.get_mapping_by_clone(int(cloned_guild_id))
        except Exception:
            row = None

    try:
        if row is not None and not isinstance(row, dict):
            row = dict(row)

        settings = row.get("settings") if row else None

        if isinstance(settings, str):
            try:
                settings = json.loads(settings)
            except Exception:
                settings = None

        if isinstance(settings, dict):
            eff.update(settings)
    except Exception:
        pass

    try:
        st = (
            str(row.get("status", "active") or "active").strip().lower()
            if row
            else "active"
        )
    except Exception:
        st = "active"

    if st == "paused" or force_disable:
        eff["ENABLE_CLONING"] = False

    if not eff.get("ENABLE_CLONING", True):
        for k in (
            "CLONE_EMOJI",
            "CLONE_ROLES",
            "CLONE_STICKER",
            "DELETE_CHANNELS",
            "DELETE_MESSAGES",
            "DELETE_ROLES",
            "DELETE_THREADS",
            "EDIT_MESSAGES",
            "RESEND_EDITED_MESSAGES",
            "MIRROR_ROLE_PERMISSIONS",
            "MIRROR_CHANNEL_PERMISSIONS",
        ):
            eff[k] = False

    return eff


DISCORD_WEBHOOK_RE = re.compile(
    r"^https?://(canary\.|ptb\.)?discord(app)?\.com/api/webhooks/\d+/.+", re.I
)


def is_discord_webhook_url(url: str) -> bool:
    """True if `url` is a Discord webhook URL."""
    return bool(DISCORD_WEBHOOK_RE.match((url or "").strip()))


def coerce_url_list(value) -> list[str]:
    """Normalize a URL value into a clean, de-duplicated list of strings.

    Accepts a comma/whitespace-separated string, or a list/tuple/set.
    Trims each entry, drops empties, and removes duplicates while
    preserving first-seen order. Does NOT validate URL format.
    """
    if isinstance(value, str):
        tokens = re.split(r"[,\s]+", value.strip())
    elif isinstance(value, (list, tuple, set)):
        tokens = list(value)
    else:
        tokens = []

    out: list[str] = []
    seen: set[str] = set()
    for tok in tokens:
        s = str(tok).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def discord_urls_from_config(config: dict) -> list[str]:
    """Extract the webhook URL list from a Discord forwarding rule config.

    Prefers `config["urls"]`; falls back to legacy single `config["url"]`.
    Returns a cleaned list (no validity filtering).
    """
    config = config or {}
    urls = coerce_url_list(config.get("urls"))
    if not urls:
        urls = coerce_url_list(config.get("url"))
    return urls
