# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


from __future__ import annotations
import asyncio, logging, discord, aiohttp
from typing import List, Dict, Tuple, Optional
from copycord.server import logctx
from copycord.common.http_session import shared_session
from copycord.server.item_hooks import ItemHooks

logger = logging.getLogger("server.roles")


def plan_role_order(
    host_roles: List[Dict],
    mapped: Dict[int, int],
    clone_roles: List[Dict],
    bot_top_position: int,
    bot_role_id: int | None = None,
) -> Dict:
    """Where each clone role goes so the cloned roles stand in the host's order.

    host_roles: [{id, position}] of the host. mapped: {host role id: clone role id}.
    clone_roles: [{id, name, position, managed, default}] of the clone. Only roles below
    the bot's top role that are not managed, @everyone or the bot's own role can move;
    a mapped role that cannot is listed in unmovable. Clone roles no host role maps to
    keep their order above the cloned ones, as the regular sync puts them.

    Returns {order: clone ids bottom to top, cloned: how many of them are mapped,
    positions: {clone id: position from 1}, moved: roles whose place changes,
    unmovable: [{id, name, reason}], changed}."""
    by_id = {int(r["id"]): r for r in clone_roles}

    def key(r: Dict) -> tuple:
        return (int(r.get("position") or 0), int(r["id"]))

    def why_not(r: Dict) -> Optional[str]:
        if r.get("default"):
            return "@everyone"
        if r.get("managed"):
            return "Managed by an integration"
        if bot_role_id is not None and int(r["id"]) == int(bot_role_id):
            return "My own role"
        if int(r.get("position") or 0) >= int(bot_top_position):
            return "Above my top role"
        return None

    cloned: list[int] = []
    unmovable: list[Dict] = []
    seen: set[int] = set()
    for h in sorted(host_roles, key=key):
        cid = mapped.get(int(h["id"]))
        role = by_id.get(int(cid)) if cid else None
        if role is None or int(role["id"]) in seen:
            continue
        seen.add(int(role["id"]))
        reason = why_not(role)
        if reason is None:
            cloned.append(int(role["id"]))
        elif not role.get("default"):
            unmovable.append({"id": int(role["id"]), "name": role.get("name") or str(role["id"]),
                              "reason": reason})
    mapped_ids = {int(c) for c in mapped.values() if c}
    others = [int(r["id"]) for r in sorted(clone_roles, key=key)
              if int(r["id"]) not in mapped_ids and why_not(r) is None]
    order = cloned + others
    # Positions count up from 1 and must stay below my top role. Only stale or
    # inconsistent positions can overflow it; leave those roles where they are.
    room = max(0, int(bot_top_position) - 1)
    for rid in cloned[room:]:
        unmovable.append({"id": rid, "name": by_id[rid].get("name") or str(rid),
                          "reason": "Above my top role"})
    cloned, order = cloned[:room], order[:room]
    current = sorted(order, key=lambda i: key(by_id[i]))
    moved = sum(1 for a, b in zip(current, order) if a != b)
    return {
        "order": order,
        "cloned": len(cloned),
        "positions": {rid: n for n, rid in enumerate(order, 1)},
        "moved": moved,
        "unmovable": unmovable,
        "changed": moved > 0,
    }


def my_role_ids(me) -> set[int]:
    """Ids of the roles the bot member holds, its top role included."""
    ids = {int(r.id) for r in (getattr(me, "roles", None) or ())}
    top = getattr(me, "top_role", None)
    if top is not None:
        ids.add(int(top.id))
    return ids


def rank_role_facts(
    roles: List[Dict], mine: set[int]
) -> Optional[Tuple[List[Dict], int, int]]:
    """Role facts renumbered 0..n-1 in Discord's hierarchy, and the bot's top role.

    Positions read right after rapid role creation can be duplicated or gappy
    (discord.py #5902, #2142), so a role below the bot can look level with or above
    it. Discord orders by position and, on a tie, puts the lower id (older role)
    higher, as discord.Role's comparison does; @everyone is always lowest.
    Returns (facts, my top role's position, its id), or None when none of the
    listed roles is one of mine."""

    def key(r: Dict) -> tuple:
        return (not r.get("default"), int(r.get("position") or 0), -int(r["id"]))

    ranked = [dict(r, position=n, raw_position=int(r.get("position") or 0))
              for n, r in enumerate(sorted(roles, key=key))]
    own = [r for r in ranked if int(r["id"]) in mine and not r.get("default")]
    if not own:
        return None
    top = max(own, key=lambda r: r["position"])
    return ranked, int(top["position"]), int(top["id"])


def role_rank(role) -> tuple:
    """A role's place in Discord's hierarchy, comparable with <: position first and,
    on a tie, the lower id (older role) higher, the same order rank_role_facts uses."""
    return (int(role.position), -int(role.id))


def below_my_top(role, me_top) -> bool:
    """Whether role sits strictly below my top role, so I can edit or delete it.
    A role sharing my top role's raw position but newer (higher id) is below it."""
    return me_top is not None and role_rank(role) < role_rank(me_top)


def top_tie_report(top_id: int, top_name, top_position: int, skipped: List[int]) -> Dict:
    """My top role with its raw position, and of the raw positions of the roles skipped
    as above it, how many share that position (tied_with_bot) or sit strictly above it."""
    top_position = int(top_position)
    return {
        "bot_top": {"id": int(top_id), "name": top_name, "position": top_position},
        "tied_with_bot": sum(1 for p in skipped if int(p) == top_position),
        "above_bot": sum(1 for p in skipped if int(p) > top_position),
        "skipped": len(skipped),
    }


def plan_tie_report(facts: List[Dict], top_id: int, unmovable: List[Dict]) -> Optional[Dict]:
    """top_tie_report for a plan_role_order plan, read from the raw positions that
    rank_role_facts keeps. None when my top role is not among the facts."""
    by_id = {int(f["id"]): f for f in facts}

    def raw(f: Dict) -> int:
        return int(f.get("raw_position", f.get("position") or 0))

    top = by_id.get(int(top_id))
    if top is None:
        return None
    skipped = [raw(by_id[int(u["id"])]) for u in unmovable
               if u["reason"] == "Above my top role" and int(u["id"]) in by_id]
    return top_tie_report(top["id"], top.get("name"), raw(top), skipped)


def bot_role_ties(facts: List[Dict], top_id: int) -> Optional[Dict]:
    """How Discord's raw positions sit around my top role, read from fetched role facts
    (raw_position when rank_role_facts renumbered them).

    tied_with_bot counts every non-default role other than my top role that shares its
    raw position, skipped or not: Discord will not let me move a role tied with mine.
    at_bottom is set when my top role's raw position is 1 or lower. None when my top
    role is not among the facts."""

    def raw(f: Dict) -> int:
        return int(f.get("raw_position", f.get("position") or 0))

    top = next((f for f in facts if int(f["id"]) == int(top_id)), None)
    if top is None:
        return None
    pos = raw(top)
    tied = sum(1 for f in facts if not f.get("default") and int(f["id"]) != int(top_id)
               and raw(f) == pos)
    return {"id": int(top["id"]), "name": top.get("name") or str(top["id"]), "position": pos,
            "tied_with_bot": tied, "at_bottom": pos <= 1}


def above_top_note(report: Dict) -> str:
    top = report["bot_top"]
    return (
        f"{report['skipped']} roles skipped as above my top role {top['name']} ({top['id']}, "
        f"raw position {top['position']}): {report['tied_with_bot']} share its raw position, "
        f"{report['above_bot']} sit strictly above it"
    )


class RoleManager:
    def __init__(
        self,
        bot,
        db,
        guild_resolver,
        ratelimit,
        clone_guild_id: int | None = None,
        delete_roles: bool | None = None,
        mirror_permissions: bool | None = None,
        emit_event_log=None,
    ):
        self.bot = bot
        self.db = db
        self.guild_resolver = guild_resolver
        self.ratelimit = ratelimit
        self.clone_guild_id = int(clone_guild_id or 0)
        self.delete_roles = bool(delete_roles) if delete_roles is not None else False
        self.mirror_permissions = (
            bool(mirror_permissions) if mirror_permissions is not None else False
        )
        self._emit_event_log = emit_event_log

        self._tasks: dict[int, asyncio.Task] = {}
        self._locks: dict[int, asyncio.Lock] = {}

        self.MAX_ROLES = 250
        self._ROLE_OP_DELAY = 1.5

    async def _emit_log(
        self,
        event_type: str,
        details: str,
        guild_id: int = None,
        guild_name: str = None,
        **kwargs,
    ):
        """Fire the event-log callback if wired up."""
        if self._emit_event_log:
            try:
                await self._emit_event_log(
                    event_type, details,
                    guild_id=guild_id, guild_name=guild_name,
                    **kwargs,
                )
            except Exception:
                pass

    def _log(self, level: str, msg: str, *args) -> None:
        """Role sync logging prefixed with the sync task context."""
        prefix = logctx.format_prefix()

        if level == "info":
            logger.info(prefix + msg, *args)
        elif level == "warning":
            logger.warning(prefix + msg, *args)
        elif level == "error":
            logger.error(prefix + msg, *args)
        else:
            logger.debug(prefix + msg, *args)

    def _get_lock_for_clone(self, clone_gid: int) -> asyncio.Lock:
        """Get/create the lock for this clone guild. Different clone guilds sync in parallel."""
        if clone_gid not in self._locks:
            self._locks[clone_gid] = asyncio.Lock()
        return self._locks[clone_gid]

    def kickoff_sync(
        self,
        roles,
        host_guild_id,
        target_clone_guild_id,
        *,
        validate_mapping: bool = True,
        delete_roles: bool | None = None,
        mirror_permissions: bool | None = None,
        update_roles: bool | None = None,
        rearrange_roles: bool | None = None,
        clone_role_icons: bool | None = None,
        clone_role_styles: bool | None = None,
    ) -> None:
        clone_gid = int(target_clone_guild_id or self.clone_guild_id)

        existing_task = self._tasks.get(clone_gid)
        if existing_task and not existing_task.done():
            logger.debug(
                "[roles] Sync already running for clone %s; skip kickoff.", clone_gid
            )
            return

        clone_guild = self.bot.get_guild(clone_gid)
        if not clone_guild:
            logger.debug("[roles] kickoff: clone guild %s not ready.", clone_gid)
            return

        if validate_mapping and self.guild_resolver and host_guild_id:
            try:
                clones = set(self.guild_resolver.clones_for_host(int(host_guild_id)))
                if clones and clone_gid not in clones:
                    self._log(
                        "warning",
                        "[roles] host %s is not mapped to clone %s; proceeding anyway",
                        host_guild_id,
                        clone_gid,
                    )
            except Exception:
                pass

        incoming_roles = roles or []
        host_id_int = int(host_guild_id) if host_guild_id else None

        eff_delete_roles = (
            self.delete_roles if delete_roles is None else bool(delete_roles)
        )
        eff_mirror_perms = (
            self.mirror_permissions
            if mirror_permissions is None
            else bool(mirror_permissions)
        )

        eff_update_roles = True if update_roles is None else bool(update_roles)

        eff_rearrange_roles = (
            False if rearrange_roles is None else bool(rearrange_roles)
        )
        eff_clone_role_icons = (
            False if clone_role_icons is None else bool(clone_role_icons)
        )
        eff_clone_role_styles = (
            False if clone_role_styles is None else bool(clone_role_styles)
        )

        logger.debug(
            "[🧩] Scheduling role sync task host=%s → clone=%s (delete_roles=%s mirror_perms=%s rearrange=%s)",
            host_id_int,
            clone_gid,
            eff_delete_roles,
            eff_mirror_perms,
            eff_rearrange_roles,
        )

        task = asyncio.create_task(
            self._run_sync(
                guild=clone_guild,
                incoming=incoming_roles,
                host_id=host_id_int,
                clone_id=clone_gid,
                delete_roles=eff_delete_roles,
                mirror_permissions=eff_mirror_perms,
                update_roles=eff_update_roles,
                rearrange_roles=eff_rearrange_roles,
                clone_role_icons=eff_clone_role_icons,
                clone_role_styles=eff_clone_role_styles,
            )
        )

        self._tasks[clone_gid] = task
        return task

    async def _run_sync(
        self,
        *,
        guild: discord.Guild,
        incoming: List[Dict],
        host_id: int | None,
        clone_id: int,
        delete_roles: bool,
        mirror_permissions: bool,
        update_roles: bool,
        rearrange_roles: bool,
        clone_role_icons: bool = False,
        clone_role_styles: bool = False,
    ) -> None:
        lock = self._get_lock_for_clone(clone_id)
        async with lock:
            if self.bot.is_closed():
                return
            try:
                deleted, updated, created, rearranged = await self._sync(
                    guild=guild,
                    incoming=incoming,
                    host_id=host_id,
                    clone_id=clone_id,
                    delete_roles=delete_roles,
                    mirror_permissions=mirror_permissions,
                    update_roles=update_roles,
                    rearrange_roles=rearrange_roles,
                    clone_role_icons=clone_role_icons,
                    clone_role_styles=clone_role_styles,
                )

                parts = []
                if deleted:
                    parts.append(f"Deleted {deleted} roles")
                if updated:
                    parts.append(f"Updated {updated} roles")
                if created:
                    parts.append(f"Created {created} roles")
                if rearranged:
                    parts.append(f"Repositioned {rearranged} roles")

                if parts:
                    summary = "; ".join(parts)
                    self._log(
                        "info",
                        "[🧩] Role sync complete: %s",
                        summary,
                    )
                    return f"Roles: {summary}"
                else:
                    self._log(
                        "info",
                        "[🧩] Role sync complete: no changes needed",
                    )
                    return ""

            except asyncio.CancelledError:
                if self.bot.is_closed():
                    return
                task = asyncio.current_task()
                if task and task.cancelled():
                    self._log("debug", "[🧩] Role sync canceled (task canceled).")
                else:
                    self._log(
                        "warning",
                        "[🧩] Role sync interrupted by discord.py reconnect; will retry next sync cycle",
                    )
            except Exception as e:
                self._log("error", "[🧩] Role sync failed: %s", e)
            finally:
                t = self._tasks.get(clone_id)
                if t and t.done():
                    pass
                else:
                    self._tasks.pop(clone_id, None)

    @staticmethod
    def _build_role_colours(colors: Optional[Dict]) -> Optional[discord.RoleColours]:
        """Build a py-cord RoleColours from the sitemap's colors dict, or None when unusable."""
        if not isinstance(colors, dict):
            return None
        try:
            primary = discord.Colour(int(colors.get("primary_color") or 0))
        except Exception:
            return None

        def _opt(v):
            try:
                return discord.Colour(int(v)) if v is not None else None
            except Exception:
                return None

        return discord.RoleColours(
            primary,
            _opt(colors.get("secondary_color")),
            _opt(colors.get("tertiary_color")),
        )

    async def _recreate_missing_role(
        self,
        *,
        guild: discord.Guild,
        orig_id: int,
        want_name: str,
        want_perms: discord.Permissions,
        want_color: discord.Color,
        want_hoist: bool,
        want_mention: bool,
        can_create: bool,
        create_suppressed_logged: bool,
        clone_by_id: Dict[int, discord.Role],
        original_guild_id: int | None,
        cloned_guild_id: int | None,
        mirror_permissions: bool,
        clone_role_icons: bool = False,
        icon_url: str | None = None,
        unicode_emoji: str | None = None,
        clone_role_styles: bool = False,
        role_colors: Dict | None = None,
    ) -> Tuple[Optional[discord.Role], int, bool, bool]:
        """Recreate a cloned role whose DB mapping exists but whose role was deleted."""
        if not can_create:
            if not create_suppressed_logged:
                self._log(
                    "warning",
                    "[🧩] Can't recreate role %r — guild at max role count (%d).",
                    want_name,
                    self.MAX_ROLES,
                )
                create_suppressed_logged = True
            return None, 0, can_create, create_suppressed_logged

        try:
            kwargs = dict(
                name=want_name,
                colour=want_color,
                hoist=want_hoist,
                mentionable=want_mention,
                reason="Copycord role sync (recreate missing clone)",
            )
            if mirror_permissions:
                kwargs["permissions"] = want_perms
            if clone_role_icons:
                if icon_url:
                    icon_bytes = await self._fetch_icon_bytes(icon_url)
                    if icon_bytes:
                        kwargs["icon"] = icon_bytes
                elif unicode_emoji:
                    kwargs["unicode_emoji"] = unicode_emoji

            if clone_role_styles:
                colours_obj = self._build_role_colours(role_colors)
                if colours_obj is not None:
                    # py-cord lets colour= override colours=, so pass only one
                    kwargs.pop("colour", None)
                    kwargs["colours"] = colours_obj

            cloned = await guild.create_role(**kwargs)
            await asyncio.sleep(self._ROLE_OP_DELAY)

            self.db.upsert_role_mapping(
                orig_id,
                want_name,
                cloned.id,
                cloned.name,
                original_guild_id=original_guild_id,
                cloned_guild_id=cloned_guild_id,
            )

            clone_by_id[cloned.id] = cloned

            self._log(
                "info",
                "[🧩] Recreated missing cloned role for upstream %r → %s (%d)",
                want_name,
                cloned.name,
                cloned.id,
            )

            can_create = len(guild.roles) < self.MAX_ROLES
            return cloned, 1, can_create, create_suppressed_logged

        except Exception as e:
            if "icon" in kwargs or "unicode_emoji" in kwargs:
                kwargs.pop("icon", None)
                kwargs.pop("unicode_emoji", None)
                try:
                    cloned = await guild.create_role(**kwargs)
                    await asyncio.sleep(self._ROLE_OP_DELAY)
                    self.db.upsert_role_mapping(
                        orig_id, want_name, cloned.id, cloned.name,
                        original_guild_id=original_guild_id,
                        cloned_guild_id=cloned_guild_id,
                    )
                    clone_by_id[cloned.id] = cloned
                    self._log(
                        "warning",
                        "[⚠️] Created role '%s' without icon — Discord rejected the icon (%s). "
                        "If the clone server isn't Boost Level 2+, boost it or set CLONE_ROLE_ICONS to false.",
                        want_name,
                        e,
                    )
                    can_create = len(guild.roles) < self.MAX_ROLES
                    return cloned, 1, can_create, create_suppressed_logged
                except Exception as e2:
                    self._log("warning", "[⚠️] Failed creating role %r: %s", want_name, e2)
            else:
                self._log(
                    "warning",
                    "[⚠️] Failed recreating missing cloned role for %r: %s",
                    want_name,
                    e,
                )
            return None, 0, can_create, create_suppressed_logged

    async def _sync(
        self,
        *,
        guild: discord.Guild,
        incoming: List[Dict],
        host_id: int | None,
        clone_id: int,
        delete_roles: bool,
        mirror_permissions: bool,
        update_roles: bool,
        rearrange_roles: bool,
        clone_role_icons: bool = False,
        clone_role_styles: bool = False,
        hooks: ItemHooks | None = None,
    ) -> Tuple[int, int, int]:
        """Mirror roles (and permissions if enabled) from one host guild into one clone guild.
        hooks hears about each role as it is done and can stop the pass between roles."""
        hooks = hooks or ItemHooks()
        me = guild.me
        me_top = me.top_role if me and me.top_role else None

        rows = self.db.get_all_role_mappings()
        current: dict[int, dict] = {}
        for r in rows:
            row = dict(r)

            if (
                host_id is None
                or int(row.get("original_guild_id") or 0) == int(host_id)
            ) and int(row.get("cloned_guild_id") or 0) == int(clone_id):
                current[int(row["original_role_id"])] = row

        incoming_filtered = {
            int(r["id"]): r
            for r in incoming
            if not r.get("managed") and not r.get("everyone")
        }

        styles_supported = "ENHANCED_ROLE_COLORS" in set(
            getattr(guild, "features", []) or []
        )
        if clone_role_styles and not styles_supported:
            has_styled = any(
                isinstance(r.get("colors"), dict)
                and (
                    r["colors"].get("secondary_color")
                    or r["colors"].get("tertiary_color")
                )
                for r in incoming_filtered.values()
            )
            if has_styled:
                self._log(
                    "info",
                    "[🧩] Clone server doesn't support enhanced role colors — "
                    "gradient/holographic styles will be applied as solid colors. "
                    "Boost to Level 2+ or set CLONE_ROLE_STYLES to false.",
                )

        clone_by_id = {r.id: r for r in guild.roles}
        blocked = {int(x) for x in self.db.get_blocked_role_ids(clone_id)}

        can_create = len(guild.roles) < self.MAX_ROLES
        create_suppressed_logged = False

        deleted = updated = created = 0
        # Raw positions of the roles skipped as above my top role, for one warning at the end.
        above_top: list[int] = []

        def skipped_above(role) -> None:
            if role is not None and not role.is_default() and not role.managed:
                above_top.append(int(role.position))

        removed =[o for o in current if o not in incoming_filtered]
        await hooks.begin(len(incoming_filtered) + (len(removed) if delete_roles else 0))

        for orig_id in list(current.keys()):
            if hooks.stop():
                break
            if orig_id not in incoming_filtered:
                row = current[orig_id]
                cloned_id = row.get("cloned_role_id")
                cloned_role = clone_by_id.get(int(cloned_id)) if cloned_id else None
                gone_name = getattr(cloned_role, "name", None) or row.get("cloned_role_name") or str(orig_id)

                if not delete_roles:

                    self.db.delete_role_mapping_for_clone(orig_id, clone_id)
                    if cloned_role:
                        self._log(
                            "info",
                            "[🧩] Host role deleted; kept cloned role %s (%d), removed mapping.",
                            cloned_role.name,
                            cloned_role.id,
                        )
                    else:
                        self._log(
                            "info",
                            "[🧩] Host role deleted; cloned missing, removed mapping only.",
                        )
                    continue

                if (
                    not cloned_role
                    or cloned_role.is_default()
                    or cloned_role.managed
                    or not below_my_top(cloned_role, me_top)
                ):

                    self.db.delete_role_mapping_for_clone(orig_id, clone_id)
                    if cloned_role:
                        skipped_above(cloned_role)
                        self._log(
                            "info",
                            "[🧩] Skipped deleting role %s (%d); removed mapping.",
                            cloned_role.name,
                            cloned_role.id,
                        )
                        await hooks.item(gone_name, "skipped", "Not deleted: managed or above my top role")
                    else:
                        self._log("info", "[🧩] Cloned role missing; removed mapping.")
                        await hooks.item(gone_name, "unchanged")
                    continue

                try:
                    hooks.check()
                    await cloned_role.delete()
                    await asyncio.sleep(self._ROLE_OP_DELAY)
                    deleted += 1
                    await hooks.item(gone_name, "deleted")
                    self._log(
                        "info",
                        "[🧩] Deleted role %s (%d)",
                        cloned_role.name,
                        cloned_role.id,
                    )
                    await self._emit_log(
                        "role_deleted",
                        f"Deleted role '{cloned_role.name}'",
                        guild_id=clone_id,
                        guild_name=getattr(guild, "name", None),
                        extra={"original_role_id": int(orig_id), "clone_role_id": int(cloned_role.id)},
                    )

                except Exception as e:
                    self._log(
                        "warning",
                        "[⚠️] Failed deleting role %s (%s); removing mapping anyway: %s",
                        getattr(cloned_role, "name", "?"),
                        cloned_id,
                        e,
                    )
                    await hooks.item(gone_name, "failed", str(e) or type(e).__name__)
                finally:
                    self.db.delete_role_mapping_for_clone(orig_id, clone_id)

        rows = self.db.get_all_role_mappings()
        current: dict[int, dict] = {}
        for r in rows:
            row = dict(r)

            if (
                host_id is None
                or int(row.get("original_guild_id") or 0) == int(host_id)
            ) and int(row.get("cloned_guild_id") or 0) == int(clone_id):
                current[int(row["original_role_id"])] = row

        clone_by_id = {r.id: r for r in guild.roles}

        for orig_id, info in incoming_filtered.items():
            if hooks.stop():
                break
            mapping = current.get(orig_id)
            # What the role's last word to hooks is when the pass falls through to the next role.
            outcome, why = "unchanged", None

            cloned_role = None
            if mapping:
                cloned_id = mapping.get("cloned_role_id")
                if cloned_id:
                    try:
                        cloned_role = clone_by_id.get(int(cloned_id))
                    except Exception:
                        cloned_role = None

            if orig_id in blocked:
                if (
                    cloned_role
                    and (not cloned_role.is_default())
                    and (not cloned_role.managed)
                    and below_my_top(cloned_role, me_top)
                ):
                    try:
                        hooks.check()
                        await cloned_role.delete(
                            reason="Blocked by Copycord role blocklist"
                        )
                        await asyncio.sleep(self._ROLE_OP_DELAY)
                        self._log(
                            "info",
                            "[🧩] Deleted blocked role %s (%d)",
                            cloned_role.name,
                            cloned_role.id,
                        )
                    except Exception as e:
                        self._log(
                            "warning",
                            "[⚠️] Failed deleting blocked role %s: %s",
                            getattr(cloned_role, "name", "?"),
                            e,
                        )
                if mapping:
                    self.db.delete_role_mapping_for_clone(orig_id, clone_id)
                await hooks.item(info.get("name") or str(orig_id), "skipped", "On the role blocklist")
                continue

            want_name = info["name"]
            want_perms = discord.Permissions(info.get("permissions", 0))
            want_color = discord.Color(info.get("color", 0))
            want_hoist = bool(info.get("hoist", False))
            want_mention = bool(info.get("mentionable", False))
            want_icon_url = info.get("icon_url")
            want_unicode_emoji = info.get("unicode_emoji")
            want_colors = info.get("colors") if clone_role_styles else None

            if mapping and not cloned_role:
                hooks.check()
                cloned_role, add, can_create, create_suppressed_logged = (
                    await self._recreate_missing_role(
                        guild=guild,
                        orig_id=orig_id,
                        want_name=want_name,
                        want_perms=want_perms,
                        want_color=want_color,
                        want_hoist=want_hoist,
                        want_mention=want_mention,
                        can_create=can_create,
                        create_suppressed_logged=create_suppressed_logged,
                        clone_by_id=clone_by_id,
                        original_guild_id=host_id,
                        cloned_guild_id=clone_id,
                        mirror_permissions=mirror_permissions,
                        clone_role_icons=clone_role_icons,
                        icon_url=want_icon_url,
                        unicode_emoji=want_unicode_emoji,
                        clone_role_styles=clone_role_styles,
                        role_colors=want_colors,
                    )
                )
                created += add
                if not cloned_role:
                    await hooks.item(want_name, "failed", "Could not recreate the deleted clone role")
                    continue
                outcome = "created"

            if not mapping:
                if not can_create:
                    if not create_suppressed_logged:
                        self._log(
                            "warning",
                            "[🧩] Can't create more roles. Guild is at max role count (%d).",
                            self.MAX_ROLES,
                        )
                        create_suppressed_logged = True
                    await hooks.item(want_name, "skipped", f"The clone server is at the {self.MAX_ROLES} role limit")
                    continue

                try:
                    kwargs = dict(
                        name=want_name,
                        colour=want_color,
                        hoist=want_hoist,
                        mentionable=want_mention,
                        reason="Copycord role sync",
                    )
                    if mirror_permissions:
                        kwargs["permissions"] = want_perms
                    if clone_role_icons:
                        if want_icon_url:
                            icon_bytes = await self._fetch_icon_bytes(want_icon_url)
                            if icon_bytes:
                                kwargs["icon"] = icon_bytes
                        elif want_unicode_emoji:
                            kwargs["unicode_emoji"] = want_unicode_emoji

                    if clone_role_styles:
                        colours_obj = self._build_role_colours(want_colors)
                        if colours_obj is not None:
                            # py-cord lets colour= override colours=, so pass only one
                            kwargs.pop("colour", None)
                            kwargs["colours"] = colours_obj

                    hooks.check()
                    new_role = await guild.create_role(**kwargs)
                    await asyncio.sleep(self._ROLE_OP_DELAY)
                    created += 1

                    self.db.upsert_role_mapping(
                        orig_id,
                        want_name,
                        new_role.id,
                        new_role.name,
                        original_guild_id=host_id,
                        cloned_guild_id=clone_id,
                    )

                    clone_by_id[new_role.id] = new_role
                    self._log(
                        "info",
                        "[🧩] Created role %s",
                        new_role.name,
                    )
                    await self._emit_log(
                        "role_created",
                        f"Created role '{new_role.name}'",
                        guild_id=clone_id,
                        guild_name=getattr(guild, "name", None),
                        extra={"original_role_id": int(orig_id), "clone_role_id": int(new_role.id)},
                    )

                    can_create = len(guild.roles) < self.MAX_ROLES
                    await hooks.item(want_name, "created")
                    continue

                except Exception as e:
                    if "icon" in kwargs or "unicode_emoji" in kwargs:
                        kwargs.pop("icon", None)
                        kwargs.pop("unicode_emoji", None)
                        try:
                            hooks.check()
                            new_role = await guild.create_role(**kwargs)
                            await asyncio.sleep(self._ROLE_OP_DELAY)
                            created += 1
                            self.db.upsert_role_mapping(
                                orig_id, want_name, new_role.id, new_role.name,
                                original_guild_id=host_id, cloned_guild_id=clone_id,
                            )
                            clone_by_id[new_role.id] = new_role
                            self._log(
                                "warning",
                                "[⚠️] Created role '%s' without icon — Discord rejected the icon (%s). "
                                "If the clone server isn't Boost Level 2+, boost it or set CLONE_ROLE_ICONS to false.",
                                want_name,
                                e,
                            )
                            can_create = len(guild.roles) < self.MAX_ROLES
                            await hooks.item(want_name, "created", "Created without its icon, Discord rejected it")
                            continue
                        except Exception as e2:
                            self._log("warning", "[⚠️] Failed creating role %s: %s", want_name, e2)
                            why = str(e2) or type(e2).__name__
                    else:
                        self._log(
                            "warning",
                            "[⚠️] Failed creating role %s: %s",
                            want_name,
                            e,
                        )
                        why = str(e) or type(e).__name__
                    await hooks.item(want_name, "failed", why)
                    continue

            if (
                cloned_role
                and (not cloned_role.is_default())
                and (not cloned_role.managed)
                and below_my_top(cloned_role, me_top)
            ):

                if not update_roles:
                    self._log(
                        "debug",
                        "[🧩] UPDATE_ROLES=False for clone %s; skipping updates for role %s (%d)",
                        clone_id,
                        cloned_role.name,
                        cloned_role.id,
                    )
                    await hooks.item(want_name, outcome if outcome == "created" else "skipped",
                                     None if outcome == "created" else "Role updates are off")
                    continue

                changes: list[str] = []

                if cloned_role.name != want_name:
                    changes.append(f"name: {cloned_role.name!r} -> {want_name!r}")

                if mirror_permissions and (
                    cloned_role.permissions.value != want_perms.value
                ):
                    added_flags, removed_flags = self._perm_diff(
                        cloned_role.permissions,
                        want_perms,
                    )
                    parts = []
                    if added_flags:
                        parts.append("+" + ",".join(added_flags))
                    if removed_flags:
                        parts.append("-" + ",".join(removed_flags))
                    changes.append(
                        "perms: "
                        + (" ".join(parts) if parts else "(bitfield change)")
                        + f" ({cloned_role.permissions.value} -> {want_perms.value})"
                    )
                elif (not self.mirror_permissions) and (
                    cloned_role.permissions.value != want_perms.value
                ):
                    logger.debug(
                        "[🧩] permissions differ for %s (%d) but MIRROR_ROLE_PERMISSIONS=False; skipping perms update.",
                        cloned_role.name,
                        cloned_role.id,
                    )

                old_color = self._color_int(cloned_role.color)
                new_color = self._color_int(want_color)
                if old_color != new_color:
                    changes.append(f"color: #{old_color:06X} -> #{new_color:06X}")

                # Gradient/holographic styles: only diff secondary/tertiary when the
                # clone supports them, or Discord strips them and every sync re-edits.
                if (
                    clone_role_styles
                    and styles_supported
                    and isinstance(want_colors, dict)
                ):
                    cur_cols = getattr(cloned_role, "colours", None)
                    cur_s = getattr(getattr(cur_cols, "secondary", None), "value", None)
                    cur_t = getattr(getattr(cur_cols, "tertiary", None), "value", None)

                    def _opt_int(v):
                        try:
                            return int(v) if v is not None else None
                        except Exception:
                            return None

                    want_s = _opt_int(want_colors.get("secondary_color"))
                    want_t = _opt_int(want_colors.get("tertiary_color"))
                    if (cur_s, cur_t) != (want_s, want_t):
                        changes.append(
                            f"style: secondary {cur_s} -> {want_s}, "
                            f"tertiary {cur_t} -> {want_t}"
                        )

                if cloned_role.hoist != want_hoist:
                    changes.append(f"hoist: {cloned_role.hoist} -> {want_hoist}")

                if cloned_role.mentionable != want_mention:
                    changes.append(
                        f"mentionable: {cloned_role.mentionable} -> {want_mention}"
                    )

                if clone_role_icons:
                    current_emoji = getattr(cloned_role, "unicode_emoji", None)
                    has_icon = getattr(cloned_role, "_icon", None) is not None

                    if want_icon_url and not has_icon:
                        changes.append("icon: added")
                    elif want_unicode_emoji and current_emoji != want_unicode_emoji:
                        changes.append(
                            f"emoji: {current_emoji!r} -> {want_unicode_emoji!r}"
                        )
                    elif not want_icon_url and not want_unicode_emoji and (has_icon or current_emoji):
                        changes.append("icon: removed")

                if changes:
                    self._log(
                        "debug",
                        "[🧩] update details for %s (%d): %s",
                        cloned_role.name,
                        cloned_role.id,
                        "; ".join(changes),
                    )
                    try:
                        kwargs = dict(
                            name=want_name,
                            colour=want_color,
                            hoist=want_hoist,
                            mentionable=want_mention,
                            reason="Copycord role sync",
                        )
                        if mirror_permissions:
                            kwargs["permissions"] = want_perms
                        if clone_role_icons:
                            if want_icon_url:
                                icon_bytes = await self._fetch_icon_bytes(want_icon_url)
                                if icon_bytes:
                                    kwargs["icon"] = icon_bytes
                            elif want_unicode_emoji:
                                kwargs["unicode_emoji"] = want_unicode_emoji
                            elif not want_icon_url and not want_unicode_emoji:
                                kwargs["icon"] = None
                                kwargs["unicode_emoji"] = None

                        if clone_role_styles:
                            colours_obj = self._build_role_colours(want_colors)
                            if colours_obj is not None:
                                # py-cord lets colour= override colours=, so pass only one
                                kwargs.pop("colour", None)
                                kwargs["colours"] = colours_obj

                        hooks.check()
                        await cloned_role.edit(**kwargs)
                        await asyncio.sleep(self._ROLE_OP_DELAY)
                        updated += 1
                        if outcome == "unchanged":
                            outcome = "updated"

                        self.db.upsert_role_mapping(
                            orig_id,
                            want_name,
                            cloned_role.id,
                            cloned_role.name,
                            original_guild_id=host_id,
                            cloned_guild_id=clone_id,
                        )
                        self._log(
                            "info",
                            "[🧩] Updated role %s",
                            cloned_role.name,
                        )
                        await self._emit_log(
                            "role_updated",
                            f"Updated role '{cloned_role.name}' ({'; '.join(changes)})",
                            guild_id=clone_id,
                            guild_name=getattr(guild, "name", None),
                            extra={"clone_role_id": int(cloned_role.id), "changes": changes},
                        )

                    except Exception as e:
                        if "icon" in kwargs or "unicode_emoji" in kwargs:
                            kwargs.pop("icon", None)
                            kwargs.pop("unicode_emoji", None)
                            try:
                                hooks.check()
                                await cloned_role.edit(**kwargs)
                                await asyncio.sleep(self._ROLE_OP_DELAY)
                                updated += 1
                                if outcome == "unchanged":
                                    outcome = "updated"
                                why = "Updated without its icon, Discord rejected it"
                                self._log(
                                    "warning",
                                    "[⚠️] Role '%s' updated without icon — Discord rejected the icon (%s). "
                                    "If the clone server isn't Boost Level 2+, boost it or set CLONE_ROLE_ICONS to false.",
                                    getattr(cloned_role, "name", "?"),
                                    e,
                                )
                            except Exception as e2:
                                self._log(
                                    "warning",
                                    "[⚠️] Failed updating role %s: %s",
                                    getattr(cloned_role, "name", "?"),
                                    e2,
                                )
                                outcome, why = "failed", str(e2) or type(e2).__name__
                        else:
                            self._log(
                                "warning",
                                "[⚠️] Failed updating role %s: %s",
                                getattr(cloned_role, "name", "?"),
                                e,
                            )
                            outcome, why = "failed", str(e) or type(e).__name__
            elif cloned_role is not None and outcome != "created":
                skipped_above(cloned_role)
                outcome, why = "skipped", "Managed or above my top role, I cannot edit it"
            await hooks.item(want_name, outcome, why)
        if above_top and me_top is not None:
            self._log("warning", "[⚠️] Role sync: %s", above_top_note(
                top_tie_report(me_top.id, me_top.name, me_top.position, above_top)))
        rearranged = 0
        if rearrange_roles:
            try:
                rearranged = await self._rearrange_roles(
                    guild=guild,
                    incoming=incoming,
                    host_id=host_id,
                    clone_id=clone_id,
                )
            except Exception as e:
                self._log(
                    "warning",
                    "[⚠️] Role rearrangement failed: %s",
                    e,
                    exc_info=True,
                )

        return deleted, updated, created, rearranged

    async def _fetch_icon_bytes(self, url: str) -> Optional[bytes]:
        """Download a role icon from a URL and return the raw bytes."""
        try:
            async with shared_session().get(url) as resp:
                if resp.status == 200:
                    return await resp.read()
        except Exception as e:
            self._log("debug", "[🧩] Failed to download role icon: %s", e)
        return None

    def _color_int(self, c) -> int:
        try:
            return int(c.value)
        except Exception:
            return int(c)

    def _perm_diff(
        self, before_perm: discord.Permissions, after_perm: discord.Permissions
    ) -> tuple[list[str], list[str]]:
        """Return (added_flags, removed_flags) between two Permissions."""
        added, removed = [], []
        for name, new in after_perm:
            old = getattr(before_perm, name)
            if new and not old:
                added.append(name)
            elif old and not new:
                removed.append(name)
        return added, removed

    def mapped_roles(self, host_id: int | None, clone_id: int) -> Dict[int, int]:
        """{host role id: clone role id} of the role mappings for this host and clone."""
        out: Dict[int, int] = {}
        for r in self.db.get_all_role_mappings():
            row = dict(r)
            if (
                host_id is None
                or int(row.get("original_guild_id") or 0) == int(host_id)
            ) and int(row.get("cloned_guild_id") or 0) == int(clone_id):
                cloned_id = int(row.get("cloned_role_id") or 0)
                if cloned_id:
                    out[int(row["original_role_id"])] = cloned_id
        return out

    @staticmethod
    def role_facts(role) -> Dict:
        """What plan_role_order reads of a discord.Role."""
        return {
            "id": int(role.id),
            "name": role.name,
            "position": int(role.position),
            "managed": bool(role.managed),
            "default": bool(role.is_default()),
        }

    async def _current_role_facts(self, guild) -> Tuple[List[Dict], int, int]:
        """Role facts plus my top role's position and id, read fresh from Discord.

        Cached positions go stale while roles are being created, which makes new
        roles look above mine, so the roles are fetched over REST. The cache is the
        fallback when the fetch fails."""
        me = guild.me
        mine = my_role_ids(me)
        try:
            fetched = await guild.fetch_roles()
        except discord.HTTPException as e:
            self._log("debug", "[🧩] Fetching roles failed (%s); using cached roles", e)
        else:
            ranked = rank_role_facts([self.role_facts(r) for r in fetched], mine)
            if ranked:
                return ranked
            self._log("debug", "[🧩] My roles are not among the fetched roles; using cached roles")
        cached = [self.role_facts(r) for r in guild.roles]
        top = me.top_role
        return rank_role_facts(cached, mine) or (cached, int(top.position), int(top.id))

    async def _edit_role_order(self, guild, plan: Dict) -> None:
        # Count up from 1, not down from the bot role: the plan keeps every
        # position below my top role, so Discord has nothing to refuse.
        await guild.edit_role_positions(
            positions={discord.Object(id=rid): pos for rid, pos in plan["positions"].items()},
            reason="Copycord role order sync",
        )

    async def _rearrange_roles(
        self,
        *,
        guild: discord.Guild,
        incoming: List[Dict],
        host_id: int | None,
        clone_id: int,
    ) -> int:
        """Rearrange cloned roles to match the host's order.

        Only roles below the bot's top role move. Returns the number of roles repositioned.
        """
        me = guild.me
        if not me:
            self._log("warning", "[🧩] Bot member not found; cannot rearrange roles")
            return 0

        bot_role = me.top_role
        if not bot_role:
            self._log("warning", "[🧩] Bot has no top role; cannot rearrange roles")
            return 0

        mapped = self.mapped_roles(host_id, clone_id)
        facts, top_pos, top_id = await self._current_role_facts(guild)
        plan = plan_role_order(incoming, mapped, facts, top_pos, top_id)

        for role in plan["unmovable"]:
            if role["reason"] == "Above my top role":
                self._log(
                    "warning",
                    "[⚠️] Cloned role '%s' is above my role — skipping reorder for it. "
                    "Move it below my role to include it in reordering.",
                    role["name"],
                )
        ties = plan_tie_report(facts, top_id, plan["unmovable"])
        if ties and ties["skipped"]:
            self._log("warning", "[⚠️] Role reorder: %s", above_top_note(ties))

        if not plan["cloned"]:
            self._log("debug", "[🧩] No cloned roles to rearrange")
            return 0

        if not plan["changed"]:
            self._log("debug", "[🧩] All roles already in correct positions")
            return 0

        try:
            try:
                await self._edit_role_order(guild, plan)
            except discord.HTTPException as e:
                if getattr(e, "code", None) != 50013:
                    raise
                # Positions can still shift under us; plan once more from a fresh read.
                self._log(
                    "debug",
                    "[🧩] Role reorder rejected (%s); refetching roles and retrying once",
                    e,
                )
                plan = plan_role_order(
                    incoming, mapped, *await self._current_role_facts(guild)
                )
                if not plan["changed"]:
                    return 0
                try:
                    await self._edit_role_order(guild, plan)
                except discord.HTTPException as e2:
                    self._log(
                        "warning",
                        "[⚠️] Failed to rearrange roles after refetching them: %s. "
                        "Make sure my role sits above every cloned role.",
                        e2,
                    )
                    return 0

            self._log(
                "info",
                "[🧩] Rearranged %d roles to match host server order",
                len(plan["positions"]),
            )

            return len(plan["positions"])

        except discord.Forbidden:
            self._log("warning", "[⚠️] Missing permissions to rearrange roles")
            return 0
        except discord.HTTPException as e:
            self._log("warning", "[⚠️] Failed to rearrange roles: %s", e)
            return 0
        except Exception as e:
            self._log(
                "error",
                "[⚠️] Unexpected error rearranging roles: %s",
                e,
                exc_info=True,
            )
            return 0
