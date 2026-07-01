# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Sticky-author identity manager.

Under the ``sticky_author`` user-token strategy this makes the token account
that posts a cloned message *look like* the host author: the server bot sets the
account's nickname to the host author's display name and grants it the cloned
roles that correspond to the author's host roles. Each author holds a token for
``USER_TOKEN_IDENTITY_TTL_MIN`` minutes; after that they rotate to a currently
unused token and the previous account's nickname/roles are reset.

Only the py-cord bot (``self.bot``) can edit members, so this manager owns token
*selection* as well — it hands the chosen ``token_id`` back to the caller, which
forces ``UserTokenSender.send(forced_token_id=...)`` to post from that exact
account. Selection state is persisted in ``mapping_token_identities``.
"""

from __future__ import annotations

import asyncio
import time

import discord


# Discord's hard limit for a member nickname.
MAX_NICK_LEN = 32


class TokenIdentityManager:
    def __init__(self, *, bot, db, logger):
        self._bot = bot
        self._db = db
        self._log = logger
        # One lock per mapping so concurrent authors don't race token selection
        # or clobber each other's nickname edits.
        self._locks: dict[str, asyncio.Lock] = {}

    # ── selection (pure, unit-testable) ──────────────────────────────────────

    @staticmethod
    def _select_token(
        identities: list[dict],
        author_id: str,
        enabled_token_ids: list[str],
        ttl_seconds: int,
        now: int,
    ) -> tuple[str | None, str | None]:
        """Decide which token this author should use.

        Returns ``(chosen_token_id, reset_prev_token_id)`` where the second is the
        author's previous token to reset when a real swap occurs (else None).
        Pure function of the persisted state so it can be tested without a bot.
        """
        enabled = [str(t) for t in enabled_token_ids]
        if not enabled:
            return None, None
        author_id = str(author_id)

        def _live(ident: dict) -> bool:
            # ttl<=0 means "hold indefinitely" (never rotate on time).
            if ttl_seconds <= 0:
                return True
            return (now - int(ident.get("assigned_at") or 0)) < ttl_seconds

        cur = next(
            (i for i in identities if str(i.get("author_id")) == author_id), None
        )
        prev = str(cur["token_id"]) if cur else None

        # 1) Author still holds a live, still-enabled token → keep it.
        if cur and str(cur["token_id"]) in enabled and _live(cur):
            return str(cur["token_id"]), None

        # 2) Tokens currently held by OTHER authors within their TTL.
        counts = {t: 0 for t in enabled}
        active: set[str] = set()
        for i in identities:
            if str(i.get("author_id")) == author_id:
                continue
            tid = str(i.get("token_id"))
            if tid in counts and _live(i):
                counts[tid] += 1
                active.add(tid)

        # 3) Prefer a token not held by another active author (and not the one we
        #    are rotating away from).
        free = [t for t in enabled if t not in active and t != prev]
        if free:
            chosen = min(free, key=lambda t: (counts[t], enabled.index(t)))
        elif prev and prev in enabled:
            # None free → remain on the current token (no reset).
            chosen = prev
        else:
            # All busy and no current assignment → share the least-used token.
            chosen = min(enabled, key=lambda t: (counts[t], enabled.index(t)))

        reset_prev = prev if (prev and prev != chosen) else None
        return chosen, reset_prev

    # ── public API ───────────────────────────────────────────────────────────

    async def prepare(
        self,
        *,
        mapping_id,
        cloned_guild_id,
        author_id,
        author_display_name,
        author_role_ids,
        settings: dict,
        tokens: list[dict],
    ) -> str | None:
        """Select/rotate the token for this author and apply its identity.

        Returns the chosen ``token_id`` (to force the sender to use it), or None
        if no token could be chosen. Never raises — identity failures are logged
        and the send proceeds regardless.
        """
        if not author_id:
            return None

        mapping_id = str(mapping_id)
        author_id = str(author_id)
        do_nick = bool(settings.get("USER_TOKEN_STICKY_NICKNAME"))
        do_roles = bool(settings.get("USER_TOKEN_STICKY_ROLES"))
        try:
            ttl_seconds = max(0, int(settings.get("USER_TOKEN_IDENTITY_TTL_MIN") or 0)) * 60
        except Exception:
            ttl_seconds = 0

        token_by_id = {str(t.get("token_id")): t for t in tokens if t.get("token_id")}
        enabled_token_ids = list(token_by_id.keys())
        if not enabled_token_ids:
            return None

        async with self._lock_for(mapping_id):
            now = int(time.time())

            # This author's current assignment (indexed PK lookup, O(1)).
            cur = self._db.get_token_identity(mapping_id, author_id)

            try:
                guild = (
                    self._bot.get_guild(int(cloned_guild_id))
                    if cloned_guild_id
                    else None
                )
            except Exception:
                guild = None

            # Desired identity — computed cheaply from cache + DB (no member,
            # no API). Recomputed each call so role-mapping changes are picked up.
            desired_nick = (
                str(author_display_name)[:MAX_NICK_LEN]
                if (do_nick and author_display_name)
                else None
            )
            desired_roles = (
                self._desired_clone_roles(guild, author_role_ids)
                if (do_roles and guild is not None)
                else []
            )
            desired_role_ids = sorted({r.id for r in desired_roles})

            live_keep = (
                cur is not None
                and str(cur.get("token_id")) in token_by_id
                and (
                    ttl_seconds <= 0
                    or (now - int(cur.get("assigned_at") or 0)) < ttl_seconds
                )
            )

            if live_keep:
                chosen = str(cur["token_id"])
                keep = True
                reset_prev = None
                # Fast path: the account already carries the identity we want →
                # no member resolution, no edits, no write.
                nick_ok = (not do_nick) or (cur.get("applied_nick") == desired_nick)
                roles_ok = (not do_roles) or (
                    list(cur.get("applied_role_ids") or []) == desired_role_ids
                )
                if nick_ok and roles_ok:
                    return chosen
            else:
                # Only the (rarer) rotation path needs the full assignment set.
                identities = self._db.list_token_identities(mapping_id)
                chosen, reset_prev = self._select_token(
                    identities, author_id, enabled_token_ids, ttl_seconds, now
                )
                if not chosen:
                    return None
                keep = cur is not None and str(cur.get("token_id")) == chosen

            # Without the clone guild cached we can't touch members; still force
            # the chosen token for the send, but don't churn the DB.
            if guild is None:
                return chosen

            applied_nick = cur.get("applied_nick") if keep else None
            applied_role_ids = list(cur.get("applied_role_ids") or []) if keep else []

            # Reset the account we're rotating away from.
            if reset_prev and cur is not None:
                try:
                    prev_row = self._db.get_mapping_token(reset_prev)
                    prev_member = await self._resolve_member(
                        guild, (prev_row or {}).get("user_id")
                    )
                    if prev_member is not None:
                        await self._reset_identity(
                            guild,
                            prev_member,
                            applied_nick=cur.get("applied_nick"),
                            applied_role_ids=cur.get("applied_role_ids") or [],
                        )
                except Exception:
                    self._log.debug(
                        "[identity] failed to reset previous token %s",
                        reset_prev,
                        exc_info=True,
                    )

            # Apply identity to the chosen account.
            chosen_tok = token_by_id.get(chosen) or {}
            member = await self._resolve_member(guild, chosen_tok.get("user_id"))
            if member is not None:
                try:
                    applied_nick, applied_role_ids = await self._apply_identity(
                        guild,
                        member,
                        desired_nick=desired_nick,
                        desired_roles=desired_roles,
                        do_nick=do_nick,
                        do_roles=do_roles,
                        prev_applied_role_ids=(
                            cur.get("applied_role_ids") or [] if keep else []
                        ),
                    )
                except Exception:
                    self._log.debug(
                        "[identity] failed to apply identity for token %s",
                        chosen,
                        exc_info=True,
                    )

            assigned_at = int(cur["assigned_at"]) if keep else now
            try:
                self._db.upsert_token_identity(
                    mapping_id=mapping_id,
                    author_id=author_id,
                    token_id=chosen,
                    cloned_guild_id=int(cloned_guild_id or 0),
                    applied_nick=applied_nick,
                    applied_role_ids=applied_role_ids,
                    assigned_at=assigned_at,
                )
            except Exception:
                self._log.debug("[identity] failed to persist assignment", exc_info=True)

            return chosen

    async def reset_mapping(self, mapping_id) -> None:
        """Clear every applied nickname/role for a mapping and drop its state.

        Used when the feature is turned off for a mapping (or the mapping is
        being removed). Best-effort — permission failures are ignored.
        """
        mapping_id = str(mapping_id)
        async with self._lock_for(mapping_id):
            try:
                identities = self._db.list_token_identities(mapping_id)
            except Exception:
                identities = []
            for ident in identities:
                try:
                    gid = int(ident.get("cloned_guild_id") or 0)
                    guild = self._bot.get_guild(gid) if gid else None
                    if guild is None:
                        continue
                    prev_row = self._db.get_mapping_token(str(ident.get("token_id")))
                    member = await self._resolve_member(
                        guild, (prev_row or {}).get("user_id")
                    )
                    if member is not None:
                        await self._reset_identity(
                            guild,
                            member,
                            applied_nick=ident.get("applied_nick"),
                            applied_role_ids=ident.get("applied_role_ids") or [],
                        )
                except Exception:
                    self._log.debug(
                        "[identity] reset_mapping entry failed", exc_info=True
                    )
            try:
                self._db.clear_token_identities(mapping_id)
            except Exception:
                pass

    # ── identity application (bot side) ──────────────────────────────────────

    async def _apply_identity(
        self,
        guild,
        member,
        *,
        desired_nick,
        desired_roles,
        do_nick: bool,
        do_roles: bool,
        prev_applied_role_ids,
    ) -> tuple[str | None, list[int]]:
        applied_nick = None
        applied_role_ids = list(int(x) for x in (prev_applied_role_ids or []))

        if do_nick and desired_nick:
            if (member.nick or "") != desired_nick:
                try:
                    await member.edit(
                        nick=desired_nick, reason="Copycord sticky identity"
                    )
                except discord.Forbidden:
                    self._log.warning(
                        "[identity] Missing permission to set nickname for %s in guild %s",
                        getattr(member, "id", "?"),
                        guild.id,
                    )
                except discord.HTTPException:
                    self._log.debug(
                        "[identity] nickname edit failed for %s",
                        getattr(member, "id", "?"),
                        exc_info=True,
                    )
            # Record the intent even if the edit was blocked, so a permission /
            # hierarchy failure is not re-attempted on every subsequent message.
            applied_nick = desired_nick

        if do_roles:
            desired_ids = {r.id for r in desired_roles}
            have_ids = {r.id for r in getattr(member, "roles", [])}

            to_add = [r for r in desired_roles if r.id not in have_ids]
            to_remove_ids = set(int(x) for x in (prev_applied_role_ids or [])) - desired_ids
            to_remove = []
            for rid in to_remove_ids:
                r = guild.get_role(int(rid))
                if r is not None and r in member.roles and self._role_assignable(guild, r):
                    to_remove.append(r)

            if to_add:
                try:
                    await member.add_roles(*to_add, reason="Copycord sticky identity")
                except discord.Forbidden:
                    self._log.warning(
                        "[identity] Missing permission to add roles for %s in guild %s",
                        getattr(member, "id", "?"),
                        guild.id,
                    )
                except discord.HTTPException:
                    self._log.debug(
                        "[identity] add_roles failed for %s",
                        getattr(member, "id", "?"),
                        exc_info=True,
                    )
            if to_remove:
                try:
                    await member.remove_roles(
                        *to_remove, reason="Copycord sticky identity"
                    )
                except (discord.Forbidden, discord.HTTPException):
                    self._log.debug(
                        "[identity] remove_roles failed for %s",
                        getattr(member, "id", "?"),
                        exc_info=True,
                    )

            applied_role_ids = sorted(desired_ids)

        return applied_nick, applied_role_ids

    async def _reset_identity(
        self, guild, member, *, applied_nick, applied_role_ids
    ) -> None:
        if applied_nick is not None and member.nick:
            try:
                await member.edit(nick=None, reason="Copycord sticky identity reset")
            except discord.Forbidden:
                self._log.warning(
                    "[identity] Missing permission to clear nickname for %s in guild %s",
                    getattr(member, "id", "?"),
                    guild.id,
                )
            except discord.HTTPException:
                self._log.debug(
                    "[identity] nickname reset failed for %s",
                    getattr(member, "id", "?"),
                    exc_info=True,
                )

        to_remove = []
        for rid in applied_role_ids or []:
            r = guild.get_role(int(rid))
            if r is not None and r in member.roles:
                to_remove.append(r)
        if to_remove:
            try:
                await member.remove_roles(
                    *to_remove, reason="Copycord sticky identity reset"
                )
            except discord.Forbidden:
                self._log.warning(
                    "[identity] Missing permission to remove roles for %s in guild %s",
                    getattr(member, "id", "?"),
                    guild.id,
                )
            except discord.HTTPException:
                self._log.debug(
                    "[identity] role reset failed for %s",
                    getattr(member, "id", "?"),
                    exc_info=True,
                )

    # ── helpers ──────────────────────────────────────────────────────────────

    def _lock_for(self, mapping_id: str) -> asyncio.Lock:
        lock = self._locks.get(mapping_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[mapping_id] = lock
        return lock

    async def _resolve_member(self, guild, user_id):
        if not guild or not user_id:
            return None
        try:
            uid = int(user_id)
        except Exception:
            return None
        m = guild.get_member(uid)
        if m is not None:
            return m
        try:
            return await guild.fetch_member(uid)
        except Exception:
            return None

    def _desired_clone_roles(self, guild, author_role_ids) -> list:
        out = []
        seen: set[int] = set()
        for hrid in author_role_ids or []:
            try:
                hrid = int(hrid)
            except Exception:
                continue
            try:
                row = self._db.get_role_mapping_for_clone(hrid, int(guild.id))
            except Exception:
                row = None
            if not row:
                continue
            try:
                crid = int(row["cloned_role_id"])
            except Exception:
                continue
            if not crid or crid in seen:
                continue
            role = guild.get_role(crid)
            if role is not None and self._role_assignable(guild, role):
                seen.add(crid)
                out.append(role)
        return out

    @staticmethod
    def _role_assignable(guild, role) -> bool:
        me = getattr(guild, "me", None)
        if role is None or me is None:
            return False
        if role.managed or role.is_default():
            return False
        try:
            return role < me.top_role
        except Exception:
            return False
