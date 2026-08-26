# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


from __future__ import annotations

import asyncio
import time

import discord

from server.token_avatar import avatar_hash_from_url, set_avatar


# Discord's hard limit for a member nickname.
MAX_NICK_LEN = 32


class TokenIdentityManager:
    def __init__(self, *, bot, db, logger, session_provider=None):
        self._bot = bot
        self._db = db
        self._log = logger
        # Used to download an author's avatar before mirroring it.
        self._session_provider = session_provider
        # One lock per mapping so concurrent authors don't race token selection
        # or clobber each other's nickname edits.
        self._locks: dict[str, asyncio.Lock] = {}

        self._bad_tokens: set[str] = set()
        # Tokens whose avatar could not be set this session. Separate from
        # _bad_tokens on purpose: a captcha on a profile edit says nothing
        # about whether the account can still send messages.
        self._avatar_skip: set[str] = set()

    def mark_token_bad(self, token_id) -> None:
        """Bench a token for the rest of this session — Discord revoked it."""
        if token_id is None:
            return
        tid = str(token_id)
        if tid not in self._bad_tokens:
            self._bad_tokens.add(tid)
            self._log.info(
                "[identity] token %s benched for this session (Discord revoked it)",
                tid,
            )

    def is_token_bad(self, token_id) -> bool:
        return str(token_id) in self._bad_tokens

    def clear_bad_tokens(self) -> None:
        """Forget all benched tokens (e.g. after the user re-verifies them)."""
        self._bad_tokens.clear()
        self._avatar_skip.clear()

    @staticmethod
    def _select_token(
        identities: list[dict],
        author_id: str,
        enabled_token_ids: list[str],
    ) -> tuple[str | None, str | None]:
        """Decide which token this author should use."""
        enabled = [str(t) for t in enabled_token_ids]
        author_id = str(author_id)
        if not enabled:
            return None, None

        cur = next(
            (i for i in identities if str(i.get("author_id")) == author_id), None
        )
        prev = str(cur["token_id"]) if cur else None

        if prev and prev in enabled:
            return prev, None

        taken = {
            str(i.get("token_id"))
            for i in identities
            if str(i.get("author_id")) != author_id
        }
        free = [t for t in enabled if t not in taken]
        if not free:

            return None, None

        chosen = free[0]
        reset_prev = prev if (prev and prev != chosen) else None
        return chosen, reset_prev

    async def prepare(
        self,
        *,
        mapping_id,
        cloned_guild_id,
        author_id,
        author_display_name,
        author_role_ids,
        author_avatar_url=None,
        settings: dict,
        tokens: list[dict],
        exclude: set | None = None,
        identity_known: bool = True,
    ) -> tuple[str | None, str | None]:
        """Assign (or keep) this author's token and apply its identity.

        ``identity_known=False`` means the caller has no display name or role
        list for this author, not that the author has neither. Message-edit
        payloads carry no identity fields, so treating absent as empty would
        clear the account's nickname and strip every mirrored role.
        """
        exhausted = (
            "webhook" if settings.get("USER_TOKEN_FALLBACK_WEBHOOK", True) else "skip"
        )
        if not author_id:
            return None, exhausted

        mapping_id = str(mapping_id)
        author_id = str(author_id)
        do_nick = bool(settings.get("USER_TOKEN_STICKY_NICKNAME"))
        do_roles = bool(settings.get("USER_TOKEN_STICKY_ROLES"))
        do_avatar = bool(settings.get("USER_TOKEN_STICKY_AVATAR"))

        skip = {str(t) for t in (exclude or ())} | self._bad_tokens
        token_by_id = {
            str(t.get("token_id")): t
            for t in tokens
            if t.get("token_id") and str(t.get("token_id")) not in skip
        }
        enabled_token_ids = list(token_by_id.keys())
        if not enabled_token_ids:
            return None, exhausted

        async with self._lock_for(mapping_id):
            now = int(time.time())

            # This author's current assignment (indexed PK lookup, O(1)).
            cur = self._db.get_token_identity(mapping_id, author_id)

            if cur is not None and str(cur.get("token_id")) in token_by_id:

                chosen = str(cur["token_id"])
                reset_prev = None
                keep = True

                # Nothing to apply and nothing to compare against: send from
                # the account this author already owns and leave its nickname
                # and roles exactly as they are.
                if not identity_known:
                    return chosen, None
            else:

                identities = self._db.list_token_identities(mapping_id)
                chosen, reset_prev = self._select_token(
                    identities, author_id, enabled_token_ids
                )
                if not chosen:
                    return None, exhausted
                keep = False

            try:
                guild = (
                    self._bot.get_guild(int(cloned_guild_id))
                    if cloned_guild_id
                    else None
                )
            except Exception:
                guild = None

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
            desired_avatar_hash = (
                avatar_hash_from_url(author_avatar_url) if do_avatar else None
            )

            if keep:
                nick_ok = (not do_nick) or (cur.get("applied_nick") == desired_nick)
                roles_ok = (not do_roles) or (
                    list(cur.get("applied_role_ids") or []) == desired_role_ids
                )
                # An unresolvable avatar URL counts as "nothing to do" rather
                # than a change, so a CDN quirk cannot cause a re-upload loop.
                avatar_ok = (
                    (not do_avatar)
                    or not desired_avatar_hash
                    or cur.get("applied_avatar_hash") == desired_avatar_hash
                )
                if nick_ok and roles_ok and avatar_ok:
                    return chosen, None

            applied_avatar_hash = cur.get("applied_avatar_hash") if keep else None
            if do_avatar and desired_avatar_hash != applied_avatar_hash:
                if await self._mirror_avatar(
                    token_by_id.get(chosen) or {}, author_avatar_url, settings
                ):
                    applied_avatar_hash = desired_avatar_hash

            # Without the clone guild cached we can't touch members. The avatar
            # above needed no guild, so persist it before bailing or it would
            # be re-uploaded on every message.
            if guild is None:
                if applied_avatar_hash != (cur.get("applied_avatar_hash") if keep else None):
                    self._persist(
                        mapping_id, author_id, chosen, cloned_guild_id,
                        cur.get("applied_nick") if keep else None,
                        cur.get("applied_role_ids") or [] if keep else [],
                        int(cur["assigned_at"]) if keep and cur.get("assigned_at") else now,
                        applied_avatar_hash,
                    )
                return chosen, None

            applied_nick = cur.get("applied_nick") if keep else None
            applied_role_ids = list(cur.get("applied_role_ids") or []) if keep else []

            # Reset the account we're swapping away from (bad/old token).
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

            assigned_at = (
                int(cur["assigned_at"]) if keep and cur.get("assigned_at") else now
            )
            self._persist(
                mapping_id,
                author_id,
                chosen,
                cloned_guild_id,
                applied_nick,
                applied_role_ids,
                assigned_at,
                applied_avatar_hash,
            )

            return chosen, None

    def _persist(
        self, mapping_id, author_id, token_id, cloned_guild_id,
        applied_nick, applied_role_ids, assigned_at, applied_avatar_hash,
    ) -> None:
        try:
            self._db.upsert_token_identity(
                mapping_id=mapping_id,
                author_id=author_id,
                token_id=token_id,
                cloned_guild_id=int(cloned_guild_id or 0),
                applied_nick=applied_nick,
                applied_role_ids=applied_role_ids,
                assigned_at=assigned_at,
                applied_avatar_hash=applied_avatar_hash,
            )
        except Exception:
            self._log.debug("[identity] failed to persist assignment", exc_info=True)

    async def _mirror_avatar(self, token_row: dict, avatar_url, settings: dict) -> bool:
        """Put this author's picture on the account sending as them.

        Only called when the hash actually moved, so a stable author costs
        nothing. Failure is not retried here -- the hash stays unrecorded, so
        the next message tries again.
        """
        token_id = str(token_row.get("token_id") or "")
        if token_id in self._avatar_skip:
            return False

        token = (token_row.get("token_value") or "").strip()
        if not token or not avatar_url:
            return False
        if self._session_provider is None:
            self._log.debug("[avatar] no HTTP session available; skipping")
            return False
        try:
            ok = await set_avatar(
                token,
                str(avatar_url),
                http_session=self._session_provider(),
                logger=self._log,
                use_proxy=bool(settings.get("USER_TOKEN_USE_PROXIES", False)),
            )
        except Exception:
            self._log.debug("[avatar] mirror failed", exc_info=True)
            ok = False

        if not ok and token_id:
            # Whatever the reason -- captcha, rate limit, a refused gateway --
            # it will not resolve by trying again on the next message, and the
            # hash is never recorded so we would retry forever.
            self._avatar_skip.add(token_id)
            self._log.info(
                "[avatar] token %s skipped for this session after a failed "
                "profile change",
                token_id,
            )
        return ok

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

            applied_nick = desired_nick

        if do_roles:
            desired_ids = {r.id for r in desired_roles}
            have_ids = {r.id for r in getattr(member, "roles", [])}

            to_add = [r for r in desired_roles if r.id not in have_ids]
            to_remove_ids = (
                set(int(x) for x in (prev_applied_role_ids or [])) - desired_ids
            )
            to_remove = []
            for rid in to_remove_ids:
                r = guild.get_role(int(rid))
                if (
                    r is not None
                    and r in member.roles
                    and self._role_assignable(guild, r)
                ):
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
        self, guild, member, *, applied_nick=None, applied_role_ids=None
    ) -> None:
        """Return a token account to a clean slate when its identity changes.

        Called when an author rotates to a different token (the previous token
        must be cleared) or when the feature is disabled. When we mirrored a
        nickname onto the account we clear it; when we mirrored roles onto it we
        strip **every** role the bot is able to remove — not just the ones we
        recorded — so nothing lingers on the previous identity even if the exact
        applied-role list was lost or another author had used the account. Roles
        are only touched when we had actually applied roles, so a nickname-only
        mapping never strips roles the account legitimately holds.
        """

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

        # (managed roles, @everyone, and roles above the bot can't be removed and

        if not applied_role_ids:
            return
        to_remove = [
            r for r in getattr(member, "roles", []) if self._role_assignable(guild, r)
        ]
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
