# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


import discord
from discord.ext import commands
from discord import (
    Option,
    Embed,
    Color,
)
from discord.errors import NotFound, Forbidden, HTTPException
from discord import errors as discord_errors
from datetime import datetime, timezone
import time
import logging
import re
import json
import io
import zipfile
import sqlite3
from typing import Optional
from copycord.common.config import Config
from copycord.common.db import DBManager
from copycord.server.rate_limiter import RateLimitManager
from copycord.server import search as message_search

logger = logging.getLogger("server")

config = Config(logger=logger)

_db_boot = DBManager(config.DB_PATH)
try:
    _ids = [
        int(m["cloned_guild_id"])
        for m in _db_boot.list_guild_mappings()
        if m.get("cloned_guild_id")
    ]
except Exception:
    _ids = []
finally:
    # The slash-command decorators below read GUILD_IDS at import, so this read stays eager.
    _db_boot.close()

GUILD_IDS: list[int] = sorted({int(g) for g in (_ids or []) if g})


def guild_scoped_slash_command(*dargs, **dkwargs):
    """Wrapper that always scopes slash commands to our mapped clone guilds."""
    dkwargs.setdefault("guild_ids", GUILD_IDS)
    return commands.slash_command(*dargs, **dkwargs)


def _format_discord_timestamp(value) -> str:
    """Format a timestamp as Discord markup: <t:TIMESTAMP:f> (<t:TIMESTAMP:R>).

    Accepts an epoch int/float, a numeric string, or a 'YYYY-MM-DD HH:MM:SS' or ISO-like string.
    """
    if not value:
        return "`?`"

    try:

        if isinstance(value, (int, float)):
            ts = int(value)
        else:
            s = str(value).strip()
            if s.isdigit():
                ts = int(s)
            else:

                try:
                    dt = datetime.fromisoformat(s)
                except ValueError:
                    try:
                        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        return f"`{s}`"

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                ts = int(dt.timestamp())
    except Exception:
        return f"`{value}`"

    return f"<t:{ts}:f> (<t:{ts}:R>)"


async def _search_author_autocomplete(actx: discord.AutocompleteContext):
    """Authors of this clone server's cloned messages. Autocomplete skips cog_check, so the
    COMMAND_USERS gate is repeated here; anyone else gets no names."""
    cog = getattr(actx.command, "cog", None)
    inter = actx.interaction
    if cog is None or inter is None or inter.guild_id is None:
        return []
    user = getattr(inter, "user", None)
    if user is None or user.id not in cog.allowed_users:
        return []
    try:
        return message_search.author_choices(cog.db, int(inter.guild_id), actx.value or "")
    except Exception:
        logger.exception("[search] author autocomplete failed")
        return []


class CloneCommands(commands.Cog):
    """Slash commands for the clone bot, restricted to allowed users."""

    role_mention_group = discord.SlashCommandGroup(
        "role_mention",
        "Manage role mentions for cloned messages in THIS server.",
        guild_ids=GUILD_IDS,
    )

    rewrite_group = discord.SlashCommandGroup(
        "rewrite",
        "Manage word/phrase rewrites for this clone mapping.",
        guild_ids=GUILD_IDS,
    )

    channel_webhook_group = discord.SlashCommandGroup(
        "channel_webhook",
        "Manage custom webhook identity for cloned messages in specific channels.",
        guild_ids=GUILD_IDS,
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = DBManager(config.DB_PATH)
        self.ratelimit = RateLimitManager()
        self.start_time = time.time()
        self.allowed_users = getattr(config, "COMMAND_USERS", []) or []

    async def cog_check(self, ctx: commands.Context):
        """Only users whose ID is set in config may run commands. Logs once per executed command, skipping the bare group shell."""
        cmd = ctx.command
        guild_name = ctx.guild.name if ctx.guild else "Unknown"

        if ctx.user.id not in self.allowed_users:
            await ctx.respond(
                "You are not authorized to use this command.", ephemeral=True
            )
            logger.warning(
                f"[⚠️] Unauthorized access: {ctx.user.name} ({ctx.user.id}) attempted to run "
                f"command '{cmd.name if cmd else 'unknown'}' in {guild_name}."
            )
            return False

        if isinstance(cmd, discord.SlashCommandGroup):
            return True

        cmd_name = getattr(cmd, "qualified_name", cmd.name if cmd else "unknown")

        logger.info(
            f"[⚡] {ctx.user.name} ({ctx.user.id}) executed the '{cmd_name}' command in {guild_name}."
        )
        return True

    def _refresh_command_guilds(self) -> list[int]:
        """Recompute which clone guilds get our slash commands and set each command's guild_ids. Returns the new list."""
        new_ids = []
        try:
            mappings = self.db.list_guild_mappings()
            for m in mappings:
                raw = m.get("cloned_guild_id")
                if raw:
                    try:
                        new_ids.append(int(raw))
                    except (ValueError, TypeError):
                        pass
            new_ids = sorted(set(new_ids))
            logger.debug(
                "[commands] Refreshed command guilds: %d mapping(s), guild_ids=%s",
                len(mappings),
                new_ids,
            )
        except Exception:
            logger.exception("[commands] Failed to refresh command guilds")

        for cmd in self.bot.application_commands:
            if getattr(cmd, "guild_ids", None) is not None:
                cmd.guild_ids = new_ids

        return new_ids

    @commands.Cog.listener()
    async def on_application_command_error(self, interaction, error):
        """Unwrap ApplicationCommandInvokeError, ignore CheckFailure to avoid log spam, and log everything else with a traceback."""
        orig = getattr(error, "original", None)
        err = orig or error

        if isinstance(err, (commands.CheckFailure, discord_errors.CheckFailure)):
            return

        cmd = interaction.command.name if interaction.command else "<unknown>"
        logger.exception(f"Error in command '{cmd}':", exc_info=err)

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.allowed_users:
            logger.warning(
                "[⚠️] No command users configured. Slash commands will not work."
            )
        else:
            logger.debug(
                "[⚙️] Commands permissions set for users: %s",
                self.allowed_users,
            )

        try:
            new_ids = self._refresh_command_guilds()
            await self.bot.sync_commands()
            logger.debug("[✅] Slash commands synced for guilds: %s", new_ids)
        except Exception:
            logger.exception("Slash command sync failed")

    def _ok_embed(
        self,
        title_or_desc: str | None = None,
        description: str | None = None,
        *,
        fields=None,
        color=discord.Color.blurple(),
        footer: str | None = None,
        show_timestamp: bool = True,
    ) -> discord.Embed:
        """Build a standard success/info embed."""
        if description is None:
            if title_or_desc is None:
                raise ValueError("description is required")
            title = None
            description = title_or_desc
        else:

            title = title_or_desc

        e = discord.Embed(title=title, description=description, color=color)

        if show_timestamp:
            from datetime import datetime, timezone

            e.timestamp = datetime.now(timezone.utc)

        if fields:
            for name, value, inline in fields:
                e.add_field(name=name, value=value, inline=inline)

        if footer:
            e.set_footer(text=footer)

        return e

    def _err_embed(self, title: str, description: str):
        return discord.Embed(
            title=title,
            description=description,
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
        )

    @guild_scoped_slash_command(
        name="ping_server",
        description="Show server latency and server information.",
    )
    async def ping(self, ctx: discord.ApplicationContext):
        """Responds with bot latency, server name, member count, and uptime."""
        latency_ms = self.bot.latency * 1000
        uptime_seconds = time.time() - self.start_time
        hours, remainder = divmod(int(uptime_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{hours}h {minutes}m {seconds}s"
        guild = ctx.guild

        embed = discord.Embed(title="📡 Pong! (Server)", timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Latency", value=f"{latency_ms:.2f} ms", inline=True)
        embed.add_field(name="Server", value=guild.name, inline=True)
        embed.add_field(name="Members", value=str(guild.member_count), inline=True)
        embed.add_field(name="Uptime", value=uptime_str, inline=True)

        await ctx.respond(embed=embed, ephemeral=True)

    @guild_scoped_slash_command(
        name="ping_client",
        description="Show client latency and server information.",
    )
    async def ping_client(self, ctx: discord.ApplicationContext):
        """Responds with gateway latency, round‑trip time, client uptime, and timestamps."""
        await ctx.defer(ephemeral=True)

        server_ts = datetime.now(timezone.utc).timestamp()
        resp = await self.bot.ws_manager.request(
            {"type": "ping", "data": {"timestamp": server_ts}}
        )

        if not resp or "data" not in resp:
            return await ctx.followup.send(
                "No response from client (timed out or error)", ephemeral=True
            )

        d = resp["data"]
        ws_latency_ms = (d.get("discord_ws_latency_s") or 0) * 1000
        round_trip_ms = (d.get("round_trip_seconds") or 0) * 1000
        client_start = datetime.fromisoformat(d.get("client_start_time"))
        uptime_delta = datetime.now(timezone.utc) - client_start
        hours, rem = divmod(int(uptime_delta.total_seconds()), 3600)
        minutes, sec = divmod(rem, 60)
        uptime_str = f"{hours}h {minutes}m {sec}s"

        embed = discord.Embed(title="📡 Pong! (Client)", timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Latency", value=f"{ws_latency_ms:.2f} ms", inline=True)
        embed.add_field(
            name="Round‑Trip Time", value=f"{round_trip_ms:.2f} ms", inline=True
        )
        embed.add_field(name="Client Uptime", value=uptime_str, inline=True)

        await ctx.followup.send(embed=embed, ephemeral=True)

    @guild_scoped_slash_command(
        name="search",
        description="Find cloned messages by original author, channel, date or attachments.",
    )
    async def search(
        self,
        ctx: discord.ApplicationContext,
        author: str = Option(
            str,
            description="Original author of the message",
            required=False,
            default=None,
            autocomplete=_search_author_autocomplete,
        ),
        channel: discord.abc.GuildChannel = Option(
            discord.abc.GuildChannel,
            description="Only messages in this channel or thread",
            required=False,
            default=None,
            channel_types=[
                discord.ChannelType.text,
                discord.ChannelType.news,
                discord.ChannelType.public_thread,
                discord.ChannelType.private_thread,
                discord.ChannelType.news_thread,
            ],
        ),
        after: str = Option(
            str,
            description="Sent on or after: YYYY-MM-DD, 7d, 24h, today or yesterday (UTC)",
            required=False,
            default=None,
        ),
        before: str = Option(
            str,
            description="Sent on or before: YYYY-MM-DD, 7d, 24h, today or yesterday (UTC)",
            required=False,
            default=None,
        ),
        has: str = Option(
            str,
            description="Only messages with attachments or embeds",
            required=False,
            default=None,
            choices=list(message_search.HAS_CHOICES),
        ),
    ):
        """Pages of cloned messages matching the filters, newest first, with jump links."""
        await message_search.run_search(
            self.db, ctx, author=author, channel=channel, after=after, before=before, has=has
        )

    @guild_scoped_slash_command(
        name="block_add",
        description="Add or remove a keyword from THIS clone servers block list.",
    )
    async def block_add(
        self,
        ctx: discord.ApplicationContext,
        keyword: str = Option(
            description="Keyword to block (toggles for this clone/source pair)",
            required=True,
        ),
    ):
        guild = ctx.guild
        if not guild:
            return await ctx.respond(
                "This command must be run inside a server.", ephemeral=True
            )

        mapping_row = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping_row:
            return await ctx.respond(
                "This server isn't mapped to a source guild, so I can't scope the block.",
                ephemeral=True,
            )

        orig_id = int(mapping_row["original_guild_id"])
        clone_id = int(mapping_row["cloned_guild_id"])

        changed, action = self.db.toggle_blocked_keyword(
            keyword,
            original_guild_id=orig_id,
            cloned_guild_id=clone_id,
        )

        if not changed:
            return await ctx.respond(f"⚠️ Couldn't toggle `{keyword}`.", ephemeral=True)

        try:
            server = getattr(self.bot, "server", None)
            if server and hasattr(server, "_clear_blocked_keywords_cache_async"):
                await server._clear_blocked_keywords_cache_async()
                logger.debug(
                    "[block_add] Cleared keywords cache after %s keyword '%s' (orig=%s, clone=%s)",
                    action,
                    keyword,
                    orig_id,
                    clone_id,
                )
        except Exception:
            logger.exception("[block_add] Failed to clear keywords cache")

        emoji = "✅" if action == "added" else "🗑️"
        await ctx.respond(
            f"{emoji} `{keyword}` {action} for this clone server.",
            ephemeral=True,
        )

    @guild_scoped_slash_command(
        name="block_list",
        description="List this clone servers blocked keywords.",
    )
    async def block_list(self, ctx: discord.ApplicationContext):
        guild = ctx.guild
        if not guild:
            return await ctx.respond(
                "This command must be run inside a server.", ephemeral=True
            )

        mapping_row = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping_row:
            return await ctx.respond(
                "This server isn't mapped to a source guild, so I can't find its block list.",
                ephemeral=True,
            )

        orig_id = int(mapping_row["original_guild_id"])

        kws = self.db.get_blocked_keywords_for_origin(orig_id)
        if not kws:
            return await ctx.respond(
                "📋 Your block list for this clone server is empty.",
                ephemeral=True,
            )

        formatted = "\n".join(f"• `{kw}`" for kw in kws)
        await ctx.respond(
            f"📋 **Blocked keywords for this clone server:**\n{formatted}",
            ephemeral=True,
        )

    @guild_scoped_slash_command(
        name="onjoin_dm",
        description="Toggle DM notifications to you when someone joins the given server ID",
    )
    async def onjoin_dm(
        self,
        ctx: discord.ApplicationContext,
        server_id: str = Option(str, "Guild/server ID to watch", required=True),
    ):
        await ctx.defer(ephemeral=True)

        try:
            gid = int(server_id)
        except ValueError:
            return await ctx.followup.send(
                embed=Embed(
                    title="Invalid server ID",
                    description="Please pass a numeric guild ID.",
                    color=Color.red(),
                ),
                ephemeral=True,
            )

        if self.db.has_onjoin_subscription(gid, ctx.user.id):
            self.db.remove_onjoin_subscription(gid, ctx.user.id)
            title = "On-Join DM Disabled"
            desc = f"You will **no longer** receive a DM when someone joins **{gid}**."
            color = Color.orange()
        else:
            self.db.add_onjoin_subscription(gid, ctx.user.id)
            title = "On-Join DM Enabled"
            desc = f"You will receive a DM when someone joins **{gid}**."
            color = Color.green()

        await ctx.followup.send(
            embed=Embed(title=title, description=desc, color=color),
            ephemeral=True,
        )

    @guild_scoped_slash_command(
        name="onjoin_role",
        description="Toggle an on-join role for THIS server (run again to remove).",
    )
    async def onjoin_role_toggle(
        self,
        ctx: discord.ApplicationContext,
        role: discord.Role = Option(discord.Role, "Role to add on join", required=True),
    ):
        t0 = time.perf_counter()
        await ctx.defer(ephemeral=True)
        guild = ctx.guild

        if not guild:
            logger.warning(
                "onjoin_role: no guild context user_id=%s role_id=%s",
                getattr(ctx.user, "id", "unknown"),
                getattr(role, "id", "unknown"),
            )
            return await ctx.followup.send(
                embed=discord.Embed(
                    title="No Guild Context",
                    description="Run this inside a server.",
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )

        if role.managed:
            logger.warning(
                "onjoin_role: reject managed role guild_id=%s role_id=%s",
                guild.id,
                role.id,
            )
            return await ctx.followup.send(
                embed=discord.Embed(
                    title="Managed Role Not Allowed",
                    description="That role is managed by an integration and cannot be assigned.",
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )

        try:
            added = self.db.toggle_onjoin_role(guild.id, role.id, ctx.user.id)
            action = "ADDED" if added else "REMOVED"
            logger.info(
                "onjoin_role: %s guild_id=%s role_id=%s by user_id=%s",
                action,
                guild.id,
                role.id,
                ctx.user.id,
            )
        except Exception:
            logger.exception(
                "onjoin_role: DB toggle failed guild_id=%s role_id=%s user_id=%s",
                guild.id,
                role.id,
                ctx.user.id,
            )
            return await ctx.followup.send(
                embed=discord.Embed(
                    title="Database Error",
                    description="Could not update on-join roles. Try again.",
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )

        title = "On-Join Role Added" if added else "On-Join Role Removed"
        desc = (
            f"{role.mention} will be granted automatically to **new members** on join."
            if added
            else f"{role.mention} will **no longer** be granted on join."
        )
        color = discord.Color.green() if added else discord.Color.orange()

        await ctx.followup.send(
            embed=discord.Embed(title=title, description=desc, color=color),
            ephemeral=True,
        )

        dt = (time.perf_counter() - t0) * 1000
        logger.debug(
            "onjoin_role: finished guild_id=%s role_id=%s in %.1fms",
            guild.id,
            role.id,
            dt,
        )

    @guild_scoped_slash_command(
        name="onjoin_roles",
        description="List or clear the on-join roles for THIS server.",
    )
    async def onjoin_roles_list(
        self,
        ctx: discord.ApplicationContext,
        clear: bool = Option(
            bool,
            "Delete ALL on-join roles for this server",
            required=False,
            default=False,
        ),
    ):
        t0 = time.perf_counter()
        await ctx.defer(ephemeral=True)
        guild = ctx.guild

        if not guild:
            logger.warning(
                "onjoin_roles: no guild context user_id=%s",
                getattr(ctx.user, "id", "unknown"),
            )
            return await ctx.followup.send("Run this inside a server.", ephemeral=True)

        if clear:
            try:
                removed = self.db.clear_onjoin_roles(guild.id)
                logger.warning(
                    "onjoin_roles: cleared %s entries guild_id=%s by user_id=%s",
                    removed,
                    guild.id,
                    ctx.user.id,
                )
            except Exception:
                logger.exception("onjoin_roles: clear failed guild_id=%s", guild.id)
                return await ctx.followup.send(
                    embed=discord.Embed(
                        title="Database Error",
                        description="Could not clear on-join roles.",
                        color=discord.Color.red(),
                    ),
                    ephemeral=True,
                )
            return await ctx.followup.send(
                embed=discord.Embed(
                    title="On-Join Roles Cleared",
                    description=f"Removed **{removed}** entr{'y' if removed == 1 else 'ies'}.",
                    color=discord.Color.orange(),
                ),
                ephemeral=True,
            )

        try:
            role_ids = self.db.get_onjoin_roles(guild.id)
        except Exception:
            logger.exception("onjoin_roles: fetch failed guild_id=%s", guild.id)
            return await ctx.followup.send(
                embed=discord.Embed(
                    title="Database Error",
                    description="Could not load on-join roles.",
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )

        if not role_ids:
            logger.info("onjoin_roles: none configured guild_id=%s", guild.id)
            return await ctx.followup.send(
                embed=discord.Embed(
                    title="On-Join Roles",
                    description="No on-join roles configured yet. Use `/onjoin_role` to add one.",
                    color=discord.Color.blurple(),
                ),
                ephemeral=True,
            )

        lines = []
        missing = 0
        for rid in role_ids:
            r = guild.get_role(rid)
            if r:
                lines.append(f"• <@&{r.id}> (`{r.id}`)")
            else:
                lines.append(f"• (missing role) `{rid}`")
                missing += 1

        logger.info(
            "onjoin_roles: listed %s roles (missing=%s) guild_id=%s",
            len(role_ids),
            missing,
            guild.id,
        )

        await ctx.followup.send(
            embed=discord.Embed(
                title="On-Join Roles",
                description="\n".join(lines),
                color=discord.Color.green(),
            ),
            ephemeral=True,
        )

        dt = (time.perf_counter() - t0) * 1000
        logger.debug("onjoin_roles: finished guild_id=%s in %.1fms", guild.id, dt)

    @guild_scoped_slash_command(
        name="onjoin_sync",
        description="Go through members and add any missing on-join roles.",
    )
    async def onjoin_sync(
        self,
        ctx: discord.ApplicationContext,
        include_bots: bool = Option(
            bool, "Also give on-join roles to bots", required=False, default=False
        ),
        dry_run: bool = Option(
            bool,
            "Show what would change without modifying roles",
            required=False,
            default=False,
        ),
    ):
        t0 = time.perf_counter()
        await ctx.defer(ephemeral=True)
        guild = ctx.guild

        if not guild:
            logger.warning(
                "onjoin_sync: no guild context user_id=%s",
                getattr(ctx.user, "id", "unknown"),
            )
            return await ctx.followup.send("Run this inside a server.", ephemeral=True)

        try:
            role_ids = self.db.get_onjoin_roles(guild.id)
        except Exception:
            logger.exception("onjoin_sync: fetch roles failed guild_id=%s", guild.id)
            return await ctx.followup.send(
                embed=discord.Embed(
                    title="Database Error",
                    description="Could not load on-join roles.",
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )

        roles = [guild.get_role(rid) for rid in role_ids if guild.get_role(rid)]
        if not roles:
            logger.info("onjoin_sync: no assignable roles guild_id=%s", guild.id)
            return await ctx.followup.send(
                embed=discord.Embed(
                    title="No On-Join Roles",
                    description="Use `/onjoin_role` to add at least one role before syncing.",
                    color=discord.Color.orange(),
                ),
                ephemeral=True,
            )

        me = guild.me or guild.get_member(self.bot.user.id)
        if not me or not guild.me.guild_permissions.manage_roles:
            logger.warning("onjoin_sync: missing Manage Roles guild_id=%s", guild.id)
            return await ctx.followup.send(
                embed=discord.Embed(
                    title="Missing Permission",
                    description="I need **Manage Roles** to run this.",
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )

        assignable = [r for r in roles if r < me.top_role and not r.managed]
        skipped_roles = [r for r in roles if r not in assignable]
        if skipped_roles:
            logger.warning(
                "onjoin_sync: skipped %s roles above bot or managed guild_id=%s role_ids=%s",
                len(skipped_roles),
                guild.id,
                [r.id for r in skipped_roles],
            )

        changed_users = 0
        changed_pairs = 0
        failed = 0

        members = list(guild.members)
        total = len(members)
        logger.debug("onjoin_sync: scanning %s members guild_id=%s", total, guild.id)

        for m in members:
            if m.bot and not include_bots:
                continue
            missing = [r for r in assignable if r not in m.roles]
            if not missing:
                continue

            if dry_run:
                changed_pairs += len(missing)
                changed_users += 1
                logger.debug(
                    "onjoin_sync: DRY missing member_id=%s roles=%s",
                    m.id,
                    [r.id for r in missing],
                )
                continue

            try:
                await m.add_roles(*missing, reason="Copycord onjoin_sync")
                changed_pairs += len(missing)
                changed_users += 1
                logger.debug(
                    "onjoin_sync: added member_id=%s roles=%s",
                    m.id,
                    [r.id for r in missing],
                )
            except Exception:
                failed += 1
                logger.exception(
                    "onjoin_sync: add_roles failed member_id=%s roles=%s",
                    m.id,
                    [r.id for r in missing],
                )

        dt = (time.perf_counter() - t0) * 1000
        logger.info(
            "[🎭] Finished role sync in %s: changed_users=%s changed_pairs=%s failed=%s duration_ms=%.1f",
            guild.name,
            changed_users,
            changed_pairs,
            failed,
            dt,
        )

        summary = (
            f"Members updated: **{changed_users}**\n"
            f"Roles granted (total pairs): **{changed_pairs}**\n"
            f"Failed updates: **{failed}**\n"
        )
        if skipped_roles:
            summary += "\n".join(
                [
                    "",
                    "⚠️ The following roles were **not** assignable (managed or above my top role):",
                    *[f"• <@&{r.id}> (`{r.id}`)" for r in skipped_roles],
                ]
            )

        await ctx.followup.send(
            embed=discord.Embed(
                title=("DRY RUN — " if dry_run else "") + "Role Sync Complete",
                description=summary,
                color=discord.Color.green() if not dry_run else discord.Color.blurple(),
            ),
            ephemeral=True,
        )

    @role_mention_group.command(
        name="add",
        description="Add a role to be mentioned at the top of cloned messages.",
    )
    async def role_mention_add(
        self,
        ctx: discord.ApplicationContext,
        role: discord.Role = discord.Option(
            discord.Role,
            description="The role to mention in cloned messages",
            required=True,
        ),
        channel_id: str = discord.Option(
            str,
            description="Cloned channel ID to filter (leave empty for all channels)",
            required=False,
            default="",
        ),
    ):
        """Add a role mention for a channel or globally."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild
        if not guild:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Guild Context",
                    "This command must be run inside a server.",
                ),
                ephemeral=True,
            )

        mapping = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    "This server isn't mapped to a source guild.",
                ),
                ephemeral=True,
            )

        original_guild_id = int(mapping["original_guild_id"])
        cloned_guild_id = int(mapping["cloned_guild_id"])

        cloned_channel_id = None
        if channel_id.strip():
            try:
                cloned_channel_id = int(channel_id.strip())
            except ValueError:
                return await ctx.followup.send(
                    embed=self._err_embed(
                        "Invalid Channel ID",
                        f"`{channel_id}` is not a valid channel ID.",
                    ),
                    ephemeral=True,
                )

        if role.managed:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Managed Role",
                    "That role is managed by an integration and cannot be mentioned.",
                ),
                ephemeral=True,
            )

        added = self.db.add_role_mention(
            original_guild_id=original_guild_id,
            cloned_guild_id=cloned_guild_id,
            cloned_role_id=role.id,
            cloned_channel_id=cloned_channel_id,
        )

        scope = (
            f"channel `{cloned_channel_id}`" if cloned_channel_id else "all channels"
        )

        if added:
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "Role Mention Added",
                    f"{role.mention} will be mentioned at the top of cloned messages from {scope}.",
                    color=discord.Color.green(),
                ),
                ephemeral=True,
            )
        else:
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "Already Configured",
                    f"{role.mention} is already configured for this scope.",
                    color=discord.Color.blurple(),
                ),
                ephemeral=True,
            )

    @role_mention_group.command(
        name="delete",
        description="Delete a role mention configuration by its ID.",
    )
    async def role_mention_delete(
        self,
        ctx: discord.ApplicationContext,
        config_id: str = discord.Option(
            str,
            description="Config ID from /role_mention list (e.g. a1b2c3d4)",
            required=True,
        ),
    ):
        """Delete a role mention by its short config ID."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild
        if not guild:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Guild Context",
                    "This command must be run inside a server.",
                ),
                ephemeral=True,
            )

        mapping = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    "This server isn't mapped to a source guild.",
                ),
                ephemeral=True,
            )

        original_guild_id = int(mapping["original_guild_id"])
        cloned_guild_id = int(mapping["cloned_guild_id"])

        cfg = config_id.strip().lower()
        if not cfg:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Invalid ID",
                    "You must provide a valid config ID from `/role_mention list`.",
                ),
                ephemeral=True,
            )

        removed = self.db.remove_role_mention_by_id(
            original_guild_id=original_guild_id,
            cloned_guild_id=cloned_guild_id,
            role_mention_id=cfg,
        )

        if removed:
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "Role Mention Deleted",
                    f"Configuration `{cfg}` has been removed.",
                    color=discord.Color.orange(),
                ),
                ephemeral=True,
            )
        else:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Not Found",
                    f"No role mention configuration with ID `{cfg}` was found for this clone.",
                ),
                ephemeral=True,
            )

    @role_mention_group.command(
        name="list",
        description="List all role mentions configured for this clone.",
    )
    async def role_mention_list(
        self,
        ctx: discord.ApplicationContext,
    ):
        """List all role mention configurations."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild
        if not guild:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Guild Context",
                    "This command must be run inside a server.",
                ),
                ephemeral=True,
            )

        mapping = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    "This server isn't mapped to a source guild.",
                ),
                ephemeral=True,
            )

        original_guild_id = int(mapping["original_guild_id"])
        cloned_guild_id = int(mapping["cloned_guild_id"])

        mentions = self.db.list_all_role_mentions(
            original_guild_id=original_guild_id,
            cloned_guild_id=cloned_guild_id,
        )

        if not mentions:
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "Role Mentions",
                    "No role mentions configured yet. Use `/role_mention add` to add one.",
                    color=discord.Color.blurple(),
                ),
                ephemeral=True,
            )

        lines: list[str] = []
        for m in mentions:
            cfg_id = m["role_mention_id"]
            role_id = m["cloned_role_id"]
            chan_id = m["cloned_channel_id"]

            role = guild.get_role(role_id)
            role_display = role.mention if role else f"<@&{role_id}> (deleted)"

            scope = "**all channels**"
            if chan_id:
                ch = guild.get_channel(chan_id)
                if ch:
                    scope = f"channel {ch.mention}"
                else:
                    scope = f"channel `{chan_id}` (deleted)"

            lines.append(f"`{cfg_id}` • {role_display} — {scope}")

        embed = self._ok_embed(
            "Role Mentions",
            "\n".join(lines),
            color=discord.Color.green(),
        )

        await ctx.followup.send(embed=embed, ephemeral=True)

    @channel_webhook_group.command(
        name="set",
        description="Set custom webhook name/avatar for ALL cloned messages in a channel.",
    )
    async def channel_webhook_set(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel = discord.Option(
            discord.TextChannel,
            description="The channel to customize (must be a cloned channel)",
            required=True,
        ),
        webhook_name: str = discord.Option(
            str,
            description="Custom name to display on ALL cloned messages in this channel",
            required=True,
            max_length=80,
            min_length=1,
        ),
        webhook_avatar_url: str = discord.Option(
            str,
            description="Custom avatar URL for ALL cloned messages (optional)",
            required=False,
        ),
    ):
        """Set custom webhook identity for all messages in a channel."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild
        if not guild:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Guild Context",
                    "This command must be run inside a server.",
                ),
                ephemeral=True,
            )

        channel_mapping = self.db.get_channel_mapping_by_clone_id(channel.id)
        if not channel_mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Not a Cloned Channel",
                    f"{channel.mention} is not a cloned channel managed by Copycord.",
                ),
                ephemeral=True,
            )

        mapping = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    "This server isn't mapped to a source guild.",
                ),
                ephemeral=True,
            )

        cloned_guild_id = int(mapping["cloned_guild_id"])

        webhook_name = webhook_name.strip()
        if not webhook_name:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Invalid Name",
                    "Webhook name cannot be empty.",
                ),
                ephemeral=True,
            )

        final_avatar = None
        if webhook_avatar_url:
            webhook_avatar_url = webhook_avatar_url.strip()
            if webhook_avatar_url:
                if not webhook_avatar_url.startswith(("http://", "https://")):
                    return await ctx.followup.send(
                        embed=self._err_embed(
                            "Invalid URL",
                            "Avatar URL must start with http:// or https://",
                        ),
                        ephemeral=True,
                    )
                final_avatar = webhook_avatar_url

        try:
            self.db.set_channel_webhook_profile(
                cloned_channel_id=channel.id,
                cloned_guild_id=cloned_guild_id,
                webhook_name=webhook_name,
                webhook_avatar_url=final_avatar,
            )
        except Exception as e:
            logger.exception("Failed to set channel webhook profile")
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Database Error",
                    f"Failed to save webhook profile: {e}",
                ),
                ephemeral=True,
            )

        fields = [
            ("Channel", channel.mention, False),
            ("Webhook Name", f"`{webhook_name}`", True),
        ]
        if final_avatar:
            fields.append(("Avatar URL", "✅ Set", True))
        else:
            fields.append(("Avatar URL", "❌ Not set (will use default)", True))

        embed = self._ok_embed(
            "Channel Webhook Profile Set",
            f"All messages cloned to {channel.mention} will now use this custom webhook identity.",
            fields=fields,
            color=discord.Color.green(),
        )

        if final_avatar:
            embed.set_thumbnail(url=final_avatar)

        await ctx.followup.send(embed=embed, ephemeral=True)

    @channel_webhook_group.command(
        name="view",
        description="View the custom webhook profile for a channel.",
    )
    async def channel_webhook_view(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel = discord.Option(
            discord.TextChannel,
            description="The channel to view",
            required=True,
        ),
    ):
        """View custom webhook identity for a channel."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild
        if not guild:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Guild Context",
                    "This command must be run inside a server.",
                ),
                ephemeral=True,
            )

        channel_mapping = self.db.get_channel_mapping_by_clone_id(channel.id)
        if not channel_mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Not a Cloned Channel",
                    f"{channel.mention} is not a cloned channel managed by Copycord.",
                ),
                ephemeral=True,
            )

        mapping = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    "This server isn't mapped to a source guild.",
                ),
                ephemeral=True,
            )

        cloned_guild_id = int(mapping["cloned_guild_id"])

        profile = self.db.get_channel_webhook_profile(
            cloned_channel_id=channel.id,
            cloned_guild_id=cloned_guild_id,
        )

        if not profile:
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "No Custom Profile",
                    f"{channel.mention} doesn't have a custom webhook profile.\n"
                    "Messages will use the original author's name and avatar.\n\n"
                    "Use `/channel_webhook set` to create one.",
                    color=discord.Color.blurple(),
                ),
                ephemeral=True,
            )

        fields = [
            ("Channel", channel.mention, False),
            ("Webhook Name", f"`{profile['webhook_name']}`", True),
        ]

        if profile.get("webhook_avatar_url"):
            avatar_url = profile["webhook_avatar_url"]
            fields.append(
                (
                    "Avatar URL",
                    (
                        f"`{avatar_url[:50]}...`"
                        if len(avatar_url) > 50
                        else f"`{avatar_url}`"
                    ),
                    True,
                )
            )
        else:
            fields.append(("Avatar URL", "❌ Not set", True))

        embed = self._ok_embed(
            "Channel Webhook Profile",
            f"All messages cloned to {channel.mention} use this webhook identity:",
            fields=fields,
            color=discord.Color.green(),
        )

        if profile.get("webhook_avatar_url"):
            embed.set_thumbnail(url=profile["webhook_avatar_url"])

        await ctx.followup.send(embed=embed, ephemeral=True)

    @channel_webhook_group.command(
        name="clear",
        description="Remove the custom webhook profile from a channel.",
    )
    async def channel_webhook_clear(
        self,
        ctx: discord.ApplicationContext,
        channel: discord.TextChannel = discord.Option(
            discord.TextChannel,
            description="The channel to clear",
            required=True,
        ),
    ):
        """Clear custom webhook identity for a channel."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild
        if not guild:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Guild Context",
                    "This command must be run inside a server.",
                ),
                ephemeral=True,
            )

        channel_mapping = self.db.get_channel_mapping_by_clone_id(channel.id)
        if not channel_mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Not a Cloned Channel",
                    f"{channel.mention} is not a cloned channel managed by Copycord.",
                ),
                ephemeral=True,
            )

        mapping = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    "This server isn't mapped to a source guild.",
                ),
                ephemeral=True,
            )

        cloned_guild_id = int(mapping["cloned_guild_id"])

        deleted = self.db.delete_channel_webhook_profile(
            cloned_channel_id=channel.id,
            cloned_guild_id=cloned_guild_id,
        )

        if deleted:
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "Profile Cleared",
                    f"Custom webhook profile removed from {channel.mention}.\n"
                    "Messages will now use the original author's name and avatar.",
                    color=discord.Color.orange(),
                ),
                ephemeral=True,
            )
        else:
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "No Profile Found",
                    f"{channel.mention} doesn't have a custom webhook profile.",
                    color=discord.Color.blurple(),
                ),
                ephemeral=True,
            )

    @channel_webhook_group.command(
        name="list",
        description="List all channels with custom webhook profiles in this server.",
    )
    async def channel_webhook_list(
        self,
        ctx: discord.ApplicationContext,
    ):
        """List all channels with custom webhook profiles."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild
        if not guild:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Guild Context",
                    "This command must be run inside a server.",
                ),
                ephemeral=True,
            )

        mapping = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    "This server isn't mapped to a source guild.",
                ),
                ephemeral=True,
            )

        cloned_guild_id = int(mapping["cloned_guild_id"])
        profiles = self.db.list_channel_webhook_profiles_for_guild(cloned_guild_id)

        if not profiles:
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "No Custom Profiles",
                    "No channels in this server have custom webhook profiles yet.",
                    color=discord.Color.blurple(),
                ),
                ephemeral=True,
            )

        lines = []
        for idx, profile in enumerate(profiles, 1):
            channel_id = profile["cloned_channel_id"]
            channel = guild.get_channel(channel_id)
            channel_mention = (
                channel.mention if channel else f"`#{channel_id}` (deleted)"
            )

            name = profile.get("webhook_name") or "(none)"
            avatar = "✅" if profile.get("webhook_avatar_url") else "❌"

            lines.append(
                f"{idx}. {channel_mention}\n   Name: `{name}` | Avatar: {avatar}"
            )

        def chunk_lines(lines_list, max_length=4000):
            chunks = []
            current = []
            current_len = 0

            for line in lines_list:
                line_len = len(line) + 1
                if current_len + line_len > max_length:
                    chunks.append("\n".join(current))
                    current = [line]
                    current_len = line_len
                else:
                    current.append(line)
                    current_len += line_len

            if current:
                chunks.append("\n".join(current))

            return chunks

        description_chunks = chunk_lines(lines)

        embed = self._ok_embed(
            f"Channel Webhook Profiles ({len(profiles)})",
            description_chunks[0],
            color=discord.Color.green(),
        )
        await ctx.followup.send(embed=embed, ephemeral=True)

        for chunk in description_chunks[1:]:
            embed = self._ok_embed(
                "Channel Webhook Profiles (continued)",
                chunk,
                color=discord.Color.green(),
            )
            await ctx.followup.send(embed=embed, ephemeral=True)

    @channel_webhook_group.command(
        name="set_all",
        description="Set custom webhook name/avatar for ALL cloned channels in this server.",
    )
    async def channel_webhook_set_all(
        self,
        ctx: discord.ApplicationContext,
        webhook_name: str = discord.Option(
            str,
            description="Custom name to display on ALL cloned messages in ALL channels",
            required=True,
            max_length=80,
            min_length=1,
        ),
        confirm: str = discord.Option(
            str,
            description='Type "confirm" to apply to all channels',
            required=True,
        ),
        webhook_avatar_url: str = discord.Option(
            str,
            description="Custom avatar URL for ALL cloned messages (optional)",
            required=False,
            default=None,
        ),
        overwrite_existing: bool = discord.Option(
            bool,
            description="Overwrite channels that already have a custom profile",
            required=False,
            default=False,
        ),
    ):
        """Set custom webhook identity for all cloned channels at once."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild
        if not guild:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Guild Context",
                    "This command must be run inside a server.",
                ),
                ephemeral=True,
            )

        if confirm.strip().lower() != "confirm":
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Confirmation Required",
                    'You must type "confirm" to apply webhook profiles to all channels.',
                ),
                ephemeral=True,
            )

        mapping = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    "This server isn't mapped to a source guild.",
                ),
                ephemeral=True,
            )

        cloned_guild_id = int(mapping["cloned_guild_id"])

        webhook_name = webhook_name.strip()
        if not webhook_name:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Invalid Name",
                    "Webhook name cannot be empty.",
                ),
                ephemeral=True,
            )

        final_avatar = None
        if webhook_avatar_url:
            webhook_avatar_url = webhook_avatar_url.strip()
            if webhook_avatar_url:
                if not webhook_avatar_url.startswith(("http://", "https://")):
                    return await ctx.followup.send(
                        embed=self._err_embed(
                            "Invalid URL",
                            "Avatar URL must start with http:// or https://",
                        ),
                        ephemeral=True,
                    )
                final_avatar = webhook_avatar_url

        try:

            all_mappings = self.db.get_all_channel_mappings()
            logger.info(
                f"[channel_webhook_set_all] Looking for channels in clone guild {cloned_guild_id}, found {len(all_mappings)} total mappings"
            )

            cloned_channels = []
            for mapping_row in all_mappings:
                try:

                    row_guild_id = mapping_row["cloned_guild_id"]

                    if row_guild_id is None:
                        continue

                    if int(row_guild_id) != cloned_guild_id:
                        continue

                    channel_id = mapping_row["cloned_channel_id"]
                    if not channel_id:
                        continue

                    channel_id = int(channel_id)

                    channel = guild.get_channel(channel_id)
                    if channel:
                        cloned_channels.append((channel_id, channel.name))
                        logger.debug(
                            f"[channel_webhook_set_all] Found channel: {channel.name} ({channel_id})"
                        )
                except Exception as e:
                    logger.debug(
                        f"[channel_webhook_set_all] Error processing mapping row: {e}"
                    )
                    continue

            logger.info(
                f"[channel_webhook_set_all] Found {len(cloned_channels)} matching cloned channels"
            )

        except Exception as e:
            logger.exception("Failed to get channel mappings")
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Database Error",
                    f"Failed to retrieve channel mappings: {e}",
                ),
                ephemeral=True,
            )

        if not cloned_channels:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Cloned Channels",
                    "No cloned channels found in this server.",
                ),
                ephemeral=True,
            )

        await ctx.followup.send(
            embed=self._ok_embed(
                "Applying Webhook Profiles...",
                f"Processing {len(cloned_channels)} channel(s)...\n"
                f"This may take a moment.",
                color=discord.Color.blurple(),
            ),
            ephemeral=True,
        )

        applied = 0
        skipped = 0
        failed = 0
        skipped_channels = []

        for channel_id, channel_name in cloned_channels:
            try:

                if not overwrite_existing:
                    existing = self.db.get_channel_webhook_profile(
                        cloned_channel_id=channel_id,
                        cloned_guild_id=cloned_guild_id,
                    )
                    if existing:
                        skipped += 1
                        skipped_channels.append(channel_name)
                        continue

                self.db.set_channel_webhook_profile(
                    cloned_channel_id=channel_id,
                    cloned_guild_id=cloned_guild_id,
                    webhook_name=webhook_name,
                    webhook_avatar_url=final_avatar,
                )
                applied += 1

            except Exception as e:
                logger.warning(
                    f"Failed to set webhook profile for channel {channel_id} ({channel_name}): {e}"
                )
                failed += 1

        result_fields = [
            ("Total Channels", str(len(cloned_channels)), True),
            ("Applied", str(applied), True),
            ("Skipped", str(skipped), True),
            ("Failed", str(failed), True),
        ]

        result_description = (
            f"**Webhook Name:** `{webhook_name}`\n"
            f"**Avatar URL:** {'✅ Set' if final_avatar else '❌ Not set'}\n\n"
        )

        if skipped > 0 and not overwrite_existing:
            result_description += (
                f"**{skipped} channel(s) skipped** (already had profiles).\n"
                f"Use `overwrite_existing: True` to replace them.\n\n"
            )

            if skipped_channels:
                preview = skipped_channels[:5]
                preview_text = ", ".join(f"`#{name}`" for name in preview)
                if len(skipped_channels) > 5:
                    preview_text += f" and {len(skipped_channels) - 5} more"
                result_description += f"Skipped: {preview_text}\n\n"

        if failed > 0:
            result_description += (
                f"⚠️ **{failed} channel(s) failed** - check logs for details.\n\n"
            )

        result_description += "All messages cloned to these channels will now use the custom webhook identity."

        color = discord.Color.green() if failed == 0 else discord.Color.orange()

        embed = self._ok_embed(
            "Webhook Profiles Applied",
            result_description,
            fields=result_fields,
            color=color,
        )

        if final_avatar:
            embed.set_thumbnail(url=final_avatar)

        await ctx.followup.send(embed=embed, ephemeral=True)

    @channel_webhook_group.command(
        name="clear_all",
        description="Remove custom webhook profiles from ALL cloned channels in this server.",
    )
    async def channel_webhook_clear_all(
        self,
        ctx: discord.ApplicationContext,
        confirm: str = discord.Option(
            str,
            description='Type "confirm" to clear all channel profiles',
            required=True,
        ),
    ):
        """Clear all channel webhook profiles at once."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild
        if not guild:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Guild Context",
                    "This command must be run inside a server.",
                ),
                ephemeral=True,
            )

        if confirm.strip().lower() != "confirm":
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Confirmation Required",
                    'You must type "confirm" to clear all webhook profiles.',
                ),
                ephemeral=True,
            )

        mapping = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    "This server isn't mapped to a source guild.",
                ),
                ephemeral=True,
            )

        cloned_guild_id = int(mapping["cloned_guild_id"])

        try:
            profiles = self.db.list_channel_webhook_profiles_for_guild(cloned_guild_id)
        except Exception as e:
            logger.exception("Failed to list channel webhook profiles")
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Database Error",
                    f"Failed to retrieve profiles: {e}",
                ),
                ephemeral=True,
            )

        if not profiles:
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "No Profiles Found",
                    "No channels in this server have custom webhook profiles.",
                    color=discord.Color.blurple(),
                ),
                ephemeral=True,
            )

        await ctx.followup.send(
            embed=self._ok_embed(
                "Clearing Webhook Profiles...",
                f"Processing {len(profiles)} channel(s)...",
                color=discord.Color.blurple(),
            ),
            ephemeral=True,
        )

        cleared = 0
        failed = 0

        for profile in profiles:
            try:
                channel_id = profile["cloned_channel_id"]
                deleted = self.db.delete_channel_webhook_profile(
                    cloned_channel_id=channel_id,
                    cloned_guild_id=cloned_guild_id,
                )
                if deleted:
                    cleared += 1
            except Exception as e:
                logger.warning(
                    f"Failed to delete webhook profile for channel {channel_id}: {e}"
                )
                failed += 1

        result_fields = [
            ("Total Profiles", str(len(profiles)), True),
            ("Cleared", str(cleared), True),
            ("Failed", str(failed), True),
        ]

        result_description = (
            "All channel webhook profiles have been removed.\n"
            "Messages will now use the original author's name and avatar."
        )

        if failed > 0:
            result_description = (
                f"⚠️ **{failed} profile(s) failed to clear** - check logs for details.\n\n"
                f"{result_description}"
            )

        color = discord.Color.orange() if failed == 0 else discord.Color.red()

        embed = self._ok_embed(
            "Webhook Profiles Cleared",
            result_description,
            fields=result_fields,
            color=color,
        )

        await ctx.followup.send(embed=embed, ephemeral=True)

    @guild_scoped_slash_command(
        name="mapping_debug",
        description="Show debug info and settings for this clone's guild mapping.",
    )
    async def mapping_debug(self, ctx: discord.ApplicationContext):
        """Ephemeral debug view of this clone guild's guild_mapping row and settings."""
        guild = ctx.guild
        if not guild:
            return await ctx.respond(
                "This command must be run inside a server.", ephemeral=True
            )

        try:
            mapping = self.db.get_mapping_by_clone(guild.id)
        except Exception as e:
            logger.exception(
                "mapping_debug: failed to load mapping for clone=%s", guild.id
            )
            return await ctx.respond(
                embed=self._err_embed(
                    "Database error",
                    f"Failed to load mapping for this guild:\n`{type(e).__name__}: {e}`",
                ),
                ephemeral=True,
            )

        if not mapping:
            return await ctx.respond(
                embed=self._err_embed(
                    "No mapping",
                    "This clone guild is not currently mapped to a source guild.",
                ),
                ephemeral=True,
            )

        mapping_id = str(mapping.get("mapping_id") or "?")
        mapping_name = (mapping.get("mapping_name") or "").strip() or "(unnamed)"
        status = (mapping.get("status") or "active").strip().lower()

        orig_id = int(mapping.get("original_guild_id") or 0)
        clone_id = int(mapping.get("cloned_guild_id") or guild.id)

        orig_name = (mapping.get("original_guild_name") or "").strip() or "(unknown)"
        clone_name = (mapping.get("cloned_guild_name") or "").strip() or guild.name

        created_at_raw = mapping.get("created_at")
        last_updated_raw = mapping.get("last_updated")

        created_at_display = _format_discord_timestamp(created_at_raw)
        last_updated_display = _format_discord_timestamp(last_updated_raw)

        settings = mapping.get("settings") or {}
        if not isinstance(settings, dict):
            try:
                settings = json.loads(str(settings))
            except Exception:

                settings = {"__raw__": str(mapping.get("settings"))}

        try:
            filters = self.db.get_filters_for_mapping(mapping_id)
        except Exception:
            filters = {
                "whitelist": {"category": set(), "channel": set()},
                "exclude": {"category": set(), "channel": set()},
                "blocked_words": [],
            }

        blocked_words = list(filters.get("blocked_words") or [])
        wl_cats = list(filters.get("whitelist", {}).get("category", []) or [])
        wl_chans = list(filters.get("whitelist", {}).get("channel", []) or [])
        ex_cats = list(filters.get("exclude", {}).get("category", []) or [])
        ex_chans = list(filters.get("exclude", {}).get("channel", []) or [])

        try:
            blocked_roles = self.db.get_blocked_role_ids(cloned_guild_id=clone_id)
        except Exception:
            blocked_roles = []

        try:
            user_filters = self.db.get_user_filters_for_mapping(mapping_id)
        except Exception:
            user_filters = {"whitelist": [], "blacklist": []}

        desc_lines = [
            f"**Mapping ID:** `{mapping_id}`",
            f"**Name:** {mapping_name}",
            f"**Status:** `{status}`",
            "",
            f"**Source Guild:** {orig_name} (`{orig_id}`)",
            f"**Clone Guild:** {clone_name} (`{clone_id}`)",
            "",
            f"**Created:** {created_at_display}",
            f"**Last Updated:** {last_updated_display}",
        ]

        embed = discord.Embed(
            title="🛠️ Guild Mapping Debug",
            description="\n".join(desc_lines),
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )

        filters_summary = [
            f"Whitelist categories: **{len(wl_cats)}**",
            f"Whitelist channels: **{len(wl_chans)}**",
            f"Exclude categories: **{len(ex_cats)}**",
            f"Exclude channels: **{len(ex_chans)}**",
            f"Blocked keywords: **{len(blocked_words)}**",
            f"Blocked roles: **{len(blocked_roles)}**",
            f"User whitelist: **{len(user_filters.get('whitelist', []) )}**",
            f"User blacklist: **{len(user_filters.get('blacklist', []) )}**",
        ]

        embed.add_field(
            name="Filters & Blocks",
            value="\n".join(filters_summary),
            inline=False,
        )

        lines: list[str] = []
        for key in sorted(settings.keys()):
            val = settings[key]
            if isinstance(val, (dict, list)):
                val_repr = json.dumps(val, separators=(",", ":"), ensure_ascii=False)
            else:
                val_repr = str(val)

            if len(val_repr) > 120:
                val_repr = val_repr[:117] + "…"

            emoji = ""
            if isinstance(val, bool):
                emoji = "✅" if val else "❌"

            if emoji:
                lines.append(f"{emoji} `{key}` = `{val_repr}`")
            else:
                lines.append(f"`{key}` = `{val_repr}`")

        def _chunk_lines(xs: list[str], limit: int = 1000) -> list[str]:
            chunks: list[str] = []
            cur = ""
            for line in xs:
                add = ("\n" if cur else "") + line
                if len(cur) + len(add) > limit:
                    chunks.append(cur or "—")
                    cur = line
                else:
                    cur += add
            if cur:
                chunks.append(cur)
            if not chunks:
                chunks.append("—")
            return chunks

        for idx, chunk in enumerate(_chunk_lines(lines)):
            embed.add_field(
                name="Settings" if idx == 0 else "Settings (cont.)",
                value=chunk,
                inline=False,
            )

        await ctx.respond(embed=embed, ephemeral=True)

    @rewrite_group.command(
        name="add",
        description="Add or update a word/phrase rewrite for THIS clone mapping.",
    )
    async def rewrite_add(
        self,
        ctx: discord.ApplicationContext,
        source_text: str = Option(
            str,
            "Word or phrase to replace in cloned messages",
            required=True,
        ),
        replacement_text: str = Option(
            str,
            "What to replace it with",
            required=True,
        ),
    ):
        """Add or update a case-insensitive rewrite for this mapping, applied after other sanitization.

        Example:
            /rewrite add source_text: hello replacement_text: yo
            /rewrite add source_text: team rocket replacement_text: team valor
        """
        guild = ctx.guild
        if not guild:
            embed = discord.Embed(
                title="Rewrite: Error",
                description="This command must be run inside a server.",
                color=discord.Color.red(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        mapping_row = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping_row:
            embed = discord.Embed(
                title="Rewrite: Error",
                description=(
                    "This server isn't mapped to a source guild, "
                    "so I can't scope the rewrite."
                ),
                color=discord.Color.red(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        source_text = (source_text or "").strip()
        if not source_text:
            embed = discord.Embed(
                title="Rewrite: Error",
                description="Source text cannot be empty.",
                color=discord.Color.red(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        orig_id = int(mapping_row["original_guild_id"])
        clone_id = int(mapping_row["cloned_guild_id"])

        try:
            existed = self.db.upsert_mapping_rewrite(
                original_guild_id=orig_id,
                cloned_guild_id=clone_id,
                source_text=source_text,
                replacement_text=replacement_text,
            )
        except Exception as e:
            logger.exception(
                "[rewrite_add] Failed to upsert mapping rewrite", exc_info=e
            )
            embed = discord.Embed(
                title="Rewrite: Error",
                description=f"Failed to save rewrite:\n`{e}`",
                color=discord.Color.red(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        try:
            server = getattr(self.bot, "server", None)
            if server and hasattr(server, "_clear_word_rewrites_cache_async"):
                await server._clear_word_rewrites_cache_async()
        except Exception:
            logger.exception("[rewrite_add] Failed to clear rewrite cache")

        verb = "updated" if existed else "added"
        embed = discord.Embed(
            title="Rewrite Saved",
            description=f"Rewrite **{verb}** for this clone mapping.",
            color=discord.Color.green(),
        )
        embed.add_field(
            name="Source",
            value=f"`{source_text}`",
            inline=True,
        )
        embed.add_field(
            name="Replacement",
            value=f"`{replacement_text}`",
            inline=True,
        )

        await ctx.respond(embed=embed, ephemeral=True)

    @rewrite_group.command(
        name="remove",
        description="Remove a word/phrase rewrite for THIS clone mapping.",
    )
    async def rewrite_remove(
        self,
        ctx: discord.ApplicationContext,
        rewrite_id: int = Option(
            int,
            "ID of the rewrite to remove (see /rewrite list)",
            required=True,
            min_value=1,
        ),
    ):
        guild = ctx.guild
        if not guild:
            embed = discord.Embed(
                title="Rewrite: Error",
                description="This command must be run inside a server.",
                color=discord.Color.red(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        mapping_row = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping_row:
            embed = discord.Embed(
                title="Rewrite: Error",
                description=(
                    "This server isn't mapped to a source guild, "
                    "so I can't scope the rewrite."
                ),
                color=discord.Color.red(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        orig_id = int(mapping_row["original_guild_id"])
        clone_id = int(mapping_row["cloned_guild_id"])

        try:
            removed = self.db.delete_mapping_rewrite(
                original_guild_id=orig_id,
                cloned_guild_id=clone_id,
                rewrite_id=rewrite_id,
            )
        except Exception as e:
            logger.exception(
                "[rewrite_remove] Failed to delete mapping rewrite", exc_info=e
            )
            embed = discord.Embed(
                title="Rewrite: Error",
                description=f"Failed to remove rewrite:\n`{e}`",
                color=discord.Color.red(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        if not removed:
            embed = discord.Embed(
                title="Rewrite Not Found",
                description=f"No rewrite found with ID `{rewrite_id}` on this mapping.",
                color=discord.Color.orange(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        try:
            server = getattr(self.bot, "server", None)
            if server and hasattr(server, "_clear_word_rewrites_cache_async"):
                await server._clear_word_rewrites_cache_async()
        except Exception:
            logger.exception("[rewrite_remove] Failed to clear rewrite cache")

        embed = discord.Embed(
            title="Rewrite Removed",
            description=f"Removed rewrite with ID `{rewrite_id}` on this clone mapping.",
            color=discord.Color.green(),
        )
        await ctx.respond(embed=embed, ephemeral=True)

    @rewrite_group.command(
        name="list",
        description="List word/phrase rewrites for THIS clone mapping.",
    )
    async def rewrite_list(self, ctx: discord.ApplicationContext):
        guild = ctx.guild
        if not guild:
            embed = discord.Embed(
                title="Rewrite: Error",
                description="This command must be run inside a server.",
                color=discord.Color.red(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        mapping_row = self.db.get_mapping_by_cloned_guild_id(guild.id)
        if not mapping_row:
            embed = discord.Embed(
                title="Rewrite: Error",
                description=(
                    "This server isn't mapped to a source guild, "
                    "so I can't find its rewrites."
                ),
                color=discord.Color.red(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        orig_id = int(mapping_row["original_guild_id"])
        clone_id = int(mapping_row["cloned_guild_id"])

        rows = self.db.list_mapping_rewrites_for_mapping(
            original_guild_id=orig_id,
            cloned_guild_id=clone_id,
        )

        if not rows:
            embed = discord.Embed(
                title="Rewrites",
                description="There are no rewrites configured for this clone mapping.",
                color=discord.Color.blurple(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        lines: list[str] = []
        for row in rows:
            rid = row.get("id")
            src = row.get("source_text") or ""
            repl = row.get("replacement_text") or ""
            lines.append(f"[`{rid}`] `{src}` → `{repl}`")

        body = "\n".join(lines)

        embed = discord.Embed(
            title="Word/Phrase Rewrites",
            description=body,
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="Use /rewrite remove <id> to delete a rule.")

        await ctx.respond(embed=embed, ephemeral=True)

    @guild_scoped_slash_command(
        name="debug_report",
        description="Generate a ZIP with redacted JSON dumps of all Copycord DB tables.",
    )
    async def debug_report(self, ctx: discord.ApplicationContext):
        """
        Generate a redacted JSON dump of all DB tables, zipped for debugging.

        - One JSON file per table: db_tables/<table>.json
        - Obvious secrets (tokens, webhook URLs, passwords, etc.) are redacted.
        - app_config values are redacted when the key looks secret (TOKEN, SECRET, etc.).
        """
        await ctx.defer(ephemeral=True)

        conn = self.db.conn
        if conn is None:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Database error",
                    "Database connection is not available (conn is None).",
                ),
                ephemeral=True,
            )

        buf = io.BytesIO()
        now = datetime.now(timezone.utc)
        generated_iso = now.isoformat()

        sensitive_col_tokens = [
            "token",
            "secret",
            "password",
            "webhook_url",
            "webhook",
            "session",
            "cookie",
            "api_key",
        ]

        sensitive_key_tokens = [
            "token",
            "secret",
            "webhook",
            "password",
            "api_key",
            "client_id",
            "client_secret",
        ]

        try:
            with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:

                cur = conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
                rows = cur.fetchall()

                table_names: list[str] = []
                for r in rows:
                    try:
                        name = r["name"]
                    except Exception:
                        name = r[0]
                    if not name:
                        continue
                    table_names.append(str(name))

                table_names = sorted(set(table_names))

                for table in table_names:
                    try:
                        cur_tbl = conn.execute(f"SELECT * FROM {table}")
                    except Exception as e:
                        # If a table can't be selected, record the error instead of crashing the whole export
                        error_payload = {
                            "_meta": {
                                "table": table,
                                "error": f"{type(e).__name__}: {e}",
                                "generated_at": generated_iso,
                            },
                            "rows": [],
                        }
                        zf.writestr(
                            f"db_tables/{table}.json",
                            json.dumps(error_payload, indent=2, default=str),
                        )
                        continue

                    cols = [d[0] for d in (cur_tbl.description or [])]
                    redacted_cols = [
                        c
                        for c in cols
                        if any(tok in c.lower() for tok in sensitive_col_tokens)
                    ]

                    table_rows: list[dict] = []
                    fetched = cur_tbl.fetchall()

                    for r in fetched:

                        row_dict_raw: dict = {}
                        for idx, col in enumerate(cols):
                            try:
                                val = r[col]
                            except Exception:
                                val = r[idx]

                            row_dict_raw[col] = val

                        row_dict_out: dict = {}
                        for col, val in row_dict_raw.items():
                            col_l = col.lower()
                            redacted = False

                            if any(tok in col_l for tok in sensitive_col_tokens):
                                redacted = True

                            if table == "app_config" and col == "value":
                                key_val = row_dict_raw.get("key")
                                if isinstance(key_val, str):
                                    key_up = key_val.upper()
                                    if any(
                                        tok.upper() in key_up
                                        for tok in sensitive_key_tokens
                                    ):
                                        redacted = True

                            if redacted and val is not None:
                                row_dict_out[col] = "[redacted]"
                            else:
                                row_dict_out[col] = val

                        table_rows.append(row_dict_out)

                    payload = {
                        "_meta": {
                            "table": table,
                            "row_count": len(table_rows),
                            "generated_at": generated_iso,
                            "redacted_columns": redacted_cols,
                            "notes": (
                                "Values for some app_config keys (TOKEN/SECRET/etc.) "
                                "are redacted as [redacted]."
                                if table == "app_config"
                                else ""
                            ),
                        },
                        "rows": table_rows,
                    }

                    zf.writestr(
                        f"db_tables/{table}.json",
                        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
                    )

        except Exception as e:
            logger.exception("debug_report: failed to build ZIP")
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Debug report failed",
                    f"An error occurred while building the debug report:\n"
                    f"`{type(e).__name__}: {e}`",
                ),
                ephemeral=True,
            )

        buf.seek(0)
        ts = now.strftime("%Y%m%d-%H%M%S")
        filename = f"copycord_debug_report_{ts}.zip"

        embed = discord.Embed(
            title="Debug Report Ready",
            description=(
                'Secrets (tokens, webhook URLs, passwords, etc.) are **replaced with** `"[redacted]"`.\n\n'
                "This report is intended for troubleshooting Copycord.\n"
                "**Only share it directly with trusted Copycord developers**."
            ),
            color=discord.Color.blurple(),
            timestamp=None,
        )

        await ctx.followup.send(
            embed=embed,
            file=discord.File(buf, filename=filename),
            ephemeral=True,
        )


def setup(bot: commands.Bot):
    bot.add_cog(CloneCommands(bot))
