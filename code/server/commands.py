# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
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
from discord.errors import NotFound, Forbidden
from discord import errors as discord_errors
from datetime import datetime, timezone
import time
import logging
import time
from common.config import Config
from common.db import DBManager
from server.rate_limiter import RateLimitManager, ActionType
from server.helpers import MemberExportService, ExportJob, PurgeAssetHelper

logger = logging.getLogger("server")

config = Config(logger=logger)
GUILD_ID = config.CLONE_GUILD_ID


class CloneCommands(commands.Cog):
    """
    Collection of slash commands for the Clone bot, restricted to allowed users.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = DBManager(config.DB_PATH)
        self.ratelimit = RateLimitManager()
        self.start_time = time.time()
        self.allowed_users = getattr(config, "COMMAND_USERS", []) or []
        self.export = MemberExportService(self.bot, self.bot.ws_manager, logger=logger)
        self._export_jobs: dict[int, ExportJob] = {}

    async def cog_check(self, ctx: commands.Context):
        """
        Global check for all commands in this cog. Only users whose ID is in set in config may execute commands.
        """
        cmd_name = ctx.command.name if ctx.command else "unknown"
        if ctx.user.id in self.allowed_users:
            logger.info(f"[⚡] User {ctx.user.id} executed the '{cmd_name}' command.")
            return True
        # deny access otherwise
        await ctx.respond("You are not authorized to use this command.", ephemeral=True)
        logger.warning(
            f"[⚠️] Unauthorized access: user {ctx.user.id} attempted to run command '{cmd_name}'"
        )
        return False

    @commands.Cog.listener()
    async def on_application_command_error(self, interaction, error):
        """
        Handle errors during slash‐command execution.

        Unwraps the original exception if it was wrapped in an ApplicationCommandInvokeError,
        silently ignores permission‐related CheckFailure errors to avoid log spam when
        unauthorized users invoke protected commands, and logs all other exceptions
        with full tracebacks for debugging.
        """
        orig = getattr(error, "original", None)
        err = orig or error

        if isinstance(err, (commands.CheckFailure, discord_errors.CheckFailure)):
            return

        cmd = interaction.command.name if interaction.command else "<unknown>"
        logger.exception(f"Error in command '{cmd}':", exc_info=err)

    @commands.Cog.listener()
    async def on_ready(self):
        """
        Fired when the bot is ready. Logs allowed users status.
        """
        if not self.allowed_users:
            logger.warning(
                "[⚠️] No allowed users configured: commands will not work for anyone."
            )
        else:
            logger.debug(f"[⚙️] Commands permissions set for users: {self.allowed_users}")

    async def _reply_or_dm(self, ctx: discord.ApplicationContext, content: str) -> None:
        """Try ephemeral followup; if the channel is gone, DM the user instead."""
        try:
            await ctx.followup.send(content, ephemeral=True)
            return
        except NotFound:
            pass
        except Forbidden:
            pass

        try:
            MAX = 2000
            if len(content) <= MAX:
                await ctx.user.send(content)
            else:
                start = 0
                while start < len(content):
                    end = min(start + MAX, len(content))
                    nl = content.rfind("\n", start, end)
                    if nl == -1 or nl <= start + 100:
                        nl = end
                    await ctx.user.send(content[start:nl])
                    start = nl
        except Forbidden:
            logger.warning("[verify_structure] Could not DM user; DMs are closed.")

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
        """
        Build a standard success/info embed.
        """
        if description is None:
            # Only one positional arg was given → it's the description; no title.
            if title_or_desc is None:
                raise ValueError("description is required")
            title = None
            description = title_or_desc
        else:
            # Two-arg form → treat first as title.
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

    @commands.slash_command(
        name="ping_server",
        description="Show server latency and server information.",
        guild_ids=[GUILD_ID],
    )
    async def ping(self, ctx: discord.ApplicationContext):
        """Responds with bot latency, server name, member count, and uptime."""
        latency_ms = self.bot.latency * 1000
        uptime_seconds = time.time() - self.start_time
        hours, remainder = divmod(int(uptime_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{hours}h {minutes}m {seconds}s"
        guild = ctx.guild

        embed = discord.Embed(title="📡 Pong! (Server)", timestamp=datetime.utcnow())
        embed.add_field(name="Latency", value=f"{latency_ms:.2f} ms", inline=True)
        embed.add_field(name="Server", value=guild.name, inline=True)
        embed.add_field(name="Members", value=str(guild.member_count), inline=True)
        embed.add_field(name="Uptime", value=uptime_str, inline=True)

        await ctx.respond(embed=embed, ephemeral=True)

    @commands.slash_command(
        name="ping_client",
        description="Show client latency and server information.",
        guild_ids=[GUILD_ID],
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

        embed = discord.Embed(title="📡 Pong! (Client)", timestamp=datetime.utcnow())
        embed.add_field(name="Latency", value=f"{ws_latency_ms:.2f} ms", inline=True)
        embed.add_field(
            name="Round‑Trip Time", value=f"{round_trip_ms:.2f} ms", inline=True
        )
        embed.add_field(name="Client Uptime", value=uptime_str, inline=True)

        await ctx.followup.send(embed=embed, ephemeral=True)

    @commands.slash_command(
        name="block_add",
        description="Add or remove a keyword from the block list.",
        guild_ids=[GUILD_ID],
    )
    async def block_add(
        self,
        ctx: discord.ApplicationContext,
        keyword: str = Option(
            description="Keyword to block (will toggle)", required=True
        ),
    ):
        """Toggle a blocked keyword in blocked_keywords."""
        if self.db.add_blocked_keyword(keyword):
            action, emoji = "added", "✅"
        elif self.db.remove_blocked_keyword(keyword):
            action, emoji = "removed", "🗑️"
        else:
            await ctx.respond(f"⚠️ Couldn’t toggle `{keyword}`.", ephemeral=True)
            return

        # push update to client
        new_list = self.db.get_blocked_keywords()
        await self.bot.ws_manager.send(
            {"type": "settings_update", "data": {"blocked_keywords": new_list}}
        )

        await ctx.respond(
            f"{emoji} `{keyword}` {action} in block list.", ephemeral=True
        )

    @commands.slash_command(
        name="block_list",
        description="List all blocked keywords.",
        guild_ids=[GUILD_ID],
    )
    async def block_list(self, ctx: discord.ApplicationContext):
        """Show currently blocked keywords."""
        kws = self.db.get_blocked_keywords()
        if not kws:
            await ctx.respond("📋 Your block list is empty.", ephemeral=True)
        else:
            formatted = "\n".join(f"• `{kw}`" for kw in kws)
            await ctx.respond(f"📋 **Blocked keywords:**\n{formatted}", ephemeral=True)


    @commands.slash_command(
        name="announcement_trigger",
        description="Register a trigger: keyword + user_id + optional channel_id",
        guild_ids=[GUILD_ID],
    )
    async def announcement_trigger(
        self,
        ctx: discord.ApplicationContext,
        keyword: str = Option(str, "Keyword to trigger on", required=True),
        user_id: str = Option(
            str,
            "User ID to filter on",
            required=True,
            min_length=17,
            max_length=20,
        ),
        channel_id: str = Option(
            str,
            "Channel ID to listen in (omit for any channel)",
            required=False,
            min_length=17,
            max_length=20,
        ),
    ):
        try:
            filter_id = int(user_id)
        except ValueError:
            return await ctx.respond(
                embed=Embed(
                    title="⚠️ Invalid User ID",
                    description=f"`{user_id}` is not a valid user ID.",
                    color=Color.red(),
                ),
                ephemeral=True,
            )

        if channel_id:
            try:
                chan_id = int(channel_id)
            except ValueError:
                return await ctx.respond(
                    embed=Embed(
                        title="⚠️ Invalid Channel ID",
                        description=f"`{channel_id}` is not a valid channel ID.",
                        color=Color.red(),
                    ),
                    ephemeral=True,
                )
        else:
            chan_id = 0

        triggers = self.db.get_announcement_triggers()
        existing = triggers.get(keyword, [])

        if chan_id == 0:
            cleared = 0
            for fid, cid in existing:
                if fid == filter_id and cid != 0:
                    self.db.remove_announcement_trigger(keyword, filter_id, cid)
                    cleared += 1

        if chan_id != 0 and (filter_id, 0) in existing:
            return await ctx.respond(
                embed=Embed(
                    title="Global Trigger Exists",
                    description=(
                        f"A global trigger for **{keyword}** by user ID `{filter_id}` "
                        "already exists. Please remove it first if you want to add a specific channel trigger."
                    ),
                    color=Color.blue(),
                ),
                ephemeral=True,
            )

        added = self.db.add_announcement_trigger(keyword, filter_id, chan_id)
        who = f"user ID `{filter_id}`"
        where = f"in channel `#{chan_id}`" if chan_id else "in any channel"

        if not added:
            embed = Embed(
                title="Trigger Already Exists",
                description=f"Trigger for **{keyword}** by {who} {where} already exists.",
                color=Color.orange(),
            )
        else:
            title = (
                "Global Trigger Registered" if chan_id == 0 else "Trigger Registered"
            )
            desc = f"Will announce **{keyword}** by {who} {where}."
            embed = Embed(title=title, description=desc, color=Color.green())

        await ctx.respond(embed=embed, ephemeral=True)

    @commands.slash_command(
        name="announcement_user",
        description="Toggle a user's subscription to a keyword (or all) announcements",
        guild_ids=[GUILD_ID],
    )
    async def announcement_user(
        self,
        ctx: discord.ApplicationContext,
        user: discord.User = Option(
            discord.User, "User to (un)subscribe (defaults to you)", required=False
        ),
        keyword: str = Option(
            str, "Keyword to subscribe to (omit for ALL announcements)", required=False
        ),
    ):
        """
        /announcement_user [@user] [keyword]
        • With keyword → toggles that user’s subscription to that keyword.
        • Without keyword → toggles GLOBAL subscription (receives all).
        Clears prior per-keyword subs when subscribing globally.
        """
        target = user or ctx.user

        sub_key = keyword or "*"

        if sub_key != "*" and self.db.get_announcement_users("*").count(target.id) > 0:
            embed = Embed(
                title="Already Subscribed Globally",
                description=f"{target.mention} is already subscribed to **all** announcements. Unsubscribe them first to subscribe to specific keywords.",
                color=Color.blue(),
            )
            return await ctx.respond(embed=embed, ephemeral=True)

        if sub_key == "*":
            self.db.conn.execute(
                "DELETE FROM announcement_subscriptions WHERE user_id = ? AND keyword != '*'",
                (target.id,),
            )
            self.db.conn.commit()

        if self.db.add_announcement_user(sub_key, target.id):
            action = "Subscribed"
            color = Color.green()
        else:
            self.db.remove_announcement_user(sub_key, target.id)
            action = "Unsubscribed"
            color = Color.orange()

        embed = Embed(title="🔔 Announcement Subscription Updated", color=color)
        embed.add_field(name="User", value=target.mention, inline=True)
        scope = keyword if keyword else "All announcements"
        embed.add_field(name="Scope", value=scope, inline=True)
        embed.add_field(name="Action", value=action, inline=True)

        await ctx.respond(embed=embed, ephemeral=True)

    @commands.slash_command(
        name="announcement_list",
        description="List all announcement triggers, or delete by index",
        guild_ids=[GUILD_ID],
    )
    async def announcement_list(
        self,
        ctx: discord.ApplicationContext,
        delete: int = Option(
            int, "Index of the trigger to delete", required=False, min_value=1
        ),
    ):
        """
        /announcement_list [delete]
        • No args → shows all active triggers (keyword + user + channel) in an embed.
        • With delete → removes that trigger and clears its subscriptions.
        """
        triggers = self.db.get_announcement_triggers()
        keys = list(triggers.keys())

        if not keys:
            return await ctx.respond("No announcement triggers set.", ephemeral=True)

        # Deletion path
        if delete is not None:
            idx = delete - 1
            if idx < 0 or idx >= len(keys):
                return await ctx.respond(
                    f"⚠️ Invalid index `{delete}`; pick 1–{len(keys)}.", ephemeral=True
                )
            kw = keys[idx]
            for filter_id, chan_id in triggers[kw]:
                self.db.remove_announcement_trigger(kw, filter_id, chan_id)
            self.db.conn.execute(
                "DELETE FROM announcement_subscriptions WHERE keyword = ?", (kw,)
            )
            self.db.conn.commit()
            return await ctx.respond(
                f"Deleted announcement trigger **{kw}** and cleared its subscribers.",
                ephemeral=True,
            )

        embed = discord.Embed(
            title="📋 Announcement Triggers", color=discord.Color.blurple()
        )

        for i, kw in enumerate(keys, start=1):
            entries = triggers[kw]
            lines = []
            for filter_id, chan_id in entries:
                user_desc = "any user" if filter_id == 0 else f"user `{filter_id}`"
                chan_desc = "any channel" if chan_id == 0 else f"channel `#{chan_id}`"
                lines.append(f"> From {user_desc} in {chan_desc}")
            embed.add_field(name=f"{i}. {kw}", value="\n".join(lines), inline=False)

        await ctx.respond(embed=embed, ephemeral=True)

        
    @commands.slash_command(
        name="onjoin_dm",
        description="Toggle DM notifications to you when someone joins the given server ID",
        guild_ids=[GUILD_ID],
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

        # Toggle behavior
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
        
    @commands.slash_command(
        name="purge_assets",
        description="Delete ALL emojis, stickers, or roles.",
        guild_ids=[GUILD_ID],
    )
    async def purge_assets(
        self,
        ctx: discord.ApplicationContext,
        kind: str = Option(str, "What to delete", required=True, choices=["emojis", "stickers", "roles"]),
        confirm: str = Option(str, "Type 'confirm' to run this DESTRUCTIVE action", required=True),
    ):
        await ctx.defer(ephemeral=True)

        if (confirm or "").strip().lower() != "confirm":
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Confirmation required",
                    "Re-run the command and type **confirm** to proceed."
                ),
                ephemeral=True,
            )

        helper = PurgeAssetHelper(self)
        guild = ctx.guild
        if not guild:
            return await ctx.followup.send("No guild context.", ephemeral=True)

        RL_EMOJI   = getattr(ActionType, "EMOJI", "EMOJI")
        RL_STICKER = getattr(ActionType, "STICKER", "STICKER")
        RL_ROLE    = getattr(ActionType, "ROLE", "ROLE")

        deleted = skipped = failed = 0
        await ctx.followup.send(
            embed=self._ok_embed("Starting purge…", f"Target: `{kind}`\nI'll DM you when finished."),
            ephemeral=True,
        )
        helper._log_purge_event(kind=kind, outcome="begin", guild_id=guild.id, user_id=ctx.user.id)

        try:
            # =============================
            # EMOJIS
            # =============================
            if kind == "emojis":
                for em in list(guild.emojis):
                    try:
                        await self.ratelimit.acquire(RL_EMOJI)
                        await em.delete(reason=f"Purge by {ctx.user.id}")
                        deleted += 1
                        helper._log_purge_event(kind="emojis", outcome="deleted",
                                                guild_id=guild.id, user_id=ctx.user.id,
                                                obj_id=em.id, name=em.name)
                    except discord.Forbidden as e:
                        skipped += 1
                        helper._log_purge_event(kind="emojis", outcome="skipped",
                                                guild_id=guild.id, user_id=ctx.user.id,
                                                obj_id=em.id, name=em.name, reason=str(e))
                    except Exception as e:
                        failed += 1
                        helper._log_purge_event(kind="emojis", outcome="failed",
                                                guild_id=guild.id, user_id=ctx.user.id,
                                                obj_id=em.id, name=em.name, reason=str(e))
                self.db.conn.execute("DELETE FROM emoji_mappings")
                self.db.conn.commit()
                helper._log_purge_event(kind="emojis", outcome="db_clear",
                                        guild_id=guild.id, user_id=ctx.user.id)

            # =============================
            # STICKERS
            # =============================
            elif kind == "stickers":
                stickers = list(getattr(guild, "stickers", []))
                if not stickers:
                    try:
                        stickers = list(await guild.fetch_stickers())
                    except Exception:
                        stickers = []
                for st in stickers:
                    try:
                        await self.ratelimit.acquire(RL_STICKER)
                        await st.delete(reason=f"Purge by {ctx.user.id}")
                        deleted += 1
                        helper._log_purge_event(kind="stickers", outcome="deleted",
                                                guild_id=guild.id, user_id=ctx.user.id,
                                                obj_id=st.id, name=st.name)
                    except discord.Forbidden as e:
                        skipped += 1
                        helper._log_purge_event(kind="stickers", outcome="skipped",
                                                guild_id=guild.id, user_id=ctx.user.id,
                                                obj_id=st.id, name=st.name, reason=str(e))
                    except Exception as e:
                        failed += 1
                        helper._log_purge_event(kind="stickers", outcome="failed",
                                                guild_id=guild.id, user_id=ctx.user.id,
                                                obj_id=st.id, name=st.name, reason=str(e))
                self.db.conn.execute("DELETE FROM sticker_mappings")
                self.db.conn.commit()
                helper._log_purge_event(kind="stickers", outcome="db_clear",
                                        guild_id=guild.id, user_id=ctx.user.id)

            # =============================
            # ROLES
            # =============================
            elif kind == "roles":
                me, top_role, roles = await helper._resolve_me_and_top(guild)
                if not me or not top_role:
                    return await ctx.followup.send(
                        embed=self._err_embed("Top role not found",
                                              "Could not resolve my top role. Try again later."),
                        ephemeral=True,
                    )
                if not me.guild_permissions.manage_roles:
                    return await ctx.followup.send(
                        embed=self._err_embed("Missing permission", "I need **Manage Roles**."),
                        ephemeral=True,
                    )

                def _undeletable(r: discord.Role) -> bool:
                    prem = getattr(r, "is_premium_subscriber", None)
                    return bool(r.is_default() or r.managed or (prem() if callable(prem) else False))

                eligible = [r for r in roles if not _undeletable(r) and r < top_role]
                for role in sorted(eligible, key=lambda r: r.position):
                    try:
                        await self.ratelimit.acquire(RL_ROLE)
                        await role.delete(reason=f"Purge by {ctx.user.id}")
                        deleted += 1
                        helper._log_purge_event(kind="roles", outcome="deleted",
                                                guild_id=guild.id, user_id=ctx.user.id,
                                                obj_id=role.id, name=role.name)
                    except discord.Forbidden as e:
                        skipped += 1
                        helper._log_purge_event(kind="roles", outcome="skipped",
                                                guild_id=guild.id, user_id=ctx.user.id,
                                                obj_id=role.id, name=role.name, reason=str(e))
                    except Exception as e:
                        failed += 1
                        helper._log_purge_event(kind="roles", outcome="failed",
                                                guild_id=guild.id, user_id=ctx.user.id,
                                                obj_id=role.id, name=role.name, reason=str(e))
                self.db.conn.execute("DELETE FROM role_mappings")
                self.db.conn.commit()
                helper._log_purge_event(kind="roles", outcome="db_clear",
                                        guild_id=guild.id, user_id=ctx.user.id)

            # =============================
            # Finalize
            # =============================
            summary = f"**Target:** `{kind}`\n**Deleted:** {deleted}\n**Skipped:** {skipped}\n**Failed:** {failed}"
            color = discord.Color.green() if failed == 0 else discord.Color.orange()
            await ctx.followup.send(embed=self._ok_embed("🧹 Purge complete", summary, color=color), ephemeral=True)
            await helper._dm_summary(ctx, "🧹 Purge complete", summary, self._ok_embed)
            helper._log_purge_event(kind=kind, outcome="done",
                                    guild_id=guild.id, user_id=ctx.user.id,
                                    counts={"deleted": deleted, "skipped": skipped, "failed": failed})

        except Exception as e:
            err_text = f"{type(e).__name__}: {e}"
            await ctx.followup.send(embed=self._err_embed("Purge failed", err_text), ephemeral=True)
            await helper._dm_summary(ctx, "Purge failed", err_text, self._err_embed)
            helper._log_purge_event(kind=kind, outcome="failed",
                                    guild_id=guild.id, user_id=ctx.user.id,
                                    reason=err_text)


def setup(bot: commands.Bot):
    bot.add_cog(CloneCommands(bot))
