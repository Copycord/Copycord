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
    CategoryChannel,
    Option,
    Embed,
    Color,
    ChannelType,
    SlashCommandOptionType,
)
from discord.errors import NotFound, Forbidden
from discord import errors as discord_errors
from datetime import datetime, timezone
import time
import logging
import asyncio
import time
from common.config import Config
from common.db import DBManager
from server.rate_limiter import RateLimitManager, ActionType
from server.helpers import MemberExportService, ExportJob

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
            logger.info(f"[⚙️] Commands permissions set for users: {self.allowed_users}")

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
        name="verify_structure",
        description="Report any channels or categories not in the last sitemap. Optionally delete them.",
        guild_ids=[GUILD_ID],
    )
    async def verify_structure(
        self,
        ctx: discord.ApplicationContext,
        delete: bool = Option(
            bool,
            "If true, delete orphaned channels/categories after reporting",
            default=False,
        ),
    ):
        """Lists—and optionally deletes—categories and channels that aren’t in the last synced sitemap."""
        await ctx.defer(ephemeral=True)

        guild = ctx.guild

        mapped_cats = {
            r["cloned_category_id"]
            for r in self.db.get_all_category_mappings()
            if r["cloned_category_id"] is not None
        }
        mapped_chs = {
            r["cloned_channel_id"]
            for r in self.db.get_all_channel_mappings()
            if r["cloned_channel_id"] is not None
        }

        orphan_categories = [c for c in guild.categories if c.id not in mapped_cats]
        orphan_channels = [
            ch
            for ch in guild.channels
            if not isinstance(ch, CategoryChannel) and ch.id not in mapped_chs
        ]
        logger.info(
            "[⚙️] Found %d orphan categories and %d orphan channels",
            len(orphan_categories),
            len(orphan_channels),
        )

        if delete:
            deleted_cats = deleted_chs = 0

            for cat in orphan_categories:
                try:
                    await self.ratelimit.acquire(ActionType.DELETE_CHANNEL)
                    await cat.delete()
                    deleted_cats += 1
                    logger.info(
                        "[🗑️] Deleted orphan category %s (ID %d)", cat.name, cat.id
                    )
                except Exception as e:
                    logger.warning("[⚠️] Failed to delete category %s: %s", cat.id, e)

            for ch in orphan_channels:
                try:
                    await self.ratelimit.acquire(ActionType.DELETE_CHANNEL)
                    await ch.delete()
                    deleted_chs += 1
                    logger.info("[🗑️] Deleted orphan channel %s (ID %d)", ch.name, ch.id)
                except Exception as e:
                    logger.warning("[⚠️] Failed to delete channel %s: %s", ch.id, e)

            if deleted_cats == 0 and deleted_chs == 0:
                msg = "No orphaned channels or categories found to delete."
            else:
                parts = []
                if deleted_cats:
                    parts.append(
                        f"{deleted_cats} categor{'y' if deleted_cats==1 else 'ies'}"
                    )
                if deleted_chs:
                    parts.append(
                        f"{deleted_chs} channel{'s' if deleted_chs!=1 else ''}"
                    )
                msg = "[⚙️] verify_structure: Deleted " + " and ".join(parts) + "."
                logger.info("[⚙️] verify_structure: Deletion summary: %s", msg)

            return await self._reply_or_dm(ctx, msg)

        # Otherwise, normal report flow
        if not orphan_categories and not orphan_channels:
            msg = "[⚙️] verify_structure: All channels and categories match the last sitemap."
            logger.info(msg)
            return await self._reply_or_dm(ctx, msg)

        report_lines = []
        if orphan_categories:
            report_lines.append(f"**Orphan Categories ({len(orphan_categories)})**:")
            report_lines += [f"- {cat.name} (ID {cat.id})" for cat in orphan_categories]

        if orphan_channels:
            report_lines.append(f"**Orphan Channels ({len(orphan_channels)})**:")
            report_lines += [f"- <#{ch.id}> ({ch.name})" for ch in orphan_channels]

        report = "\n".join(report_lines)
        if len(report) > 1900:
            report = report[:1900] + "\n…(truncated)"

        await self._reply_or_dm(ctx, report)

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
        name="clone_messages",
        description="Clone all messages for a *cloned* channel (oldest → newest), queueing live traffic meanwhile.",
        guild_ids=[GUILD_ID],
    )
    async def clone_messages(
        self,
        ctx: discord.ApplicationContext,
        clone_channel: discord.abc.GuildChannel = Option(
            SlashCommandOptionType.channel,
            "Pick the channel to sync",
            required=True,
            channel_types=[ChannelType.text, ChannelType.news],
        ),
    ):
        await ctx.defer(ephemeral=True)

        try:
            row = self.db.conn.execute(
                "SELECT original_channel_id FROM channel_mappings WHERE cloned_channel_id = ?",
                (clone_channel.id,),
            ).fetchone()
        except Exception as e:
            logger.exception(
                "[⛔] DB error resolving original for cloned %s: %s",
                clone_channel.id,
                e,
            )
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Database Error",
                    "I couldn’t resolve the channel mapping. Please try again.",
                ),
                ephemeral=True,
            )

        if not row:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "No Mapping Found",
                    f"I don’t have a mapping for {clone_channel.mention}. Run a structure sync first.",
                ),
                ephemeral=True,
            )

        try:
            original_id = int(row["original_channel_id"])
        except Exception:
            original_id = int(row[0])

        receiver = getattr(self.bot, "server", None)
        if receiver is None:
            logger.error("[⛔] Server receiver not available on bot")
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Internal Error", "Server receiver is unavailable."
                ),
                ephemeral=True,
            )

        try:
            ok, conflict = await receiver.backfill.try_begin_global_sync(
                original_id, ctx.user.id
            )
        except Exception:
            ok, conflict = False, None

        if not ok:
            conflict_orig = int(conflict.get("original_id", 0)) if conflict else None
            conflict_user = int(conflict.get("user_id", 0)) if conflict else None

            conflict_clone_id = None
            if conflict_orig:
                try:
                    row2 = self.db.conn.execute(
                        "SELECT cloned_channel_id FROM channel_mappings WHERE original_channel_id = ?",
                        (conflict_orig,),
                    ).fetchone()
                    if row2:
                        conflict_clone_id = int(
                            row2["cloned_channel_id"]
                            if hasattr(row2, "__getitem__")
                            else row2[0]
                        )
                except Exception:
                    pass

            where = (
                f"<#{conflict_clone_id}>"
                if conflict_clone_id
                else (
                    f"channel `{conflict_orig}`" if conflict_orig else "another channel"
                )
            )
            who = f" by <@{conflict_user}>" if conflict_user else ""
            desc = f"Only one channel message sync can run at a time for this server.\nCurrent sync: {where}{who}"
            return await ctx.followup.send(
                embed=self._ok_embed("⏳ A channel sync is already running", desc),
                ephemeral=True,
            )

        try:
            receiver.backfill.mark_backfill(original_id)
            receiver.backfill.register_sink(
                original_id,
                msg=None,
                user_id=ctx.user.id,
                clone_channel_id=clone_channel.id,
            )
        except Exception as e:
            logger.exception(
                "[⛔] Failed registering backfill sink for %s: %s", original_id, e
            )
            try:
                await receiver.backfill.end_global_sync(original_id)
            except Exception:
                pass
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Internal Error", "Failed setting up progress tracking."
                ),
                ephemeral=True,
            )

        await ctx.followup.send(
            embed=self._ok_embed(
                "Starting message sync",
                f"Cloning all messages in {clone_channel.mention} (oldest → newest)…\n"
                f"New messages in this channel will be queued and forwarded after the sync is finished.",
            ),
            ephemeral=True,
        )

        try:
            await self.bot.ws_manager.send(
                {"type": "clone_messages", "data": {"channel_id": original_id}}
            )
        except Exception as e:
            logger.exception(
                "[⛔] Failed to send WS backfill request for %s: %s", original_id, e
            )
            try:
                receiver.backfill.unmark_backfill(original_id)
            except AttributeError:
                receiver.backfill._flags.discard(original_id)
            try:
                await receiver.backfill.clear_sink(original_id)
            except Exception:
                pass
            try:
                await receiver.backfill.end_global_sync(original_id)
            except Exception:
                pass

            return await ctx.followup.send(
                embed=self._err_embed(
                    "Client Unreachable",
                    "I couldn’t contact the client to start the backfill.",
                ),
                ephemeral=True,
            )

    @commands.slash_command(
        name="start_member_export",
        description="Fully scrape the host server and export server members",
        guild_ids=[GUILD_ID],
    )
    async def start_member_export(
        self,
        ctx: discord.ApplicationContext,
        include_names: bool = Option(
            bool, "Include metadata (id, username, bot)", default=False
        ),
        num_sessions: int = Option(
            int,
            "**CAUTION**: more sessions = possibly higher ban risk",
            required=False,
            default=1,
            min_value=1,
            max_value=5,
        ),
    ):
        await ctx.defer(ephemeral=True)

        gid = ctx.guild_id
        if gid in self._export_jobs:
            desc = "Use `/cancel_member_export` to stop it."
            return await ctx.followup.send(
                embed=self._ok_embed(
                    "Export already running", desc, show_timestamp=False
                ),
                ephemeral=True,
            )

        async def runner():
            try:
                await self.export.background_finish_and_dm(
                    user_id=ctx.user.id,
                    include_names=bool(include_names),
                    num_sessions=int(num_sessions),
                )
            except asyncio.CancelledError:
                pass
            finally:
                self._export_jobs.pop(gid, None)

        task = asyncio.create_task(runner(), name=f"member-export:{gid}")
        self._export_jobs[gid] = ExportJob(
            task=task,
            user_id=ctx.user.id,
            include_names=bool(include_names),
            started_at=time.monotonic(),
        )

        d = "Make sure your DMs are open—I’ll send the results there when they’re ready."
        f = "This may take a while on large servers. You can stop early with /cancel_member_export."
        await ctx.followup.send(
            embed=self._ok_embed(
                "Member scrape started", d, footer=f, show_timestamp=False
            ),
            ephemeral=True,
        )

    @commands.slash_command(
        name="cancel_member_export",
        description="Cancel the running member export and DM whatever has been gathered so far",
        guild_ids=[GUILD_ID],
    )
    async def cancel_member_export(self, ctx: discord.ApplicationContext):
        await ctx.defer(ephemeral=True)

        gid = ctx.guild_id
        job = self._export_jobs.get(gid)  # don't pop yet
        logger.info(
            f"[export] cancel_member_export guild={gid} job_present={bool(job)}"
        )

        include_names = bool(getattr(job, "include_names", False))

        snapshot = []
        try:
            resp = await self.bot.ws_manager.request(
                {"type": "scrape_snapshot", "data": {"include_names": include_names}},
                timeout=12.0,
            )
            if resp:
                if "data" in resp and resp.get("data"):
                    snapshot = resp["data"].get("members") or []
                    logger.info(f"[export] snapshot_inline count={len(snapshot)}")
                elif "stream" in resp and resp["stream"]:
                    s = resp["stream"]
                    logger.info(
                        f"[export] snapshot_stream sid={s.get('id')} size={s.get('size')} chunk={s.get('chunk_size')}"
                    )
                    data = await self.export.consume_stream(
                        s["id"],
                        0,
                        int(s.get("chunk_size", self.export.default_chunk_size)),
                        timeout=15.0,
                    )
                    snapshot = data.get("members") or []
                    logger.info(f"[export] snapshot_stream_done count={len(snapshot)}")
            else:
                logger.info("[export] snapshot_empty_response")
        except Exception as e:
            logger.info(f"[export] snapshot_failed: {e!r}")

        try:
            ack = await self.bot.ws_manager.request({"type": "scrape_cancel"})
            logger.debug(f"[export] cancel_ack={ack}")
        except Exception as e:
            logger.debug(f"[export] cancel_send_failed: {e!r}")

        if job:
            try:
                job.task.cancel()
                logger.debug("[export] server_task_cancelled")
            except Exception as e:
                logger.debug(f"[export] server_task_cancel_error: {e!r}")
            try:
                await asyncio.wait_for(job.task, timeout=5.0)
                logger.debug("[export] server_task_await_done")
            except Exception as e:
                logger.debug(f"[export] server_task_await_timeout_or_error: {e!r}")
            self._export_jobs.pop(gid, None)

        if snapshot:
            try:
                filename, payload, summary = self.export.build_member_export_payload(
                    snapshot, include_names
                )
                elapsed = None
                try:
                    elapsed = (
                        time.monotonic()
                        - float(getattr(job, "started_at", time.monotonic()))
                        if job
                        else None
                    )
                except Exception:
                    pass
                await self.export.dm_export_file(
                    (job.user_id if job else ctx.user.id),
                    filename=filename,
                    payload=payload,
                    summary=summary,
                    duration_secs=elapsed,
                    total=len(snapshot),
                    include_names=include_names,
                )
                logger.info(f"[export] dm_sent count={len(snapshot)} file={filename}")
            except Exception as e:
                logger.info(f"[export] dm_failed: {e!r}")
            msg = f"Stopped early. I sent **{len(snapshot)}** members to your DMs."
        else:
            msg = "Stopped. If any data was gathered, you’ll receive a DM."

        try:
            ack = await self.bot.ws_manager.request({"type": "scrape_unblock"})
            logger.debug(f"[export] unblock_ack={ack}")
        except Exception as e:
            logger.debug(f"[export] unblock_failed: {e!r}")

        return await ctx.followup.send(
            embed=self._ok_embed("Member export canceled", msg, show_timestamp=False),
            ephemeral=True,
        )


def setup(bot: commands.Bot):
    bot.add_cog(CloneCommands(bot))
