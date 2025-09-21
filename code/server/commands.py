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
from discord.errors import NotFound, Forbidden, HTTPException
from discord import errors as discord_errors
from datetime import datetime, timezone
import time
import logging
import time
from common.config import Config
from common.db import DBManager
from server.rate_limiter import RateLimitManager, ActionType
from server.helpers import PurgeAssetHelper

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
            logger.debug(
                f"[⚙️] Commands permissions set for users: {self.allowed_users}"
            )

    async def _reply_or_dm(
        self,
        ctx: discord.ApplicationContext,
        content: str | None = None,
        *,
        embed: discord.Embed | None = None,
        ephemeral: bool = True,
        mention_on_channel_fallback: bool = True,
    ) -> None:
        """
        DM-first delivery:
        1) Try DM (uses bot token, no webhook expiry)
        2) If DM blocked, try interaction response/followup (ephemeral)
        3) If 401 (invalid/expired token) or other failure, try channel.send (optional @mention)
        4) Log if everything fails
        """
        user = getattr(ctx, "user", None) or getattr(ctx, "author", None)

        if user:
            try:
                if content and len(content) > 2000:
                    start = 0
                    while start < len(content):
                        end = min(start + 2000, len(content))
                        nl = content.rfind("\n", start, end)
                        if nl == -1 or nl <= start + 100:
                            nl = end
                        await user.send(
                            content[start:nl], embed=None if start else embed
                        )
                        start = nl
                else:
                    await user.send(content=content, embed=embed)
                return
            except (Forbidden, NotFound):
                pass

        try:
            if not ctx.response.is_done():
                await ctx.respond(content=content, embed=embed, ephemeral=ephemeral)
                return
            await ctx.followup.send(content=content, embed=embed, ephemeral=ephemeral)
            return
        except HTTPException as e:
            if getattr(e, "status", None) != 401:
                pass

        ch = getattr(ctx, "channel", None)
        if ch:
            try:
                prefix = (
                    f"{user.mention} " if (mention_on_channel_fallback and user) else ""
                )
                await ch.send(prefix + (content or ""), embed=embed)
                return
            except (Forbidden, NotFound):
                pass

        if hasattr(self, "log"):
            self.log.warning(
                "[_reply_or_dm] Failed to deliver via DM, followup, and channel."
            )
        else:
            logger.warning(
                "[_reply_or_dm] Failed to deliver via DM, followup, and channel."
            )

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
        name="announcement_trigger_add",
        description="Register a trigger: guild_id + keyword + user_id + optional channel_id",
        guild_ids=[GUILD_ID],
    )
    async def announcement_trigger(
        self,
        ctx: discord.ApplicationContext,
        guild_id: str = Option(
            str,
            "Guild ID to scope this trigger",
            required=True,
            min_length=17,
            max_length=20,
            regex=r"^\d{17,20}$",
        ),
        keyword: str = Option(str, "Keyword to trigger on", required=True),
        user_id: str = Option(
            str,
            "User ID to filter on (0=any user)",
            required=True,
            min_length=1,
            max_length=20,
        ),
        channel_id: str = Option(
            str,
            "Channel ID to listen in (0=any channel)",
            required=False,
            min_length=1,
            max_length=20,
        ),
    ):
        try:
            gid = int(guild_id)
            filter_id = int(user_id)
            chan_id = int(channel_id) if channel_id else 0
        except ValueError:
            return await ctx.respond("Invalid numeric IDs.", ephemeral=True)

        triggers = self.db.get_announcement_triggers(gid)
        existing = triggers.get(keyword, [])

        if chan_id == 0:
            for fid, cid in existing:
                if fid == filter_id and cid != 0:
                    self.db.remove_announcement_trigger(gid, keyword, filter_id, cid)

        if chan_id != 0 and (filter_id, 0) in existing:
            return await ctx.respond(
                embed=Embed(
                    title="Global Trigger Exists",
                    description=(
                        f"A global trigger for **{keyword}** (user `{filter_id}`) already exists."
                    ),
                    color=Color.blue(),
                ),
                ephemeral=True,
            )

        added = self.db.add_announcement_trigger(gid, keyword, filter_id, chan_id)
        who = "any user" if filter_id == 0 else f"user `{filter_id}`"
        where = "any channel" if chan_id == 0 else f"channel `#{chan_id}`"

        if not added:
            embed = Embed(
                title="Trigger Already Exists",
                description=f"[Guild: `{gid}`] **{keyword}** from {who} in {where} already exists.",
                color=Color.orange(),
            )
        else:
            title = (
                "Global Trigger Registered" if chan_id == 0 else "Trigger Registered"
            )
            desc = (
                f"[Guild: `{gid}`] Will announce **{keyword}** from {who} in {where}."
            )
            embed = Embed(title=title, description=desc, color=Color.green())

        await ctx.respond(embed=embed, ephemeral=True)

    @commands.slash_command(
        name="announce_subscription_toggle",
        description="Toggle a user's subscription to a keyword (or all) announcements in a guild",
        guild_ids=[GUILD_ID],
    )
    async def announcement_user(
        self,
        ctx: discord.ApplicationContext,
        guild_id: str = Option(
            str,
            "Guild ID to scope the subscription (0 = all guilds)",
            required=True,
            min_length=1,
            max_length=20,
        ),
        user: discord.User = Option(
            discord.User,
            "User to subscribe (defaults to you)",
            required=False,
        ),
        keyword: str = Option(
            str,
            "Keyword to subscribe to (leave empty to subscribe to all)",
            required=False,
        ),
    ):
        target = user or ctx.user
        sub_key = keyword or "*"

        try:
            gid = int(guild_id)
        except ValueError:
            return await ctx.respond("Invalid guild_id.", ephemeral=True)

        if sub_key == "*":
            self.db.conn.execute(
                "DELETE FROM announcement_subscriptions WHERE guild_id = ? AND user_id = ? AND keyword != '*'",
                (gid, target.id),
            )
            self.db.conn.commit()

        if sub_key != "*":
            rows = self.db.conn.execute(
                "SELECT 1 FROM announcement_subscriptions WHERE guild_id = ? AND user_id = ? AND keyword = '*'",
                (gid, target.id),
            ).fetchone()
            if rows:
                embed = Embed(
                    title="Already Subscribed to All",
                    description=f"{target.mention} already receives all announcements in this guild.",
                    color=Color.blue(),
                )
                return await ctx.respond(embed=embed, ephemeral=True)

        if self.db.add_announcement_user(gid, sub_key, target.id):
            action, color = "Subscribed", Color.green()
        else:
            self.db.remove_announcement_user(gid, sub_key, target.id)
            action, color = "Unsubscribed", Color.orange()

        embed = Embed(title="🔔 Subscription Updated", color=color)
        embed.add_field(name="Guild", value=str(gid), inline=True)
        embed.add_field(name="User", value=target.mention, inline=True)
        embed.add_field(name="Scope", value=sub_key, inline=True)
        embed.add_field(name="Action", value=action, inline=True)
        await ctx.respond(embed=embed, ephemeral=True)

    @commands.slash_command(
        name="announce_trigger_list",
        description="List/delete ALL announcement triggers across every guild",
        guild_ids=[GUILD_ID],
    )
    async def announcement_list(
        self,
        ctx: discord.ApplicationContext,
        delete: int = Option(int, "Index to delete", required=False, min_value=1),
    ):
        rows = self.db.get_all_announcement_triggers_flat()
        if not rows:
            return await ctx.respond("No announcement triggers found.", ephemeral=True)

        if delete is not None:
            if delete < 1 or delete > len(rows):
                return await ctx.respond("Invalid index.", ephemeral=True)

            r = rows[delete - 1]
            gid = int(r["guild_id"])
            kw = r["keyword"]
            fuid = int(r["filter_user_id"])
            cid = int(r["channel_id"])

            removed = self.db.remove_announcement_trigger(gid, kw, fuid, cid)

            still_has = self.db.conn.execute(
                "SELECT 1 FROM announcement_triggers WHERE guild_id = ? AND keyword = ? LIMIT 1",
                (gid, kw),
            ).fetchone()
            if not still_has:
                self.db.conn.execute(
                    "DELETE FROM announcement_subscriptions WHERE guild_id = ? AND keyword = ?",
                    (gid, kw),
                )
            self.db.conn.commit()

            if removed:
                who = "any user" if fuid == 0 else f"user `{fuid}`"
                where = "any channel" if cid == 0 else f"`#{cid}`"
                return await ctx.respond(
                    f"🗑️ Deleted: [Guild: `{gid}`] **{kw}** — {who}, {where}",
                    ephemeral=True,
                )
            else:
                return await ctx.respond("Nothing was deleted (row no longer exists).", ephemeral=True)

        subs_cache: dict[tuple[int, str], int] = {}

        def _subs_for(gid: int, kw: str) -> int:
            key = (gid, kw)
            if key not in subs_cache:
                user_ids = self.db.get_announcement_users(gid, kw)
                subs_cache[key] = len(set(int(u) for u in user_ids))
            return subs_cache[key]

        lines: list[str] = []
        for i, r in enumerate(rows, start=1):
            gid = int(r["guild_id"])
            kw = r["keyword"]
            fuid = int(r["filter_user_id"])
            cid = int(r["channel_id"])

            who = "any user" if fuid == 0 else f"user `{fuid}`"
            where = "any channel" if cid == 0 else f"`#{cid}`"

            subs = _subs_for(gid, kw)
            suffix = f" ({subs} subscriber{'s' if subs != 1 else ''})"
            lines.append(f"{i}. [Guild: `{gid}`] **{kw}** — {who}, {where}{suffix}")

        def _chunk_lines(xs: list[str], limit: int = 1024) -> list[str]:
            chunks, cur = [], ""
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

        embed = discord.Embed(
            title="📋 Announcement Triggers",
            description="Use `/announce_trigger_list delete:<index>` to delete a specific row.",
            color=discord.Color.blurple(),
        )
        for j, chunk in enumerate(_chunk_lines(lines)):
            embed.add_field(
                name="Triggers" if j == 0 else "Triggers (cont.)",
                value=chunk,
                inline=False,
            )

        await ctx.respond(embed=embed, ephemeral=True)

    @commands.slash_command(
        name="announce_subscription_list",
        description="List/delete ALL announcement subscriptions",
        guild_ids=[GUILD_ID],
    )
    async def announcement_subscriptions(
        self,
        ctx: discord.ApplicationContext,
        delete: int = Option(int, "Index to delete", required=False, min_value=1),
    ):
        rows = self.db.get_all_announcement_subscriptions_flat()
        if not rows:
            return await ctx.respond(
                "No announcement subscriptions found.", ephemeral=True
            )

        # Delete by flat index
        if delete is not None:
            idx = delete - 1
            if idx < 0 or idx >= len(rows):
                return await ctx.respond(
                    f"⚠️ Invalid index `{delete}`; pick 1–{len(rows)}.",
                    ephemeral=True,
                )

            r = rows[idx]
            gid = int(r["guild_id"])
            kw = r["keyword"]
            uid = int(r["user_id"])

            removed = self.db.remove_announcement_user(gid, kw, uid)
            if removed:
                who = f"<@{uid}> ({uid})"
                scope = f"[Guild: `{gid}`] **{kw}**"
                return await ctx.respond(
                    f"🗑️ Deleted subscription: {scope} — {who}", ephemeral=True
                )
            else:
                return await ctx.respond(
                    "Nothing was deleted (row no longer exists).", ephemeral=True
                )

        # Build the list (chunked to fit embed field limits)
        lines: list[str] = []
        for i, r in enumerate(rows, start=1):
            gid = int(r["guild_id"])
            kw = r["keyword"]
            uid = int(r["user_id"])
            lines.append(f"{i}. [Guild: `{gid}`] **{kw}** — <@{uid}> ({uid})")

        def _chunk_lines(xs: list[str], limit: int = 1024) -> list[str]:
            chunks, cur = [], ""
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

        embed = discord.Embed(
            title="🔔 Announcement Subscriptions",
            description="Use `/announce_subscription_list delete:<index>` to delete a specific row.",
            color=discord.Color.green(),
        )
        for j, chunk in enumerate(_chunk_lines(lines)):
            embed.add_field(
                name="Subscriptions" if j == 0 else "Subscriptions (cont.)",
                value=chunk,
                inline=False,
            )

        await ctx.respond(embed=embed, ephemeral=True)

    @commands.slash_command(
        name="announce_help",
        description="How to use the announcement trigger & subscription commands",
        guild_ids=[GUILD_ID],
    )
    async def announce_help(self, ctx: discord.ApplicationContext):
        def spacer():
            # Zero-width space section divider
            embed.add_field(name="\u200b", value="\u200b", inline=False)

        embed = discord.Embed(
            title="🧭 Announcements — Help",
            description=(
                "Set up **triggers** that fire on messages, and **subscriptions** for who should be notified.\n"
                "IDs are raw numbers. Use `0` to mean **global** (any user / any channel / any guild*).\n"
            ),
            color=discord.Color.purple(),
        )

        embed.add_field(
            name="Basics",
            value=(
                "• `guild_id`: server ID — `0` = *all guilds*.\n"
                "• `user_id`: `0` = *any user*.\n"
                "• `channel_id`: `0` = *any channel*.\n"
                "• Use the `delete:` option on list commands to remove by **index**.\n"
            ),
            inline=False,
        )

        spacer()

        embed.add_field(
            name="🟢 Add a Trigger",
            value=(
                "**/announcement_trigger_add**\n"
                "Register: `guild_id + keyword + user_id [+ channel_id]`\n\n"
                "**Examples**\n"
                "```\n"
                "/announcement_trigger_add guild_id:0 keyword:long user_id:123456787654321\n"
                "/announcement_trigger_add guild_id:123456789012345678 keyword:short user_id:123456789 channel_id:987654321098765432\n"
                "```\n"
            ),
            inline=False,
        )

        spacer()

        embed.add_field(
            name="📋 List/Delete Triggers",
            value=(
                "**/announce_trigger_list**\n"
                "Shows all triggers across every guild.\n\n"
                "**Delete by index**\n"
                "```\n"
                "/announce_trigger_list delete:3\n"
                "```\n"
            ),
            inline=False,
        )

        spacer()

        embed.add_field(
            name="🔔 Toggle Subscription",
            value=(
                "**/announce_subscription_toggle**\n"
                "Subscribe/unsubscribe a user to a keyword (or all) in a guild.\n\n"
                "**Examples**\n"
                "```\n"
                "/announce_subscription_toggle guild_id:0 keyword:lol\n"
                "/announce_subscription_toggle guild_id:123456789012345678 keyword:* user:@SomeUser\n"
                "```\n"
            ),
            inline=False,
        )

        spacer()

        embed.add_field(
            name="📬 List/Delete Subscriptions",
            value=(
                "**/announce_subscription_list**\n"
                "Shows all subscriptions.\n\n"
                "**Delete by index**\n"
                "```\n"
                "/announce_subscription_list delete:7\n"
                "```\n"
            ),
            inline=False,
        )

        spacer()

        embed.add_field(
            name="Notes",
            value=(
                "• Deleting the *last* trigger for a keyword in a guild also removes its subscriptions for that keyword/guild.\n"
                "• Matching: whole word, emoji name (`<:name:ID>`/`<a:name:ID>`), or substring fallback.\n"
                "• Get IDs via **Developer Mode** → right-click → *Copy ID*.\n"
            ),
            inline=False,
        )

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
        kind: str = Option(
            str,
            "What to delete",
            required=True,
            choices=["emojis", "stickers", "roles"],
        ),
        confirm: str = Option(
            str, "Type 'confirm' to run this DESTRUCTIVE action", required=True
        ),
        unmapped_only: bool = Option(
            bool,
            "Only delete assets that are NOT mapped in the DB",
            required=False,
            default=False,
        ),
    ):
        await ctx.defer(ephemeral=True)

        if (confirm or "").strip().lower() != "confirm":
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Confirmation required",
                    "Re-run the command and type **confirm** to proceed.",
                ),
                ephemeral=True,
            )

        helper = PurgeAssetHelper(self)
        guild = ctx.guild
        if not guild:
            return await ctx.followup.send("No guild context.", ephemeral=True)

        RL_EMOJI = getattr(ActionType, "EMOJI", "EMOJI")
        RL_STICKER = getattr(ActionType, "STICKER", "STICKER")
        RL_ROLE = getattr(ActionType, "ROLE", "ROLE")

        deleted = skipped = failed = 0
        deleted_ids: list[int] = []

        await ctx.followup.send(
            embed=self._ok_embed(
                "Starting purge…",
                f"Target: `{kind}`\nMode: `{'unmapped_only' if unmapped_only else 'ALL'}`\nI'll DM you when finished.",
            ),
            ephemeral=True,
        )
        helper._log_purge_event(
            kind=kind,
            outcome="begin",
            guild_id=guild.id,
            user_id=ctx.user.id,
            reason=f"Manual purge (mode={'unmapped_only' if unmapped_only else 'all'})",
        )

        def _is_mapped(kind_name: str, cloned_id: int) -> bool:
            if kind_name == "emojis":
                row = self.db.conn.execute(
                    "SELECT 1 FROM emoji_mappings WHERE cloned_emoji_id=? LIMIT 1",
                    (int(cloned_id),),
                ).fetchone()
                return bool(row)
            if kind_name == "stickers":
                row = self.db.conn.execute(
                    "SELECT 1 FROM sticker_mappings WHERE cloned_sticker_id=? LIMIT 1",
                    (int(cloned_id),),
                ).fetchone()
                return bool(row)
            if kind_name == "roles":
                row = self.db.conn.execute(
                    "SELECT 1 FROM role_mappings WHERE cloned_role_id=? LIMIT 1",
                    (int(cloned_id),),
                ).fetchone()
                return bool(row)
            return False

        try:
            # =============================
            # EMOJIS
            # =============================
            if kind == "emojis":
                for em in list(guild.emojis):
                    if unmapped_only and _is_mapped("emojis", em.id):
                        skipped += 1
                        helper._log_purge_event(
                            "emojis",
                            "skipped",
                            guild.id,
                            ctx.user.id,
                            em.id,
                            em.name,
                            "Unmapped-only mode: mapped in DB",
                        )
                        continue
                    try:
                        await self.ratelimit.acquire(RL_EMOJI)
                        await em.delete(reason=f"Purge by {ctx.user.id}")
                        deleted += 1
                        deleted_ids.append(int(em.id))
                        helper._log_purge_event(
                            "emojis",
                            "deleted",
                            guild.id,
                            ctx.user.id,
                            em.id,
                            em.name,
                            "Manual purge",
                        )
                    except discord.Forbidden as e:
                        skipped += 1
                        helper._log_purge_event(
                            "emojis",
                            "skipped",
                            guild.id,
                            ctx.user.id,
                            em.id,
                            em.name,
                            f"Manual purge: {e}",
                        )
                    except Exception as e:
                        failed += 1
                        helper._log_purge_event(
                            "emojis",
                            "failed",
                            guild.id,
                            ctx.user.id,
                            em.id,
                            em.name,
                            f"Manual purge: {e}",
                        )

                if unmapped_only:
                    if deleted_ids:
                        placeholders = ",".join("?" * len(deleted_ids))
                        self.db.conn.execute(
                            f"DELETE FROM emoji_mappings WHERE cloned_emoji_id IN ({placeholders})",
                            (*deleted_ids,),
                        )
                        self.db.conn.commit()
                else:
                    self.db.conn.execute("DELETE FROM emoji_mappings")
                    self.db.conn.commit()

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
                    if unmapped_only and _is_mapped("stickers", st.id):
                        skipped += 1
                        helper._log_purge_event(
                            "stickers",
                            "skipped",
                            guild.id,
                            ctx.user.id,
                            st.id,
                            st.name,
                            "Unmapped-only mode: mapped in DB",
                        )
                        continue
                    try:
                        await self.ratelimit.acquire(RL_STICKER)
                        await st.delete(reason=f"Purge by {ctx.user.id}")
                        deleted += 1
                        deleted_ids.append(int(st.id))
                        helper._log_purge_event(
                            "stickers",
                            "deleted",
                            guild.id,
                            ctx.user.id,
                            st.id,
                            st.name,
                            "Manual purge",
                        )
                    except discord.Forbidden as e:
                        skipped += 1
                        helper._log_purge_event(
                            "stickers",
                            "skipped",
                            guild.id,
                            ctx.user.id,
                            st.id,
                            st.name,
                            f"Manual purge: {e}",
                        )
                    except Exception as e:
                        failed += 1
                        helper._log_purge_event(
                            "stickers",
                            "failed",
                            guild.id,
                            ctx.user.id,
                            st.id,
                            st.name,
                            f"Manual purge: {e}",
                        )

                if unmapped_only:
                    if deleted_ids:
                        placeholders = ",".join("?" * len(deleted_ids))
                        self.db.conn.execute(
                            f"DELETE FROM sticker_mappings WHERE cloned_sticker_id IN ({placeholders})",
                            (*deleted_ids,),
                        )
                        self.db.conn.commit()
                else:
                    self.db.conn.execute("DELETE FROM sticker_mappings")
                    self.db.conn.commit()

            # =============================
            # ROLES
            # =============================
            elif kind == "roles":
                me, top_role, roles = await helper._resolve_me_and_top(guild)
                if not me or not top_role:
                    return await ctx.followup.send(
                        embed=self._err_embed(
                            "Top role not found",
                            "Could not resolve my top role. Try again later.",
                        ),
                        ephemeral=True,
                    )
                if not me.guild_permissions.manage_roles:
                    return await ctx.followup.send(
                        embed=self._err_embed(
                            "Missing permission", "I need **Manage Roles**."
                        ),
                        ephemeral=True,
                    )

                def _undeletable(r: discord.Role) -> bool:
                    prem = getattr(r, "is_premium_subscriber", None)
                    return bool(
                        r.is_default()
                        or r.managed
                        or (prem() if callable(prem) else False)
                    )

                eligible = [r for r in roles if not _undeletable(r) and r < top_role]
                for role in sorted(eligible, key=lambda r: r.position):
                    if unmapped_only and _is_mapped("roles", role.id):
                        skipped += 1
                        helper._log_purge_event(
                            "roles",
                            "skipped",
                            guild.id,
                            ctx.user.id,
                            role.id,
                            role.name,
                            "Unmapped-only mode: mapped in DB",
                        )
                        continue
                    try:
                        await self.ratelimit.acquire(RL_ROLE)
                        await role.delete(reason=f"Purge by {ctx.user.id}")
                        deleted += 1
                        deleted_ids.append(int(role.id))
                        helper._log_purge_event(
                            "roles",
                            "deleted",
                            guild.id,
                            ctx.user.id,
                            role.id,
                            role.name,
                            "Manual purge",
                        )
                    except discord.Forbidden as e:
                        skipped += 1
                        helper._log_purge_event(
                            "roles",
                            "skipped",
                            guild.id,
                            ctx.user.id,
                            role.id,
                            role.name,
                            f"Manual purge: {e}",
                        )
                    except Exception as e:
                        failed += 1
                        helper._log_purge_event(
                            "roles",
                            "failed",
                            guild.id,
                            ctx.user.id,
                            role.id,
                            role.name,
                            f"Manual purge: {e}",
                        )

                if unmapped_only:
                    if deleted_ids:
                        placeholders = ",".join("?" * len(deleted_ids))
                        self.db.conn.execute(
                            f"DELETE FROM role_mappings WHERE cloned_role_id IN ({placeholders})",
                            (*deleted_ids,),
                        )
                        self.db.conn.commit()
                else:
                    self.db.conn.execute("DELETE FROM role_mappings")
                    self.db.conn.commit()

            # =============================
            # Finalize
            # =============================
            summary = (
                f"**Target:** `{kind}`\n"
                f"**Mode:** `{'unmapped_only' if unmapped_only else 'ALL'}`\n"
                f"**Deleted:** {deleted}\n**Skipped:** {skipped}\n**Failed:** {failed}"
            )
            color = discord.Color.green() if failed == 0 else discord.Color.orange()
            await self._reply_or_dm(
                ctx,
                embed=self._ok_embed("Purge complete", summary, color=color),
                ephemeral=True,
                mention_on_channel_fallback=True,
            )

        except Exception as e:
            err_text = f"{type(e).__name__}: {e}"
            await self._reply_or_dm(
                ctx,
                embed=self._err_embed("Purge failed", err_text),
                ephemeral=True,
                mention_on_channel_fallback=True,
            )

    @commands.slash_command(
        name="role_block",
        description="Block a role from being cloned/updated. Provide either the cloned role (picker) or its ID.",
        guild_ids=[GUILD_ID],
    )
    async def role_block(
        self,
        ctx: discord.ApplicationContext,
        role: discord.Role = Option(
            discord.Role, "Pick the CLONED role to block", required=False
        ),
        role_id: str = Option(
            str, "CLONED role ID to block", required=False, min_length=17, max_length=20
        ),
    ):
        """
        Adds a role to the block list using its original_role_id from the DB.
        If a clone exists, it is deleted and its mapping removed to enforce the block.
        """
        await ctx.defer(ephemeral=True)

        if not role and not role_id:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Missing input", "Provide either a role selection or a role ID."
                ),
                ephemeral=True,
            )
        if role and role_id:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Too many inputs", "Provide only one of: role OR role_id."
                ),
                ephemeral=True,
            )

        # Resolve cloned role id
        try:
            cloned_id = int(role.id if role else role_id)
        except ValueError:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Invalid ID", f"`{role_id}` is not a valid numeric role ID."
                ),
                ephemeral=True,
            )

        # Find original via mapping
        mapping = self.db.get_role_mapping_by_cloned_id(cloned_id)
        if not mapping:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Mapping not found",
                    "I couldn't find a role mapping for that cloned role. Make sure this role was created by Copycord.",
                ),
                ephemeral=True,
            )

        original_role_id = int(mapping["original_role_id"])
        original_role_name = mapping["original_role_name"]

        newly_added = self.db.add_role_block(original_role_id)
        # Enforce the block immediately: delete clone if present and remove mapping
        g = ctx.guild
        cloned_obj = g.get_role(cloned_id) if g else None

        # Try to delete if safe
        deleted = False
        if cloned_obj:
            me = g.me if g else None
            bot_top = me.top_role.position if me and me.top_role else 0
            if (
                (not cloned_obj.is_default())
                and (not cloned_obj.managed)
                and cloned_obj.position < bot_top
            ):
                try:
                    await self.ratelimit.acquire(ActionType.ROLE)
                    await cloned_obj.delete(reason=f"Blocked by {ctx.user.id}")
                    deleted = True
                except Exception as e:
                    logger.warning(
                        "[role_block] Failed deleting role %s (%d): %s",
                        getattr(cloned_obj, "name", "?"),
                        cloned_id,
                        e,
                    )

        # Always drop the mapping so it won't be updated/re-created
        self.db.delete_role_mapping(original_role_id)

        if newly_added:
            title = "Role Blocked"
            desc = (
                f"**{original_role_name}** (`orig:{original_role_id}`) is now blocked.\n"
                f"{'🗑️ Deleted cloned role.' if deleted else '↩️ No clone deleted (not found / not permitted).'}\n"
                "It will be skipped during future role syncs."
            )
            color = discord.Color.green()
        else:
            title = "Role Already Blocked"
            desc = (
                f"**{original_role_name}** (`orig:{original_role_id}`) was already on the block list.\n"
                f"{'🗑️ Deleted cloned role.' if deleted else '↩️ No clone deleted (not found / not permitted).'}"
            )
            color = discord.Color.blurple()

        await ctx.followup.send(
            embed=self._ok_embed(title, desc, color=color), ephemeral=True
        )

    @commands.slash_command(
        name="role_block_clear",
        description="Clear the entire role block list (allows previously blocked roles to be synced again).",
        guild_ids=[GUILD_ID],
    )
    async def role_block_clear(self, ctx: discord.ApplicationContext):
        """
        Clears all entries in the role block list.
        This does NOT recreate any roles automatically; it only removes the block entries.
        Future role syncs may recreate those roles if they exist on the source.
        """
        await ctx.defer(ephemeral=True)

        try:
            removed = self.db.clear_role_blocks()
            if removed == 0:
                return await ctx.followup.send(
                    embed=self._ok_embed(
                        "Role Block List", "The block list is already empty."
                    ),
                    ephemeral=True,
                )

            await ctx.followup.send(
                embed=self._ok_embed(
                    "Role Block List Cleared",
                    f"Removed **{removed}** entr{'y' if removed == 1 else 'ies'} from the role block list.\n"
                    "Previously blocked roles may be recreated on the next role sync if they still exist on the source.",
                ),
                ephemeral=True,
            )
        except Exception as e:
            await ctx.followup.send(
                embed=self._err_embed(
                    "Failed to Clear Block List",
                    f"An error occurred while clearing the role block list:\n`{e}`",
                ),
                ephemeral=True,
            )

    @commands.slash_command(
        name="export_dms",
        description="Export DM history for a given user and stream to a webhook (server forwards).",
        guild_ids=[GUILD_ID],
    )
    async def export_dm_history_cmd(
        self,
        ctx: discord.ApplicationContext,
        user_id: str = Option(str, "Target user ID to export DMs from", required=True),
        webhook_url: str = Option(
            str, "Webhook URL to receive the stream", required=True
        ),
        json_file: bool = Option(
            bool,
            "Also save a JSON snapshot (default: true)",
            required=False,
            default=False,
        ),
    ):
        await ctx.defer(ephemeral=True)

        try:
            target_id = int(user_id)
        except ValueError:
            return await ctx.followup.send(
                embed=self._err_embed(
                    "Invalid User ID", f"`{user_id}` is not a valid user ID."
                ),
                ephemeral=True,
            )

        payload = {
            "type": "export_dm_history",
            "data": {
                "user_id": target_id,
                "webhook_url": webhook_url,
                "json_file": json_file,
            },
        }

        try:
            resp = await self.bot.ws_manager.request(payload)

            if not resp or not resp.get("ok"):
                err = (resp or {}).get("error") or "Client did not accept the request."
                if err == "dm-export-in-progress":
                    return await ctx.followup.send(
                        embed=self._err_embed(
                            "Export Already Running",
                            "A DM export is currently in progress. Please wait until it finishes before starting another.",
                        ),
                        ephemeral=True,
                    )
                return await ctx.followup.send(
                    embed=self._err_embed("Export Rejected", err),
                    ephemeral=True,
                )

        except Exception as e:
            return await ctx.followup.send(
                embed=self._err_embed("Export Failed", f"WebSocket request error: {e}"),
                ephemeral=True,
            )

        return await ctx.followup.send(
            embed=self._ok_embed(
                "Export Started",
                f"Streaming DMs for user `{target_id}` → webhook. "
                + (
                    "A JSON snapshot will also be saved."
                    if json_file
                    else "JSON snapshot is disabled."
                ),
            ),
            ephemeral=True,
        )


def setup(bot: commands.Bot):
    bot.add_cog(CloneCommands(bot))
