# Copycord v4.0.0: discord.py-self Usage Inventory

Status: planning artifact for v4.0.0. No code has changed. Line numbers are
against main at 914232b (v3.28.0).

## How to read this

Copycord uses two different Discord libraries today:

- **client role** uses `discord.py-self==2.1.0` (dolfies' selfbot fork). This
  drives a **user account** over the gateway and REST. This is what v4.0.0
  removes.
- **server role** uses `py-cord==2.7.2` (a real bot library) for a **bot
  account** that builds the mirror server. This is a legitimate bot and is out
  of scope for removal. See the recommendation at the end.

The client requirement and the server requirement conflict on purpose: the two
packages both own the top-level `discord` import name and cannot coexist in one
interpreter. That is why the Dockerfile builds three separate venvs
(`code/dockerfile:33-35`) and why `tests/requirements-test.txt` carries the
warning "client tests must not import `discord`".

Totals found by `git grep`:

- `import discord` / `from discord ...` statements in `code/`: 23 (spread over
  11 files).
- Files importing discord: 6 client files, 6 server files (plus 3 test files).
- Qualified `discord.<name>` references in `code/`: client 94, server ~346.
- Duck-typed `getattr(obj, "<discord-attr>", ...)` accesses in `code/client`:
  ~660 (these do not name `discord` but depend on the shape of discord.py-self
  model objects, so the replacement model layer has to reproduce them).

Scope of this doc: the **client** surface, which v4.0.0 replaces. The server
surface is listed once at the end for completeness and left on py-cord.

---

## 1. Client bootstrap, gateway lifecycle, connection management

File: `code/client/client.py`

| Line(s) | discord.py-self API | What it does | v4 replacement |
|---|---|---|---|
| 22-28 | `import discord`, `ChannelType, ForumChannel, MessageType`, `commands`, `ConnectionClosed, LoginFailure` | module imports | `selfclient` package: own enums/constants, own `ConnectionClosed`, own `LoginFailure` |
| 50 | `from discord.client import _loop as _DISCORD_LOOP_SENTINEL` | internal loop sentinel for `clear()` reset | gone; own client owns its lifecycle, no sentinel needed |
| 84-93, 166 | `logging.getLogger("discord.*")` + `_HandledReconnectFilter` | silence library loggers, drop proxy retry tracebacks | our gateway logs at the level we choose; filter becomes unnecessary |
| 217-219 | `commands.Bot(command_prefix="!", self_bot=True, proxy=...)` | the entire client object | `selfclient.Client(token, proxy=..., intents/capabilities=...)` |
| 232-254 | `self.bot.event(self.on_*)` x23 | register gateway event handlers | `client.on("MESSAGE_CREATE", handler)` dispatch table (see wizard `gateway.py:222`) |
| 317 | `self.bot.close()` | shutdown from signal handler | `await client.close()` |
| 770, 803 | `discord.Forbidden` raise/catch | permission errors | `selfclient.errors.Forbidden` |
| 976-999 | `message.type == MessageType.thread_created / channel_name_change`, `isinstance(ch, discord.Thread)` | ignore-rules on system messages | message type ints (THREAD_CREATED=18, CHANNEL_NAME_CHANGE=4); thread detection via channel-type index (wizard `gateway.py:288 is_thread`) |
| 1059-1070 | `on_message(message: discord.Message)` | primary event, forwards to `forwarding.handle_new_message` | handler receives the raw MESSAGE_CREATE `data` dict; model wrapper rebuilds a `Message` view |
| 1183-1184, 1256, 1383-1384, 1582-1583, 1676-1677, 1741-1742 | `ChannelType.public_thread/private_thread/forum`, `isinstance(parent, ForumChannel)` | thread/forum classification across edit/delete handlers | channel-type ints + parent lookup (wizard `is_forum_post`) |
| 1282-1498 | `on_message_edit`, `on_raw_message_edit(payload: discord.RawMessageUpdateEvent)` | edits + raw edits (thread detection) | MESSAGE_UPDATE dispatch; raw payload is already the dict we get |
| 1663-1746 | `on_message_delete`, `on_raw_message_delete(payload: discord.RawMessageDeleteEvent)` | deletes | MESSAGE_DELETE dispatch |
| 1790-1847 | `on_thread_delete/update(thread: discord.Thread)`, `isinstance(after.parent, ForumChannel)` | thread lifecycle | THREAD_DELETE/THREAD_UPDATE dispatch; index maintained in gateway |
| 1878-1912 | `on_guild_channel_create/delete/update(channel: discord.abc.GuildChannel)` | channel lifecycle | CHANNEL_CREATE/DELETE/UPDATE dispatch |
| 1977-2004 | `isinstance(after, discord.VoiceChannel)`, `isinstance(after, ForumChannel)`, `ChannelType.forum` | voice/forum branches in channel update | channel-type ints |
| 2093-2115 | `isinstance(val, (discord.Emoji, discord.PartialEmoji))` | forum default-reaction emoji normalization | inspect raw emoji dict (`id`/`name`) |
| 2236-2286 | `on_guild_role_*`, `on_guild_join/remove/update(... discord.Role/Guild)` | role + guild lifecycle | GUILD_ROLE_*/GUILD_CREATE/DELETE/UPDATE dispatch |
| 2301-2329 | `_asset_hash_from_attr(g: discord.Guild, attr)` | icon/banner/splash hash extraction | read hashes off the raw guild dict |
| 2366-2398 | `on_member_join(member: discord.Member)`, `member.guild`, `member.display_avatar.url` | join DM/role trigger | GUILD_MEMBER_ADD dispatch; build avatar URL from `user.avatar` hash |
| 2400-2423 | `on_guild_emojis_update`, `on_guild_stickers_update`, `_guild_row_from_obj(g: discord.Guild)` | emoji/sticker/guild row snapshots | GUILD_EMOJIS_UPDATE / GUILD_STICKERS_UPDATE dispatch |
| 2570, 2905-2953, 3023 | `LoginFailure`, `ConnectionClosed` (code 4004) | login/close error handling + backup token rotation | own exceptions; close codes already mapped in wizard `gateway.py:373-389` |
| 2591-2595 | `on_connect` sets gateway-ready event | detect READY | own gateway sets the event on READY dispatch |
| 2629-2651 | `_switch_to_proxy`: set `bot.http.proxy`, `bot.ws.close(code=4000)` | hot proxy switch on live socket | own gateway exposes `set_proxy()` + `reconnect(code=4000)`; wizard rotates on reconnect (`gateway.py:259`) |
| 2694-2732 | `_resolve_client_build` + `refresh_build_info` | fetch live build before login | keep `common/selfbot_headers.refresh_build_info`; feed build into our IDENTIFY props |
| 2734-2793 | `_apply_client_build`: monkeypatch `discord.utils.Headers.default`, build super-properties, set `bot.http.headers` | force the library to use our fingerprint | **deleted entirely.** This whole monkeypatch exists only to bend discord.py-self to our fingerprint. Our client builds IDENTIFY props and REST headers directly from `common/selfbot_headers` (already the source of truth) |
| 2795-2893 | `_start_with_watchdog`, `_abort_stalled_connect`, `_clear_bot_state`, `_reset_bot_for_retry` | watchdog around `bot.start()`, teardown of half-open connects, library state reset (`bot._ready`, `bot.clear()`, `bot._closing_task`, `bot.loop`, `http._started`) | mostly **deleted.** These work around discord.py-self's connect loop and private state. Our gateway's own `run()` loop (wizard `gateway.py:225-270`) has reconnect/backoff built in; a connect timeout is a timeout on our own connect coroutine |
| 2736, 2784 | `discord.utils.Headers` | header object | gone (we send headers as a plain dict) |

Notes:
- `self.bot.http.proxy` is set at 387, 872, 2641, 2719, 3019. All become
  `client.set_proxy(...)` / gateway reconnect.
- `self.bot.guilds`, `get_guild`, `get_channel`, `fetch_channel`, `is_closed`,
  `is_ready`, `wait_until_ready`, `start`, `close` are the public client surface
  our `selfclient.Client` must provide (see model/client table below).

## 2. Sitemap scraping (guild/channel/role/emoji/sticker/thread serialization)

File: `code/client/sitemap.py`

| Line(s) | API | What it does | v4 replacement |
|---|---|---|---|
| 41-42, 111, 173, 406, 462 | `self.bot.guilds`, `self.bot.get_guild(id)` | pick/resolve the guild to clone | gateway caches full guild objects from READY/GUILD_CREATE (wizard `channels_of`, `_guild_channels`); expose `client.get_guild(id)` returning a model wrapper |
| 239, 252 | `discord.RateLimited`, `discord.HTTPException` | 429/HTTP handling while building | own `RateLimited`/`HTTPException` with `.retry_after`, `.status`, `.json` |
| 571, 640, 692 | `ForumChannel` default reaction, `discord.VideoQualityMode`, `discord.StickerType.guild` | serialize forum/voice/sticker attrs | enum int constants in `selfclient.models` |
| 686, 2547, 2554 | `guild.fetch_stickers()`, `guild.fetch_emojis()` | REST fetch of emojis/stickers | `client.http.get(f"/guilds/{id}/stickers")` etc (raw dicts; READY often already carries them) |
| 741-760 | `guild._state.http.get_roles(guild.id)` | **private-API** raw role payload for gradient/holographic colors the library cannot parse | `client.http.get(f"/guilds/{id}/roles")` -> raw list (our own HTTP returns raw dicts, so no need to reach around a model layer) |
| 852-959 | `isinstance(ch, discord.TextChannel/VoiceChannel/StageChannel)` | channel-type dispatch during serialization | channel-type ints |
| 1040-1043, 1278-1298 | `self.bot.fetch_channel`, `isinstance(thr, discord.Thread/CategoryChannel)` | thread/category classification | fetch returns raw dict; classify by `type` |
| 1330 | `obj: discord.abc.GuildChannel` + `_serialize_role_overwrites` | permission overwrites serialization | iterate raw `permission_overwrites` list from the channel dict |
| 1361, 1407 | `thr: discord.Thread`, `before/after: discord.Role` | type hints | model wrappers |

The sitemap is the single biggest consumer of discord.py-self model objects. It
reads dozens of attributes off `Guild`, `Role`, every channel type, `Thread`,
and forum `available_tags`. Because the gateway already delivers all of these as
raw JSON (wizard proves READY/GUILD_CREATE carry the full channel list,
`gateway.py:313-319`), the cleanest replacement is for `selfclient.models` to
provide thin attribute-compatible wrappers over those raw dicts, so the sitemap
keeps reading `g.name`, `role.color`, `ch.topic` with minimal edits.

## 3. Message serialization and mention resolution

File: `code/client/message_utils.py`

| Line(s) | API | What it does | v4 replacement |
|---|---|---|---|
| 33 | `bot: discord.Client` | constructor type | `selfclient.Client` |
| 37-76 | `g.get_member(uid)`, `g.fetch_member(uid)`, `self.bot.fetch_user(uid)` | resolve mentions to names | member cache from gateway + `client.http.get(f"/users/{id}")` / guild member endpoint |
| 93, 101, 311 | `isinstance(m, Member)`, `guild.get_member(int(uid))` | member vs user naming | model wrapper + member cache |
| 207-280 | `serialize(message: discord.Message)`, `_build_role_mentions_payload`, `author_role_ids` | the core message -> dict serializer | operate on the MESSAGE_CREATE dict directly (it already has `mentions`, `mention_roles`, `attachments`, `embeds`, `message_reference`, `message_snapshots`); keep a `Message` wrapper so existing attribute reads (`message.author`, `.system_content`, `.reference`) still work |
| 345-430 | `Snapshot` shim, `display_avatar` | forward-snapshot author shim | keep the shim; it is already hand-rolled, just drop the discord typing |
| 460-551 | `bot.http.token`, `build_headers(token)`, raw `GET /api/v9/channels/{id}/messages`, `bot.get_channel`, `bot.fetch_channel`, `ch.fetch_message`, `ref.channel_id/message_id` | **already a raw REST fallback** for resolving forwards | port to `client.http.get(...)`; the token now lives on our client, not `bot.http` |

This file is already half-migrated: the forward-snapshot path at 460-551 bypasses
discord.py-self and calls Discord REST directly with `build_headers`. That is the
model for the whole file.

## 4. Message forwarding (external webhooks/pushover/telegram)

File: `code/client/forwarding.py`

| Line(s) | API | What it does | v4 replacement |
|---|---|---|---|
| 23 | `import discord` | import | drop |
| 646-709 | `discord_message: discord.Message`, `_looks_like_forward_wrapper`, `_resolve_forward_wrapper` | detect forwarded-message wrappers via `system_content` | read `system_content`/`flags` off the message wrapper |
| 826-906 | `_get_message_attributes(message: discord.Message)`, `isinstance(author, (discord.Member, discord.User))`, `getattr(message, "system_content", "")` | pull attributes for the forwarder | model wrapper; `isinstance` becomes a check on the author dict shape |

Forwarding is almost entirely `getattr`-based already and uses `aiohttp` for the
actual sends. Minimal surface: only the `discord.Message` type hints and the one
`isinstance(author, (Member, User))` branch.

## 5. Export / backfill runners (history, threads, DM export, assets)

File: `code/client/export_runners.py`

| Line(s) | API | What it does | v4 replacement |
|---|---|---|---|
| 36-37 | `ChannelType, ForumChannel, MessageType, Object as DiscordObject`, `HTTPException, Forbidden` | imports | `selfclient.models` enums; `selfclient.Object(id=...)`; own errors |
| 131-365 | `th.permissions_for(me).read_message_history`, `guild.get_member(self.bot.user.id)` | permission gating before reading a channel/thread | compute permissions from role + overwrite dicts (porting discord.py's `permissions_for`), or attempt-and-catch-403 like wizard `history.py:504` |
| 141-162, 2203-2290 | `ch.archived_threads(limit, private, joined)` | iterate archived threads (3 lists) | `selfclient` archived-thread paginator; wizard `history.py:551-613` is a direct port (public/private/joined paths, `has_more` cursor) |
| 345-463, 2298-2358 | `self.bot.fetch_channel`, `self.bot.get_channel`, `ch.history(**kwargs)`, `DiscordObject(id=...)` for before/after | message history walk | `selfclient` messages paginator; wizard `history.py:494-540` (newest-first, `before` cursor, `after_id` floor); `Object(id=)` becomes a plain id string in params |
| 1150-1289 | `_find_dm_channel`, `user.dm_channel`, `self.bot.private_channels`, `isinstance(ch, discord.DMChannel)`, `dm.history(...)`, `self.bot.get_user` | DM history export | `POST /users/@me/channels` to open the DM (raw), then the same messages paginator; `private_channels` comes from READY |
| 1476-1478 | `MessageType.default`, `getattr(MessageType, name)` | allowed message-type filter | message-type ints |
| 1623-1696 | `ChannelType.public_thread/private_thread`, `isinstance(wrapper_parent, ForumChannel)` | thread/forum classification | channel-type ints |
| 2547-2554 | `guild.fetch_emojis()`, `guild.fetch_stickers()` | asset export | raw REST |

## 6. Client helpers / UI controller

File: `code/client/helpers.py`

| Line(s) | API | What it does | v4 replacement |
|---|---|---|---|
| 29, 302 | `bot: discord.Client`, `_pick_guild -> discord.Guild` | type hints | `selfclient` types |
| 306-308 | `self.bot.get_guild(id)`, `self.bot.guilds[0]` | guild selection | client guild cache |
| 348-365 | `dm_member_by_id`: `bot.get_user`, `bot.fetch_user`, `member.send(message)`, `discord.Forbidden/NotFound` | send a DM to a user by id | `POST /users/@me/channels` then `POST /channels/{dm}/messages`; own errors |
| 431-491 | `dump_message_debug(message: discord.Message)`, `message.author.display_avatar.url`, `message.system_content` | debug dump | model wrapper attributes |

---

## Client public surface that `selfclient.Client` must provide

Collected from all call sites above. This is the target API of the new client:

- Lifecycle: `start(token)` / `run()`, `close()`, `is_closed()`, `is_ready()`,
  `wait_until_ready()`, event registration (`on(event, handler)` or
  `@event`-style), `user` (the logged-in account).
- Caches from gateway state: `guilds`, `get_guild(id)`, `get_channel(id)`,
  `get_user(id)`, `private_channels`, per-guild `get_member(id)`.
- REST (raw, returns dicts): `fetch_channel(id)`, `fetch_user(id)`,
  `fetch_member(guild, id)`, `fetch_message(channel, id)`, `fetch_emojis(guild)`,
  `fetch_stickers(guild)`, `get_roles(guild)`, channel `history(...)`,
  channel `archived_threads(...)`, open-DM, send-message, send-DM.
- Proxy: `set_proxy(url)` + live reconnect.
- Errors: `LoginFailure`, `ConnectionClosed` (with `.code`), `HTTPException`
  (with `.status`, `.json`, `.retry_after`), `Forbidden`, `NotFound`,
  `RateLimited` (with `.retry_after`).
- Models (attribute-compatible wrappers over raw dicts): `Message` (+
  `system_content`, `flags`, `reference`, `author`, `attachments`, `embeds`,
  `stickers`, `mentions`, `role_mentions`, `channel_mentions`, `display_avatar`),
  `Guild`, `Role`, `Member`/`User`, channel types (`TextChannel`,
  `VoiceChannel`, `StageChannel`, `CategoryChannel`, `ForumChannel`, `Thread`,
  `DMChannel`), `Emoji`/`PartialEmoji`, `Object`.
- Enums: `ChannelType`, `MessageType`, `StickerType`, `VideoQualityMode`.

---

## Server role (py-cord) - NOT removed in v4.0.0

The server role runs a **bot account** and legitimately uses `py-cord==2.7.2`.
Files and counts:

- `code/server/server.py` (101 refs): `discord.Bot(intents=discord.Intents.all())`
  at 174, `bot.start(SERVER_TOKEN)` at 13753, `Webhook` management, `discord.http.Route`
  for raw calls at 3230/11552.
- `code/server/commands.py` (163 refs): `commands.Bot`, slash command tree,
  `discord.Embed`, error types.
- `code/server/roles.py`, `stickers.py`, `permission_sync.py`, `emojis.py`,
  `helpers.py`, `token_identity.py`, `token_avatar.py`, `backfill.py`,
  `guild_resolver.py`: all build the mirror guild via the bot.

Recommendation: **keep py-cord on the server for v4.0.0.** It drives a real bot
token with a bot's privileges (managing webhooks, creating channels/roles,
uploading emojis/stickers) which is exactly what a bot library is for and
carries no selfbot detection risk. Removing it would be a far larger, separate
project with no anti-detection payoff. v4.0.0's dependency conflict disappears
anyway once the client no longer needs `discord.py-self`: the server can keep
`discord` (py-cord) as the only `discord`-named package in the collapsed
process, and the client uses `selfclient`, which never imports `discord`.

Note the server already uses `common/selfbot_headers` (`get_tls_session`,
`build_headers`) for its **user-token** side work (`token_sender.py`,
`token_avatar.py`, `token_identity.py` send messages / set avatars / set nicks as
the user account via raw curl_cffi REST). That user-token REST path is already
off discord.py-self and should move onto the same `selfclient.http` module.
