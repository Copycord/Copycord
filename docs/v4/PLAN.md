# Copycord v4.0.0 Revamp Plan

Status: proposal. Planning only. No code has changed under this plan except the
two documents in `docs/v4/`. Line references are against main at 914232b
(v3.28.0). Companion document: `docs/v4/DISCORD_PY_SELF_INVENTORY.md`.

Merge model: all v4 work lands on a long-lived `v4` integration branch cut from
main. Feature branches merge into `v4` only after Mac tests and confirms each
one. `v4` merges to main only at the v4.0.0 release.

---

## 1. Executive summary

v4.0.0 has four goals, in priority order:

1. **Remove `discord.py-self` (dolfies) from the client.** Replace it with our
   own user-account client package, `code/selfclient/`, that logs in with a
   token, holds the gateway connection, dispatches events, and makes REST calls
   while presenting as a real Discord web client (Chrome TLS/JA3 via curl_cffi,
   web super-properties, live build number, web IDENTIFY, zlib-stream, heartbeat
   jitter, resume, proxy support). Every current client feature keeps working.
   We already own most of the hard parts: `common/selfbot_headers.py` builds the
   fingerprint, the standalone scraper (`admin/standalone_scraper.py:817-1168`)
   already runs a raw gateway session, and the server's user-token REST path is
   already off the library. The sister projects `wizard` and `matrix` are
   working, readable reference implementations of exactly this client, and large
   parts port almost verbatim.

2. **Collapse the 3 containers into 1 process.** `admin`, `server`, and `client`
   become one process with three internal asyncio tasks. Decision and rationale
   in section 5. This is feasible specifically *because* removing discord.py-self
   removes the library conflict that forced three venvs in the first place.

3. **Professional non-Docker install.** Ship Copycord as a `uv`/pip-installable
   package exposing a `copycord` CLI, with a `copycord service install` step that
   registers a systemd unit (Linux), a launchd agent (macOS), or a Windows
   service. This replaces `install-tools/` (the hand-rolled `.exe` launcher and
   per-OS venv scripts). Rationale in section 6.

4. **Bump Python 3.11 -> 3.12.** Details and incompatible-dependency audit in
   section 4.

The through-line: Copycord has been drifting toward a hand-rolled web-client
fingerprint for several releases ("Send every Discord request to API v9" #194,
"Derive the claimed Chrome version from the TLS target" #193, "Prefer favorited
proxies for the client gateway" #195). discord.py-self is now the *last* thing
standing between Copycord and a fully owned, fully consistent web-client
identity, and it actively fights that goal: today the code monkeypatches
`discord.utils.Headers.default` (`client.py:2782`) to force the library to use
our fingerprint, and pokes private state (`bot._ready`, `http._started`,
`guild._state.http`) to make it behave. v4 removes the fight.

---

## 2. Current architecture

### 2.1 Processes and roles

One Docker image, three containers selected by `ROLE` (`docker-compose.yml`,
`code/entrypoint.sh`):

- **admin** (`ROLE=admin`): FastAPI + uvicorn web UI on :8080
  (`code/admin/app.py`, 6797 lines). Serves the config/control UI, the message
  bus (`/bus` websocket), the standalone member scraper
  (`admin/standalone_scraper.py`, which has its *own* raw gateway client), and
  proxies control commands to server/client.
- **server** (`ROLE=server`): runs `control/control.py`, which supervises a
  child `server.server` process. The server is a **py-cord bot** that builds the
  mirror guild (`code/server/server.py`, 13779 lines; `commands.py`, 3886).
- **client** (`ROLE=client`): runs `control/control.py`, which supervises a
  child `client.client` process. The client is the **discord.py-self selfbot**
  that monitors the source guild(s) (`code/client/client.py`, 3079 lines).

The `control.py` wrapper exists so the admin UI can start/stop the
server/client child via a websocket (`control.py:247-293`), teeing child stdout
to a log file and reading config from the DB (`_load_env_for_child`,
`control.py:106-127`).

### 2.2 Transports between parts

- **Admin bus**: `AdminBus` (`common/websockets.py:489`) - server and client
  publish status/logs to the admin `/bus`.
- **Control ws**: admin -> `control.py` (:9101 server, :9102 client) for
  start/stop/status (`control.py:247`).
- **Agent ws**: `WebsocketManager` (`common/websockets.py:57`) - client sends
  scraped sitemaps/messages to the server (:8765), server sends commands to the
  client (:8766). Message-typed JSON (`client.py:407` `_on_ws` handles
  `mappings_reload`, `clone_messages`, `sitemap_request`, `export_dm_history`,
  `export_messages`, `pull_assets`, `backfills_queue_query`, ...).
- **Shared SQLite**: `common/db.py` (5824 lines) on a shared `/data` volume.
  `DBManager` is used by all three roles; schema is self-migrating via
  `_ensure_table` (table rebuild on column drift, `db.py:1204-1296`).

### 2.3 Fingerprint / anti-detection (already ours)

- `common/selfbot_headers.py` (544 lines): the fingerprint authority.
  `make_fingerprint(token)` (165) builds per-token stable super-properties,
  gateway properties, REST headers, installation id. `refresh_build_info` (137)
  scrapes the live build number from Discord web assets. `get_tls_session` (358)
  returns a per-token curl_cffi `AsyncSession` impersonating `TLS_IMPERSONATE`
  (DEFAULT_CHROME). `CHROME_MAJOR` is derived from the TLS target so UA /
  Sec-CH-UA / browser_version cannot drift (#193).
- `common/proxy_pool.py` (321): async proxy leasing per token.
- `client/proxy_rotator.py` (468): gateway proxy rotation + favorites (#195).
- `admin/standalone_scraper.py:817`: a complete raw-websocket gateway session
  (op 10 hello, op 2 identify, op 1 heartbeat with jitter at line 904, op 8
  member-query pump, op 11/9/7 handling). Proof we can run the gateway without
  discord.py-self; today it is used only for member scraping.

### 2.4 What is still on discord.py-self

Only the **client role** (6 files). See the inventory doc for the full table.
The library drives: gateway connect/resume, all `on_*` events, guild/channel/
role/emoji/sticker/thread model objects for the sitemap, message model objects
for serialization/forwarding, and history/thread/DM pagination for backfill and
export.

---

## 3. Target architecture

### 3.1 New package: `code/selfclient/`

A self-contained user-account client, no `discord` import anywhere in it. Module
layout:

```
code/selfclient/
  __init__.py        # public exports: Client, errors, models
  client.py          # Client: ties gateway + http + state cache together,
                     #   event dispatch, lifecycle (start/close/run), caches
                     #   (guilds/channels/users/members/private_channels)
  gateway.py         # GatewayClient: raw websockets, HELLO/IDENTIFY/RESUME,
                     #   heartbeat loop w/ jitter, op 37 guild subscribe,
                     #   zlib-stream decompression, reconnect/backoff, dispatch
  http.py            # HTTPClient: curl_cffi AsyncSession, REST verbs, v9 base,
                     #   429/bucket pacing, paginators (messages, archived
                     #   threads), raw-dict responses, proxy
  fingerprint.py     # thin re-export / home of the header+super-props builder
                     #   (consolidates common/selfbot_headers; see 3.4)
  models.py          # attribute-compatible wrappers over raw gateway/REST dicts:
                     #   Message, Guild, Role, Member, User, channel types,
                     #   Emoji/PartialEmoji, Object; permissions_for()
  enums.py           # ChannelType, MessageType, StickerType, VideoQualityMode,
                     #   gateway op codes, close codes, capabilities bits
  events.py          # dispatch table + raw-payload -> model mapping for each
                     #   gateway event the client role listens to
  errors.py          # LoginFailure, ConnectionClosed(code), HTTPException
                     #   (status/json/retry_after), Forbidden, NotFound,
                     #   RateLimited(retry_after)
```

The `Client` public surface is exactly the list at the end of the inventory doc.
`code/client/*` keeps its structure; the only change is `import discord` ->
`from selfclient import ...` and the model-attribute reads continue to work
because `selfclient.models` reproduces the shapes the code already uses.

### 3.2 What ports from wizard / matrix (reuse map)

wizard and matrix are the same client we are building, already working. Read
`wizard/core/gateway.py`, `wizard/core/headers.py`, `wizard/core/history.py`;
matrix is the older/smaller ancestor (`matrix/gateway.py`, `headers.py`).

| selfclient module | Ports from | Notes / gaps |
|---|---|---|
| `gateway.py` | `wizard/core/gateway.py` (whole file, 785 lines) | Near-verbatim: HELLO/IDENTIFY/RESUME (`:441-527`), heartbeat jitter + zombie detection (`:458-471`), op 37 `BULK_GUILD_SUBSCRIBE` frames with the typing+threads "never split" rule (`:84-133`), `resume_gateway_url` + query re-apply (`:39-51`), close-code handling incl. fatal 4004/4010-4014 (`:373-389`), reconnect backoff with jitter (`:266-270`). **Gap:** wizard does not use zlib-stream (`compress: False`, `gateway.py:500`); we should add `encoding=json&compress=zlib-stream` support (see 3.3) since the real web client uses it. **Gap:** wizard indexes channels but does not emit the full `on_guild_channel_*` / `on_thread_*` / `on_message_edit`/`delete` event set Copycord's client needs - our `events.py` adds those dispatches. |
| `http.py` | `wizard/core/history.py` (613 lines) | Near-verbatim: `Reader` over a curl_cffi `AsyncSession` impersonating the same target as the headers (`:171-198`), v9 base (`:59`), 429 retry with `retry_after` from body (`:417-491`), bucket pacing from `X-RateLimit-*` (`:211-228`), `messages()` newest-first paginator with `after_id` floor (`:494-540`), `archived_threads()` public/private/joined (`:551-613`), snowflake-at-time bound (`:201-208`), secret-header redaction in logs (`:96-98`). **Gap:** wizard's reader is read-only; Copycord also needs write (send message, send DM, open DM) - add POST verbs with the CORS Origin rule noted in `wizard/headers.py:447-451`. |
| `fingerprint.py` | Copycord's own `common/selfbot_headers.py` **is already better** than wizard's `headers.py` in places (it is the one that derives CHROME_MAJOR from the TLS target, #193) | Keep Copycord's. Adopt from wizard: the GREASE Sec-CH-UA algorithm (`wizard/headers.py:303-348`) which is more correct than Copycord's static `_sec_ch_ua` (`selfbot_headers.py:260`); the three-source build fetch (API -> login GLOBAL_ENV -> asset scrape, `wizard/headers.py:247-283`) which is more robust than Copycord's single web scrape; the launch-signature bit-mask (`wizard/headers.py:351-363`). |
| `models.py` | neither (wizard/matrix work on raw dicts, no model layer) | **This is the main net-new code.** Copycord's client reads ~660 model attributes, so unlike wizard we cannot operate purely on dicts without rewriting the client, forwarding, sitemap and export files line-by-line. The wrapper layer is what keeps those files nearly unchanged. `permissions_for()` must be ported from discord.py's algorithm (role perms + @everyone + overwrites). |
| `events.py` | wizard dispatch pattern (`gateway.py:581-697`) + Copycord's own `on_*` handlers | Map each raw event to the model wrapper the existing handler expects. |

The standalone scraper's gateway (`admin/standalone_scraper.py:817`) folds into
`selfclient.gateway` too, so there is one gateway implementation, not two.

### 3.3 Detection-avoidance design

Everything here is already partly implemented; v4 consolidates it into
`selfclient` and closes the listed gaps.

- **TLS/HTTP fingerprint.** Every request carrying a user token goes through
  curl_cffi impersonating `DEFAULT_CHROME` (`selfbot_headers.py:54-62`). Python's
  native TLS/JA3 is refused by Cloudflare regardless of headers
  (`wizard/headers.py:78-83`). The gateway websocket upgrade also carries the
  Chrome UA + Origin (`wizard/gateway.py:413-416`).
- **Headers.** REST header set from `build_headers` (`selfbot_headers.py:294`):
  Priority `u=1, i`, Sec-CH-UA (GREASE), Sec-Fetch-*, X-Discord-Locale,
  X-Discord-Timezone, X-Super-Properties (base64), X-Debug-Options, X-Installation-Id,
  bare Authorization. Match the captured-web-client reference
  (`selfbot_headers.py:477-544` `diff_against_real_client`). No Origin on GET
  (CORS rule, `wizard/headers.py:447`).
- **Build number.** `refresh_build_info` before login, three sources cheapest
  first (adopt wizard's API->login->scrape), reject anything below a plausible
  floor, loud warning on fallback because a stale build is a top detection
  signal (`wizard/headers.py:247-283`). Feeds `client_build_number` into both
  super-properties and gateway IDENTIFY props.
- **IDENTIFY properties.** Web super-properties shape, 19 fields in captured
  order - order is load-bearing because the server sees the serialized+base64
  string (`wizard/headers.py:497-533`). Gateway props differ from REST props
  (installation_id present, is_fast_connect, no launch_signature). Launch id /
  heartbeat-session id / launch signature are per-process (minted once per
  launch, shared between gateway and REST so they describe one client), while
  installation id and os_version are per-token-stable (seeded from
  SHA256(token)) (`wizard/headers.py:371-404`).
- **capabilities.** `1734653` = web client capabilities minus bit 15
  CHANNEL_OBFUSCATION, so READY carries real channel names instead of
  `___hidden___` (`wizard/gateway.py:71-77`). (matrix uses `1767421`; wizard's
  value with the obfuscation bit cleared is the one we want for the sitemap.)
- **zlib-stream (gap to close).** The real web client connects with
  `&compress=zlib-stream` and inflates a shared zlib context across frames.
  wizard uses `compress: False` for simplicity; Copycord should implement
  zlib-stream (shared `zlib.decompressobj`, flush on the 4-byte `Z_SYNC_FLUSH`
  suffix) so the gateway handshake matches the browser. Keep plain-json as a
  fallback.
- **Heartbeat.** Interval from HELLO, first beat after `interval * rand(0,1)`
  jitter (`standalone_scraper.py:904`, `wizard/gateway.py:458`), zombie
  detection if no ACK within `interval*2` (`wizard/gateway.py:462`). op 1
  bypasses the send rate-limiter.
- **Gateway send rate limit.** 110 sends / 60s sliding window, heartbeats exempt
  (`wizard/gateway.py:136-156`).
- **Resume correctness.** Close with 4000 (not 1000) to keep the session;
  re-apply `v`/`encoding` query to `resume_gateway_url`; re-send op 37
  subscriptions on RESUMED because the server replays none
  (`wizard/gateway.py:64-69, 556-575, 664-666`).
- **op 37 guild subscriptions.** typing+threads+activities+member_updates in one
  frame per guild, never split (the thread-events-stop-forever footgun,
  `wizard/gateway.py:84-95`); batch under 15360 bytes.
- **op 41 time-spent** session heartbeat every 30 min (`wizard/gateway.py:533`).
- **Rate-limit handling (REST).** Per-bucket pacing from `X-RateLimit-Remaining`
  / `-Reset-After`, 429 retry from body `retry_after`, scope-aware logging
  (`wizard/history.py:211-228, 417-491`).
- **Proxy support.** Per-token curl_cffi session leased a proxy from
  `proxy_pool` so cookies are tied to the exit IP (`selfbot_headers.py:412-419`);
  gateway dials through the proxy and rotates on failure
  (`wizard/gateway.py:259-261`); favorites + hot-switch on live socket preserved
  (`client.py:2629-2692`, #195); timezone derived from the exit IP
  (`matrix/proxies.py:120`).

### 3.4 fingerprint consolidation

`common/selfbot_headers.py` stays the authority but moves/aliases under
`selfclient.fingerprint`. The server's user-token REST (`token_sender.py`,
`token_avatar.py`, `token_identity.py`) and admin (`app.py`, `standalone_scraper.py`)
import the same module, so there is exactly one fingerprint definition in the
process. This is a rename + re-export, not a rewrite, to keep the server side
untouched.

---

## 4. Dependency and Python version bump

### 4.1 Current pins

- Python **3.11-slim** (`code/dockerfile:16`, both per-role Dockerfiles, CI
  `ci.yml` x3, `ruff.toml` `target-version = "py311"`, installer requires
  3.11.x and the Windows preflight asserts it).
- Client: `discord.py-self==2.1.0`, `websockets==11.0.3`, `PyYAML`, `aiohttp`,
  `audioop-lts; python_version>=3.13`.
- Server: `py-cord==2.7.2`, `websockets==11.0.3`, `aiohttp`, `aiohttp-socks`,
  `curl_cffi==0.16.2`, `Pillow==12.2.0`, `PyYAML`.
- Admin: `fastapi==0.116.1`, `uvicorn[standard]==0.35.0`, `jinja2`,
  `python-multipart`, `websockets==11.0.3`, `itsdangerous`.

### 4.2 Recommendation: target Python 3.12

Rationale: 3.12 is a mature, widely packaged release (apt/brew/winget, and `uv`
can fetch it), gets security fixes well past Copycord v4's lifetime, and keeps
`asyncio`/`websockets`/`curl_cffi` on well-tested ground. 3.13 is possible but
buys little here and adds friction: the `audioop` stdlib module was removed in
3.13 (hence the existing `audioop-lts` shim) and py-cord's voice imports touch
it. Since we keep py-cord on the server, 3.12 avoids that whole class of problem.
**3.12, not 3.13.**

### 4.3 Incompatible / affected dependencies

- **`discord.py-self==2.1.0`**: removed entirely (the point of goal 1). This is
  also the dep most sensitive to Python version, so removing it widens the
  version window.
- **`audioop-lts`**: only needed on >=3.13; on 3.12 the stdlib `audioop` still
  exists, so the marker simply never installs it. No action beyond keeping the
  marker.
- **`websockets==11.0.3`**: 11.x predates the `additional_headers` /
  `extra_headers` rename and the new asyncio client API that wizard uses
  (`websockets.connect(..., additional_headers=...)`, `proxy=...`,
  `wizard/gateway.py:350-355`). The selfclient gateway needs a **websockets >=
  13** (recommend pinning the current 14.x/15.x line) for `additional_headers`
  and native `proxy=`. Audit the admin/server/control uses of `websockets` (all
  use `.serve`/`.connect`, `common/websockets.py`, `control.py:269`,
  `app.py:1487`) for the 13.x `WebSocketServerProtocol` deprecation and the
  `extra_headers` -> `additional_headers` change before bumping.
- **`curl_cffi==0.16.2`**: already present and central; keep (its
  `impersonate.DEFAULT_CHROME` is the fingerprint anchor). Verify the pinned
  version exposes `AsyncSession` and `impersonate.DEFAULT_CHROME` on 3.12 (it
  does).
- **`py-cord==2.7.2`**: supports 3.12; keep. Confirm against its classifiers on
  the exact patch.
- **`Pillow`, `fastapi`, `uvicorn`, `jinja2`, `PyYAML`, `aiohttp`,
  `aiohttp-socks`, `itsdangerous`**: all support 3.12 at the pinned versions; no
  change required, bump only if the lockfile resolver prefers newer.
- `datetime.utcnow()` is used widely (`db.py`, `app.py`, `export_runners.py`,
  `commands.py`): deprecated (not removed) in 3.12; not a blocker, but the bump
  branch should migrate to `datetime.now(timezone.utc)` to kill the warnings.
- `asyncio.get_event_loop()` (several files): deprecated pattern; tidy where the
  single-process refactor touches it.

No new third-party dependency is introduced by this plan; `selfclient` is built
from stdlib + already-present `curl_cffi` + `websockets` (bumped) + `zlib`.
(Packaging in section 6 uses `uv`/`hatchling` as build tooling, which are dev/build
dependencies, not runtime.)

---

## 5. Container collapse decision

**Decision: collapse the three containers into one process with three internal
asyncio tasks (admin API, server/bot, client/selfbot), keeping the websocket
message contracts between them as in-process async calls behind the same
interfaces. Ship one container. Keep the ability to run roles separately as an
advanced/optional mode.**

### 5.1 Why it is feasible now

The three-venv/three-container split exists for one concrete reason: `py-cord`
and `discord.py-self` both own the `discord` import name and cannot share an
interpreter (stated in `tests/requirements-test.txt`). Remove discord.py-self
and that reason is gone - `selfclient` never imports `discord`, so the client
(selfclient) and server (py-cord) can finally run in one interpreter.
`control.py` already starts the role as a child process via `subprocess`
(`control.py:181`); in one process it becomes `asyncio.create_task`.

### 5.2 Why collapse rather than keep split

- The split's only transport need between server and client is typed JSON over
  localhost websockets (`WebsocketManager`). In one process those become direct
  `await` calls or an `asyncio.Queue`, removing two websocket servers, the
  `control.py` supervisor, the pidfile/stdout-tee machinery, and the
  admin->control start/stop ws.
- Config is already a shared SQLite DB, so there is no shared-state problem to
  solve - one process simplifies DB access (one connection pool, no cross-process
  WAL contention).
- Operationally: one container, one log stream, one health check, one restart
  policy. The current `depends_on` ordering and the `ADMIN_HOST`/`SERVER_WS_HOST`
  env dance (`config.py:51-70`) disappear.

### 5.3 Risks and the mitigation

- **Fault isolation.** Today a client crash cannot take down the admin UI. In
  one process an unhandled exception could. Mitigation: each role runs as a
  supervised task with its own restart-with-backoff wrapper (the logic already
  exists in `control.py` and the client's own reconnect loop); the admin API task
  is never blocked by the others (all I/O is async). A crash in one task restarts
  that task, not the process.
- **Blocking work.** py-cord and selfclient share one event loop. The build
  fetch and cookie prime are already pushed to threads (`wizard/gateway.py:231`
  `asyncio.to_thread`); keep that discipline. curl_cffi AsyncSession is
  non-blocking.
- **Memory/CPU.** One process is strictly less overhead than three.

### 5.4 Keep the escape hatch

Retain a `copycord run --role {admin,server,client}` mode so an operator who
*wants* separation (e.g. client on a proxied host, server elsewhere) can still
split, talking over the existing websocket contracts. Default `copycord run`
(no role) runs all three in-process. This makes the collapse a default, not a
removal of capability, and de-risks the migration (the split path stays tested).

---

## 6. Non-Docker install design

**Recommendation: ship Copycord as a Python package installed with `uv` (pip
compatible), exposing a `copycord` console-script CLI, with a
`copycord service install` subcommand that writes the native service unit for
the host OS. Deprecate `install-tools/` (the bespoke `.exe`/`launcher.py`/venv
scripts).**

### 6.1 Why uv/pipx package over a frozen binary

- A frozen binary (PyInstaller, the current `Copycord.exe` approach) bundles a
  Python and every dep into one artifact. It is heavy, must be rebuilt per OS/arch,
  is the exact thing that makes the current installer fragile, and
  anti-virus/Cloudflare-style heuristics dislike opaque bundles. curl_cffi ships
  compiled binaries per platform/Python, which fights single-file freezing.
- `uv tool install copycord` (or `pipx install copycord`) gives an isolated
  venv, a stable `copycord` entry point on PATH, trivial `copycord self update`
  (or `uv tool upgrade`), and reproducible installs from a lockfile. It uses the
  platform wheels for curl_cffi/Pillow directly. This is the modern,
  low-maintenance path and it is cross-platform with one codebase.
- The admin frontend (currently built with Node at image build time,
  `code/dockerfile:1-8`) ships as **prebuilt static assets inside the wheel**, so
  end users need no Node. The Node build stays a maintainer/CI step.

### 6.2 CLI shape

```
copycord run [--role admin|server|client]   # default: all-in-one
copycord service install [--user]           # write + enable native service
copycord service {start,stop,status,uninstall}
copycord config {get,set,import,export}      # DB-backed config (admin UI remains)
copycord doctor                              # preflight: python, ports, token,
                                             #   build fetch, proxy reachability
copycord version
```

- Entry point declared in `pyproject.toml` `[project.scripts]`.
- `service install` writes: a **systemd** unit (`~/.config/systemd/user/` with
  `--user`, else `/etc/systemd/system/`) on Linux; a **launchd** plist
  (`~/Library/LaunchAgents/com.copycord.plist`) on macOS; a **Windows service**
  via `pywin32`/`sc.exe` (or a Scheduled Task fallback) on Windows. Each runs
  `copycord run` with the data dir and restart-on-failure configured.
- Data dir default `~/.local/share/copycord` (XDG) / `%LOCALAPPDATA%\Copycord`,
  overridable with `--data-dir` / `DATA_DIR` (the env var already exists,
  `config.py:21`).

### 6.3 Packaging specifics

- `pyproject.toml` with `hatchling` build backend, `requires-python = ">=3.12"`,
  runtime deps = the union of today's three requirement files minus
  `discord.py-self`, plus bumped `websockets`.
- A `uv.lock` committed for reproducible installs.
- Docker image is still produced (the collapsed single container) for users who
  prefer it; it installs the same package, so there is one source of truth.

---

## 7. Data / config migration from v3 and backward compatibility

### 7.1 Database

- Schema is self-migrating: `DBManager._init_schema` + `_ensure_table` rebuild
  tables on column drift automatically (`db.py:1204-1296`). A v3 `data.db` opened
  by v4 upgrades in place. **No destructive migration is planned.**
- v4 adds nothing that requires dropping data. If `selfclient` needs new config
  keys (e.g. `GATEWAY_COMPRESS`), they are added to `app_config` with defaults,
  read through the existing `Config`/`get_config` path (`config.py:30-45`).
- Action for the bump branch: add a `schema_version` row to `app_config` so
  future migrations can be ordered, and have v4 **back up `data.db` once** before
  first open (the backup scheduler already exists, `common/backup_scheduler.py`).

### 7.2 Env vars / config

- All current env vars keep working; `Config` reads DB-first then env
  (`config.py:30-45`). The collapse removes the *need* for the inter-service URL
  vars (`SERVER_WS_HOST`, `CLIENT_WS_HOST`, `ADMIN_HOST`, `*_CTRL_URL`,
  `WS_*_URL`) but they are still honored in `--role` split mode, so no config
  breaks.
- `ROLE` is still read; default becomes "all-in-one" when unset in the package
  CLI, while the Docker image can still set `ROLE` for split mode.
- `CLIENT_TOKEN`, `SERVER_TOKEN`, `CLONE_GUILD_ID`, `HOST_GUILD_ID`,
  `COMMAND_USERS`, all the `CLONE_*`/`DELETE_*` flags (`control.py:62-78`
  ALLOWED_ENV): unchanged.

### 7.3 Backward compatibility statement

- v4 is a major version: the container topology and install method change. The
  **data** (DB, mappings, backups) is forward-compatible with no user action.
- The docker-compose from v3 (three services) keeps working against the v4 image
  in `--role` split mode, so existing Docker users are not forced onto the
  single-container layout on day one. The v4 compose default ships as one
  service; migrating is "replace three services with one".
- `install-tools/` is marked deprecated in v4.0.0 and removed in a later release;
  its README points at `uv tool install copycord`.

---

## 8. Testing strategy

### 8.1 Unit tests with recorded gateway fixtures

- Capture real gateway frames once (READY, GUILD_CREATE, MESSAGE_CREATE for a
  normal channel / thread / forum post, MESSAGE_UPDATE, THREAD_LIST_SYNC,
  CHANNEL_UPDATE, GUILD_ROLE_UPDATE) and store as JSON fixtures under
  `tests/fixtures/gateway/`. Feed them through `selfclient.events` + `models` and
  assert the dispatched model objects and the serialized sitemap/message dicts
  match golden output. This is how we prove the model layer reproduces
  discord.py-self's shapes without a live account.
- Port wizard's approach: `tests/unit/bot/test_gateway_resume.py` exists in
  wizard and exercises resume/sequence/close-code logic against a fake socket;
  mirror it for `selfclient.gateway` (HELLO->IDENTIFY, resume path, 4004 fatal,
  op 37 batching/no-split, heartbeat zombie).
- HTTP: unit-test the 429/bucket pacing and the paginators against a fake
  `AsyncSession` returning canned responses with `X-RateLimit-*` headers (wizard
  `history.py` logic).
- Keep the existing suites green and extend them: `test_api_version.py` (must
  stay all-v9 - and now it must also scan `selfclient` for the base URL),
  `test_selfbot_headers.py`, `test_client_build_fetch.py`,
  `test_favorite_proxies.py`, `test_proxy_error_detection.py`. Note four tests
  (`test_client_build_fetch.py`, `test_system_message_forwarding.py`,
  `test_token_identity.py`) currently `import discord`/`discord.utils.Headers` -
  these are rewritten against `selfclient`.
- CI: bump `setup-python` to 3.12 in all three jobs, `ruff.toml` to `py312`. The
  three-venv install note in CI simplifies once the dep conflict is gone.

### 8.2 Mac manual acceptance checklist (per feature, per branch)

Each feature branch is "done" only when Mac runs it against a real source guild
+ mirror and confirms. Master checklist (the client features that must all keep
working):

- [ ] Client logs in with a user token; gateway reaches READY; survives a forced
      disconnect via RESUME (no full re-IDENTIFY) with no lost messages.
- [ ] Live message mirror: new message in a normal channel appears on the mirror.
- [ ] Thread message mirror: message in a thread and in a forum post both
      mirror (proves op 37 threads subscription + THREAD_LIST_SYNC).
- [ ] Message edit and delete mirror (raw + modeled paths).
- [ ] Forwarded-message ("forward wrapper") resolution still works, including the
      REST snapshot fallback.
- [ ] Sitemap build: channels, categories, voice/stage, forums (with tags +
      default reaction emoji), roles (including gradient/holographic colors via
      raw role fetch), emojis, stickers - all clone correctly, real names (no
      `___hidden___`).
- [ ] Channel/role/emoji/sticker/guild create/update/delete events mirror live.
- [ ] Member join -> on-join DM and on-join role trigger fire.
- [ ] Backfill a channel's history (message pagination, before/after bounds).
- [ ] Export messages and export DM history (DM open + history pagination).
- [ ] Pull assets / attachment download + re-upload.
- [ ] Proxy: runs through a proxy; rotates on a dead proxy; prefers favorites;
      hot-switches back to a recovered favorite on the live socket.
- [ ] Backup/primary token rotation on 4004 / LoginFailure.
- [ ] Header fidelity: `diff_against_real_client` reports a match on live wire
      headers.
- [ ] Single-process mode: admin UI + server bot + client all run in one
      process; a client exception restarts only the client task.
- [ ] `--role` split mode still works (v3 compose compatibility).
- [ ] `uv tool install` + `copycord service install` + `copycord run` on Linux
      (systemd), and at least a smoke test on Windows/macOS.
- [ ] v3 `data.db` opens and upgrades cleanly; mappings intact.

---

## 9. Risks

- **R1 - Model-layer fidelity.** The ~660 attribute reads are the long tail. A
  missed attribute or a wrong default silently corrupts a clone. Mitigation:
  golden-fixture tests (8.1); migrate file-by-file behind the wrapper so the diff
  per branch is small; keep discord.py-self installed on the `v4` branch until
  `selfclient-events` is signed off, so a wrapper gap is caught against the real
  library's output.
- **R2 - Detection.** A subtly wrong IDENTIFY/header gets the token challenged or
  banned. Mitigation: reuse wizard/matrix shapes verbatim where they exist (they
  are field-tested), keep `diff_against_real_client`, land the fingerprint on its
  own branch first and let Mac soak it on a throwaway token before anything else
  depends on it. Build-number staleness is the top signal - keep the loud
  fallback warning.
- **R3 - zlib-stream.** New code path not present in wizard; a framing bug
  desyncs the socket. Mitigation: implement behind a config flag with plain-json
  fallback; test against recorded compressed frames.
- **R4 - Single-process fault isolation** (see 5.3). Mitigation: supervised
  tasks + restart/backoff; admin task independence.
- **R5 - websockets major bump** breaks the admin/server/control ws servers.
  Mitigation: do the bump on its own branch, audit `common/websockets.py`,
  `control.py`, `app.py` for `extra_headers`/`WebSocketServerProtocol` changes.
- **R6 - curl_cffi platform wheels** for the packaged install on odd
  OS/arch/Python combos. Mitigation: `copycord doctor` checks import at install
  time; document supported matrix (3.12, x86_64/arm64, Linux/macOS/Windows).
- **R7 - Scope.** This is a large release. Mitigation: the phased roadmap below
  is ordered so each branch is independently testable and mergeable to `v4`, and
  the client keeps working on discord.py-self until `selfclient-events` lands.

---

## 10. Phased roadmap (feature branches)

All branches cut from and merge into `v4` (long-lived, off main). Each merges
only after Mac's acceptance for that branch passes. Order reflects dependencies.

### v4/selfclient-core
- **Scope:** Create `code/selfclient/` skeleton: `http.py` (curl_cffi
  AsyncSession, v9 base, verbs, 429/bucket pacing, paginators), `fingerprint.py`
  (consolidate `common/selfbot_headers`, adopt wizard GREASE Sec-CH-UA +
  three-source build fetch + launch-signature mask), `errors.py`, `enums.py`,
  `models.py` (wrappers + `permissions_for`), `Object`. No gateway yet. No
  client wiring yet.
- **Files touched:** new `code/selfclient/*`; `common/selfbot_headers.py`
  (re-export shim). Nothing in `code/client/*` changes behavior yet.
- **Acceptance (Mac):** unit tests for http pacing, paginators, fingerprint, and
  model wrappers pass; `diff_against_real_client` still matches; existing suites
  green. No runtime behavior change to the running client.
- **Depends on:** nothing.

### v4/selfclient-gateway
- **Scope:** `selfclient/gateway.py` ported from `wizard/core/gateway.py`:
  HELLO/IDENTIFY/RESUME, heartbeat+jitter+zombie, op 37 subscribe (no-split
  rule), op 41, reconnect/backoff, close codes, capabilities 1734653, proxy dial
  + rotate, resume_gateway_url query re-apply. Add zlib-stream (flag, json
  fallback). Fold in the standalone scraper's gateway needs. Not yet wired to the
  client role.
- **Files touched:** new `selfclient/gateway.py`, `selfclient/events.py`
  (dispatch table only).
- **Acceptance (Mac):** `test_gateway_resume`-style unit tests pass; a standalone
  smoke script connects a real token to READY, receives MESSAGE_CREATE in a
  channel and a thread, survives a forced reconnect via RESUME, logs a header
  match. Run on a throwaway token first.
- **Depends on:** selfclient-core.

### v4/selfclient-events
- **Scope:** Replace discord.py-self in the client role. Rewrite
  `code/client/client.py` to use `selfclient.Client`, map every `on_*` to a
  dispatch handler, remove the `Headers.default` monkeypatch and all private-state
  resets (`client.py:2734-2893`). Port `sitemap.py`, `message_utils.py`,
  `forwarding.py`, `export_runners.py`, `helpers.py` onto `selfclient.models` and
  `selfclient.http`. Remove `discord.py-self` from `code/client/requirements.txt`.
- **Files touched:** all of `code/client/*`; `code/client/requirements.txt`;
  the four tests that `import discord`.
- **Acceptance (Mac):** the full client checklist in 8.2 (mirror, threads/forums,
  edits/deletes, forwards, sitemap, lifecycle events, member join, backfill,
  export, assets, proxy, token rotation) against a real source guild + mirror.
  This is the big one.
- **Depends on:** selfclient-core, selfclient-gateway.

### v4/py-bump
- **Scope:** Python 3.11 -> 3.12 across Dockerfiles, CI (`setup-python` x3),
  `ruff.toml` (`py312`), installer/preflight. Bump `websockets` to >=13 and audit
  `common/websockets.py`, `control.py`, `app.py` for the API changes. Migrate
  `datetime.utcnow()` -> `datetime.now(timezone.utc)`; tidy
  `asyncio.get_event_loop()`.
- **Files touched:** `code/dockerfile`, `code/*/Dockerfile`,
  `.github/workflows/ci.yml`, `.github/workflows/*.yml` (python-version),
  `ruff.toml`, `common/websockets.py`, `control.py`, `admin/app.py`, requirement
  files, `db.py`/`app.py`/`export_runners.py`/`backup_scheduler.py` (utcnow).
- **Acceptance (Mac):** full suite green on 3.12; admin/server/client all start;
  websocket bus + control + agent transports work end to end.
- **Depends on:** can run in parallel with selfclient-* but merge after
  selfclient-events so the client's `websockets` use is the new gateway's.

### v4/single-process
- **Scope:** Collapse to one process. New `copycord run` entry that starts admin
  (FastAPI), server (py-cord bot), client (selfclient) as supervised asyncio
  tasks; replace cross-process `WebsocketManager` calls with in-process
  queues/awaits behind the same interfaces; keep `--role` split mode using the
  existing websocket contracts. Retire `control.py` subprocess supervision in the
  default path (keep for split mode). One Dockerfile/compose service.
- **Files touched:** new top-level entry (e.g. `code/copycord/__main__.py`),
  `control/control.py`, `common/websockets.py` (in-process shim),
  `docker-compose.yml`, `code/dockerfile`, `entrypoint.sh`, `config.py` (role
  defaults).
- **Acceptance (Mac):** single container runs all three; a forced client
  exception restarts only the client task and leaves the admin UI up; `--role`
  split mode still works against a v3-style compose; end-to-end mirror works in
  both modes.
- **Depends on:** selfclient-events (removes the dep conflict), py-bump.

### v4/installer
- **Scope:** `pyproject.toml` (hatchling, `requires-python>=3.12`, unified deps,
  `[project.scripts] copycord`), `uv.lock`, prebuilt frontend assets shipped in
  the wheel, `copycord` CLI (`run`/`service`/`config`/`doctor`/`version`),
  `service install` for systemd/launchd/Windows. Deprecate `install-tools/` (README
  pointer to `uv tool install copycord`).
- **Files touched:** new `pyproject.toml`, `uv.lock`, `code/copycord/cli.py`,
  service-unit templates; `install-tools/README.md` (deprecation);
  frontend-build CI step to emit wheel assets.
- **Acceptance (Mac):** `uv tool install .` then `copycord doctor`,
  `copycord service install --user`, `copycord run` on Linux (systemd) brings up
  a working instance from a clean machine; smoke test on Windows + macOS;
  upgrade path (`uv tool upgrade`) works; v3 `data.db` picked up.
- **Depends on:** single-process (the CLI runs the collapsed process), py-bump.

### Release
- Merge `v4` -> main, tag v4.0.0. Ship one container image + the published
  package. `CURRENT_VERSION` bump (`config.py:17`).

Dependency order summary:
`selfclient-core` -> `selfclient-gateway` -> `selfclient-events`; `py-bump` in
parallel then merged; `single-process` after events+bump; `installer` last; then
`v4` -> main at release.
