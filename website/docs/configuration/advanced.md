---
sidebar_position: 3
title: Advanced Configuration
---

# Advanced Configuration

Change the dashboard port, update behavior, client proxies, where data is stored, sync timing and alerts.

## Dashboard port

To change the dashboard port with the installer, set `ADMIN_PORT` in the [settings file](/docs/getting-started/configuration), then restart Copycord:

```bash title="Terminal"
copycord config set ADMIN_PORT 9060
copycord restart
```

For a single run, set the variable in the environment: `ADMIN_PORT=9060 copycord run`. When another program uses the port, `copycord doctor --fix` offers to move the dashboard to the next free port.

In Docker, change the left number of the port mapping in `docker-compose.yml`, then run `docker compose up -d`:

```yaml title="docker-compose.yml"
    ports:
      - '9060:8080'  # dashboard at http://localhost:9060
```

To open the dashboard to other devices, see [Security defaults](/docs/configuration/security#dashboard-address).

## Update settings

Set these keys with `copycord config set KEY VALUE`; they apply after a restart. See [Update check on start](/docs/cli#update-check) and [Auto-update](/docs/cli#auto-update).

| Key | Default | Description |
|-----|---------|-------------|
| `UPDATE_CHECK_ON_START` | `true` | Prints a hint at start when a newer version exists on your update channel. `COPYCORD_NO_UPDATE_CHECK=1` also turns it off. |
| `AUTO_UPDATE` | `false` | `true` lets the background service check every 6 hours, update itself and restart. A new version that does not start is replaced by the previous one. |
| `AUTO_UPDATE_ALL_CHANNELS` | `false` | `true` lets auto-update follow the `beta` and `dev` channels too. Otherwise only `stable` updates itself. |

Auto-update works only for a Copycord installed with the installer (or pipx) and running as a [service](/docs/cli#copycord-service). It does not update Docker or git checkouts.

## Proxy support

Route all of the client's traffic to Discord through a proxy, so Discord does not see your real IP address. HTTP and SOCKS5 proxies are supported.

### Add proxies

1. On the **Configuration** page, in the **Proxy** card, turn on **Enable proxies for client**.
2. Type or paste one or more proxies and press **Enter**. Changes save automatically.

Supported formats:

```
host:port
host:port:user:pass
user:pass@host:port
http://user:pass@proxy1.example.com:8080
socks5://user:pass@proxy2.example.com:1080
```

To test the list, click **Test All**. Copycord connects to Discord through each proxy and marks it healthy, slow or failed; you can remove the failed and slow ones from the results. A long list is tested as a background task with a progress bar that you can stop. **Clear** removes every proxy.

### Rotation and failover

The client uses one proxy until it fails, then switches to the next working one. A failed proxy is skipped for the **Suspend duration**, then tried again.

To switch on a schedule, turn on **Rotate every** and set an interval. Intervals of 1 to 4 hours work best: switching often can itself look suspicious to Discord. Residential proxies work better than datacenter proxies.

### Favorite proxies

To prefer a proxy, click its **★**. The client then connects through a favorite whenever one works, moves to another favorite when one drops, and uses a normal proxy only when no favorite works. With **Return to a favorite every** turned on, the client tests the favorites on that interval while it uses a normal proxy and switches back when one works. Without favorites, all proxies are treated the same.

### Proxy settings

To change these, click **Settings** on the Proxy card.

| Setting | Default | Description |
|---------|---------|-------------|
| **Suspend duration** | 300 s | How long a failed proxy is skipped. Applies after the client restarts. |
| **Batch size** | 50 | How many proxies **Test All** tests at once. |
| **Slow threshold** | 3 s | Proxies slower than this are marked slow in the test results. |

The client sends Discord the time zone of the address it connects from, through the proxy or your own connection. When Copycord cannot look it up, it logs a warning and uses `America/New_York`.

The Scrape members task on the **Tasks** page spreads its requests over the same proxy list. Its **Scraper tokens and proxies** card edits that list too.

### What Discord sees

This check runs only when `COPYCORD_ENV` is `dev`. On a normal install the client sends no request to `tls.peet.ws` and writes no traffic record. If the client cannot fetch the current Discord build, it uses the build that ships with Copycord, in dev mode too.

In dev mode the client records each account's Discord traffic in `captures/<date>-<time>-<account code>/events.jsonl` in the [data directory](/docs/cli#data-dir): the headers and bodies of its requests, every gateway message, and the device details sent when the account connects. Each time the client connects to Discord, it also adds a `wire_probe` record of what Discord sees from the account. Use it to check that an account goes out through the proxy you expect and looks like a real Discord client.

Each `wire_probe` record holds:

- The request headers as they arrive, with the authorization header removed.
- The TLS and HTTP/2 fingerprints of the connection.
- The IP address Discord sees, and the proxy, with its credentials masked.
- A comparison with a real Discord client, listing anything missing, different, or sent that a real client would not send.

To take this reading, the client sends one request per connect, through the account's own connection and proxy, to the public echo service `tls.peet.ws`. The request carries a placeholder instead of the account's token. Tokens and other secrets are redacted and accounts are named by a short code, but the record still describes your accounts, so keep it private.

## Ports

Only the dashboard port (`ADMIN_PORT`, default `8080`) must be reachable. Copycord also uses two ports on `127.0.0.1` for its parts to talk to each other: `9101` and `9102`. If another program uses one of them, set other ports in the environment:

```env
CONTROL_PORT_SERVER=9201
CONTROL_PORT_CLIENT=9202
```

## Database location

The database is `data.db` in the [data directory](/docs/cli#data-dir): `~/.copycord/data` on Linux and macOS, `%USERPROFILE%\.copycord\data` on Windows and `/data` in Docker (your `./data` folder).

To use another data directory, start Copycord with `--data-dir` or set `DATA_DIR`. Logs, backups and `.env` move with it:

```bash title="Terminal"
copycord run --data-dir /custom/path
```

To move only the database file, set `DB_PATH`, for example `DB_PATH=/custom/path/data.db`.

The folder must be writable. To check it, run `copycord doctor --data-dir /custom/path`. In Docker, mount the folder as a volume.

## Auto start

By default, the server bot and the client do not start when Copycord starts. To start them automatically:

1. Stop the bots.
2. On the **Configuration** page, in the **Global Configuration** card, turn on **Auto Start**.
3. Click **Save**.

On the next start, Copycord checks your tokens and starts both bots if they are valid.

## Log size

Copycord trims `server.out` and `client.out` every 5 minutes so they do not grow without limit. To change the limit, set **Max Log Size** in the **Global Configuration** card. The default is `10` MB; `0` turns trimming off. See [Where logs go](/docs/dashboard/system#where-logs-go).

## Sync timing

When the client starts, it reads the channels, roles, emojis and stickers of each source server and sends them to the bot. It reads one server at a time, with a pause between servers, to stay under Discord's rate limits. After that, changes are synced as they happen.

To change the timing, use the **Sync Settings** card on the **Configuration** page.

| Setting | Default | Description |
|---------|---------|-------------|
| Startup Delay | 15 s | Wait after login before reading the servers. |
| Inter-Guild Delay | 3 s | Pause between servers. |
| Randomize Order | On | Reads the servers in a random order on each start. |

When Discord answers a request with a rate limit, Copycord waits as long as Discord asks and tries that server again first. Each webhook sends at most 5 messages every 2.5 seconds.

For testing only, `SITEMAP_STRESS_TEST=true` makes the client read all servers over and over until it is stopped. Never use it in production.

## Notifications

Get a message in a Discord channel when something goes wrong.

1. In a Discord channel, create a webhook: **Server Settings > Integrations > Webhooks**.
2. On the dashboard, open the **Notifications** card and paste the webhook URL.
3. Choose the events. All four are on by default.
4. To check the URL, click **Test**.

| Event | Sent when |
|-------|-----------|
| **Client Offline** | The client disconnected or stopped. |
| **Server Offline** | The server bot disconnected or stopped. |
| **All Proxies Dead** | Every proxy failed. |
| **Token Invalid** | A token was revoked or is no longer valid. |

Each event is sent at most once every 5 minutes.
