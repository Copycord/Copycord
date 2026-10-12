---
sidebar_position: 3
title: Advanced Configuration
---

# Advanced Configuration

This page covers advanced settings and deployment configurations.

## Changing the dashboard port

### Docker

Edit the port mapping of the `copycord` service in `docker-compose.yml`:

```yaml
copycord:
  ports:
    - '9060:8080'  # Dashboard at http://localhost:9060
```

### Install script

Set the `ADMIN_PORT` environment variable when starting Copycord:

```bash
ADMIN_PORT=9060 copycord run
```

For a background service, see [Environment variables for the service](/docs/cli#environment-variables-for-the-service).

## Proxy support

Copycord routes all client (self bot) traffic through a proxy when enabled — including login, REST API calls, and the gateway WebSocket connection. This means your real IP never touches Discord.

### Configuring client proxies

1. Go to the **Configuration** page → **Proxy** card
2. Enable **"Enable proxies for client"**
3. Paste one or more proxies — supported formats:
   ```
   host:port
   host:port:user:pass
   user:pass@host:port
   http://user:pass@proxy1.example.com:8080
   socks5://user:pass@proxy2.example.com:1080
   ```
4. Proxies auto-save as you add or remove them

Both HTTP and SOCKS5 proxies are supported.

### Proxy rotation

By default, one proxy is selected and used for all traffic. It only switches if the proxy fails.

To rotate proxies on a schedule, enable **"Rotate every"** and set an interval (e.g. 1 hour). The client will automatically switch to the next healthy proxy after the set duration.

:::tip
Longer rotation intervals (1-4 hours) are recommended. Rotating too frequently can itself be a detection signal. Residential proxies are strongly preferred over datacenter IPs.
:::

### Testing proxies

Click **Test All** to verify your proxies. Each proxy is tested by connecting to Discord's API through it. Results show which proxies are healthy, slow, or failed. You can remove failed and slow proxies directly from the results.

For large proxy lists, the test runs as a background task with a live progress bar. You can stop it at any time.

### Failover

If the active proxy drops or goes down, the client detects it automatically via the Discord gateway connection and switches to the next available proxy. No polling or extra API calls are made — detection is fully reactive.

A failed proxy is temporarily suspended so the client doesn't reconnect to it immediately. After the suspend duration expires, the proxy becomes eligible again.

### Timezone

The `X-Discord-Timezone` header sent with every request matches the timezone of the IP the account actually exits through, so it lines up with the proxy (or your own connection when no proxy is used). Copycord looks the timezone up once per proxy, caches it, and looks it up again when an account moves to a different proxy.

If the lookup fails, Copycord logs one warning and uses `America/New_York` until the next successful lookup.

### Proxy settings

Click the **Settings** button on the Proxy card to configure advanced options:

| Setting | Default | Description |
|---------|---------|-------------|
| **Suspend duration** | 300s | How long a failed proxy stays blocked before retrying |
| **Batch size** | 50 | Number of proxies tested concurrently during "Test All" |
| **Slow threshold** | 3s | Proxies slower than this are flagged in test results |

All settings are saved to the database. Suspend duration takes effect on next client restart. Test settings apply immediately.

### Scraper proxies

The member scraper shares the same proxy list. When proxies are configured, the scraper automatically uses them to distribute requests across different IPs.

## Message retention

By default, Copycord keeps message mapping records indefinitely. To automatically clean up old records:

### Via slash command

```
/env msg_cleanup days:30
```

This sets the retention period to 30 days. Message records older than this are automatically cleaned up.

### Via environment variable

```
MESSAGE_RETENTION_DAYS=30
```

## Internal ports

The dashboard, server bot and client run in one process and talk to each other inside it, so there are no WebSocket URLs to configure. Only the dashboard port (`ADMIN_PORT`, default `8080`) needs to be reachable.

Copycord also listens on two control ports on `127.0.0.1` only: `9101` (server) and `9102` (client). If another program already uses one of them, move them with environment variables:

```env
CONTROL_PORT_SERVER=9201
CONTROL_PORT_CLIENT=9202
```

## Database location

The SQLite database is `data.db` in the data directory: `/data/data.db` in Docker (your `./data` volume), otherwise `~/.copycord` (Windows: `%USERPROFILE%\.copycord`), or the folder in `COPYCORD_HOME`.

To use another data directory, start Copycord with `--data-dir` (or set `DATA_DIR`). Logs and backups move with it:

```bash
copycord run --data-dir /custom/path
```

To move only the database file, set `DB_PATH`:

```env
DB_PATH=/custom/path/data.db
```

:::warning
Make sure the directory is writable. For Docker, mount the appropriate volume. `copycord doctor --data-dir /custom/path` checks it.
:::

## Auto-start

By default, Copycord does not automatically start the server and client bots when it launches. To enable auto-start:

1. Go to **Global Configuration** in the dashboard
2. Set **AUTO_START** to `True`
3. Click **Save**

On the next launch, Copycord will validate your tokens and start both bots automatically if they are valid.

## Log pruning

Copycord automatically prunes log files to prevent them from growing indefinitely. Configure the maximum size via **MAX_LOG_SIZE_MB** in Global Configuration:

- Default: `10` MB
- Set to `0` to disable pruning
- Checks run every 5 minutes
- Applies to `server.out` and `client.out` (see [Where logs go](/docs/dashboard/system#where-logs-go))

The pruner uses memory-efficient seek-based reading — only the tail of the file is loaded into memory, even for very large log files. Writes are atomic (via temp file) to prevent data loss.

## Sync timing

When the client starts, it scans each server you're cloning and sends the full structure (channels, roles, emojis, stickers) to the bot so it knows what to sync. Servers are processed one at a time with a delay between each to avoid hitting Discord's rate limits.

After the initial startup sync, all changes are detected in real time via Discord gateway events — no periodic re-syncing is needed.

Configure sync timing on the **Configuration** page under the **Sync Settings** card:

| Setting | Default | Description |
|---------|---------|-------------|
| Startup Delay | 15s | Wait time after login before building sitemaps |
| Inter-Guild Delay | 3s | Pause between each server during startup sync |
| Randomize Order | On | Process servers in a random order each startup to avoid predictable patterns |

### Rate limit handling

If Discord returns a `429 Too Many Requests` response during sitemap generation, Copycord extracts the `retry_after` value from the response and waits the required duration before retrying. The affected server is re-queued at the front of the processing queue so no data is lost.

For webhook message sending, Copycord uses a lightweight rate limiter (5 per 2.5s per webhook) to stay within Discord's message rate limits.

### Stress testing

To test how the system handles rate limits under load, enable the stress test mode via environment variable:

```yaml
copycord:
  environment:
    SITEMAP_STRESS_TEST: "true"
```

Without Docker: `SITEMAP_STRESS_TEST=true copycord run`.

This continuously loops sitemap generation for all servers until the client is stopped. Only use this for testing — never in production.

If you're using proxies for the client, traffic is routed through the proxy IP, keeping your real IP hidden from Discord.

## Notifications

Copycord can send alerts to a Discord channel via webhooks when something goes wrong.

### Setup

1. Create a webhook in a Discord channel (Server Settings → Integrations → Webhooks)
2. Open the **Notifications** card on the dashboard
3. Paste the webhook URL
4. Toggle which events you want to be notified about

### Events

| Event | Description |
|---|---|
| **Client Offline** | The self-bot client has disconnected or stopped |
| **Server Offline** | The server bot has disconnected or stopped |
| **All Proxies Dead** | Every configured proxy has failed |
| **Token Invalid** | A bot token has been revoked or is no longer valid |

Each event type has a 5-minute cooldown to prevent notification spam. Use the **Test** button to verify your webhook URL is working.
