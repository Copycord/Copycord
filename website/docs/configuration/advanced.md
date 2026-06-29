---
sidebar_position: 3
title: Advanced Configuration
---

# Advanced Configuration

This page covers advanced settings and deployment configurations.

## Changing the dashboard port

### Docker

Edit the port mapping in `docker-compose.yml`:

```yaml
admin:
  ports:
    - '9060:8080'  # Dashboard at http://localhost:9060
```

### Manual install

Edit `ADMIN_PORT` in `code/.env`:

```
ADMIN_PORT=9060
```

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

## Custom WebSocket URLs

For non-standard deployments where services aren't on the same Docker network, configure WebSocket URLs manually:

```env
WS_SERVER_URL=ws://custom-host:8765
WS_CLIENT_URL=ws://custom-host:8766
WS_SERVER_CTRL_URL=ws://custom-host:9101
WS_CLIENT_CTRL_URL=ws://custom-host:9102
```

## Database location

By default, the SQLite database is at `/data/data.db` (Docker) or `data/data.db` (manual). To change it:

```env
DB_PATH=/custom/path/data.db
DATA_DIR=/custom/path
```

:::warning
Make sure the directory exists and is writable. For Docker, mount the appropriate volume.
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
- Applies to `server.out` and `client.out`

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
client:
  environment:
    SITEMAP_STRESS_TEST: "true"
```

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
