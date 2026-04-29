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

## Proxy support for scraping

Copycord supports proxy rotation for the member scraper and server bot operations. Configure proxies through the dashboard:

1. Go to the **Scraper** page → Proxy settings
2. Add proxy URLs (one per line), e.g.:
   ```
   http://user:pass@proxy1.example.com:8080
   socks5://user:pass@proxy2.example.com:1080
   ```
3. Save

Both HTTP and SOCKS5 proxies are supported.

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

## Rate limit behavior

Copycord relies on discord.py's native rate limit handling for structure sync operations (channel creation, editing, role operations, etc.). When Discord returns a `429 Too Many Requests` response, the library automatically waits the required `Retry-After` duration before retrying.

For webhook message sending, Copycord uses a lightweight rate limiter (5 per 2.5s per webhook) to stay within Discord's message rate limits.

If you're using proxies, requests are distributed across different IPs, allowing higher throughput.
