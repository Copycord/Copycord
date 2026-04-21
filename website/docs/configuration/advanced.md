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

## Rate limit behavior

Copycord has built-in rate limiters that respect Discord's API limits:

| Action | Rate | Notes |
|--------|------|-------|
| Webhook messages | 5 per 2.5s | Per webhook |
| Channel creation | 2 per 15s | Per guild |
| Webhook creation | 1 per 30s | Per guild |
| Channel editing | 3 per 15s | Per guild |
| Role operations | 1 per 10s | Per guild |
| Emoji operations | 1 per 60s | Per guild |
| Sticker operations | 1 per 60s | Per guild |

These limits are handled automatically. If you're using proxies, some limits can be relaxed.
