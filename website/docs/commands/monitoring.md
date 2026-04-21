---
sidebar_position: 2
title: Monitoring
---

# Monitoring Commands

These commands let you check the health and status of your Copycord bots.

## `/ping_server`

Shows the server bot's latency, server information, and uptime.

**Usage:**
```
/ping_server
```

**Response includes:**
- Bot latency (milliseconds)
- Server name and member count
- Bot uptime since last restart

---

## `/ping_client`

Measures the client bot's latency, round-trip time to the server, and client uptime.

**Usage:**
```
/ping_client
```

**Response includes:**
- Discord WebSocket latency
- Round-trip time between server and client
- Client uptime since last restart

:::tip
Use these commands to quickly verify both bots are online and responsive. If latency is unusually high, check your network connection or server resources.
:::
