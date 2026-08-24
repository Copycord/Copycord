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

---

## `/token_debug`

Downloads a JSON file describing how each of this mapping's user tokens actually
appears to Discord. Useful when messages send but something about them looks
wrong, or after changing proxy settings.

**Usage:**
```
/token_debug [limit]
```

**Options:**
- `limit` — how many tokens to include (default 25, maximum 100). If some are
  left out the file says how many.

**The file contains, per token:**
- The account's username and a stable short reference for matching it to the logs
- Its leased proxy, with credentials masked
- The **egress IP** the outside world actually sees for that account
- The exact headers sent, captured from a real request rather than read off the
  configuration — the two are not the same
- The decoded device fingerprint the account reports to Discord
- A comparison against a real Discord desktop client, listing anything missing,
  mismatched, or sent that a real client would not send

The response is only visible to you. Authorization headers are redacted and
never leave the server, and proxy passwords are masked — but the file still
describes your accounts, so keep it private.

:::tip
Run it twice a few minutes apart to check proxy stickiness: the same token
should report the same `egress_ip` both times.
:::
