---
sidebar_position: 2
title: Monitoring
---

# Monitoring commands

These commands check that the server bot and the client are online, and show how your user tokens appear to Discord.

## `/ping_server`

Shows the server bot's latency, the server's details and the bot's uptime.

```
/ping_server
```

This command has no options.

The reply shows the latency in milliseconds, the server's name and member count, and the time since the bot last started.

## `/ping_client`

Shows the client's latency, the round-trip time between the client and the server bot, and the client's uptime.

```
/ping_client
```

This command has no options.

The reply shows the Discord connection latency, the round-trip time, and the time since the client last started. High values usually point to a slow network or a busy machine.

## `/token_debug`

Downloads a JSON file that shows how each of this mapping's user tokens appears to Discord. Use it when token messages send but look wrong, or after you change proxy settings.

```
/token_debug [limit:<number>]
```

| Option | Required | Description |
|--------|----------|-------------|
| `limit` | No | How many tokens to include. Default: 25. Maximum: 100. When tokens are left out, the file says how many |

```
/token_debug
/token_debug limit:50
```

For each token, the file holds:

- The account's username, and a short reference that matches it in the logs.
- Its proxy, with the credentials masked.
- The IP address that Discord sees for the account.
- The headers that were actually sent, taken from a real request.
- The device details the account reports to Discord.
- A comparison with a real Discord desktop client, listing anything missing, different, or sent that a real client would not send.

Only you can see the reply. Authorization headers and proxy passwords are removed, but the file still describes your accounts, so keep it private.

To check that each account keeps its proxy, run the command twice a few minutes apart. Each token should show the same `egress_ip` both times.
