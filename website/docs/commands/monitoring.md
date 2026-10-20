---
sidebar_position: 2
title: Monitoring
---

# Monitoring commands

These commands check that the server bot and the client are online. To see how your accounts appear to Discord, read [What Discord sees](/docs/configuration/advanced#what-discord-sees).

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
