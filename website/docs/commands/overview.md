---
sidebar_position: 1
title: Commands overview
---

# Slash commands overview

Copycord's slash commands manage your clone servers from Discord. This page lists every command and what each one needs.

## Requirements

- Your Discord user ID is in `COMMAND_USERS`. Set it in **Command Users** on the dashboard, with `copycord config set COMMAND_USERS <id>`, or as the `COMMAND_USERS` environment variable.
- You run the command in the clone server, not the source server.
- The server bot and the client are running.

## Reading the syntax

Each command is listed with what it does, its syntax, a table of its options and examples. In the syntax, `<value>` is a value you fill in and `[option:<value>]` is optional. In Discord, type the command name, then fill in each option in the field Discord shows for it.

Message, DM and asset exports, history imports, purges and asset syncs are not slash commands. They run as tasks on the dashboard's [Tasks](/docs/dashboard/tasks) page.

## All commands

| Command | What it does | Page |
|---------|--------------|------|
| `/ping_server` | Shows the server bot's latency and server details | [Monitoring](/docs/commands/monitoring) |
| `/ping_client` | Shows the client's latency and uptime | [Monitoring](/docs/commands/monitoring) |
| `/token_debug` | Shows how each user token appears to Discord | [Monitoring](/docs/commands/monitoring) |
| `/block_add` | Blocks or unblocks a keyword | [Filtering](/docs/commands/filtering) |
| `/block_list` | Lists the blocked keywords | [Filtering](/docs/commands/filtering) |
| `/onjoin_role` | Adds or removes an on-join role | [Roles](/docs/commands/roles) |
| `/onjoin_roles` | Lists or clears the on-join roles | [Roles](/docs/commands/roles) |
| `/onjoin_sync` | Gives the on-join roles to current members | [Roles](/docs/commands/roles) |
| `/onjoin_dm` | Turns join DM alerts for a server on or off | [Roles](/docs/commands/roles) |
| `/channel_webhook set`, `view`, `clear`, `list` | Manages one channel's webhook name and avatar | [Webhooks](/docs/commands/webhooks) |
| `/channel_webhook set_all`, `clear_all` | Manages the webhook name and avatar of every channel | [Webhooks](/docs/commands/webhooks) |
| `/role_mention add`, `list`, `delete` | Manages roles mentioned on cloned messages | [Webhooks](/docs/commands/webhooks#role-mentions) |
| `/mapping_debug` | Shows the mapping's data and settings | [Exports and debugging](/docs/commands/exports) |
| `/debug_report` | Makes a ZIP of the database with secrets removed | [Exports and debugging](/docs/commands/exports) |
| `/rewrite add`, `list`, `remove` | Manages find-and-replace rules | [Rewrites](/docs/commands/rewrites) |
