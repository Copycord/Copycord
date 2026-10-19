---
sidebar_position: 1
title: Commands Overview
---

# Slash Commands Overview

Copycord provides slash commands for managing your clone servers directly from Discord.

## Before you start

:::important Prerequisites
1. Your Discord user ID must be in the `COMMAND_USERS` configuration (set via the dashboard, `copycord config set COMMAND_USERS <id>`, or the `COMMAND_USERS` environment variable)
2. Commands must be run in the **clone server**, not the source server
3. Both the server bot and client bot must be running
:::

## Command categories

| Category | Commands | Purpose |
|----------|----------|---------|
| [Monitoring](/docs/commands/monitoring) | `ping_server`, `ping_client`, `token_debug` | Check bot status, latency, and how user tokens appear to Discord |
| [Filtering](/docs/commands/filtering) | `block_add`, `block_list` | Block messages by keyword |
| [Announcements](/docs/commands/announcements) | `announcement_trigger_add`, `announce_subscription_toggle`, etc. | Keyword-triggered notifications |
| [Roles](/docs/commands/roles) | `onjoin_role`, `onjoin_dm`, `role_block`, `role_mention`, etc. | Role management and auto-assignment |
| [Assets](/docs/commands/assets) | `pull_assets` | Export emojis and stickers (purging is a Tasks page task) |
| [Webhooks](/docs/commands/webhooks) | `channel_webhook set/view/clear/list` | Custom webhook identities per channel |
| [Exports and debugging](/docs/commands/exports) | `mapping_debug`, `debug_report` | Debug info and a redacted database dump |
| [Rewrites](/docs/commands/rewrites) | `rewrite add/list/remove` | Find-and-replace rules for messages |

Message and DM exports, history imports, purges and asset syncs are not slash commands. They run as tasks on the dashboard's [Tasks](/docs/dashboard/tasks) page.

## Quick reference

All commands at a glance:

| Command | Purpose |
|---------|---------|
| `/ping_server` | Bot latency and server info |
| `/ping_client` | Client latency and uptime |
| `/token_debug [limit]` | How each user token appears to Discord |
| `/block_add <keyword>` | Toggle keyword block |
| `/block_list` | List blocked keywords |
| `/announcement_trigger_add` | Create announcement trigger |
| `/announce_trigger_list` | List or delete triggers |
| `/announce_subscription_toggle` | Subscribe to announcements |
| `/announce_subscription_list` | List or delete subscriptions |
| `/announce_help` | Announcement system help |
| `/onjoin_dm <server_id>` | Toggle join DM alerts |
| `/onjoin_role <role>` | Toggle on-join role |
| `/onjoin_roles` | List or clear on-join roles |
| `/onjoin_sync` | Sync on-join roles to members |
| `/pull_assets <asset>` | Export emojis and stickers to an archive |
| `/role_block <role\|role_id>` | Block role from cloning |
| `/role_block_clear` | Clear all role blocks |
| `/role_mention add\|list\|delete` | Auto-mention roles on messages |
| `/channel_webhook set\|view\|clear\|list` | Custom webhook identity |
| `/channel_webhook set_all\|clear_all` | Bulk webhook identity |
| `/mapping_debug` | Debug guild mapping |
| `/debug_report` | ZIP of redacted database tables for debugging |
| `/rewrite add\|list\|remove` | Word and phrase rewrites |
