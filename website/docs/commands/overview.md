---
sidebar_position: 1
title: Commands Overview
---

# Slash Commands Overview

Copycord provides a comprehensive set of slash commands for managing your clone servers directly from Discord.

## Before you start

:::important Prerequisites
1. Your Discord user ID must be in the `COMMAND_USERS` configuration (set via the dashboard or `.env`)
2. Commands must be run in the **clone server**, not the source server
3. Both the server bot and client bot must be running
:::

## Command categories

| Category | Commands | Purpose |
|----------|----------|---------|
| [Monitoring](/docs/commands/monitoring) | `ping_server`, `ping_client` | Check bot status and latency |
| [Filtering](/docs/commands/filtering) | `block_add`, `block_list` | Block messages by keyword |
| [Announcements](/docs/commands/announcements) | `announcement_trigger_add`, `announce_subscription_toggle`, etc. | Keyword-triggered notifications |
| [Roles](/docs/commands/roles) | `onjoin_role`, `onjoin_dm`, `role_block`, `role_mention`, etc. | Role management and auto-assignment |
| [Assets](/docs/commands/assets) | `purge_assets`, `pull_assets` | Manage emojis, stickers, and roles |
| [Webhooks](/docs/commands/webhooks) | `channel_webhook set/view/clear/list` | Custom webhook identities per channel |
| [Exports](/docs/commands/exports) | `export_dms`, `mapping_debug` | Export DM history and debug info |
| [Rewrites](/docs/commands/rewrites) | `rewrite add/list/remove` | Find-and-replace rules for messages |

## Quick reference

All commands at a glance:

```
/ping_server                              — Bot latency and server info
/ping_client                              — Client latency and uptime
/block_add <keyword>                      — Toggle keyword block
/block_list                               — List blocked keywords
/announcement_trigger_add                 — Create announcement trigger
/announce_trigger_list                    — List/delete triggers
/announce_subscription_toggle             — Subscribe to announcements
/announce_subscription_list               — List/delete subscriptions
/announce_help                            — Announcement system help
/onjoin_dm <server_id>                    — Toggle join DM alerts
/onjoin_role <role>                       — Toggle on-join role
/onjoin_roles                             — List on-join roles
/onjoin_sync                              — Sync on-join roles to members
/purge_assets <type> <confirm>            — Bulk delete assets
/pull_assets <asset>                      — Export emojis/stickers to ZIP
/role_block <role|roleid>                 — Block role from cloning
/role_block_clear                         — Clear all role blocks
/role_mention add|list|delete             — Auto-mention roles on messages
/channel_webhook set|view|clear|list      — Custom webhook identity
/channel_webhook set_all|clear_all        — Bulk webhook identity
/export_dms <user_id>                     — Export DM history
/mapping_debug                            — Debug guild mapping
/env msg_cleanup days:<n>                 — Set message retention
/rewrite add|list|remove                  — Word/phrase rewrites
```
