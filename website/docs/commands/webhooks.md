---
sidebar_position: 7
title: Webhooks
---

# Webhook Commands

Customize how cloned messages appear in your clone server by configuring webhook identities and role mention pings.

## Channel Webhook Identity

Override the webhook name and avatar used when posting cloned messages. By default, Copycord uses the original author's name and avatar.

### `/channel_webhook set`

Set a custom webhook identity for a specific channel.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `channel` | Yes | The cloned text channel to customize |
| `webhook_name` | Yes | Custom name shown on cloned messages (max 80 chars) |
| `webhook_avatar_url` | No | Custom avatar image URL |

**Examples:**
```
/channel_webhook set #cloned-chat "Copycord Relay"
/channel_webhook set #cloned-chat "Copycord Relay" webhook_avatar_url:https://example.com/avatar.png
```

---

### `/channel_webhook view`

View the current custom webhook profile for a channel.

```
/channel_webhook view #cloned-chat
```

---

### `/channel_webhook clear`

Remove the custom webhook profile from a channel. Messages will revert to showing the original author's name and avatar.

```
/channel_webhook clear #cloned-chat
```

---

### `/channel_webhook list`

List all channels that have custom webhook profiles.

```
/channel_webhook list
```

---

### `/channel_webhook set_all`

Apply a webhook name and avatar to **all** cloned channels at once.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `webhook_name` | Yes | Name to use on all cloned messages |
| `confirm` | Yes | Must be `confirm` to execute |
| `webhook_avatar_url` | No | Avatar URL for all channels |
| `overwrite_existing` | No | Replace existing per-channel profiles (default: false) |

**Examples:**
```
/channel_webhook set_all "Copycord Relay" confirm
/channel_webhook set_all "Copycord Relay" confirm webhook_avatar_url:https://example.com/avatar.png overwrite_existing:true
```

---

### `/channel_webhook clear_all`

Remove custom webhook profiles from all channels.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `confirm` | Yes | Must be `confirm` to execute |

```
/channel_webhook clear_all confirm
```

---

## Role Mentions {#role-mentions}

Configure roles that get auto-mentioned at the top of cloned messages. These commands must be run in the **cloned server**.

### `/role_mention add`

Add a role to be mentioned on cloned messages.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `role` | Yes | The role in the cloned guild to mention |
| `channel_id` | No | Scope to a specific cloned channel (omit for all channels) |

**Behavior:**
- If `channel_id` is **empty**: the role is mentioned on all cloned messages in the server
- If `channel_id` is **set**: the role is only mentioned in that specific channel
- Managed roles (created by bots/integrations) are rejected

**Examples:**
```
/role_mention add @Alerts
/role_mention add @RaidPing channel_id:123456789012345678
```

---

### `/role_mention list`

List all role mention configurations for the current clone.

Each entry shows:
- A short **config ID** (e.g., `a1b2c3d4`)
- The **role** being mentioned
- The **scope** (all channels or a specific channel)

```
/role_mention list
```

---

### `/role_mention delete`

Remove a role mention configuration by its config ID (shown in `/role_mention list`).

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `config_id` | Yes | The short ID from `/role_mention list` |

```
/role_mention delete a1b2c3d4
```
