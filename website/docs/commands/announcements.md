---
sidebar_position: 4
title: Announcements
---

# Announcement Commands

The announcement system lets you set up keyword-triggered notifications. When a message in a source server matches a trigger, all subscribed users get a DM notification.

## How it works

1. **Create a trigger** — Define a keyword to watch for, optionally scoped to a specific guild, user, or channel
2. **Subscribe users** — Users subscribe to receive DM notifications when triggers fire
3. **Get notified** — When a matching message appears, subscribers get a DM with the message details

---

## `/announcement_trigger_add`

Registers a new announcement trigger.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `guild_id` | Yes | Server ID to watch (`0` = all servers) |
| `keyword` | Yes | Word to match in messages |
| `user_id` | Yes | Only match messages from this user (`0` = any user) |
| `channel_id` | No | Only match in this channel (`0` or omit = any channel) |

**Examples:**
```
/announcement_trigger_add guild_id:0 keyword:lol user_id:0
/announcement_trigger_add guild_id:123456789012345678 keyword:trade user_id:111111111111111111
/announcement_trigger_add guild_id:123456789012345678 keyword:raid user_id:0 channel_id:987654321098765432
```

---

## `/announce_trigger_list`

Lists all announcement triggers, or deletes one by index.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `delete` | No | 1-based index of the trigger to remove |

**Examples:**
```
/announce_trigger_list
/announce_trigger_list delete:2
```

---

## `/announce_subscription_toggle`

Subscribes or unsubscribes a user to announcement triggers for a keyword.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `guild_id` | Yes | Server ID scope (`0` = all servers) |
| `user` | No | Discord user to toggle (defaults to yourself) |
| `keyword` | No | Keyword to subscribe to (`*` = all keywords) |

**Examples:**
```
/announce_subscription_toggle guild_id:0 keyword:lol
/announce_subscription_toggle guild_id:123456789012345678
/announce_subscription_toggle guild_id:123456789012345678 keyword:trade user:@Mac
```

---

## `/announce_subscription_list`

Lists all subscriptions, or deletes one by index.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `delete` | No | 1-based index of the subscription to remove |

**Examples:**
```
/announce_subscription_list
/announce_subscription_list delete:7
```

---

## `/announce_help`

Displays a formatted help embed explaining how to use all announcement commands.

**Usage:**
```
/announce_help
```
