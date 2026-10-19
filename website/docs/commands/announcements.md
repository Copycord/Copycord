---
sidebar_position: 4
title: Announcements
---

# Announcement commands

Announcements send a DM to subscribed users when a message in a source server contains a keyword. A trigger defines the keyword to watch for; a subscription says who gets the DM.

When a message matches a trigger, each user subscribed to that keyword, or to all keywords, gets a DM with the message details.

## `/announcement_trigger_add`

Adds a trigger: a keyword to watch for in one server, optionally limited to one user or one channel.

```
/announcement_trigger_add guild_id:<id> keyword:<word> user_id:<id> [channel_id:<id>]
```

| Option | Required | Description |
|--------|----------|-------------|
| `guild_id` | Yes | The ID of the server to watch, 17 to 20 digits |
| `keyword` | Yes | The word to match in messages |
| `user_id` | Yes | Match only messages from this user. `0` matches any user |
| `channel_id` | No | Match only messages in this channel. `0`, or leaving it out, matches any channel |

```
/announcement_trigger_add guild_id:123456789012345678 keyword:lol user_id:0
/announcement_trigger_add guild_id:123456789012345678 keyword:trade user_id:111111111111111111
/announcement_trigger_add guild_id:123456789012345678 keyword:raid user_id:0 channel_id:987654321098765432
```

## `/announce_trigger_list`

Lists every announcement trigger, or deletes one by its number in the list.

```
/announce_trigger_list [delete:<number>]
```

| Option | Required | Description |
|--------|----------|-------------|
| `delete` | No | The number of the trigger to delete, starting at 1 |

```
/announce_trigger_list
/announce_trigger_list delete:2
```

## `/announce_subscription_toggle`

Subscribes a user to a keyword's announcements, or unsubscribes them when they are already subscribed.

```
/announce_subscription_toggle guild_id:<id> [user:<user>] [keyword:<word>]
```

| Option | Required | Description |
|--------|----------|-------------|
| `guild_id` | Yes | The server the subscription applies to. `0` means all servers |
| `user` | No | The user to subscribe. Default: you |
| `keyword` | No | The keyword to subscribe to. Leave it out to subscribe to all keywords, shown as `*` |

```
/announce_subscription_toggle guild_id:0 keyword:lol
/announce_subscription_toggle guild_id:123456789012345678
/announce_subscription_toggle guild_id:123456789012345678 keyword:trade user:@Mac
```

## `/announce_subscription_list`

Lists every subscription, or deletes one by its number in the list.

```
/announce_subscription_list [delete:<number>]
```

| Option | Required | Description |
|--------|----------|-------------|
| `delete` | No | The number of the subscription to delete, starting at 1 |

```
/announce_subscription_list
/announce_subscription_list delete:7
```

## `/announce_help`

Shows a help message that explains the announcement commands.

```
/announce_help
```

This command has no options.
