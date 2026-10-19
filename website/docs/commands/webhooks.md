---
sidebar_position: 7
title: Webhooks
---

# Webhook commands

These commands change how cloned messages look in the clone server: the name and avatar they are posted under, and the roles they mention.

## Channel webhook identity

By default, each cloned message shows its original author's name and avatar. The `/channel_webhook` commands post a channel's messages under one fixed name and avatar instead.

### `/channel_webhook set`

Sets a custom name and avatar for every cloned message in one channel.

```
/channel_webhook set channel:<channel> webhook_name:<name> [webhook_avatar_url:<url>]
```

| Option | Required | Description |
|--------|----------|-------------|
| `channel` | Yes | The cloned text channel |
| `webhook_name` | Yes | The name shown on the channel's cloned messages, up to 80 characters |
| `webhook_avatar_url` | No | The address of the avatar image |

```
/channel_webhook set channel:#cloned-chat webhook_name:Copycord Relay
/channel_webhook set channel:#cloned-chat webhook_name:Copycord Relay webhook_avatar_url:https://example.com/avatar.png
```

### `/channel_webhook view`

Shows the custom name and avatar of one channel.

```
/channel_webhook view channel:<channel>
```

| Option | Required | Description |
|--------|----------|-------------|
| `channel` | Yes | The cloned text channel |

```
/channel_webhook view channel:#cloned-chat
```

### `/channel_webhook clear`

Removes the custom name and avatar of one channel. Its messages show the original authors again.

```
/channel_webhook clear channel:<channel>
```

| Option | Required | Description |
|--------|----------|-------------|
| `channel` | Yes | The cloned text channel |

```
/channel_webhook clear channel:#cloned-chat
```

### `/channel_webhook list`

Lists the channels that have a custom name and avatar.

```
/channel_webhook list
```

This command has no options.

### `/channel_webhook set_all`

Sets a custom name and avatar for every cloned channel in this server.

```
/channel_webhook set_all webhook_name:<name> confirm:confirm [webhook_avatar_url:<url>] [overwrite_existing:true]
```

| Option | Required | Description |
|--------|----------|-------------|
| `webhook_name` | Yes | The name shown on all cloned messages, up to 80 characters |
| `confirm` | Yes | Type `confirm` to apply the change |
| `webhook_avatar_url` | No | The address of the avatar image |
| `overwrite_existing` | No | `true` also replaces channels that already have a custom name and avatar. Default: `false` |

```
/channel_webhook set_all webhook_name:Copycord Relay confirm:confirm
/channel_webhook set_all webhook_name:Copycord Relay confirm:confirm webhook_avatar_url:https://example.com/avatar.png overwrite_existing:true
```

### `/channel_webhook clear_all`

Removes the custom name and avatar of every cloned channel in this server.

```
/channel_webhook clear_all confirm:confirm
```

| Option | Required | Description |
|--------|----------|-------------|
| `confirm` | Yes | Type `confirm` to apply the change |

```
/channel_webhook clear_all confirm:confirm
```

## Role mentions {#role-mentions}

The `/role_mention` commands add a mention of a role at the top of cloned messages, for example to ping an alerts role. Run them in the clone server.

### `/role_mention add`

Adds a role to mention on cloned messages, in every channel or in one channel.

```
/role_mention add role:<role> [channel_id:<id>]
```

| Option | Required | Description |
|--------|----------|-------------|
| `role` | Yes | The role in the clone server to mention |
| `channel_id` | No | The ID of a cloned channel. Leave it out to mention the role in every channel |

```
/role_mention add role:@Alerts
/role_mention add role:@RaidPing channel_id:123456789012345678
```

Roles managed by a bot or an integration are rejected.

### `/role_mention list`

Lists the role mentions of this clone server.

```
/role_mention list
```

This command has no options.

Each entry shows a short ID, such as `a1b2c3d4`, the role, and whether it applies to every channel or one channel. Use the ID with `/role_mention delete`.

### `/role_mention delete`

Deletes a role mention by its ID.

```
/role_mention delete config_id:<id>
```

| Option | Required | Description |
|--------|----------|-------------|
| `config_id` | Yes | The short ID shown by `/role_mention list` |

```
/role_mention delete config_id:a1b2c3d4
```
