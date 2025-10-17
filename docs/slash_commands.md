<p align="left">
  <img src="../logo/logo.png" alt="Copycord Logo" width="100"/>
</p>

## Bot Commands Usage

This document provides easy-to-follow instructions on how to use the available slash commands in the Bot.

> [!IMPORTANT]
> *Ensure the bot has the correct permissions and that `COMMAND_USERS` in `config` includes your user ID.*

---

### `/ping_server`

**Description:** Shows the bot's latency, server name, member count, and uptime.

**Usage:**

```
/ping_server
```

---

### `/ping_client`

**Description:** Measures client latency, round‑trip time, and client uptime.

**Usage:**

```
/ping_client
```

---

### `/block_add <keyword>`

**Description:** Toggles a keyword in the block list. Messages with blocked keywords will be dropped.

**Usage:**

```
/block_add spoiler
```

---

### `/block_list`

**Description:** Lists all currently blocked keywords.

**Usage:**

```
/block_list
```

---

### `/announcement_trigger_add <guild_id> <keyword> <user_id> [channel_id]`

**Description:**  
Registers a trigger so that when a message in the given guild matches the keyword (and optional filters), it will announce to subscribers.

**Parameters:**
- `guild_id`: Discord server ID (`0 = all guilds`)  
- `keyword`: Word to match  
- `user_id`: Discord user ID (`0 = any user`)  
- `channel_id`: Channel ID to listen in (`0 = any channel`, omit for any)

**Usage Examples:**
```
/announcement_trigger_add guild_id:0 keyword:lol user_id:0
/announcement_trigger_add guild_id:123456789012345678 keyword:trade user_id:111111111111111111
/announcement_trigger_add guild_id:123456789012345678 keyword:raid user_id:0 channel_id:987654321098765432
```

---

### `/announce_trigger_list [delete:<n>]`

**Description:**  
Lists all current announcement triggers across every guild, or deletes one by its index.

**Parameters:**
- `delete`: The 1-based index of the trigger to remove

**Usage Examples:**
```
/announce_trigger_list
/announce_trigger_list delete:2
```

---

### `/announce_subscription_toggle <guild_id> [@user] [keyword]`

**Description:**  
Subscribes or unsubscribes a user to announcements for a keyword — or for all keywords.

**Parameters:**
- `guild_id`: Discord server ID (`0 = all guilds`)  
- `@user` (optional): The Discord user to toggle (defaults to yourself)  
- `keyword` (optional): The keyword to subscribe to (`* = all keywords`)  

**Usage Examples:**
```
/announce_subscription_toggle guild_id:0 keyword:lol
/announce_subscription_toggle guild_id:123456789012345678
/announce_subscription_toggle guild_id:123456789012345678 keyword:* user:@Mac
/announce_subscription_toggle guild_id:123456789012345678 keyword:trade user:@Mac
```

---

### `/announce_subscription_list [delete:<n>]`

**Description:**  
Lists all announcement subscriptions across every guild, or deletes one by its index.

**Parameters:**
- `delete`: The 1-based index of the subscription to remove

**Usage Examples:**
```
/announce_subscription_list
/announce_subscription_list delete:7
```

---

### `/announce_help`

**Description:**  
Shows a formatted help embed that explains how to use all the announcement commands.

---

### `/onjoin_dm <server_id>`

**Description:** Toggles DM notifications when someone joins the specified server. (Only works for servers with 1k or less members)
If enabled, you’ll receive a direct message with the new member’s details whenever someone joins that guild.
> **Note:** Your account connected to Copycord must be a member of the server to detect when someone joins the server.

**Parameters:**
* `server_id` (required): The Discord server (guild) ID you want to watch.

**Usage Example:**


```
/onjoin_dm 123456789012345678
```

### `/onjoin_role <role>`
**Description:** Toggle an on-join role for the current server. Run once to add the role; run again with the same role to remove it.

**Options:**
- `role` *(required, role)* — The role to add or remove from the on-join list.

**Usage Example:**
```
/onjoin_role @Member
```

**Notes:**
- Multiple on-join roles are supported (run the command once per role).
- Managed roles or roles above the bot’s top role cannot be assigned.
- The bot must have **Manage Roles** permission.

---

### `/onjoin_roles [clear]`
**Description:** List all configured on-join roles for the current server, or clear them all.

**Options:**
- `clear` *(optional, boolean, default: false)* — If `true`, removes **all** on-join roles for this server.

**Usage Example:**
```
/onjoin_roles clear:true
```

**Notes:**
- If `clear:false` (default), the command lists current on-join roles.
- Missing or deleted roles are shown as “missing” in the list.


---

### `/onjoin_sync [include_bots] [dry_run]`
**Description:** Scan all server members and add any **missing** on-join roles.

**Options:**
- `include_bots` *(optional, boolean, default: false)* — If `true`, bots are included in the sync.
- `dry_run` *(optional, boolean, default: false)* — If `true`, shows what would change without modifying roles.

**Usage Example:**
```
/onjoin_sync include_bots:true dry_run:true
```

---
### `/purge_assets <type> <confirm>`
**Description:** Purge all stickers, emojis, or roles.

**Usage Example:**
```
/purge_assets roles confirm
```
> **Note:** Make sure the copycord role is positioned at the top.

---

### `/role_block <role> <roleid>`
**Description:** Block a role from being added during sync.

**Usage Example:**
```
/role_block @member 12345678987654321
```

---
### `/role_block_clear`
**Description:** Clear all blocked roles.

**Usage Example:**
```
/role_block_clear
```

---

### `/export_dms <user_id> [webhook_url] [json_file]`
**Description:** Export a user’s DM history to a JSON file. Optionally forwards each message to a specified webhook.

**Options:**
- `user_id` *(required, string)* — The target user’s Discord ID to export DMs from.
- `webhook_url` *(optional, string, default: empty)* — If provided, messages are forwarded in real time to this webhook. If omitted or empty, forwarding is disabled.
- `json_file` *(optional, boolean, default: true)* — Whether to save a JSON snapshot of the DM export.

**Usage Example:**
```
/export_dms 123456789012345678 https://discord.com/api/webhooks/123/abc true
```

**Notes:**
- Only one DM export per user can run at a time.

---
### `/pull_assets <asset> [guild_id]`
**Description:** Export server **emojis** and/or **stickers** into a compressed folder.

**Options:**
- `asset` *(required, string; choices: `both`, `emojis`, `stickers`; default: `both`)* — Which assets to export.
- `guild_id` *(optional, string)* — Target server’s Discord ID. If omitted, the host guild is used.

**Usage Example:**
```
/pull_assets both
/pull_assets emojis
/pull_assets stickers 159962941502783488
```

**Notes:**
- The resulting archive is saved under: `/data/assets`.
- Inside the archive, assets are organized as:
---
