<p align="left">
  <img src="../logo/logo.png" alt="Copycord Logo" width="100"/>
</p>

## Bot Commands Usage

This document provides easy-to-follow instructions on how to use the available slash commands in the Bot.

> [!IMPORTANT]
> *Ensure the bot has the correct permissions and that `COMMAND_USERS` in `config` includes your user ID.*

---

### 1. `/ping_server`

**Description:** Shows the bot's latency, server name, member count, and uptime.

**Usage:**

```
/ping_server
```

---

### 2. `/ping_client`

**Description:** Measures client latency, round‑trip time, and client uptime.

**Usage:**

```
/ping_client
```

---

### 3. `/block_add <keyword>`

**Description:** Toggles a keyword in the block list. Messages with blocked keywords will be dropped.

**Usage:**

```
/block_add spoiler
```

---

### 4. `/block_list`

**Description:** Lists all currently blocked keywords.

**Usage:**

```
/block_list
```

---

### 5. `/verify_structure [delete:<true|false>]`

**Description:** Reports channels and categories not present in the last sitemap. Optionally deletes them.

**Usage:**

```
/verify_structure
/verify_structure delete:true
```

**Parameters:**

* `delete` (optional): `true` to delete orphans, `false` (default) to only report.


---


### 6. `/announcement_trigger <keyword> <user_id> [channel_id]`

**Description:** Registers a trigger so that when a specific user posts a message containing the keyword, a DM is sent to subscribers.

**Parameters:**

* `keyword`: Word to match
* `user_id`: Discord user ID to filter messages
* `channel_id`: Channel ID to listen in (omit for any channel)

**Usage Examples:**

```
/announcement_trigger `trade update` 123456789012345678
/announcement_trigger `I'm taking a long` 123456789012345678 987654321098765432
```

---

### 7. `/announcement_user [@user] [keyword]`

**Description:** Subscribes or unsubscribes a user to announcements for a keyword—or all keywords.

**Parameters:**

* `@user` (optional): The Discord user to toggle (defaults to yourself)
* `keyword` (optional): The keyword to subscribe to (omit for all)

**Usage Examples:**

```
/announcement_user            # toggle yourself for all keywords
/announcement_user @Tom        # toggle Tom for all keywords
/announcement_user trade       # toggle yourself for 'trade' only
/announcement_user @Tom trade  # toggle Tom for 'trade'
```

---

### 8. `/announcement_list [delete:<n>]`

**Description:** Lists current announcement triggers or deletes one by its index.

**Parameters:**

* `delete`: The 1-based index of the trigger to remove

**Usage Examples:**

```
/announcement_list            # show all triggers
/announcement_list delete:2   # remove trigger #2
```


---

### 9. `/clone_messages <#cloned_channel>`

**Description:** Clones the full message history from the mapped **host** channel into the specified **cloned** channel, streaming **oldest → newest**. While the backfill runs, new (live) messages for that channel are **buffered** and sent immediately after the backfill completes. You’ll receive a **DM summary** when it’s done.

**Parameters:**

* `#cloned_channel` (required): The cloned channel to populate with history.

**Usage Examples:**

```
/clone_messages <#general>
```

---

### 10. `/start_member_export <include_names> <num_sessions>`

**Description:** Begins scraping all members from the host guild using Discord’s member list gateway API. The scraper will run until it reaches the reported **guild member count**, ensuring near-complete coverage.  

**Parameters:**
* `include_names` (true/false): Whether to include usernames alongside IDs.  
* `num_sessions` (1–5): Number of parallel sessions to use. More sessions = faster scrape, but higher chance of flagging your account.

**Usage Examples:**

```
/start_member_export include_names:true num_sessions:3
/start_member_export include_names:false num_sessions:5
```

---

### 11. `/cancel_member_export`

**Description:** Cancels any running member export job. The scraper stops safely and send you a DM with any results found.

---

### 12. `/onjoin_dm <server_id>`

**Description:** Toggles DM notifications when someone joins the specified server. (Only works for servers with 1k or less members)
If enabled, you’ll receive a direct message with the new member’s details whenever someone joins that guild.
> **Note:** Your account connected to Copycord must be a member of the server to detect when someone joins the server.

**Parameters:**
* `server_id` (required): The Discord server (guild) ID you want to watch.

**Usage Example:**


```
/onjoin_dm 123456789012345678
```
