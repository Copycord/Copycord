---
title: Exports and Downloads
---

# Exports and Downloads

Three task types on the [Tasks](/docs/dashboard/tasks) page read data with the client account and save it to the data directory. All three need the server and client bots running. Open the **Tasks** page, pick the type in **Task types** and click **Run task**. The run page has a **Download** link for the result once the run finishes.

## Export messages

Reads a server's messages with the client account, keeps the ones that pass the filters and saves them to a JSON file. Messages can also be forwarded to a webhook.

You can also start it from the [Guilds](/docs/dashboard/guilds) page: **Export messages** on a guild card opens this form with that server picked.

### Options

| Option | Description |
|--------|-------------|
| **Server** | One of the servers the client account is in. Type to filter by name or ID |
| **Channel ID** | Optional: export only this channel |
| **User ID** | Optional: export only this user's messages |
| **Webhook URL** | Optional: every exported message is also posted to this webhook |
| **After** / **Before** | Optional date range, in your local time |
| **Download media** | Save attachments of the ticked types (**Images**, **Videos**, **Audio**, **Other files**) next to the JSON file, in `media/<type>/` |

### Filters

- **Include**: **Text content**, **Embeds**, **Attachments**, **Links**, **Emojis**, **Replies**, **Bot messages**, **System messages**, **Pinned messages**, **Stickers**, **Mentions** and **Threads**. All are ticked by default
- **Attachment types**: which attachment types count when **Attachments** is ticked
- **Only messages with a word**: keep only messages containing the **Word**
- **Minimum length**: characters, 0 for any
- **Minimum reactions**: all reactions together, 0 for any

### Output

The JSON file is saved under `exports/<guild_id>/` in the data directory (`/data` in Docker). Only one export per server can run at a time. The run's summary counts the messages scanned, exported, forwarded and failed.

## Export DMs

Reads the DM history between the client account and one user, oldest first, then saves it as a JSON file, forwards it to a webhook, or both.

| Option | Description |
|--------|-------------|
| **User ID** | The user's numeric Discord ID. The client account needs an open DM with this user |
| **Save a JSON file** | On by default. Saved as `exports/dm/<user id>/<time>/messages.json` in the data directory |
| **Webhook URL** | Optional: every message is also posted to this webhook |

Pick at least one destination: the JSON file, the webhook or both. Only one DM export per user can run at a time.

## Download avatars

Lists the members the client account can see in one of its servers, downloads each profile picture and zips them behind a download link that expires.

| Option | Description |
|--------|-------------|
| **Server** | One of the servers the client account is in. Type to filter by name or ID |
| **Link expires after (hours)** | From 1 to 168 hours (7 days). Default: 24 |
| **Parallel downloads** | From 1 to 32. Default: 8. Higher is faster; lower it if Discord rate limits the run |

The run page shows the zip link until it expires, then **Link expired**. Download avatars runs one at a time; a second run waits as queued. The run's summary counts the members found and the avatars downloaded, skipped and failed.
