---
title: Exports and downloads
---

# Exports and downloads

Three task types on the [Tasks](/docs/dashboard/tasks) page save data that your user account (the client) can read: **Export messages**, **Export DMs** and **Download avatars**. All three need the server bot and the client running.

Each run saves its result in the data directory (`/data` in Docker). When the run ends, its run page has a **Download** link for the file.

## Export messages

**Export messages** reads a server's messages, keeps the ones that pass your filters and saves them to a JSON file. It can also post each exported message to a webhook.

To export a server's messages:

1. Open the **Tasks** page, pick **Export messages** in **Task types** and click **Run task**. Or, on the [Guilds](/docs/dashboard/guilds) page, open the menu on the server's card and click **Export messages**.
2. In **Server**, pick the server. Type to filter by name or ID.
3. Optional: set any of the options and filters below.
4. Click **Export messages**.

The file is saved under `exports/<guild_id>/`. Only one export per server can run at a time. The run's summary counts the messages scanned, exported, forwarded and failed.

| Option | What it does |
|--------|--------------|
| **Channel ID** | Export only this channel |
| **User ID** | Export only this user's messages |
| **Webhook URL** | Also post every exported message to this webhook |
| **After** and **Before** | Export only messages in this date range, in your local time |
| **Download media** | Save the attachments of the ticked types (**Images**, **Videos**, **Audio**, **Other files**) next to the JSON file, in `media/<type>/` |

| Filter | What it does |
|--------|--------------|
| **Include** | The kinds of message content to keep: **Text content**, **Embeds**, **Attachments**, **Links**, **Emojis**, **Replies**, **Bot messages**, **System messages**, **Pinned messages**, **Stickers**, **Mentions** and **Threads**. All are ticked by default |
| **Attachment types** | Which attachment types count when **Attachments** is ticked |
| **Only messages with a word** | Keep only messages that contain the **Word** |
| **Minimum length** | Keep only messages with at least this many characters. `0` keeps all |
| **Minimum reactions** | Keep only messages with at least this many reactions in total. `0` keeps all |

## Export DMs

**Export DMs** reads the DM history between your user account and one user, oldest first, and saves it as a JSON file, posts it to a webhook, or both.

To export a DM history:

1. Open the **Tasks** page, pick **Export DMs** in **Task types** and click **Run task**.
2. In **User ID**, enter the user's numeric Discord ID. Your user account needs an open DM with this user.
3. Choose where the messages go. **Save a JSON file** is on by default; to also post every message to a webhook, enter it in **Webhook URL**. Pick at least one.
4. Click **Export DMs**.

The file is saved as `exports/dm/<user id>/<time>/messages.json`. Only one DM export per user can run at a time.

## Download avatars

**Download avatars** collects the profile pictures of the members your user account can see in one of its servers, and zips them behind a download link that expires.

To download avatars:

1. Open the **Tasks** page, pick **Download avatars** in **Task types** and click **Run task**.
2. In **Server**, pick the server. Type to filter by name or ID.
3. Optional: in **Link expires after (hours)**, enter 1 to 168 hours (7 days). The default is 24.
4. Optional: in **Parallel downloads**, enter 1 to 32. The default is 8. Higher is faster; lower it if Discord rate limits the run.
5. Click **Download avatars**.

The run page shows the zip link until it expires, then **Link expired**. Avatar downloads run one at a time; a second run waits as **Queued**. The run's summary counts the members found and the avatars downloaded, skipped and failed.
