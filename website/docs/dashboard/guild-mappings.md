---
sidebar_position: 2
title: Guild Mappings
---

# Guild Mappings

A **guild mapping** is the core concept in Copycord: it defines a link between a source server and a clone server. Everything Copycord does revolves around these mappings.

Mappings live in the **Cloning Configuration** section of the **Configuration** page (the dashboard home). Mappings can only be added or edited while the bots are stopped; while they run, the cards are locked and show **Stop the bots to edit mappings**.

## Creating a mapping

1. Stop the bots, then open the **Configuration** page
2. Click the **+** tile (**Add new mapping**) in **Cloning Configuration**
3. Enter a **Mapping Name**
4. Enter the **Host Guild** ID: the server to mirror from, which your user account is in
5. Enter the **Clone Guild** ID: the server to mirror to, where your bot is installed
6. Adjust the settings and click **Save Mapping**

The host and clone guild IDs cannot be changed after the mapping is created. When the bots start, Copycord syncs the structure (channels, categories, roles) from the host to the clone.

## Managing mappings

Each mapping card has buttons for **Settings**, **Filters** (see [Filters](/docs/dashboard/filters)), pause or resume, **Clone mapping** and **Delete mapping**. Use **Search mappings** to find a mapping by name.

### Pause / Resume

You can temporarily pause a mapping without deleting it. Paused mappings stop all syncing: no messages, edits, deletes, or structure changes will be forwarded.

Click the pause or play button on a mapping card to pause or resume it. A paused card is greyed out.

### Clone a mapping

**Clone mapping** opens the mapping form prefilled with that mapping's settings, so you can create a similar mapping quickly.

### Edit settings

Each mapping has its own set of [cloning options](/docs/configuration/cloning-options) that control what gets synced. Click **Settings** on a mapping to view and edit them; **Search settings** filters the list. Settings are organized into sections:

- **General**: master cloning toggle, message cloning, on-demand webhooks
- **Channels**: channel deletion, renaming, repositioning, permissions, topic, NSFW flag, slowmode, voice, stage and forum properties
- **Messages**: message editing, deletion, resending
- **Roles**: role cloning, deletion, permissions, icons, styles and order
- **Assets**: emoji, stickers, voice and stage channels
- **Server Identity**: server icon, banner, invite and discovery splash, tag, description

### Message features

Click **Optional Message Features** at the bottom of the mapping settings to customize how cloned messages appear:

- **Tag Replies**: prepend a link when a message is a reply
- **Anonymize Users**: replace usernames and avatars with random names and images
- **Disable @everyone**: strip @everyone and @here pings
- **Disable Role Mentions**: strip role mention pings
- **Append Timestamp**: show the original message timestamp
- **Append Author**: show the original author's username below the cloned message

A live preview shows how the combined settings affect the cloned message.

### Token message cloning

By default Copycord posts cloned messages through channel **webhooks**. The **Token Message Cloning** section lets you instead post them from real **user accounts** (self-bot tokens), so messages appear to come from ordinary members rather than a webhook.

To use it:

1. Open a mapping and expand **Token Message Cloning**.
2. Add one or more **user tokens**, one at a time with **Add user token** or several at once, one per line. Each token is validated when you add it: the account must be a member of the **clone** server, or it is rejected.
3. Enable the **Send messages as users** toggle.

When enabled, each cloned message is sent by one of the enabled tokens via the Discord API. If an account can't deliver a message, Copycord uses another enabled token where possible; if none can and **Fall back to webhook** is on, it uses the normal webhook so no message is lost. Tokens are shown masked and can be individually enabled, disabled, or removed. The **Token options** menu can **Verify tokens** at once (and offer to delete the ones that fail) or **Delete all tokens**.

:::warning
Using self-bot accounts to send messages is against Discord's Terms of Service and can get those accounts banned. Each account must be in the clone server with permission to post in the target channels.
:::

#### Options

- **Account selection**: how a token is chosen for each message:
  - **Rotate evenly**: spread messages across all enabled tokens (round-robin).
  - **Sticky per author**: pin each source author to one token permanently, so that author's messages always come from the same account. The account is only swapped if its token goes bad (disabled, or revoked/removed from the clone server), in which case the author moves to an unused token and the old account's mirrored nickname and roles are cleared. A mirrored **avatar** is not cleared, because the account's original picture was never recorded; the old account keeps the author's picture until it is assigned to someone else. If no unused token is free, the message follows **Fall back to webhook**.
- **Mirror author nickname** *(Sticky per author only)*: rename the assigned account in the clone server to the host author's display name, so it looks like that member.
- **Mirror author roles** *(Sticky per author only)*: give the assigned account the cloned roles that match the host author's roles.
- **Mirror author avatar** *(Sticky per author only)*: set the assigned account's profile picture to the host author's. Unlike the nickname and roles this changes the **account itself**, not just its appearance in the clone server, so that account looks like the author everywhere it is a member. The picture is only re-uploaded when the author actually changes theirs. If Discord refuses the change (a captcha, for instance), that account is left alone for the rest of the session rather than retried on every message.
- **Fall back to webhook**: if no token can deliver a message (every token failed, or *Sticky per author* has no free token left for a new identity), send via the normal webhook instead of dropping it. With this off, the message is skipped.
- **Show typing indicator**: while a message waits out its **Send delay**, show the "typing…" indicator the whole time, so a 5 to 10 second delay looks like 5 to 10 seconds of typing before the message appears. With no send delay set it's just a brief blip.
- **Attachments as links**: post the source attachment links instead of re-downloading and re-uploading the files.
- **Send through proxies**: route each account's requests through its own proxy from `proxies.txt`, so accounts do not all share the server's IP. No two accounts use the same proxy at once, and an account keeps the same proxy across sessions. Off by default; with no proxy file the setting does nothing.
- **Send delay**: wait a random number of seconds between the **Minimum send delay** and **Maximum send delay** before each message is sent (its "composition time"). Messages to the same channel take turns, so they still never arrive in a burst.

Some behavior differs from webhook sending:

- **Identity**: the message appears as whichever account sent it; a user account cannot set a per-message username or avatar the way a webhook can. Use **Sticky per author** with nickname, role and avatar mirroring to make an account resemble the original author instead.
- **Bots, webhooks & rich embeds**: messages authored by bots or webhooks, and any message containing a rich embed, are always sent by the normal webhook (a user account cannot reproduce them), so their author identity and embed are preserved.
- **Embeds**: user accounts cannot post rich embeds, so plain embeds are flattened into text and links.
- **Attachments**: files are re-downloaded from the source and re-uploaded by the sending account, unless **Attachments as links** is on.
- **Stickers**: cloned custom stickers are sent by the token; standard Discord stickers require the account to have Nitro; a custom sticker that isn't cloned into the clone server falls back to the bot posting its image.
- **Custom emoji**: an account without Nitro cannot use every emoji, and Discord silently turns the ones it cannot into plain `:name:` text. Those are removed from the message instead. Emoji cloned into the clone server are kept, since any member may use their own server's emoji; animated emoji and emoji that were never cloned are dropped.
- **Threads**: token accounts create both **text threads** and **forum-thread starter posts** (and post the messages inside them), so the thread creator and the "started a thread" system message reflect the sending account. If a token can't create the thread, the bot (text) or webhook (forum) creates it instead.
- **Backfill**: a [Backfill channels](/docs/dashboard/backfill) run also sends through tokens when enabled, with no artificial delay between token sends.
- **Edits**: an edit is applied by the account that posted the message, since Discord only lets an author edit their own. If that account is no longer available the message is re-sent instead, subject to **Resend Edited Messages**.
- **Deletes**: the bot removes token-sent messages using its Manage Messages permission, so deletion keeps working even after the sending account is disabled or removed. If the bot is refused, the sending account is asked instead.
- **Replies**: a reply is posted as a real Discord reply rather than the link that webhook sending has to fall back on, because a user account can do what a webhook cannot. Controlled by the same **Tag replies** message feature. If the message being replied to was never cloned there is nothing to point at, so it is sent as an ordinary message.

### Delete a mapping

Click **Delete mapping** on its card and confirm. Deleting a mapping removes the link between the source and clone servers. It does **not** delete any channels, roles, or messages that were already created in the clone server.

## Multiple mappings

Copycord supports multiple guild mappings simultaneously. You can:

- Clone several source servers into one clone server
- Clone one source server into multiple clone servers
- Any combination of source → clone pairs

Each mapping operates independently with its own filters and settings.

## How syncing works

When a mapping is active, Copycord continuously:

1. **Watches** the source server for changes (via the client self-bot)
2. **Forwards** new messages to the clone via webhooks (or user tokens)
3. **Syncs** structural changes (new channels, renames, deletions)
4. **Updates** roles, emojis, and stickers as they change
5. **Tracks** message edits and deletes

When the client starts, it sends the full structure of each host server (see [Structure Sync](/docs/features/structure-sync)). After that, changes such as new channels and renames are detected and applied as they happen via Discord gateway events. To bring roles, emojis and stickers back in line on demand, run a [Sync assets](/docs/dashboard/purge-and-sync#sync-assets) task.
