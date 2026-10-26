---
sidebar_position: 2
title: Guild mappings
---

# Guild mappings

A guild mapping links a source server (the host) to the server that receives the copy (the clone). Use this page to create, edit, pause and delete mappings.

Mappings are in the **Cloning Configuration** card of the **Configuration** page. You can add or edit them only while the bots are stopped; while the bots run, the cards are locked and show **Stop the bots to edit mappings**.

You can have as many mappings as you need, for example several host servers into one clone server, or one host server into several clones. Each mapping has its own settings and filters.

## Create a mapping

1. On the **Bots** card, click **Stop**.
2. In **Cloning Configuration**, click the **+** tile (**Add new mapping**).
3. In **Mapping Name**, enter a name for the mapping.
4. In **Host Guild**, enter the ID of the server to copy from. Your user account must be in it.
5. In **Clone Guild**, enter the ID of the server to copy to. Your bot must be installed in it.
6. Optional: change the settings.
7. Click **Save Mapping**.

You cannot change the host and clone guild IDs after you create the mapping. When you start the bots, Copycord copies the host's channels, categories and roles to the clone; see [Structure sync](/docs/features/structure-sync).

To find a mapping later, type its name in **Search mappings**.

## Edit a mapping's settings

1. Stop the bots.
2. On the mapping card, click **Settings**.
3. Change the settings. To find one, type in **Search settings**.
4. Click **Save Mapping**.

The settings are grouped into sections. Every option is described in [Cloning options](/docs/configuration/cloning-options).

| Section | What it controls |
|---------|------------------|
| **General** | Cloning on or off, message cloning, on-demand webhooks |
| **Channels** | Channel deletion, renaming, order, permissions, topic, NSFW flag, slowmode, and voice, stage and forum properties |
| **Messages** | Message edits, deletes and resends |
| **Roles** | Role cloning, deletion, permissions, icons, styles and order |
| **Assets** | Emoji, stickers, voice and stage channels |
| **Server Identity** | Server icon, banner, invite and discovery splash, tag and description |

To choose what is cloned for a mapping, click **Filters** on its card instead; see [Filters](/docs/dashboard/filters).

## Change how cloned messages look

1. Open the mapping's **Settings**.
2. At the bottom, click **Optional Message Features**.
3. Turn on the features you want. A live preview shows the result.
4. Close the **Optional Message Features** window, then click **Save Mapping**.

| Feature | Effect |
|---------|--------|
| **Tag Replies** | Add a link to the original message when a message is a reply |
| **Anonymize Users** | Replace usernames and avatars with random names and images |
| **Disable @everyone** | Remove `@everyone` and `@here` pings |
| **Disable Role Mentions** | Remove role pings |
| **Append Timestamp** | Show the original message time |
| **Append Author** | Show the original author's username below the message |

## Pause or resume a mapping

- To stop all syncing for a mapping without deleting it, click the pause button on its card. A paused mapping copies no messages, edits, deletes or structure changes, and its card is greyed out. To resume, click the play button.

## Copy a mapping

- To create a mapping with the same settings as an existing one, click **Clone mapping** on its card. The mapping form opens with that mapping's settings filled in.

## Delete a mapping

1. On the mapping card, click **Delete mapping**.
2. Confirm.

Deleting a mapping removes the link between the two servers. The channels, roles and messages already created in the clone server stay.

## Token message cloning

Copycord posts cloned messages through channel webhooks. With token message cloning, a mapping posts them from real user accounts instead, so messages come from ordinary members.

:::warning
This feature uses user tokens. Discord may revoke your tokens or disable them. Only use accounts you are willing to lose.
:::

To send a mapping's messages from user accounts:

1. Open the mapping's **Settings** and expand **Token Message Cloning**.
2. Add the user tokens: one at a time with **Add user token**, or several at once, one per line. Each account must be a member of the clone server and able to post in the target channels; a token whose account is not in the clone server is rejected.
3. Turn on **Send messages as users**.
4. Click **Save Mapping**.

Each cloned message is then sent by one of the enabled tokens. If an account cannot deliver a message, Copycord tries another enabled token, then follows **When no account is free**.

Tokens are shown masked. You can enable, disable or remove each one. The **Token options** menu has **Verify tokens**, which checks every token and offers to delete the ones that fail, **Release all tokens**, which frees every account from the author it is assigned to and resets the nicknames and roles it applied (the tokens stay and keep sending), and **Delete all tokens**.

**Verify tokens** first asks whether to check through your proxies or directly from the server; the choice starts at the mapping's **Send through proxies** setting. Checks run in parallel: through proxies, one at a time per proxy and up to 20 at once; directly, 3 at a time.

While a mapping is active, every enabled token stays connected to Discord, so it is ready to send. Up to 10 accounts connect at once, and logins from one IP address start a quarter of a second apart, slowing down when Discord answers with a rate limit and speeding up again afterwards. A paused mapping keeps none of its tokens online. When Copycord stops, it closes all sessions at once and gives each one at most 3 seconds.

### Options

| Option | What it does |
|--------|--------------|
| **Account selection** | How a token is picked for each message. **Rotate evenly** spreads messages across all enabled tokens in turn. **Sticky per author** gives each source author one account for good; see below |
| **Mirror author nickname** | Sticky per author only. Renames the assigned account in the clone server to the author's display name |
| **Mirror author roles** | Sticky per author only. Gives the assigned account the cloned roles that match the author's roles |
| **Mirror author avatar** | Sticky per author only. Sets the assigned account's profile picture to the author's. This changes the account everywhere, not only in the clone server. The picture is uploaded again only when the author changes theirs. If Discord refuses the change, for example with a captcha, that account is skipped for the rest of the session |
| **When no account is free** | What happens when no token can deliver a message, or **Sticky per author** has no free token for a new author. **Send with the webhook** (the default) sends it through the webhook. **Drop the message** skips it. **Reuse the account idle longest** gives the new author the account that has not sent for the longest time, if it has been idle for at least **Reuse after idle (minutes)** (default 30); the old author's nickname and roles are reset on that account, and the old author gets a new account the next time they post. When no account has been idle that long, the webhook sends it. Mappings saved before this option existed keep their old **Fall back to webhook** choice: on means webhook, off means drop |
| **Show typing indicator** | Show "typing" for the whole **Send delay** before the message appears. With no delay it shows only briefly |
| **Attachments as links** | Post links to the source attachments instead of uploading the files again |
| **Send through proxies** | Send each account's requests through its own proxy from `proxies.txt`, so the accounts do not share one IP address. No two accounts use the same proxy at once, and each account keeps its proxy across sessions. Recommended, but off by default: with it off, the accounts connect and send from the server's own IP address. With it on and no proxies configured, the mapping's accounts stay offline. A token used by several mappings goes through a proxy when any of them has this on |
| **Send delay** | Wait a random time between **Minimum send delay** and **Maximum send delay** seconds before each message. Messages to the same channel take turns, so they never arrive in a burst |

With **Sticky per author**, an author moves to another account only when their token stops working: it is disabled, revoked or removed from the clone server. The author then gets an unused token, and the old account's mirrored nickname and roles are cleared. Its mirrored avatar stays until the account is assigned to another author, because Copycord has no record of the original picture. If no unused token is free, **When no account is free** decides what happens.

### How token sending differs from webhooks

- **Identity**: a message shows the account that sent it. A user account cannot set a name and avatar per message the way a webhook can; use **Sticky per author** with the mirror options to make an account look like the author.
- **Bot messages, webhook messages and rich embeds**: always sent through the webhook, because a user account cannot reproduce them.
- **Embeds**: user accounts cannot post rich embeds, so plain embeds become text and links.
- **Attachments**: downloaded from the source and uploaded by the sending account, unless **Attachments as links** is on.
- **Stickers**: the account sends custom stickers that were cloned into the clone server. Standard Discord stickers need Nitro on the account. A custom sticker that was not cloned is posted as an image by the bot.
- **Custom emoji**: emoji cloned into the clone server are kept. Animated emoji and emoji that were not cloned are removed, because Discord turns emoji an account without Nitro cannot use into plain `:name:` text.
- **Threads**: the account creates text threads and forum posts and posts inside them, so it shows as the thread's creator. If it cannot, the bot (text threads) or the webhook (forum posts) creates the thread.
- **Backfill**: a [Backfill channels](/docs/dashboard/backfill) run also sends through the tokens, without the send delay.
- **Edits**: the account that posted a message edits it, because Discord lets only the author edit a message. If that account is gone, the message is sent again instead, subject to **Resend Edited Messages**.
- **Deletes**: the bot deletes token messages with its Manage Messages permission, so deletes keep working after an account is removed. If the bot is refused, the sending account deletes the message.
- **Replies**: sent as real Discord replies instead of a link, controlled by **Tag Replies**. If the original message was never cloned, the reply is sent as an ordinary message.
