---
sidebar_position: 2
title: Cloning Options
---

# Cloning Options

Choose what Copycord syncs from a source server to its clone. Each guild mapping has its own options. To change them, stop the bots, then open the mapping in the **Cloning Configuration** card on the **Configuration** page. To find an option, type in **Search settings…** at the top of the window.

## General

| Option | Default | Description |
|--------|---------|-------------|
| Enable Cloning | On | Turns all cloning for this mapping on or off |
| Clone Messages | On | Clone messages in real time through webhooks. When off, no webhooks are created during sync |
| On-Demand Webhooks | On | Create a channel's webhook when it receives its first message instead of during sync, which makes the first sync faster |

## Messages

| Option | Default | Description |
|--------|---------|-------------|
| Edit Messages | On | Edit cloned messages when the source message is edited |
| Resend Edited Messages | On | Resend edited messages as new messages. Requires Edit Messages to be enabled |
| Delete Messages | On | Delete cloned messages when the source message is deleted |

## Channels

| Option | Default | Description |
|--------|---------|-------------|
| Delete Removed Channels | On | Delete cloned channels when removed from the source |
| Delete Removed Threads | On | Delete cloned threads when removed from the source |
| Reposition Channels | On | Sync channel order/position from source |
| Rename Channels | On | Sync channel name changes |
| Sync NSFW Flag | Off | Sync the NSFW (age-restricted) flag |
| Sync Channel Topic | Off | Sync channel topic/description |
| Sync Slowmode | Off | Sync slowmode (message cooldown) settings |
| Sync Forum Properties | Off | Sync forum layout, tags, and posting guidelines |
| Mirror Channel Permissions | Off | Mirror channel-level permission overwrites. Requires Clone Roles to be enabled |

## Assets

| Option | Default | Description |
|--------|---------|-------------|
| Clone Emoji | On | Clone custom emojis |
| Clone Stickers | On | Clone custom stickers |
| Clone Voice Channels | On | Clone voice channels |
| Sync Voice Properties | Off | Sync voice channel bitrate and user limit |
| Clone Stage Channels | On | Clone stage channels |
| Sync Stage Properties | Off | Sync stage channel properties |

## Roles

| Option | Default | Description |
|--------|---------|-------------|
| Clone Roles | On | Clone roles from the source server |
| Update Role Properties | On | Keep role properties (name, color, permissions) in sync after creation. When off, you can freely edit roles in the clone |
| Delete Removed Roles | On | Delete cloned roles when removed from the source |
| Mirror Role Permissions | Off | Mirror role permissions from source |
| Rearrange Roles | Off | Sync role ordering/position |
| Clone Role Icons | Off | Clone role icons (requires Boost Level 2+) |
| Clone Role Styles | Off | Clone role color styles (solid, gradient, holographic). Gradient/holographic styles require the clone server to have Boost Level 2+; otherwise the solid primary color is applied |

## Server identity

| Option | Default | Description |
|--------|---------|-------------|
| Clone Server Icon | Off | Clone the server icon |
| Clone Server Banner | Off | Clone the server banner |
| Clone Invite Splash | Off | Clone the invite splash image |
| Clone Discovery Splash | Off | Clone the discovery splash image |
| Clone Server Tag | Off | Clone the server tag and badge. **Disabled**: Discord does not allow bots to set server tags yet, so the toggle is inactive until they do |
| Sync Server Description | Off | Sync the server description |

## Message customization

These options are in the **Optional Message Features** window of the mapping.

| Option | Default | Description |
|--------|---------|-------------|
| Tag Replies | Off | Prepend a link to the original message when cloning a reply |
| Anonymize Users | Off | Replace usernames and avatars with random identities |
| Disable @everyone | Off | Strip @everyone and @here mentions from cloned messages |
| Disable Role Mentions | Off | Strip role mentions from cloned messages |
| Append Timestamp | Off | Append the original message timestamp below the cloned message |
| Append Author | Off | Append the original author's username below the cloned message |

## Sending as users

By default the bot posts cloned messages through webhooks. The **Token Message Cloning** section of a mapping sends them from Discord user accounts instead. The accounts must be members of the clone server.

:::danger Accounts can be banned
This uses user tokens. Discord may rate limit these accounts or ban them permanently at any time. Only use accounts you are willing to lose.
:::

| Option | Default | Description |
|--------|---------|-------------|
| Send messages as users | Off | Send cloned messages from the user accounts below instead of webhooks |
| Account selection | Rotate evenly | **Rotate evenly** cycles through the accounts so each sends an equal share. **Sticky per author** always sends a source user's messages from the same account |
| Mirror author nickname | Off | Sticky per author only: rename the assigned account in the clone server to the author's display name |
| Mirror author avatar | Off | Sticky per author only: try to set the account's avatar to the author's |
| Mirror author roles | Off | Sticky per author only: give the assigned account the cloned roles that match the author's roles |
| Fall back to webhook | On | When no account can send a message, send it through the webhook instead of dropping it |
| Show typing indicator | Off | Show "typing…" briefly before each message |
| Attachments as links | Off | Post links to the source attachments instead of uploading the files again |
| Send through proxies | Off | Give each account its own proxy from the [proxy list](/docs/configuration/advanced#proxy-support) |
| Send delay | 0 to 0 s | A random pause before each message, between the minimum and maximum, in seconds |

To add accounts, click **Add user token**; for a new mapping, save it and open it again first. You can add one token or several at once; each is checked when you add it. The token menu has **Verify tokens** and **Delete all tokens**.

## Recommended settings

Start with the defaults: they clone channels, messages, roles and emojis. Turn on more options as you need them. Mirror Channel Permissions and Mirror Role Permissions make more requests to Discord and can slow syncs on large servers.
