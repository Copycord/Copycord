---
sidebar_position: 7
title: Message forwarding
---

# Message forwarding

The **Forwarding** page sends messages from your source servers to Discord webhooks, Telegram or Pushover as they arrive, for example to get a phone alert when someone mentions a keyword. Forwarding works alongside cloning, or on its own. For how forwarding works, see [External forwarding](/docs/features/message-forwarding).

## Create a forwarding rule

1. Open the **Forwarding** page and click **New Forward**.
2. In **Name**, enter a label for the rule.
3. In **Guild scope**, pick the source server the rule watches, or **All guilds**.
4. In **Provider**, pick **Discord**, **Telegram** or **Pushover**, then fill in its settings:
   - Discord: one or more webhook URLs in **Webhook URLs**. Optional: **Bot Username (optional)** and **Bot Avatar URL (optional)**.
   - Telegram: the **Bot token** and the **Chat ID**.
   - Pushover: the **App token** and the **User / group key**.
5. Optional: set the filters. See [Filter options](#filter-options).
6. Make sure **Rule enabled** is on.
7. Click **Save Rule**.

For Telegram and Pushover, **Save Rule** first checks the credentials with the service; if the check fails, the rule is not saved. Discord webhook URLs are checked when the rule is saved.

## Forward to several Discord webhooks

A Discord rule can post each matching message to several webhooks.

1. In the rule's **Webhook URLs** field, paste a webhook URL and press Enter. The URL becomes a chip.
2. Repeat for each webhook.
3. Click **Save Rule**.

The rule needs at least one valid webhook URL. Invalid URLs are rejected when you save, and duplicates are removed. The rule's username and avatar apply to every URL.

## Filter options

All filters are optional. A rule with no filters forwards every message from its **Guild scope**.

| Filter | Forwards |
|--------|----------|
| **Match any of these words / phrases** | Messages that contain any of them |
| **Require all of these words (optional)** | Only messages that contain all of them |
| **Limit to channel IDs (optional)** | Only messages from these channels |
| **Limit to user IDs (optional)** | Only messages from these users |

Click **More filters** for these switches. All are off by default.

| Switch | When on |
|--------|---------|
| **Case sensitive** | Words match only with the same capitalization |
| **Search text in embeds** | Words also match inside embeds |
| **Bot messages** | The rule can match messages sent by bots |
| **System messages** | The rule also forwards Discord's own notices: members joining, boosts, pins and thread creation |
| **Has attachments** | The rule matches only messages with attachments |

## Manage rules

Each rule card shows how many messages the rule has forwarded.

- To change a rule, click **Edit**.
- To copy a rule, click **Duplicate**.
- To turn a rule off or on, click **Disable** or **Enable**.
- To remove a rule, click **Delete** and confirm.

To find a rule, type in **Search rules**.

## Forward without cloning

To use Copycord only as a forwarder, keep a mapping but turn off its cloning:

1. Set up Copycord with your client token and server token as usual.
2. [Create a guild mapping](/docs/dashboard/guild-mappings#create-a-mapping) with the server to watch as the host and your bot's server as the clone.
3. In the mapping's **Settings**, turn off **Enable Cloning** and click **Save Mapping**.
4. On the **Forwarding** page, create your rules.

With cloning off, Copycord creates no channels or webhooks in the clone server and posts nothing there. The client still reads the host server, and your forwarding rules work as usual.
