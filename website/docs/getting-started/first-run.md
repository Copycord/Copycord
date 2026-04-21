---
sidebar_position: 5
title: First Run
---

# First Run

Now that Copycord is installed, let's configure it and create your first server clone.

## 1. Open the dashboard

Open your browser and go to:

```
http://localhost:8080
```

If you set a password during installation, you'll see a login screen. Enter the password you configured.

## 2. Enter your tokens

On the main dashboard page, you'll find fields to enter your tokens:

1. **Server Token** — Paste the **bot token** from the Discord Developer Portal
2. **Client Token** — Paste the **user token** you extracted from your browser

Click **Save** to store the tokens.

## 3. Set command users

In the dashboard settings, add your Discord user ID to **Command Users**. This authorizes you to use Copycord's slash commands in your clone server.

:::tip How to find your Discord user ID
1. Open Discord Settings → Advanced → Enable **Developer Mode**
2. Right-click your username anywhere in Discord
3. Click **Copy User ID**
:::

## 4. Start the bots

Click the **Start** buttons to launch both the server bot and client bot. The status indicators will turn green when both are connected.

## 5. Create a guild mapping

A **guild mapping** links a source server to your clone server:

1. Click **Create Mapping** (or go to the Guilds page)
2. Select the **Source Server** — the server you want to clone (must be accessible by your user account)
3. Select the **Clone Server** — the empty server where your bot is
4. Click **Create**

## 6. Watch the magic

Once the mapping is created and both bots are running, Copycord will:

1. **Scan** the source server structure
2. **Create** matching channels, categories, and roles in the clone
3. **Start forwarding** new messages in real time

You'll see activity in the dashboard logs and messages appearing in your clone server.

## What's next?

Now that your first clone is running, explore these features:

- **[Backfill history](/docs/dashboard/backfill)** — Import old messages from before you set up Copycord
- **[Configure filters](/docs/dashboard/filters)** — Choose which channels to include or exclude
- **[Customize cloning options](/docs/configuration/cloning-options)** — Fine-tune what gets synced
- **[Set up forwarding](/docs/dashboard/forwarding)** — Send messages to Telegram or other services
- **[Learn slash commands](/docs/commands/overview)** — Manage your clone from within Discord

:::tip Multiple servers
You can create as many guild mappings as you need — Copycord supports cloning multiple source servers simultaneously.
:::
