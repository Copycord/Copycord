---
sidebar_position: 2
title: Discord Setup
---

# Discord Setup

This guide walks you through everything you need to set up on Discord before running Copycord.

## Step 1: Create the clone server

Create a **new Discord server** that will receive all the mirrored content. This is where your bot will live.

1. Open Discord and click the **+** button in the server list
2. Choose **Create My Own** and select any template
3. Give it a name (e.g., "My Clone Server")

:::tip
Keep the clone server empty — Copycord will create all channels, roles, and emojis automatically.
:::

## Step 2: Obtain your user token

The user token allows Copycord's client to read messages from source servers as your Discord account.

:::warning Use an alt account
Since self-botting violates Discord's Terms of Service, we strongly recommend using an **alternate Discord account** — not your main account. The alt account must be a member of the source server(s) you want to clone.
:::

### How to get your user token

1. Open **Discord in your web browser** (not the desktop app) and log in with the account you want to use
2. Press **F12** (or **Ctrl+Shift+I** / **Cmd+Option+I**) to open Developer Tools
3. Enable **device emulation mode** by pressing **Ctrl+Shift+M** (or **Cmd+Shift+M**)
4. Go to the **Console** tab and paste this code:

```js
const iframe = document.createElement('iframe')
console.log(
  'Token: %c%s',
  'font-size:16px;',
  JSON.parse(
    document.body.appendChild(iframe).contentWindow.localStorage.token
  )
)
iframe.remove()
```

5. Press **Enter** — your token will appear in the console
6. **Copy and store this token securely** — treat it like a password

:::danger Keep your token secret
Anyone with your user token has full access to your Discord account. Never share it publicly or commit it to version control.
:::

## Step 3: Create and configure the bot

The bot token is used by Copycord's server component to manage the cloned server.

### Create the bot application

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications)
2. Click **New Application** and give it a name (e.g., "Copycord")
3. Click **Create**

### Configure the bot

1. In the left sidebar, click **Installation**
   - Set the **Install Link** to `None`
   - Click **Save Changes**

2. In the left sidebar, click **Bot**
   - Click **Reset Token** and copy the new token — store it securely
   - Disable **Public Bot** (so only you can add it to servers)
   - Enable these **Privileged Gateway Intents**:
     - ✅ **Presence Intent**
     - ✅ **Server Members Intent**
     - ✅ **Message Content Intent**
   - Click **Save Changes**

### Invite the bot to your clone server

1. In the left sidebar, click **OAuth2**
2. Under **OAuth2 URL Generator**:
   - **Scopes**: check `bot`
   - **Bot Permissions**: check `Administrator`
3. Copy the generated URL and open it in your browser
4. Select your **clone server** from the dropdown and click **Authorize**

:::info Why Administrator?
Copycord needs to create channels, manage roles, create webhooks, and manage messages. Administrator permission covers all of these. You can use more granular permissions, but Administrator is the simplest and most reliable option.
:::

## Summary

At this point you should have:

- ✅ A **clone server** with the bot added
- ✅ A **user token** from your alt account (which is a member of source servers)
- ✅ A **bot token** from the Developer Portal

Next, choose your installation method:
- [Docker Install](/docs/getting-started/docker-install) (recommended)
- [Manual Install](/docs/getting-started/manual-install)
