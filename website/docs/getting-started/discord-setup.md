---
sidebar_position: 1
title: Discord Setup
---

# Discord Setup

Create the clone server, get a user token and create the bot that Copycord needs. Do this before you log in to the dashboard for the first time.

## Create the clone server

The clone server receives everything Copycord copies. Leave it empty: Copycord creates the channels, roles and emojis.

1. In Discord, click **+** in the server list.
2. Click **Create My Own** and choose any template.
3. Enter a name and click **Create**.

## Get your user token

Copycord's client reads the source servers with this Discord account.

:::warning Use an alternate account
Logging in to Discord with a user token is against Discord's Terms of Service. Use an alternate account, not your main one. The account must be a member of every server you want to clone.
:::

1. Open Discord in your web browser, not the desktop app, and log in with the account.
2. To open the developer tools, press **F12** (or **Ctrl+Shift+I**, on macOS **Cmd+Option+I**).
3. To turn on device emulation, press **Ctrl+Shift+M** (on macOS **Cmd+Shift+M**).
4. Click the **Console** tab and paste this code:

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

5. Press **Enter**. The token appears in the console.
6. Copy the token and keep it somewhere safe.

Anyone with your user token has full access to the account. Never share it or commit it to a repository.

## Create the bot

Copycord's server part uses the bot to manage the clone server.

1. Open the [Discord Developer Portal](https://discord.com/developers/applications).
2. Click **New Application**, enter a name such as `Copycord` and click **Create**.
3. In the sidebar, click **Installation**, set **Install Link** to **None** and click **Save Changes**.
4. In the sidebar, click **Bot**.
5. Click **Reset Token**, then copy the new token and keep it somewhere safe.
6. Turn off **Public Bot**, so only you can add the bot to servers.
7. Under **Privileged Gateway Intents**, turn on **Presence Intent**, **Server Members Intent** and **Message Content Intent**.
8. Click **Save Changes**.

## Invite the bot to your clone server

1. In the sidebar, click **OAuth2**.
2. Under **OAuth2 URL Generator**, select the `bot` scope and the `Administrator` bot permission.
3. Open the generated URL in your browser.
4. Choose your clone server and click **Authorize**.

You can also invite the bot later from the dashboard: once the bot token is saved, **Add Bot to Server** opens the same invite.

Copycord creates channels, roles, webhooks and messages, and Administrator covers all of them. You can grant narrower permissions instead, but missing ones make syncs fail.

## Next steps

You now have a clone server with the bot in it, a user token and a bot token. If you have not installed Copycord yet, continue with the [Quick Start](/docs/getting-started/quick-start). Otherwise continue with [First Run](/docs/getting-started/first-run).
