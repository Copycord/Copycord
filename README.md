[![Release](https://img.shields.io/github/v/release/Copycord/Copycord?label=Release&color=718956&logo=github&logoColor=718956)](https://github.com/Copycord/Copycord/releases/latest)
[![Downloads](https://img.shields.io/badge/dynamic/json?url=https://ghcr-badge.elias.eu.org/api/Copycord/Copycord/copycord&query=downloadCount&label=Downloads&logo=github&color=718956&logoColor=718956)](https://github.com/Copycord/Copycord/pkgs/container/copycord)
[![Discord](https://img.shields.io/discord/1406152440377638952?label=Discord&logo=discord&color=718956&logoColor=718956)](https://discord.gg/ArFdqrJHBj)

_⭐️ Love Copycord? Give us a star and join the conversation in our Discord community!_

# <img width="24px" src="logo/logo.png" alt="Copycord"></img> Copycord

Copycord is the ultimate Discord server mirroring tool. Effortlessly clone entire servers including channels, roles, emojis, and history while keeping everything in perfect sync with real-time message forwarding and structure updates. With powerful filters, custom branding options, DM and export tools, and a sleek web dashboard, Copycord gives you complete control to replicate, manage, and customize servers your way.

> [!TIP]
> **✨ Copycord Features**
>
> <details>
> <summary><b>Full Server Cloning</b></summary>
> Instantly mirror categories, channels, and message history from any target server—with the option to include roles, emojis, and stickers, all fully controlled through the web UI.
> </details>
>
> <details>
> <summary><b>Live Message Forwarding</b></summary>
> Every new message is forwarded in real time to your clone via webhooks, keeping both servers perfectly in sync including edits and deletes.
> </details>
>
> <details>
> <summary><b>Dynamic Structure Sync</b></summary>
> Copycord constantly watches for changes in the source server (new channels, renames, role updates) and applies them to your clone automatically.
> </details>
>
> <details>
> <summary><b>Advanced Channel Filtering</b></summary>
> Choose exactly which channels to include or exclude for maximum control over your clone’s layout.
> </details>
>
> <details>
> <summary><b>Custom Branding</b></summary>
> Rename channels, customize webhook names/icons, and make the clone feel like your own personalized server.
> </details>
>
> <details>
> <summary><b>Smart Message Filtering</b></summary>
> Automatically block or drop unwanted messages based on custom keyword rules.
> </details>
>
> <details>
> <summary><b>Member List Scraper</b></summary>
> Use the member scraper to grab User IDs, Usernames, Avatars, and Bios from any server.
> </details>
>
> <details>
> <summary><b>Deep History Import</b></summary>
> Clone an entire channel’s message history, not just the new ones.
> </details>
>
> <details>
> <summary><b>Universal Message Export</b></summary>
> Export all messages from any server into a JSON file with optional filtering, Webhook forwarding, and attachment downloading.
> </details>
>
> <details>
> <summary><b>DM History Export</b></summary>
> Export all DM messages from any user's inbox into a JSON file with optional Webhook forwarding.
> </details>
>
> <details>
> <summary><b>Real-Time DM Alerts</b></summary>
> Get instant DM notifications for key messages from any server — and subscribe your clone server members to get notifications too.
> </details>
>
> <details>
> <summary><b>Your Own Bot, Your Rules</b></summary>
> Run a fully independent Discord bot that you control—no restrictions.
> </details>
>
> <details>
> <summary><b>Sleek Web Dashboard</b></summary>
> Manage everything through a modern, easy-to-use web app.
> </details>

## Getting Started

### Prerequisites

- [Docker](https://github.com/Copycord/Copycord/blob/main/docs/Instructions.md)
- Discord Account Token + Discord Bot Token

### Setup

1. **Prepare the clone server**  
   Create a new Discord server to receive mirrored content and house your bot.

2. **Obtain your user token**

   - Log into Discord in a browser with your account.
   - Open Developer Tools (F12 or Ctrl+Shift+I)
   - Enable device emulation mode (Ctrl+Shift+M), then paste the code below into the console and press Enter:
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
   - Copy and store this token securely.

3. **Create and configure the bot**
   - In the [Discord Developer Portal](https://discord.com/developers/applications), create a new bot.
   - Under **Installation**, set the Install Link to `None` and click save.
   - Under **Bot**, click reset token and store your bot token somewhere secure, disable `Public Bot`, and enable these intents:
     - `Presence`
     - `Server Members`
     - `Message Content`
   - Under **OAuth2**, generate an invite url with (Scopes: `bot`, Bot Permissions: `Administrator`) and invite the bot to your clone server.

## Configuration

### 1. Create a copycord folder and add docker-compose.yml

```
copycord/
├── docker-compose.yml # docker compose file
└── data/ # data folder will be created automatically
```

`docker-compose.yml`

```yaml
services:
  admin:
    image: ghcr.io/copycord/copycord:v2.7.0
    container_name: copycord-admin
    environment:
      - ROLE=admin
    ports:
      - '8080:8080'
    volumes:
      - ./data:/data
    restart: unless-stopped

  server:
    image: ghcr.io/copycord/copycord:v2.7.0
    container_name: copycord-server
    environment:
      - ROLE=server
    volumes:
      - ./data:/data
    depends_on:
      - admin
    restart: unless-stopped

  client:
    image: ghcr.io/copycord/copycord:v2.7.0
    container_name: copycord-client
    environment:
      - ROLE=client
    volumes:
      - ./data:/data
    depends_on:
      - admin
    restart: unless-stopped
```

### 2. Launch Copycord

Make sure you have Docker & Docker Compose installed, then open a command prompt in the same directory and run:

```bash
docker-compose up -d
```

This will pull the latest images and start the web ui: http://localhost:8080

### 2. Configure Copycord via the web ui

<p align="left">
  <img src="logo/dashboard.png" alt="Dashboard" width="1000"/>
</p>

### Configuration

| Option                    | Default | Description                                                           |
| ------------------------- | ------- | --------------------------------------------------------------------- |
| `SERVER_TOKEN`            | none    | Your custom Discord bot token                                         |
| `CLIENT_TOKEN`            | none    | Your personal Discord account token                                   |
| `HOST_GUILD_ID`           | none    | The ID of the target server you want to clone                         |
| `CLONE_GUILD_ID`          | none    | The ID of the clone guild you created                                 |
| `COMMAND_USERS`           | none    | User IDs allowed to execute slash commands in the clone server        |
| `EDIT_MESSAGES`           | true    | Edit cloned messages after they are edited in the host server.        |
| `DELETE_MESSAGES`         | true    | Delete cloned messages after they are deleted in the host server.     |
| `DELETE_CHANNELS`         | true    | Delete categories + channels when deleted in the target server        |
| `DELETE_THREADS`          | true    | Delete threads when deleted in the target server                      |
| `DELETE_ROLES`            | true    | Delete roles when deleted in the target server                        |
| `CLONE_EMOJI`             | true    | Clone emojis                                                          |
| `CLONE_STICKER`           | true    | Clone stickers                                                        |
| `CLONE_ROLES`             | true    | Clone roles                                                           |
| `MIRROR_ROLE_PERMISSIONS` | false   | Clone role permission settings (does not apply to channels)           |
| `MIRROR_CHANNEL_PERMISSIONS` | false   | Mirror channel permissions from the host                           |
| `ENABLE_CLONING`          | true    | Turn cloning on/off for the target server (listener mode if disabled) |
| `LOG_LEVEL`               | INFO    | Level of logs to show (`INFO` / `DEBUG`)                              |

##

### Slash commands

- [Slash Commands Wiki](docs/slash_commands.md)

##

> [!WARNING]
> Copycord uses self-bot functionality, which is against Discord’s Terms of Service and could lead to account suspension or termination. While uncommon, we strongly recommend using an alternate account to minimize risk.

## Contributing & Support

Feel free to [open an issue](https://github.com/Copycord/Copycord/issues) if you hit any road bumps or want to request new features.

We appreciate all contributions:

1. Fork the repository.
2. Create a new branch from `main` with a descriptive name.
3. Commit your changes and open a [Pull Request](https://github.com/Copycord/Copycord/pulls), detailing your feature or fix.
4. See the [Contributing Guide](https://github.com/Copycord/Copycord/tree/main/docs/contribute/CONTRIBUTING.md) for build & testing instructions.

Thank you for helping improve Copycord!

# Buy me a coffee

If you are enjoying Copycord, consider buying me a coffee!

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/A0A41KPDX4)
