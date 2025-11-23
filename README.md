[![Release](https://img.shields.io/github/v/release/Copycord/Copycord?label=Release&color=2E7D32&labelColor=2E7D32&logo=github&logoColor=FFFFFF)](https://github.com/Copycord/Copycord/releases/latest)
[![Discord](https://img.shields.io/discord/1406152440377638952?label=Discord&logo=discord&color=C62828&labelColor=C62828&logoColor=FFFFFF)](https://discord.gg/ArFdqrJHBj)



_⭐️ Love Copycord? Give us a star and join the conversation in our Discord community!_

# <img width="24px" src="logo/logo.png" alt="Copycord"></img> Copycord

Copycord is the ultimate Discord server mirroring tool. Effortlessly clone multiple servers at once including channels, roles, emojis, and history while keeping everything in perfect sync with real-time message forwarding and structure updates. With powerful filters, custom branding options, DM and export tools, and a sleek web dashboard, Copycord gives you complete control to replicate, manage, and customize servers your way.

> [!TIP]
> **✨ Copycord Features**
>
> <details>
> <summary><b>Multi-Server Cloning</b></summary>
> Instantly mirror categories, channels, and message history from target servers—with the option to include roles, emojis, and stickers, and much more, all fully controlled through the web UI.
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

## Docker Install

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
    image: ghcr.io/copycord/copycord:v3.1.4
    container_name: copycord-admin
    environment:
      - ROLE=admin
      - PASSWORD=copycord # change or comment out to disable login
    ports:
      - '8080:8080' # change this port if needed (ex: "9060:8080")
    volumes:
      - ./data:/data
    restart: unless-stopped

  server:
    image: ghcr.io/copycord/copycord:v3.1.4
    container_name: copycord-server
    environment:
      - ROLE=server
    volumes:
      - ./data:/data
    depends_on:
      - admin
    restart: unless-stopped

  client:
    image: ghcr.io/copycord/copycord:v3.1.4
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

## Manual Install (NEW) ✨

<details>
<summary><strong>Manual Install (Windows & Linux)</strong></summary>

### Windows

**Requirements**:

- Python 3.11
- Node.js + npm

1. Download the Windows installer bundle:  
   [`copycord.zip`](https://github.com/Copycord/Copycord/raw/refs/heads/main/install-tools/windows/copycord.zip)
2. Right-click the zip and choose **Extract All…** (for example, extract it to your **Desktop**).
3. Open the extracted `copycord` folder.
4. Double-click **`Copycord.exe`** and select the install option.  
   - This will download the latest Copycord build and set up everything in the same folder.
5. To start Copycord, double-click **`Copycord.exe`** and select the run option.  
   - This will start Copycord and the web UI: <http://localhost:8080>
6. When a new Copycord version is released, double-click **`Copycord.exe`** and select the update option.

---

### Linux

**Requirements**

- Python 3.11
- Node.js + npm  
  (Ubuntu/Debian example):
  - `curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -`
  - `sudo apt install -y nodejs`
- `python3-venv` + `python3.11-venv`  
  - `sudo apt install -y python3-venv python3.11-venv`

---

### Install Copycord (Linux)

1. Create a Copycord folder and download the launcher:

   ```bash
   mkdir -p ~/copycord
   cd ~/copycord
   curl -L "https://raw.githubusercontent.com/Copycord/Copycord/refs/heads/main/install-tools/source/launcher.py" -o launcher.py
   ```

2. Run the launcher and install:

   ```bash
   cd ~/copycord
   python3 launcher.py
   ```

   - When the menu appears, choose: `1) Install Copycord`
   - This will:
     - Download the latest Copycord version
     - Build the admin frontend
     - Create `code/`, `venvs/`, and `data/`
     - Generate `copycord_linux.sh` (Linux start script)

---

### Run Copycord (Linux)

You can start Copycord in either of these ways (from `~/copycord`):

- Using the launcher menu:

  ```bash
  cd ~/copycord
  python3 launcher.py
  ```

  - Choose: `4) Run Copycord (Linux)`

- Or run the start script directly:

  ```bash
  cd ~/copycord
  ./copycord_linux.sh
  ```

  (If needed: `chmod +x copycord_linux.sh` once.)

Once started, open the web UI in your browser:

- <http://localhost:8080>

---

### Update Copycord (Linux)

1. From the same folder (`~/copycord`), run:

   ```bash
   python3 launcher.py
   ```

2. Choose: `2) Update Copycord`  
   - This will update your Copycord install to the latest version.

---

### Environment Configuration

- Set the admin password, change ports, etc., in the `.env` file located in the `code` directory after installation.

</details>


### Web UI Configuration

<details>
<summary><strong>Copycord Configuration Options (click to expand)</strong></summary>

<br>

| Option                         | Default | Description                                                                                    |
| ------------------------------ | ------- | ---------------------------------------------------------------------------------------------- |
| `ENABLE_CLONING`               | true    | Master switch for cloning                                                                      |
| `CLONE_MESSAGES`               | true    | Clone messages in real-time                                                                    |
| `DELETE_CHANNELS`              | true    | Delete channels/categories removed in the host                                                 |
| `DELETE_THREADS`              | true    | Delete threads removed in the host                                                             |
| `DELETE_ROLES`                 | true    | Delete roles that no longer exist in the host                                                  |
| `UPDATE_ROLES`                | true    | Allow updating role properties after creation                                                  |
| `DELETE_MESSAGES`             | true    | Delete cloned messages when the host message is deleted                                        |
| `MIRROR_CHANNEL_PERMISSIONS`   | false   | Mirror channel permissions from the host                                                       |
| `CLONE_ROLES`                  | true    | Clone roles                                                                                    |
| `CLONE_EMOJI`                  | true    | Clone emojis                                                                                   |
| `CLONE_STICKER`                | true    | Clone stickers                                                                                 |
| `EDIT_MESSAGES`               | true    | Edit cloned messages when host messages are edited                                             |
| `MIRROR_ROLE_PERMISSIONS`      | false   | Mirror role permissions                                                                        |
| `REPOSITION_CHANNELS`          | true    | Sync channel order                                                                             |
| `RENAME_CHANNELS`              | true    | Sync channel renames                                                                           |
| `SYNC_CHANNEL_NSFW`            | false   | Sync NSFW flags                                                                                |
| `SYNC_CHANNEL_TOPIC`           | false   | Sync channel topics                                                                            |
| `SYNC_CHANNEL_SLOWMODE`        | false   | Sync slowmode settings                                                                         |
| `REARRANGE_ROLES`              | false   | Sync role order                                                                                |
| `CLONE_VOICE`                  | true    | Clone voice channels                                                                           |
| `CLONE_VOICE_PROPERTIES`       | false   | Sync voice channel bitrate & user limit                                                        |
| `CLONE_STAGE`                  | true    | Clone stage channels                                                                           |
| `CLONE_STAGE_PROPERTIES`       | false   | Sync stage channel properties                                                                  |
| `CLONE_GUILD_ICON`             | false   | Sync guild icon                                                                                |
| `CLONE_GUILD_BANNER`           | false   | Sync guild banner                                                                              |
| `CLONE_GUILD_SPLASH`           | false   | Sync guild splash                                                                              |
| `CLONE_GUILD_DISCOVERY_SPLASH` | false   | Sync guild discovery splash                                                                    |
| `SYNC_GUILD_DESCRIPTION`       | false   | Sync guild description                                                                         |
| `SYNC_FORUM_PROPERTIES`        | false   | Sync forum properties (layout, tags, guidelines, etc.)                                         |

</details>

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
