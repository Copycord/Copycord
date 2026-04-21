---
sidebar_position: 4
title: Manual Install
---

# Manual Install

If you prefer not to use Docker, Copycord provides installers for both Windows and Linux.

## Windows

### Requirements

- [Python 3.11](https://www.python.org/downloads/) (make sure to check "Add to PATH" during install)
- [Node.js LTS](https://nodejs.org/) + npm

### Install

1. Download the Windows installer bundle: [copycord.zip](https://github.com/Copycord/Copycord/raw/refs/heads/main/install-tools/windows/copycord.zip)

2. **Right-click** the zip file and choose **Extract All...** (extract it to a convenient location, like your Desktop)

3. Open the extracted `copycord` folder

4. **Double-click `Copycord.exe`** and select **Install**
   - This downloads the latest Copycord build and sets up everything in the same folder

5. **Double-click `Copycord.exe`** again and select **Run**
   - This starts all Copycord services
   - The web dashboard opens at: **http://localhost:8080**

### Update

When a new version is released, double-click `Copycord.exe` and select the **Update** option.

### Configuration

After installation, you can customize settings in the `.env` file located at `code/.env` inside your Copycord folder.

---

## Linux

### Requirements

- **Python 3.11**
- **Node.js LTS** + **npm**
- **python3-venv** + **python3.11-venv**

Install dependencies on Ubuntu/Debian:

```bash
# Node.js
curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
sudo apt install -y nodejs

# Python venv
sudo apt install -y python3-venv python3.11-venv
```

### Install

1. Create a folder and download the launcher:

```bash
mkdir -p ~/copycord
cd ~/copycord
curl -L "https://raw.githubusercontent.com/Copycord/Copycord/refs/heads/main/install-tools/source/launcher.py" -o launcher.py
```

2. Run the launcher:

```bash
cd ~/copycord
python3 launcher.py
```

3. When the menu appears, choose: **1) Install Copycord**

This will:
- Download the latest Copycord version
- Build the admin frontend
- Create `code/`, `venvs/`, and `data/` directories
- Generate `copycord_linux.sh` (the start script)

### Run

You can start Copycord in two ways:

**Option A** — Using the launcher menu:

```bash
cd ~/copycord
python3 launcher.py
# Choose: 4) Run Copycord (Linux)
```

**Option B** — Using the start script directly:

```bash
cd ~/copycord
chmod +x copycord_linux.sh   # only needed once
./copycord_linux.sh
```

The web dashboard will be available at: **http://localhost:8080**

### Update

```bash
cd ~/copycord
python3 launcher.py
# Choose: 2) Update Copycord
```

### Configuration

After installation, customize settings in the `.env` file at `code/.env` inside your Copycord folder.

---

## Folder structure after install

```
copycord/
├── code/                # Copycord source code
│   ├── .env             # Environment configuration
│   ├── admin/           # Web dashboard
│   ├── server/          # Discord bot
│   ├── client/          # Self-bot client
│   └── common/          # Shared code
├── data/                # Database and backups
│   ├── data.db          # SQLite database
│   └── backups/         # Automatic backups
└── venvs/               # Python virtual environments (Linux/manual)
```

## Next steps

Head to [First Run](/docs/getting-started/first-run) to configure your tokens and create your first server mapping.
