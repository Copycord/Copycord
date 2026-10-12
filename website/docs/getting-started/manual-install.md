---
sidebar_position: 3
title: Install Script
---

# Install Script

The install script is the simplest way to run Copycord without Docker. It works on **Linux**, **macOS** and **Windows** and installs Copycord for your user from the latest [GitHub release](https://github.com/Copycord/Copycord/releases).

You do not need to install Python, Node.js or create virtual environments yourself. The script installs [uv](https://docs.astral.sh/uv/) if it is missing, and uv takes care of a suitable Python (3.12 or newer) and of Copycord's dependencies.

## Install

### Linux and macOS

Open a terminal and run:

```bash
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
```

### Windows

Open **PowerShell** (not as Administrator) and run:

```powershell
irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
```

### What the script does

1. Installs `uv` if it is not already on your `PATH`
2. Finds the release to install (the newest stable release by default)
3. Installs the release wheel with `uv tool install`, which puts the `copycord` command in uv's tool directory
4. Adds that directory to your `PATH` if it is not there yet (`uv tool update-shell`)
5. Prints the next steps

It does not touch Docker or any existing Docker setup.

:::tip Command not found?
If `copycord` is not found right after the install, open a new terminal so the updated `PATH` is picked up.
:::

### Install options

The script reads these environment variables:

| Variable | Values | What it does |
|----------|--------|--------------|
| `COPYCORD_CHANNEL` | `stable` (default), `beta` | `beta` also considers pre-releases. The channel is saved and used by `copycord update` later. |
| `COPYCORD_VERSION` | e.g. `4.0.0` | Install exactly this release instead of the newest one. |
| `COPYCORD_ALLOW_ROOT` | `1` | Allow running the script as root (or as Administrator on Windows). It refuses by default because Copycord installs for your user. |

Examples:

```bash
# Linux/macOS: install the newest beta and keep updating from the beta channel
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | COPYCORD_CHANNEL=beta sh

# Linux/macOS: install one exact release
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | COPYCORD_VERSION=4.0.0 sh
```

```powershell
# Windows: install the newest beta
$env:COPYCORD_CHANNEL = 'beta'; irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
```

## Check the setup

```bash
copycord doctor
```

`copycord doctor` checks this machine without touching the network: the Python version, that `ROLE` is not set, that the data directory and database are usable, that the dashboard port is free, that the dashboard files are present and that the required packages import. Each line starts with `PASS`, `WARN` or `FAIL`, for example:

```
PASS  python: Python 3.12.7
PASS  ROLE: unset
PASS  data dir: /home/you/.local/share/copycord will be created on first run
PASS  database: /home/you/.local/share/copycord/data.db will be created on first run
PASS  admin port: port 8080 is free
```

It exits with a non-zero status when any check fails.

## Run Copycord

```bash
copycord run
```

This starts the dashboard, the server bot and the client in one process. Open the dashboard at:

**http://localhost:8080**

Press **Ctrl+C** to stop Copycord.

### Where your data lives

Copycord keeps its database, logs and backups in a per-user data directory:

| OS | Data directory |
|----|----------------|
| Linux | `~/.local/share/copycord` |
| macOS | `~/Library/Application Support/Copycord` |
| Windows | `%LOCALAPPDATA%\Copycord` |

To use another folder, pass `--data-dir` (or set the `DATA_DIR` environment variable):

```bash
copycord run --data-dir ~/copycord-data
```

### Password and port

The dashboard listens on all network interfaces. Set these environment variables before starting Copycord:

| Variable | Default | What it does |
|----------|---------|--------------|
| `PASSWORD` | unset (no login) | Password for the dashboard login page |
| `ADMIN_PORT` | `8080` | Port of the dashboard |

```bash
# Linux/macOS
PASSWORD=changeme ADMIN_PORT=9060 copycord run
```

```powershell
# Windows (PowerShell)
$env:PASSWORD = 'changeme'; $env:ADMIN_PORT = '9060'; copycord run
```

:::warning Set a password on shared networks
Without `PASSWORD` anyone who can reach the port can open your dashboard and see your tokens. Set a password whenever the machine is reachable from other devices.
:::

## Run Copycord in the background

To keep Copycord running without an open terminal, install it as a service:

```bash
# Linux: a service for your user (systemctl --user)
copycord service install --user

# Linux: a system service (runs as you when started with sudo)
sudo "$(command -v copycord)" service install

# macOS and Windows
copycord service install
```

On Linux a `--user` service stops when you log out. To keep it running and start it at boot, run the command `copycord service install --user` prints: `sudo loginctl enable-linger <your user>`.

Check, stop or remove it with `copycord service status`, `copycord service stop` and `copycord service uninstall`. See the [CLI reference](/docs/cli#copycord-service) for how the service works on each OS, including how to pass `PASSWORD` to it.

## Update

```bash
copycord update
```

This installs the newest release on your channel and restarts the service if it is running. See [Updating](/docs/cli#copycord-update) for channels and options.

## Uninstall

```bash
copycord service uninstall   # only if you installed the service
uv tool uninstall copycord
```

Your data directory is left in place. Delete it yourself if you no longer need it.

## Next steps

Head to [First Run](/docs/getting-started/first-run) to configure your tokens and create your first server mapping.

:::note Coming from v3?
The old launcher, `Copycord.exe` and the `install-tools/` folder are deprecated. See [Upgrading from v3](/docs/getting-started/upgrading-from-v3) to move your existing data.
:::
