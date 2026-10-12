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

The script prints a short plan, then runs numbered steps:

1. Installs `uv` if it is not already on your `PATH`
2. Finds the release to install (the newest stable release by default)
3. Installs the release wheel with `uv tool install`, which puts the `copycord` command in uv's tool directory
4. Adds that directory to your `PATH` if it is not there yet (`uv tool update-shell`)
5. Runs `copycord doctor` and prints the next commands: `copycord`, `copycord update` and the web UI URL (`http://localhost:8080`)

It does not touch Docker or any existing Docker setup.

:::tip Command not found?
If `copycord` is not found right after the install, open a new terminal so the updated `PATH` is picked up.
:::

### Install methods

| Method | What it installs | Needs |
|--------|------------------|-------|
| `release` (default) | The wheel attached to a GitHub release | uv (installed for you) |
| `git` | Any branch, tag or commit of a repository, built from source. No release needed. | uv, git and Node.js 20+ (Node is installed for your user if it is missing) |

The git method clones the repository into `~/.copycord/src` (Windows: `%USERPROFILE%\.copycord\src`), checks out the ref, builds the admin frontend with `npm ci` and `npm run build`, then runs `uv tool install --force` on the checkout. Running it again updates the same checkout. When Node.js 20 or newer is missing, the script downloads the official Node.js build from nodejs.org, checks its SHA-256 and unpacks it into `~/.copycord/node`, without sudo.

### Test a branch

Install a branch straight from GitHub, for example `v4-dev`:

```bash
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/v4-dev/install.sh | sh -s -- --version v4-dev
```

```powershell
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/Copycord/Copycord/v4-dev/install.ps1))) -Version v4-dev
```

`--version` alone picks the method: a release version (`4.0.0`, `v4.0.0`, `4.0.0b1`, `4.0.0-rc.1`) installs that release, and anything else (a branch, tag or commit SHA) installs from git at that ref.

A branch of a fork works the same way with `--repo`:

```bash
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --repo you/Copycord --version my-fix
```

A git install of a `Copycord/Copycord` branch saves the `dev` channel with that branch, so `copycord update` keeps following it. For a tag, a commit or a fork the channel is left as it is; run the script again to update.

### Install options

Pass flags after `sh -s --` on Linux and macOS. On Windows, run the script as a script block to pass parameters (see the example above). Flags win over environment variables.

| Flag (`install.sh`) | Parameter (`install.ps1`) | Environment | What it does |
|---------------------|---------------------------|-------------|--------------|
| `--repo owner/name` | `-Repo owner/name` | `COPYCORD_REPO` | Repository to install from (default `Copycord/Copycord`). |
| `--version <value>` | `-Version <value>` (alias `-Tag`) | `COPYCORD_VERSION` | A release version such as `4.0.0` installs that release; a branch, tag or commit SHA installs from git at that ref. With no value the newest release is installed. |
| `--beta` | `-Beta` | | Release method: also consider pre-releases. The channel is saved and used by `copycord update`. |
| `--home <path>` | `-CopycordHome <path>` | `COPYCORD_HOME` | Copycord home: data, `src/` and `node/` (default `~/.copycord`). A custom one is saved in `~/.copycord-home` so the CLI and the service use it. |
| `--git-dir <path>` | `-GitDir <path>` | | Git method: checkout directory. |
| `--verify` | `-Verify` | | Run `copycord --version` and `copycord doctor` after installing and fail if either fails. |
| `--dry-run` | `-DryRun` | | Print the plan and every action without changing anything. |
| `--verbose` | `-ShowOutput` | | Show the full output of every step instead of a spinner. |
| `--help` | `-Help` | | Show the options. |
| | | `NO_COLOR=1` | Plain output. Colors are only used on a terminal. |

The other environment variables (`COPYCORD_CHANNEL`, `COPYCORD_BETA`, `COPYCORD_GIT_DIR`, `COPYCORD_VERIFY`, `COPYCORD_DRY_RUN`, `COPYCORD_VERBOSE`, `COPYCORD_NODE_VERSION`, `COPYCORD_ALLOW_ROOT`) are rarely needed; most mirror the flags.

Advanced: `--install-method release|git` (alias `--method`), `--git` and `--release` (PowerShell: `-InstallMethod`, `-Git`) force the method instead of inferring it from `--version`. `COPYCORD_INSTALL_METHOD` does the same.

Examples:

```bash
# Linux/macOS: install the newest beta and keep updating from the beta channel
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --beta

# Linux/macOS: install one exact release and check it
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --version 4.0.0 --verify

# Linux/macOS: see what the script would do
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --version v4-dev --dry-run
```

```powershell
# Windows: install the newest beta
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1))) -Beta
```

:::note Building from source without the script
`uv tool install` on a checkout builds the admin frontend through `hatch_build.py` when `code/admin/frontend/dist` is missing, and stops with an error if npm is not on `PATH`. Set `COPYCORD_SKIP_FRONTEND=1` to build without the admin UI.
:::

## Check the setup

```bash
copycord doctor
```

`copycord doctor` checks this machine without touching the network: the Python version, that `ROLE` is not set, that the data directory and database are usable, that the dashboard port is free, that the dashboard files are present and that the required packages import. Each line starts with `PASS`, `WARN` or `FAIL`, for example:

```
PASS  python: Python 3.12.7
PASS  ROLE: unset
PASS  data dir: /home/you/.copycord will be created on first run
PASS  database: /home/you/.copycord/data.db will be created on first run
PASS  admin port: port 8080 is free
```

It exits with a non-zero status when any check fails.

## Run Copycord

```bash
copycord run
```

This starts the dashboard, the server bot and the client in one process. Open the dashboard at:

**http://localhost:8080**

and finish the setup there: the server bot token, the client token and the other settings are all configured in the dashboard. See [Discord Setup](/docs/getting-started/discord-setup) for how to get the tokens.

Press **Ctrl+C** to stop Copycord.

### Where your data lives

Copycord keeps its database, logs and backups in a per-user home directory (`COPYCORD_HOME` changes it):

| OS | Data directory |
|----|----------------|
| Linux, macOS | `~/.copycord` |
| Windows | `%USERPROFILE%\.copycord` |

To use another folder, pass `--data-dir` (or set the `DATA_DIR` environment variable):

```bash
copycord run --data-dir ~/copycord-data
```

### Settings that live in .env {#env-file}

The dashboard listens on all network interfaces. A few settings, such as the dashboard password and port, are read from the environment only, so the dashboard cannot change them. Copycord reads them from `.env` in the Copycord home on every start, for `copycord run` and the service alike:

| OS | File |
|----|------|
| Linux, macOS | `~/.copycord/.env` |
| Windows | `%USERPROFILE%\.copycord\.env` |

The installer makes sure this file exists. It writes it on first install and never overwrites it, with every key set to its default and a one-line description above it (see the [full file](/docs/cli#env-file)).

:::warning Change the default password
The default dashboard password is `copycord`. Anyone who can reach the port and knows the default can open your dashboard and see your tokens, so change it right after installing.
:::

| Key | Default | What it does |
|-----|---------|--------------|
| `PASSWORD` | `copycord` | Password for the dashboard login page; no login page when unset |
| `ADMIN_PORT` | `8080` | Port of the dashboard |
| `TZ` | `UTC` | Time zone of the daily backup |
| `BACKUP_DIR` | `<home>/backups` | Where the daily backups go |

To change a setting, edit its line, then restart Copycord:

```bash
nano ~/.copycord/.env                # Linux/macOS
notepad "$HOME\.copycord\.env"       # Windows (PowerShell)
copycord restart
```

Or let `copycord config set` edit the file for you:

```bash
copycord config set PASSWORD 'new-pass'
copycord restart
```

`copycord restart` restarts the service. If you run Copycord in a terminal instead, stop it with **Ctrl+C** and run `copycord run` again.

Variables set in the real environment win over the file:

```bash
# Linux/macOS
PASSWORD=changeme ADMIN_PORT=9060 copycord run
```

```powershell
# Windows (PowerShell)
$env:PASSWORD = 'changeme'; $env:ADMIN_PORT = '9060'; copycord run
```

The [CLI reference](/docs/cli#env-file) lists every key.

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
