---
sidebar_position: 5
title: Install Options
description: Installer flags, testing a branch, the Copycord home, updating and uninstalling.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Install Options

The [Quick Start](/docs/getting-started/quick-start) covers the default install. This page covers everything else the installer can do: its flags, installing a branch, a custom Copycord home, and how to update and uninstall.

## What the installer does

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell (not as Administrator)"
irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
```

</TabItem>
<TabItem value="docker" label="Docker">

The installer is not used with Docker. See [Docker Install](/docs/getting-started/docker-install).

</TabItem>
</Tabs>

It prints the banner and first checks for an existing install. When the `copycord` command is there and `copycord --version` shows what this run would install (the same release version, or for a git ref the same commit as the remote head of that ref, read with `git ls-remote`), it prints `✓ Copycord v4-dev (b2d8c1c) is already installed` and a hint to run `copycord update` or rerun with `--force` (`-Force` on Windows), then exits without changing anything. An older install is upgraded. When the check cannot be done (no network, unknown output), it installs as usual. A dry run skips the check.

Otherwise it runs numbered steps:

1. **Preparing uv:** installs [uv](https://docs.astral.sh/uv/) when it is not on your `PATH`. uv provides Python 3.12 and Copycord's dependencies.
2. **Finding the release:** the newest stable release, or the one you asked for.
3. **Installing Copycord:** `uv tool install --force` with the release wheel, a per-user install with no sudo.
4. **Finishing up:** adds uv's tool directory to your `PATH` when needed (`uv tool update-shell`), writes [`.env`](/docs/getting-started/configuration) in the Copycord home if it does not exist yet, and saves the update channel when you chose one.
5. **Repairing (upgrades and reinstalls only):** when Copycord or its home was already there, runs [`copycord doctor --fix --brief`](/docs/cli#doctor-fix) to apply the automatic repairs, such as starting a stopped service or taking an overdue backup. It prints a line for each repair, any failed check and `All checks passed` or the number of warnings; it never fails the install. A first install skips this step.

It ends with a box that shows the settings file, the next commands (`copycord run`, `copycord restart`, `copycord update`) and a **Needs your attention** list: change the default password `copycord` (while it is still set) and finish the setup in the dashboard at `http://localhost:8080`. It does not start Copycord and does not touch Docker. It refuses to run as root: Copycord installs for your user.

## Flags

On Linux and macOS, pass flags after `sh -s --`. On Windows, set the environment variable before the `irm` line, or run the script as a script block to pass parameters. Flags win over environment variables.

| `install.sh` | `install.ps1` | Environment | What it does |
|--------------|---------------|-------------|--------------|
| `--version <value>` | `-Version <value>` | `COPYCORD_VERSION` | A release version (`4.0.0`, `v4.0.0`, `4.0.0b1`) installs that release. Anything else (a branch, tag or commit SHA) builds from git at that ref. |
| `--beta` | `-Beta` | `COPYCORD_BETA=1` | The newest release, pre-releases included. Saved as the `copycord update` channel. |
| `--home <path>` | `-CopycordHome <path>` | `COPYCORD_HOME` | The [Copycord home](#home) (default `~/.copycord`). |
| `--repo owner/name` | `-Repo owner/name` | `COPYCORD_REPO` | Repository to install from (default `Copycord/Copycord`). |
| `--git-dir <path>` | `-GitDir <path>` | `COPYCORD_GIT_DIR` | Git builds: the checkout directory (default `<home>/src`). |
| `--verify` | `-Verify` | `COPYCORD_VERIFY=1` | Run `copycord --version` and `copycord doctor` after installing and fail if either fails. |
| `--dry-run` | `-DryRun` | `COPYCORD_DRY_RUN=1` | Print the plan and every action, change nothing. |
| `--verbose` | `-ShowOutput` | `COPYCORD_VERBOSE=1` | Show the full output of every step instead of a spinner. |
| `--force` | `-Force` | `COPYCORD_FORCE=1` | Reinstall even when the same version is already installed. |
| `--help` | `-Help` | | Show the options. |
| | | `NO_COLOR=1` | Plain output. |

Rarely needed: `--install-method release|git` (PowerShell `-InstallMethod`) forces the method instead of inferring it from `--version`, `COPYCORD_NODE_VERSION` picks the Node.js version for git builds, and `COPYCORD_ALLOW_ROOT=1` allows running as root.

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
# the newest beta, and keep updating from the beta channel
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --beta

# one exact release, checked afterwards
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --version 4.0.0 --verify

# only show what it would do
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --dry-run
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell"
# the newest beta, and keep updating from the beta channel
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1))) -Beta

# one exact release, checked afterwards
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1))) -Version 4.0.0 -Verify
```

</TabItem>
<TabItem value="docker" label="Docker">

Pin a release with the image tag, for example `ghcr.io/copycord/copycord:v4.0.0`.

</TabItem>
</Tabs>

## Test a branch {#branches}

Testers can install any branch, tag or commit without a release. The installer then clones the repository into `<home>/src`, builds the dashboard with npm and installs that checkout. It needs git; when Node.js 20 or newer is missing it downloads the official build from nodejs.org, checks its SHA-256 and unpacks it into `<home>/node`, without sudo.

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --version v4-dev
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell"
$env:COPYCORD_VERSION = 'v4-dev'
irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
```

</TabItem>
<TabItem value="docker" label="Docker">

Branches are not published as images. Use the installer to test one.

</TabItem>
</Tabs>

A branch of `Copycord/Copycord` is saved as the `dev` update channel, so `copycord update` keeps following it. A branch of a fork works with `--repo you/Copycord --version my-fix`; to update a fork, run the installer again. To go back to releases, run `copycord update --channel stable`.

A release install can also switch to a branch with `copycord update --channel dev --ref v4-dev`; without Node.js it downloads Node into `<home>/node` the same way.

## The Copycord home {#home}

Everything Copycord stores lives in one folder: `data.db`, the logs, the daily backups, `.env`, and for git builds `src/` and `node/`.

| OS | Default |
|----|---------|
| Linux, macOS | `~/.copycord` |
| Windows | `%USERPROFILE%\.copycord` |

`--home` (or `COPYCORD_HOME`) picks another folder. The installer saves a custom home in `~/.copycord-home`, so `copycord run`, the service and the other commands find it without the environment variable. See [The data directory](/docs/cli#data-dir) for `--data-dir`.

## Update

```bash title="Terminal"
copycord update                    # the newest release on your channel
copycord update --channel beta     # switch to beta (or stable, dev) and update
copycord update status             # only show what is installed and what is available
```

`copycord run` prints a one-line hint, at most once a day, when an update is out. A running service is restarted after an update. With `copycord config set AUTO_UPDATE true`, a Copycord running as a service updates itself. See [`copycord update`](/docs/cli#copycord-update).

### Downgrading and backups {#downgrade}

Every `copycord update`, upgrade or downgrade, first copies the database to `data-pre-update-<version>-<UTC timestamp>.db` in the backups directory (`<data dir>/backups` unless `BACKUP_DIR` is set) and keeps the newest 5. A failed backup stops the update; `--no-backup` skips it. Copycord refuses to start on a database from a newer version and tells you to `copycord update` back or restore one of those backups, so a downgrade never changes your data. See [Downgrading and backups](/docs/cli#downgrade).

## Uninstall

```bash title="Terminal"
copycord uninstall                  # remove Copycord; asks whether to delete your data
copycord uninstall --yes            # no prompts, keep the Copycord home
copycord uninstall --yes --purge    # no prompts, delete the Copycord home: database, logs, backups, .env
```

After you confirm the uninstall, it asks whether to also delete your data (the Copycord home). The default is No, which keeps it and prints where it is; Yes asks you to type `DELETE`. In a script, `--purge` deletes and `--keep-data` keeps; `--yes` on its own keeps the data. `--dry-run` shows the steps without changing anything. See [`copycord uninstall`](/docs/cli#copycord-uninstall).

:::note Coming from v3?
The old launcher (`Copycord.exe`, `launcher.py`) is replaced by the install script. See [Upgrading from v3](/docs/getting-started/upgrading-from-v3).
:::
