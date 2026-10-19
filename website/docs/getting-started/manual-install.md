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

1. **Preparing uv:** installs [uv](https://docs.astral.sh/uv/) when it is not on your `PATH`. uv provides Python 3.14 and Copycord's dependencies.
2. **Finding the release:** the newest stable release, the newest pre-release with `--beta`, or the one you asked for with `--version`.
3. **Installing Copycord:** downloads the release wheel, checks it against the SHA-256 checksum GitHub lists for it, and runs `uv tool install --force` with it, a per-user install with no sudo.
4. **Finishing up:** adds uv's tool directory to your `PATH` when needed (`uv tool update-shell`), saves a custom home in `~/.copycord-home`, writes [`.env`](/docs/getting-started/configuration) in the Copycord home if it does not exist yet (with `copycord config init`), and saves the update channel when you chose one.
5. **Repairing (upgrades and reinstalls only):** when Copycord or its home was already there, runs [`copycord doctor --fix --brief`](/docs/cli#doctor-fix) to apply the automatic repairs, such as starting a stopped service or taking an overdue backup. It prints a line for each repair, any failed check and `All checks passed` or the number of warnings; it never fails the install. A first install skips this step.
6. **Verifying (`--verify` only):** runs `copycord --version` and `copycord doctor` and fails the install if either fails.

A [git build](#branches) replaces steps 2 and 3 with **Installing Node** (only when Node.js 22 or newer is missing), **Fetching** the ref, **Building the admin frontend** and **Installing Copycord** from the checkout.

It ends with a box that shows the Copycord home, the settings file, the next commands (`copycord run`, `copycord restart`, `copycord update`) and a **Needs your attention** list: the dashboard address (the port and address from your settings file) and the dashboard password. The installer makes up a random password when it creates the settings file and shows it in that box; `install.sh` shows it only on that first install, `install.ps1` shows the password from the settings file on every run. It is saved as `PASSWORD` in `.env`; if you lose it, `copycord config get PASSWORD --show-secrets` prints it, and `copycord config set PASSWORD 'your-new-password'` changes it. An existing settings file is never changed, so a reinstall keeps your password. If an older install still has the default password `copycord`, `install.sh` asks you to change it. Before replacing a running [service](/docs/cli#copycord-service), the installer stops it and starts it again afterwards, also when the install fails. It does not start Copycord otherwise and does not touch Docker. It refuses to run as root (on Windows, as Administrator): Copycord installs for your user.

## Flags

On Linux and macOS, pass flags after `sh -s --`. On Windows, set the environment variable before the `irm` line, or run the script as a script block to pass parameters. Flags win over environment variables.

| `install.sh` | `install.ps1` | Environment | What it does |
|--------------|---------------|-------------|--------------|
| `--version <value>` | `-Version <value>` (alias `-Tag`) | `COPYCORD_VERSION` | A release version (`4.0.0`, `v4.0.0`, `4.0.0b1`) installs that release. Anything else (a branch, tag or commit SHA) builds from git at that ref. |
| `--beta` | `-Beta` | `COPYCORD_BETA=1` or `COPYCORD_CHANNEL=beta` | The newest release, pre-releases included. Saved as the `copycord update` channel. |
| `--home <path>` | `-CopycordHome <path>` (alias `-Home`) | `COPYCORD_HOME` | The [Copycord home](#home) (default `~/.copycord`). Must be an absolute path. |
| `--repo owner/name` | `-Repo owner/name` | `COPYCORD_REPO` | Repository to install from (default `Copycord/Copycord`). |
| `--git-dir <path>` (alias `--dir`) | `-GitDir <path>` (alias `-Dir`) | `COPYCORD_GIT_DIR` | Git builds: a folder the installer owns (default `<home>/src`). Each install replaces every file in it, so never point it at your own copy of the code. |
| `--verify` | `-Verify` | `COPYCORD_VERIFY=1` | Run `copycord --version` and `copycord doctor` after installing and fail if either fails. |
| `--dry-run` | `-DryRun` | `COPYCORD_DRY_RUN=1` | Print the plan and every action, change nothing. |
| `--verbose` | `-ShowOutput` (alias `-Verbose`) | `COPYCORD_VERBOSE=1` | Show the full output of every step instead of a spinner. |
| `--force` | `-Force` | `COPYCORD_FORCE=1` | Reinstall even when the same version is already installed. |
| `--help` | `-Help` | | Show the options. |
| | | `NO_COLOR=1` | Plain output. |

:::warning `--git-dir` is not for your own copy of the code
A git build resets the folder to the version you asked for and deletes every other file in it. To protect your work, `install.sh` only does that to a folder it created itself, or to an unchanged copy of the same repository. Any other folder stops the install with a message and nothing is changed; pick an empty or new folder instead.
:::

Release installs check the downloaded wheel against the checksum GitHub lists for it and stop if they differ. Set `GITHUB_TOKEN` if many installs share one network and GitHub starts refusing the release lookup.

Rarely needed: `--install-method release|git` (alias `--method`; `--git` and `--release` for short; PowerShell `-InstallMethod` or `-Git`; or `COPYCORD_INSTALL_METHOD`) forces the method instead of inferring it from `--version`, `COPYCORD_NODE_VERSION` picks the Node.js version for git builds (default 22.12.0), `COPYCORD_UV_VERSION` picks the uv version `install.ps1` installs, and `COPYCORD_ALLOW_ROOT=1` allows running as root or Administrator.

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

Testers can install any branch, tag or commit without a release. The installer then clones the repository into `<home>/src`, builds the dashboard with npm and installs that checkout. It needs git; when Node.js 22 or newer is missing it downloads the official build from nodejs.org, checks its SHA-256 and unpacks it into `<home>/node`, without sudo.

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/v4-dev/install.sh | sh -s -- --version v4-dev
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell"
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/Copycord/Copycord/v4-dev/install.ps1))) -Version v4-dev
```

</TabItem>
<TabItem value="docker" label="Docker">

Branches are not published as images. Use the installer to test one.

</TabItem>
</Tabs>

A branch of `Copycord/Copycord` is saved as the `dev` update channel, so `copycord update` keeps following it. A branch of a fork works with `--repo you/Copycord --version my-fix`; to update a fork, run the installer again. To go back to releases, run `copycord update --channel stable`.

A release install can also switch to a branch with `copycord update --channel dev --ref v4-dev`; without Node.js it downloads Node into `<home>/node` the same way.

## The Copycord home {#home}

Everything Copycord stores lives in one folder: `.env`, the `data` folder (`data.db`, `secret.key`, the daily backups and exports), the `logs` folder, and for git builds `src/` and `node/`.

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

`copycord run` prints a one-line hint at start when an update is out; it asks GitHub at most once a day. Auto-update is off by default. A running service is restarted after an update. With `copycord config set AUTO_UPDATE true`, a Copycord running as a service updates itself on the stable channel, and goes back to the previous version if the new one does not start. See [`copycord update`](/docs/cli#copycord-update).

### Downgrading and backups {#downgrade}

Every `copycord update`, upgrade or downgrade, first copies the database to `data-pre-update-<version>-<UTC timestamp>.db` in the backups directory (`<data dir>/backups` unless `BACKUP_DIR` is set) and keeps the newest 5. A failed backup stops the update; `--no-backup` skips it. Copycord refuses to start on a database from a newer version and tells you to `copycord update` back or restore one of those backups, so a downgrade never changes your data. See [Downgrading and backups](/docs/cli#downgrade).

## Uninstall

```bash title="Terminal"
copycord uninstall                  # remove Copycord; asks whether to delete your data
copycord uninstall --yes            # no prompts, keep the Copycord home
copycord uninstall --yes --purge    # no prompts, delete the Copycord home: database, logs, backups, .env
```

It asks one question about your data (the Copycord home): **Keep my data**, **Delete everything (database, logs, backups, settings)** or **Cancel**. Enter keeps it, and the end of the output says where it is. In a script, `--purge` deletes and `--keep-data` keeps; `--yes` on its own keeps the data. `--dry-run` shows the steps without changing anything. See [`copycord uninstall`](/docs/cli#copycord-uninstall).

:::note Coming from v3?
The old launcher (`Copycord.exe`, `launcher.py`) is replaced by the install script. See [Upgrading from v3](/docs/getting-started/upgrading-from-v3).
:::
