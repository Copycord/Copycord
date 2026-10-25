---
sidebar_position: 5
title: Install Options
description: Installer flags, testing a branch, the Copycord home, updating and uninstalling.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Install Options

Change how the installer works: install a specific version, test a branch, use another folder, or update and uninstall Copycord. For the default install, see the [Quick Start](/docs/getting-started/quick-start). The installer is not used with Docker; see [Docker Install](/docs/getting-started/docker-install).

## What the installer does

The installer installs Copycord for the user who runs it. Run it as your normal user, not with `sudo` or as Administrator. On a Linux server where you log in as root, it installs for root. It works in these steps:

1. **Preparing uv:** installs [uv](https://docs.astral.sh/uv/), the tool that provides Python 3.14 and Copycord's packages, when it is missing.
2. **Finding the release:** picks the newest stable release, the newest pre-release with `--beta`, or the version you give with `--version`.
3. **Installing Copycord:** downloads the release, checks it against the SHA-256 checksum GitHub lists for it and installs it.
4. **Finishing up:** adds `copycord` to your `PATH`, saves a custom home, writes [`.env`](/docs/getting-started/configuration) if it does not exist yet and saves the update channel when you chose one.
5. **Repairing:** on an upgrade or reinstall only, runs [`copycord doctor --fix --brief`](/docs/cli#doctor-fix) to apply automatic repairs, such as starting a stopped service. This step never fails the install.
6. **Verifying:** with `--verify` only, runs `copycord --version` and `copycord doctor` and fails the install if either fails.

When the same version is already installed, the installer says so and exits without changes; `--force` reinstalls. An older version is upgraded.

The installer ends with a box that shows the Copycord home, the settings file, the next commands, the dashboard address and the dashboard password. `install.sh` shows the password only on the first install; `install.ps1` shows it on every run. A reinstall never changes an existing `.env`, so your password stays the same. If the password is still `copycord` from an older install, `install.sh` asks you to change it.

If Copycord runs as a [service](/docs/cli#copycord-service), the installer stops it before installing and starts it again afterwards, also when the install fails. Otherwise it does not start Copycord.

## Flags

To pass flags on Linux and macOS, add them after `sh -s --`. On Windows, run the script as a script block with parameters, or set the environment variable before the `irm` line. Flags win over environment variables.

| `install.sh` | `install.ps1` | Environment variable | Description |
|--------------|---------------|----------------------|-------------|
| `--version <value>` | `-Version <value>` (alias `-Tag`) | `COPYCORD_VERSION` | A release version (`4.0.0.1` or `v4.0.0.1`) installs that release. Any other value (a branch, tag or commit) [builds from git](#branches). |
| `--beta` | `-Beta` | `COPYCORD_BETA=1` or `COPYCORD_CHANNEL=beta` | Installs the newest release, pre-releases included, and saves `beta` as the update channel. |
| `--home <path>` | `-CopycordHome <path>` (alias `-Home`) | `COPYCORD_HOME` | The [Copycord home](#home). Must be an absolute path. |
| `--repo owner/name` | `-Repo owner/name` | `COPYCORD_REPO` | Repository to install from. Default `Copycord/Copycord`. |
| `--git-dir <path>` (alias `--dir`) | `-GitDir <path>` (alias `-Dir`) | `COPYCORD_GIT_DIR` | Folder for git builds. Default `<home>/src`. |
| `--verify` | `-Verify` | `COPYCORD_VERIFY=1` | Runs `copycord --version` and `copycord doctor` after installing and fails if either fails. |
| `--dry-run` | `-DryRun` | `COPYCORD_DRY_RUN=1` | Prints the plan and every action without changing anything. |
| `--verbose` | `-ShowOutput` (alias `-Verbose`) | `COPYCORD_VERBOSE=1` | Shows the full output of every step. |
| `--force` | `-Force` | `COPYCORD_FORCE=1` | Reinstalls the version that is already installed. |
| `--help` | `-Help` | | Shows the options. |
| | | `NO_COLOR=1` | Prints without color. |

:::warning `--git-dir` deletes files
A git build resets the folder to the version you asked for and deletes every other file in it. Never point `--git-dir` at your own copy of the code. `install.sh` only resets a folder it created or an unchanged copy of the same repository, and stops without changes for any other folder.
:::

Less common settings:

| Flag or variable | Description |
|------------------|-------------|
| `--install-method release\|git` (alias `--method`, or `--git` and `--release`); PowerShell `-InstallMethod` or `-Git`; `COPYCORD_INSTALL_METHOD` | Forces the install method instead of choosing it from `--version`. |
| `COPYCORD_NODE_VERSION` | Node.js version for git builds. Default `22.12.0`. |
| `COPYCORD_UV_VERSION` | uv version that `install.ps1` installs. |
| `COPYCORD_ALLOW_ROOT=1` | Allows running as root or Administrator. |
| `GITHUB_TOKEN` | A GitHub token, for when many installs share one network and GitHub refuses the release lookup. |

Examples:

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
# the newest beta, and keep updating from the beta channel
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --beta

# one exact release, checked afterwards
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --version 4.0.0.1 --verify

# only show what it would do
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --dry-run
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell"
# the newest beta, and keep updating from the beta channel
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1))) -Beta

# one exact release, checked afterwards
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1))) -Version 4.0.0.1 -Verify
```

</TabItem>
<TabItem value="docker" label="Docker">

To pin a release, use its image tag, for example `ghcr.io/copycord/copycord:v4.0.0.1`. `latest` follows full releases and `beta` follows pre-releases.

</TabItem>
</Tabs>

## Test a branch {#branches}

To test a branch, tag or commit that has no release, pass it to `--version`. The installer clones the repository into `<home>/src`, builds the dashboard and installs that copy. This needs git: on Linux and macOS, when git is missing and you run it in a terminal, the installer installs git through your package manager (Homebrew on macOS; apk, apt-get, pacman, dnf or yum on Linux), asking for your password when it needs sudo; on Windows, when git is missing, the installer downloads MinGit from the Git for Windows release, checks its SHA-256 checksum and unpacks it into `<home>\git`, without admin rights. `copycord update` on the dev channel does the same. When Node.js 22 or newer is missing, the installer downloads the official build, checks its SHA-256 checksum and unpacks it into `<home>/node`, without sudo.

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

A branch of `Copycord/Copycord` is saved as the `dev` update channel, so `copycord update` keeps following it. For a branch of a fork, add `--repo you/Copycord`; to update a fork, run the installer again. To go back to releases, run `copycord update --channel stable`.

An installed release can also switch to a branch with `copycord update --channel dev --ref v4-dev`. See [update channels](/docs/cli#update-channels).

## The Copycord home {#home}

The Copycord home is the folder that holds everything Copycord stores: `.env`, the `data` folder (`data.db`, `secret.key`, backups and exports), the `logs` folder and, for git builds, `src` and `node`.

| OS | Default |
|----|---------|
| Linux, macOS | `~/.copycord` |
| Windows | `%USERPROFILE%\.copycord` |

To use another folder, install with `--home` or set `COPYCORD_HOME`. The installer saves a custom home in `~/.copycord-home`, so every `copycord` command and the service find it. To keep only the data somewhere else, see [The data directory](/docs/cli#data-dir).

## Update

```bash title="Terminal"
copycord update                    # the newest release on your channel
copycord update --channel beta     # switch to beta (or stable, dev) and update
copycord update status             # show what is installed and what is available
```

A running service is restarted after an update. When an update is out, `copycord run` prints a hint at start. To let the service update itself, see [Auto-update](/docs/cli#auto-update).

### Downgrading and backups {#downgrade}

Every `copycord update` first copies the database to the backups folder, and Copycord does not start on a database from a newer version. To go back to an older version, see [Downgrading and backups](/docs/cli#downgrade).

## Uninstall

```bash title="Terminal"
copycord uninstall                  # remove Copycord; asks whether to delete your data
copycord uninstall --yes            # no questions, keep the Copycord home
copycord uninstall --yes --purge    # no questions, delete the Copycord home: database, logs, backups, .env
```

Without options, `copycord uninstall` asks whether to keep your data, delete it or cancel; **Enter** keeps it. See [`copycord uninstall`](/docs/cli#copycord-uninstall).
