---
sidebar_position: 4
title: Configuration File (.env)
description: The settings file the installer writes, how to edit it and how to apply a change.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Configuration File (.env)

Almost everything in Copycord is set in the dashboard: tokens, mappings, filters, cloning options. A few settings are needed before the dashboard starts, such as its password and port. Those live in `.env` in the Copycord home, which `copycord` reads on every start, for `copycord run` and the service alike.

## Where it is

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```text title="Settings file"
~/.copycord/.env
```

</TabItem>
<TabItem value="windows" label="Windows">

```text title="Settings file"
%USERPROFILE%\.copycord\.env
```

</TabItem>
<TabItem value="docker" label="Docker">

Docker does not use `.env` in the Copycord home. Set these keys under `environment:` in `docker-compose.yml`, then run `docker compose up -d`. See [Docker Install](/docs/getting-started/docker-install).

</TabItem>
</Tabs>

With a custom home (`--home` or `COPYCORD_HOME`, see [Install Options](/docs/getting-started/manual-install#home)) it is `.env` in that folder.

## The file the installer writes

The installer writes the file on the first install (with `copycord config init`, which you can also run yourself), with every key set to its default, and never overwrites it afterwards. `BACKUP_DIR` is shown commented out, so the backups go to `data/backups` in the Copycord home. On Linux, for a user named `alice`:

```ini title="~/.copycord/.env"
# Copycord settings that are read from the environment only.
# Everything else is set in the dashboard. Each value below is the default.
# Edit one here or run `copycord config set KEY VALUE`, then run
# `copycord restart` to apply it.

# dashboard login password; change it from the default (empty: no login page)
PASSWORD=copycord

# SECRET_KEY is left out: Copycord keeps its own key in <home>/data/secret.key

# dashboard port
ADMIN_PORT=8080

# time zone for scheduled backups and export timestamps, e.g. America/New_York
TZ=UTC

# DEBUG or INFO; the dashboard's log level wins once it is saved
LOG_LEVEL=INFO

# HUMAN or JSON log lines
LOG_FORMAT=HUMAN

# where the daily database backups go; remove the # to use another folder
#BACKUP_DIR=/home/alice/.copycord/data/backups

# time of the daily backup, HH:MM
BACKUP_AT=03:17

# how many daily backups to keep
BACKUP_RETAIN=14

# 1 turns off the update check on start; 0 keeps it on
COPYCORD_NO_UPDATE_CHECK=0
```

An empty value, such as `TZ=`, means the default, except for `PASSWORD`: an empty password turns the login page off.

:::warning Change the default password
The default dashboard password is `copycord` and the dashboard listens on all network interfaces. Anyone who can reach the port and knows the default can open your dashboard and read your tokens. Change it right after installing.
:::

## Change a setting

Edit the line in the file, or let `copycord config set` do it for you. Then restart Copycord so it reads the file again.

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
nano ~/.copycord/.env
# or
copycord config set PASSWORD 'your-new-password'

copycord restart
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell"
notepad "$HOME\.copycord\.env"
# or
copycord config set PASSWORD 'your-new-password'

copycord restart
```

</TabItem>
<TabItem value="docker" label="Docker">

```yaml title="docker-compose.yml"
    environment:
      - PASSWORD=your-new-password
```

```bash title="Terminal"
docker compose up -d
```

</TabItem>
</Tabs>

`copycord restart` restarts the [service](/docs/cli#copycord-service). If Copycord runs in a terminal, press **Ctrl+C** and run `copycord run` again instead.

`copycord config set` updates the key's line in place and keeps your comments. `copycord config get PASSWORD` prints `********` (add `--show-secrets` to see it), and `copycord config unset PASSWORD` removes the line, which turns the login page off.

Variables set in the real environment win over the file, for example `ADMIN_PORT=9060 copycord run`.

## The keys

| Key | Default | What it does |
|-----|---------|--------------|
| `PASSWORD` | `copycord` | Dashboard login password; no login page when empty or unset. |
| `ADMIN_PORT` | `8080` | Dashboard port. |
| `TZ` | `UTC` | Time zone for scheduled backups and export timestamps. |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG`; the dashboard's value wins once it is saved. |
| `LOG_FORMAT` | `HUMAN` | `HUMAN` or `JSON` log lines. |
| `BACKUP_DIR` | `<data dir>/backups` | Where the daily database backups go. |
| `BACKUP_AT` | `03:17` | Time of the daily backup, HH:MM. |
| `BACKUP_RETAIN` | `14` | How many daily backups to keep. |
| `COPYCORD_NO_UPDATE_CHECK` | `0` | `1` turns off the update hint on start. |

The [CLI reference](/docs/cli#env-file) covers `SECRET_KEY` and the rarely needed tuning keys.
