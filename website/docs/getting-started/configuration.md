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

The installer writes the file on the first install (with `copycord config init`, which you can also run yourself), with every key set to its default, and never overwrites it afterwards. `PASSWORD` is random, and the backups go to `data/backups` in the Copycord home. On Linux, for a user named `alice`:

```ini title="~/.copycord/.env"
# Copycord settings. Everything else is in the dashboard.
# After a change, run: copycord restart

# Dashboard password
PASSWORD=k7Qe2Vd9sLmP0aZr4TwXbg

# Dashboard port
ADMIN_PORT=8080

# Who can open the dashboard: 127.0.0.1 = this computer, 0.0.0.0 = your network
DASHBOARD_HOST=127.0.0.1

# Time zone for backups and exports
TZ=UTC

# Log detail: INFO or DEBUG
LOG_LEVEL=INFO

# Log style: HUMAN or JSON
LOG_FORMAT=HUMAN

# Backup folder
BACKUP_DIR=/home/alice/.copycord/data/backups

# Daily backup time (HH:MM)
BACKUP_AT=03:17

# Backups to keep
BACKUP_RETAIN=14

# Skip the update check: 1 = yes, 0 = no
COPYCORD_NO_UPDATE_CHECK=0
```

An empty value, such as `TZ=`, means the default. The dashboard always asks for the password. If `PASSWORD` is empty or missing, `copycord run` makes a random one, saves it in this file and shows it once. In Docker, Copycord does not start until you set `PASSWORD`.

:::note The dashboard password
The installer writes a random `PASSWORD` into a new `.env` and shows it once; `copycord config get PASSWORD --show-secrets` prints it again. `copycord doctor --fix` also writes a random one when it recreates a missing `.env`. A `.env` from an older install can still have the old default `copycord`: change it, since anyone who can reach the dashboard and knows the default can read your tokens.
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

`copycord config set` updates the key's line in place and keeps your comments. `copycord config get PASSWORD` prints `********` (add `--show-secrets` to see it). After `copycord config unset PASSWORD`, the next start makes a new random password.

Variables set in the real environment win over the file, for example `ADMIN_PORT=9060 copycord run`.

## The keys

| Key | Default | What it does |
|-----|---------|--------------|
| `PASSWORD` | a random password from the installer | Dashboard password; always required. |
| `ADMIN_PORT` | `8080` | Dashboard port. |
| `DASHBOARD_HOST` | `127.0.0.1` | Where the dashboard listens: `127.0.0.1` is this computer only, `0.0.0.0` opens it to your network (then a real `PASSWORD` is required). See [Security defaults](/docs/configuration/security). |
| `TZ` | `UTC` | Time zone for scheduled backups and export timestamps. |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG`; the dashboard's value wins once it is saved. |
| `LOG_FORMAT` | `HUMAN` | `HUMAN` or `JSON` log lines. |
| `BACKUP_DIR` | `<data dir>/backups` | Where the daily database backups go. |
| `BACKUP_AT` | `03:17` | Time of the daily backup, HH:MM. |
| `BACKUP_RETAIN` | `14` | How many daily backups to keep. |
| `COPYCORD_NO_UPDATE_CHECK` | `0` | `1` turns off the update check on start. |

`copycord config set` and `get` also know `SECRET_KEY`, which is not in the file: Copycord keeps its own key in `secret.key` in the data folder. The [CLI reference](/docs/cli#env-file) covers it and the rarely needed `BACKFILL_*` and `FORWARDING_*` tuning keys.
