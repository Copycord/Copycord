---
sidebar_position: 4
title: Configuration File (.env)
description: The settings file the installer writes, how to edit it and how to apply a change.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Configuration File (.env)

Change the settings Copycord needs before the dashboard starts, such as the dashboard password and port. Everything else, including tokens, mappings, filters and cloning options, is set in the dashboard.

## Where it is

The file is `.env` in the Copycord home. Copycord reads it every time it starts, with `copycord run` or as a service.

| OS | File |
|----|------|
| Linux, macOS | `~/.copycord/.env` |
| Windows | `%USERPROFILE%\.copycord\.env` |
| Docker | Not used. Set the keys under `environment:` in `docker-compose.yml`. |

With a [custom home](/docs/getting-started/manual-install#home), the file is `.env` in that folder.

## Change a setting

To change a setting, edit the file or run `copycord config set`, then restart Copycord.

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
copycord config set PASSWORD 'your-new-password'
copycord restart
```

To edit the file by hand, run `nano ~/.copycord/.env`.

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell"
copycord config set PASSWORD 'your-new-password'
copycord restart
```

To edit the file by hand, run `notepad "$HOME\.copycord\.env"`.

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

`copycord config set` changes the key's line in place and keeps your comments. `copycord config get PASSWORD` prints `********`; add `--show-secrets` to see the value. Variables set in the environment win over the file, for example `ADMIN_PORT=9060 copycord run`.

## Keys

An empty value, such as `TZ=`, means the default.

| Key | Default | Description |
|-----|---------|-------------|
| `PASSWORD` | A random password | Dashboard password. Always required. |
| `ADMIN_PORT` | `8080` | Dashboard port. |
| `DASHBOARD_HOST` | `127.0.0.1` | Where the dashboard listens. `127.0.0.1` allows only this computer; `0.0.0.0` opens it to your network and requires your own `PASSWORD`. See [Security defaults](/docs/configuration/security). |
| `TZ` | `UTC` | Time zone for scheduled backups and export timestamps. |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG`. The value saved in the dashboard wins. |
| `LOG_FORMAT` | `HUMAN` | `HUMAN` or `JSON` log lines. |
| `BACKUP_DIR` | `<data dir>/backups` | Folder for the daily database backups. |
| `BACKUP_AT` | `03:17` | Time of the daily backup, as `HH:MM`. |
| `BACKUP_RETAIN` | `14` | Number of daily backups to keep. |
| `COPYCORD_NO_UPDATE_CHECK` | `0` | `1` turns off the update check on start. |

The [CLI reference](/docs/cli#env-file) also covers `SECRET_KEY` and the `BACKFILL_*` and `FORWARDING_*` tuning keys.

## The default file

The installer writes the file on the first install, with every key at its default, and never overwrites it. You can write it yourself with `copycord config init`. On Linux, for a user named `alice`:

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

## The dashboard password

The installer and `copycord doctor --fix` write a random `PASSWORD` when they create the file. If `PASSWORD` is empty or missing, `copycord run` creates a random one, saves it in the file and shows it once. After `copycord config unset PASSWORD`, the next start creates a new one. In Docker, Copycord does not start until you set `PASSWORD`.

:::warning
A `.env` from an older install can still have the password `copycord`. Change it: anyone who can reach the dashboard and knows that password can read your tokens.
:::
