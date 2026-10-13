---
sidebar_position: 8
title: Upgrading from v3
---

# Upgrading from v3

Copycord v4 runs the dashboard (admin), the server bot and the client together as one app. Docker users run one container instead of three, and non-Docker users install with the [one-command installer](/docs/getting-started/quick-start) instead of the v3 launcher. Your database is kept: v4 opens the same `data.db` and takes a backup of it first.

## What changes

| v3 | v4 |
|----|----|
| Three containers: `admin`, `server`, `client` | One `copycord` container |
| `ROLE=admin`, `ROLE=server`, `ROLE=client` | No `ROLE`; v4 refuses to start while it is set |
| Old launcher (`Copycord.exe`, `launcher.py`) | Install script and the [`copycord` command](/docs/cli) |
| Settings in `code/.env` (non-Docker) | The dashboard, plus a few keys in [`<home>/.env`](/docs/getting-started/configuration) |
| Python 3.11, Node.js and venvs (non-Docker) | Python 3.12 provided by uv; nothing else to install |
| Client on discord.py-self | Client on Copycord's own built-in library |

## Automatic backup of your database

The first time v4 opens a database that v3 created, it copies it to:

```
<data dir>/backups/pre-v4-<timestamp>.db
```

For Docker that is `./data/backups/pre-v4-<timestamp>.db`. The copy is made before v4 changes anything, and only once: v4 then records a `schema_version` in the database so the backup is not repeated.

If you ever need to go back to v3, stop Copycord, replace `data.db` with this backup (renamed to `data.db`) and start your v3 setup again.

## Docker

A v3 compose file had three services, each with a `ROLE`:

```yaml
# v3: replace this
services:
  admin:
    image: ghcr.io/copycord/copycord:v3.28.0
    environment:
      - ROLE=admin
      - PASSWORD=copycord
    ports:
      - '8080:8080'
    volumes:
      - ./data:/data
  server:
    image: ghcr.io/copycord/copycord:v3.28.0
    environment:
      - ROLE=server
    volumes:
      - ./data:/data
  client:
    image: ghcr.io/copycord/copycord:v3.28.0
    environment:
      - ROLE=client
    volumes:
      - ./data:/data
```

Replace it with one service that uses the **same `./data` volume**:

```yaml
# v4
services:
  copycord:
    image: ghcr.io/copycord/copycord:latest # or pin a v4 release tag
    container_name: copycord
    environment:
      - PASSWORD=copycord  # keep whatever you had on the admin service
    ports:
      - '8080:8080'
    volumes:
      - ./data:/data
    restart: unless-stopped
```

1. Stop v3: `docker compose down`
2. Edit `docker-compose.yml`: delete the `server` and `client` services, rename `admin` to `copycord` (or start from the example above), remove every `ROLE` line and update the image tag
3. Keep any other environment variables you set (such as `PASSWORD`) on the `copycord` service
4. Start v4: `docker compose up -d --remove-orphans` (`--remove-orphans` removes the old `server` and `client` containers)

Your mappings, settings and history are in `./data/data.db` and are picked up as they are.

:::warning ROLE must be removed
If `ROLE` is still set, v4 stops right away with:

```
ROLE is set, but Copycord v4 runs admin, server and client in one container and the ROLE option was removed. Delete ROLE from your compose file (and the extra server/client services), or stay on a v3 image tag.
```
:::

### Staying on v3

To keep the three-container layout for now, pin your services to a v3 image tag such as `ghcr.io/copycord/copycord:v3.28.0` instead of `latest`. v3 images keep working; they do not get v4 features.

## Without Docker (v3 launcher)

The old launcher (`Copycord.exe` on Windows, `launcher.py` on Linux/macOS) is replaced by the install script in v4 and is no longer shipped. Move to the install script:

### 1. Stop v3

Close the Copycord windows (Windows) or press **Ctrl+C** in the terminal running `copycord_linux.sh` (Linux/macOS).

### 2. Note your settings

v4 does not read `code/.env`. Open it and note any values you changed, typically `PASSWORD` and `ADMIN_PORT`. In v4 they live in `<home>/.env`: edit the file or run `copycord config set PASSWORD 'new-pass'`, then `copycord restart` (see [Configuration File (.env)](/docs/getting-started/configuration)). Tokens, mappings and dashboard settings are stored in the database and need no action.

### 3. Install v4

```bash
# Linux/macOS
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
```

```powershell
# Windows (PowerShell)
irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
```

### 4. Bring your data

Your v3 data is in the `data/` folder of your v3 install (for example `~/copycord/data` on Linux). v4 uses a per-user home directory instead:

| OS | v4 data directory |
|----|-------------------|
| Linux, macOS | `~/.copycord` |
| Windows | `%USERPROFILE%\.copycord` |

Set `COPYCORD_HOME` (or pass `--home` to the installer) to use another folder; see [The Copycord home](/docs/getting-started/manual-install#home).

Choose one of these:

**Option A: keep the data where it is.** Point v4 at the old folder with `--data-dir`:

```bash
copycord doctor --data-dir ~/copycord/data
copycord run --data-dir ~/copycord/data
```

Use the same flag when you install the service, so it uses that folder too:

```bash
copycord service install --user --data-dir ~/copycord/data
```

**Option B: copy the data into the v4 data directory.** With Copycord stopped:

```bash
# Linux and macOS
mkdir -p ~/.copycord
cp -a ~/copycord/data/. ~/.copycord/
```

```powershell
# Windows (adjust the source path to your v3 folder)
New-Item -ItemType Directory -Force "$env:USERPROFILE\.copycord" | Out-Null
Copy-Item -Recurse -Force "C:\Copycord\data\*" "$env:USERPROFILE\.copycord\"
```

Then run `copycord doctor` and `copycord run` without `--data-dir`.

### 5. Check and clean up

`copycord doctor` should report `PASS  database: ... opens`. Start Copycord, open the dashboard and check your mappings. The first start writes the `pre-v4` backup described above.

Once v4 works, you can delete the v3 `code/` and `venvs/` folders, the launcher and the start scripts. Keep the `data/` folder if you chose option A.
