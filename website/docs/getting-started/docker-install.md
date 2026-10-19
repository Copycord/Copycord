---
sidebar_position: 6
title: Docker (advanced)
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Docker Install

Run Copycord in a Docker container. Use this if you already run your apps with Docker; otherwise the [Quick Start](/docs/getting-started/quick-start) installer is simpler.

## Install Docker

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

On Linux (Ubuntu, Debian and most others), run:

```bash title="Terminal"
curl -fsSL https://get.docker.com | sh
```

On macOS, install [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop).

</TabItem>
<TabItem value="windows" label="Windows">

Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop) and restart if asked.

</TabItem>
<TabItem value="docker" label="Docker">

Docker is already installed. Continue with the next section.

</TabItem>
</Tabs>

To check the install, run `docker compose version`.

## Start Copycord

1. Create a folder named `copycord`.
2. In that folder, create a file named `docker-compose.yml` with this content, and put your own password after `PASSWORD=`:

   ```yaml title="docker-compose.yml"
   services:
     copycord:
       image: ghcr.io/copycord/copycord:latest # or a release tag, such as v4.0.0
       container_name: copycord
       environment:
         - PASSWORD=  # required
       ports:
         - '8080:8080'
       volumes:
         - ./data:/data
       restart: unless-stopped
   ```

3. In the same folder, start the container:

   ```bash title="Terminal"
   docker compose up -d
   ```

   Docker downloads the image and starts Copycord. Your data is stored in the `data` folder next to `docker-compose.yml`.

4. Open `http://localhost:8080`, log in with your `PASSWORD` and continue with [First Run](/docs/getting-started/first-run).

The container accepts connections from every network, so Copycord does not start while `PASSWORD` is empty or `copycord`. If another layer, such as a reverse proxy with its own login, already protects the dashboard, `COPYCORD_ALLOW_INSECURE_DASHBOARD=1` skips this check.

## Manage the container

Run these commands in the folder with `docker-compose.yml`.

| Task | Command |
|------|---------|
| Check that it runs | `docker compose ps` |
| Follow the log | `docker compose logs -f` |
| Stop | `docker compose down` |
| Start or apply a change to `docker-compose.yml` | `docker compose up -d` |

The log shows the dashboard's lines and the warnings and errors from the server and client, tagged `[admin]`, `[server]` or `[client]`. The full logs are on the dashboard's **Logs** page and in `data/server.out`, `data/client.out` and `data/scraper.out`. See [Where logs go](/docs/dashboard/system#where-logs-go).

### Update Copycord

1. If you pinned a release tag, change it in `docker-compose.yml` to the [latest release](https://github.com/Copycord/Copycord/releases).
2. Pull the new image and restart:

   ```bash title="Terminal"
   docker compose pull
   docker compose up -d
   ```

### Change the dashboard port

To use another port, change the left number of the port mapping, then run `docker compose up -d`:

```yaml title="docker-compose.yml"
    ports:
      - '9060:8080'  # dashboard at http://localhost:9060
```

### Run copycord commands

The [`copycord` command](/docs/cli) is installed in the container. To run it, prefix it with `docker compose exec copycord /opt/venvs/copycord/bin/`:

```bash title="Terminal"
docker compose exec copycord /opt/venvs/copycord/bin/copycord doctor
docker compose exec copycord /opt/venvs/copycord/bin/copycord backup
```

To restore a backup, see [`copycord restore`](/docs/cli#copycord-restore). `copycord update` and `copycord service` do not apply to Docker.

## File ownership

Copycord runs as the user `copycord` (uid and gid 1000). The container starts as root only to give the `data` folder to that user, then switches to it. Where ownership cannot be changed, such as Docker Desktop bind mounts on Windows, Copycord keeps running as root.

If you set `user:` in the compose file and Copycord exits with "is not writable", give the folder to uid 1000 once on the host:

```bash title="Terminal"
sudo chown -R 1000:1000 ./data
```

If you ran Copycord v3 with Docker, see [Upgrading from v3](/docs/getting-started/upgrading-from-v3).
