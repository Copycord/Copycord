---
sidebar_position: 6
title: Docker (advanced)
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Docker Install

Docker runs Copycord in a single container with everything it needs. Use it if you already run your apps with Docker; otherwise the [one-command installer](/docs/getting-started/quick-start) is simpler.

## Install Docker

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

On Linux (Ubuntu, Debian and most others):

```bash title="Terminal"
curl -fsSL https://get.docker.com | sh
docker --version
docker compose version
```

On macOS, install [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop) and follow the wizard.

</TabItem>
<TabItem value="windows" label="Windows">

Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop), follow the wizard and restart if asked.

</TabItem>
<TabItem value="docker" label="Docker">

Docker is already installed. Continue below.

</TabItem>
</Tabs>

## Set up Copycord

### 1. Create the project folder

Create a new folder for Copycord and add the `docker-compose.yml` file:

```
copycord/
├── docker-compose.yml
└── data/              ← created automatically
```

### 2. Create docker-compose.yml

Create a file named `docker-compose.yml` with the following content. Copycord runs as one container with the dashboard, server and client together:

```yaml title="docker-compose.yml"
services:
  copycord:
    image: ghcr.io/copycord/copycord:latest # pinning a release tag (e.g. v4.0.0) is recommended
    container_name: copycord
    environment:
      - PASSWORD=copycord  # change this or remove the line to disable login
    ports:
      - '8080:8080'       # change the left port if 8080 is taken (e.g. "9060:8080")
    volumes:
      - ./data:/data
    restart: unless-stopped
```

:::tip Customizing the password
Change `PASSWORD=copycord` to your preferred password, or remove the line entirely to disable dashboard authentication.
:::

:::note Upgrading from v3
v3 used three services (`admin`, `server`, `client`) with a `ROLE` variable. Copycord v4 is this one service and refuses to start while `ROLE` is set. See [Upgrading from v3](/docs/getting-started/upgrading-from-v3).
:::

### 3. Launch Copycord

Open a terminal in the folder containing `docker-compose.yml` and run:

```bash
docker compose up -d
```

This pulls the Copycord image and starts the container. The dashboard will be available at:

**http://localhost:8080**

### Verify it's running

```bash
docker compose ps
```

You should see one container running: `copycord`.

## Managing Copycord

### View logs

```bash
docker compose logs -f
```

This shows the dashboard's log plus warnings and errors from the server and client, each line tagged `[admin]`, `[server]` or `[client]`. The full server and client logs are in `./data/server.out` and `./data/client.out` and in the log viewers of the dashboard. See [Logs](/docs/dashboard/system#where-logs-go).

### Run CLI commands

The [`copycord` command](/docs/cli) is installed inside the container. For example, to run the checks or read a setting:

```bash
docker compose exec copycord /opt/venvs/copycord/bin/copycord doctor
docker compose exec copycord /opt/venvs/copycord/bin/copycord config get LOG_LEVEL
```

`copycord update` and `copycord service` are not used with Docker; update by pulling a new image as shown below.

### Stop Copycord

```bash
docker compose down
```

Or use **Docker Desktop** on Windows/macOS to stop the container from the GUI.

### Update Copycord

1. If you pinned a release tag, edit `docker-compose.yml` and change it to the [latest release](https://github.com/Copycord/Copycord/releases)
2. Pull and restart:

```bash
docker compose pull
docker compose up -d
```

### Change the dashboard port

Edit `docker-compose.yml` and change the port mapping on the `copycord` service:

```yaml
ports:
  - '9060:8080'  # dashboard now at http://localhost:9060
```

Then restart: `docker compose up -d`

## Next steps

Open **http://localhost:8080**, log in with `copycord` (or the `PASSWORD` you set) and continue with [First Run](/docs/getting-started/first-run).
