---
sidebar_position: 3
title: Docker Install
---

# Docker Install

Docker is the recommended way to run Copycord. It handles all dependencies and makes updates easy.

## Install Docker

### Windows

1. Download and install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop)
2. Follow the installation wizard
3. Restart your computer if prompted

### Linux (Ubuntu/Debian)

```bash
curl -fsSL https://get.docker.com | sh
```

Verify the installation:

```bash
docker --version
docker compose version
```

### macOS

1. Download and install [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop)
2. Follow the installation wizard

## Set up Copycord

### 1. Create the project folder

Create a new folder for Copycord and add the `docker-compose.yml` file:

```
copycord/
├── docker-compose.yml
└── data/              ← created automatically
```

### 2. Create docker-compose.yml

Create a file named `docker-compose.yml` with the following content:

```yaml
services:
  admin:
    image: ghcr.io/copycord/copycord:v3.20.0
    container_name: copycord-admin
    environment:
      - ROLE=admin
      - PASSWORD=copycord  # change this or remove the line to disable login
    ports:
      - '8080:8080'       # change the left port if 8080 is taken (e.g. "9060:8080")
    volumes:
      - ./data:/data
    restart: unless-stopped

  server:
    image: ghcr.io/copycord/copycord:v3.20.0
    container_name: copycord-server
    environment:
      - ROLE=server
    volumes:
      - ./data:/data
    depends_on:
      - admin
    restart: unless-stopped

  client:
    image: ghcr.io/copycord/copycord:v3.20.0
    container_name: copycord-client
    environment:
      - ROLE=client
    volumes:
      - ./data:/data
    depends_on:
      - admin
    restart: unless-stopped
```

:::tip Customizing the password
Change `PASSWORD=copycord` to your preferred password, or remove the line entirely to disable dashboard authentication.
:::

### 3. Launch Copycord

Open a terminal in the folder containing `docker-compose.yml` and run:

```bash
docker compose up -d
```

This pulls the Copycord images and starts all three services. The dashboard will be available at:

**http://localhost:8080**

### Verify it's running

```bash
docker compose ps
```

You should see three containers running: `copycord-admin`, `copycord-server`, and `copycord-client`.

## Managing Copycord

### View logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f admin
docker compose logs -f server
docker compose logs -f client
```

### Stop Copycord

```bash
docker compose down
```

Or use **Docker Desktop** on Windows/macOS to stop the containers from the GUI.

### Update Copycord

1. Edit `docker-compose.yml` and update the image tag to the [latest release](https://github.com/Copycord/Copycord/releases)
2. Pull and restart:

```bash
docker compose pull
docker compose up -d
```

### Change the dashboard port

Edit `docker-compose.yml` and change the port mapping on the admin service:

```yaml
ports:
  - '9060:8080'  # dashboard now at http://localhost:9060
```

Then restart: `docker compose up -d`

## Next steps

Head to [First Run](/docs/getting-started/first-run) to configure your tokens and create your first server mapping.
