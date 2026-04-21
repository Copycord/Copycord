---
sidebar_position: 8
title: Contributing
---

# Contributing

We welcome contributions to Copycord! Whether it's bug fixes, new features, or documentation improvements, here's how to get started.

## Getting started

1. **Fork** the [repository](https://github.com/Copycord/Copycord)
2. **Clone** your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Copycord.git
   cd Copycord
   ```
3. Create a new **branch** from `main`:
   ```bash
   git checkout -b my-feature
   ```

## Development setup

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Node.js (LTS) + npm

### Running locally with Docker

Create a `compose.yml` file in the project root and use it to build and run Copycord from source:

```yaml
# Local build
services:
  admin:
    build:
      context: .
      dockerfile: code/Dockerfile
    container_name: copycord-admin
    environment:
      - ROLE=admin
      - PASSWORD=12345
    ports:
      - "8080:8080" # you can change this port if needed (ex: "9060:8080")
    volumes:
      - ./data:/data

  server:
    build:
      context: .
      dockerfile: code/Dockerfile
    container_name: copycord-server
    environment:
      - ROLE=server
    volumes:
      - ./data:/data
    depends_on:
      - admin

  client:
    build:
      context: .
      dockerfile: code/Dockerfile
    container_name: copycord-client
    environment:
      - ROLE=client
    volumes:
      - ./data:/data
    depends_on:
      - admin
```

Then build and start:

```bash
docker compose up --build
```

This builds all three services from your local source code instead of pulling pre-built images, so any changes you make are reflected immediately on rebuild.

## Project structure

```
copycord/
├── code/
│   ├── admin/           # FastAPI web dashboard
│   │   ├── app.py       # Main FastAPI application
│   │   ├── auth.py      # Authentication
│   │   ├── frontend/    # Vite frontend source
│   │   ├── templates/   # Jinja2 HTML templates
│   │   └── static/      # Built frontend assets
│   ├── server/          # Discord bot (py-cord)
│   │   ├── server.py    # Core sync engine
│   │   ├── commands.py  # Slash commands
│   │   ├── backfill.py  # History import
│   │   ├── roles.py     # Role management
│   │   ├── emojis.py    # Emoji cloning
│   │   └── stickers.py  # Sticker cloning
│   ├── client/          # Self-bot (discord.py-self)
│   │   ├── client.py    # Main client
│   │   ├── forwarding.py # External forwarding
│   │   ├── scraper.py   # Member scraper
│   │   └── sitemap.py   # Guild structure mapping
│   ├── common/          # Shared code
│   │   ├── config.py    # Configuration management
│   │   ├── db.py        # SQLite database manager
│   │   └── websockets.py # WebSocket communication
│   └── control/         # Process control
│       └── control.py   # WebSocket control server
├── compose.yml          # Development Docker Compose
└── website/             # Docusaurus documentation
```

## Submitting changes

1. **Commit** your changes with clear, descriptive messages
2. **Push** to your fork
3. Open a [Pull Request](https://github.com/Copycord/Copycord/pulls) against the `main` branch
4. Describe your changes — what they do and why

## Reporting issues

Found a bug or have a feature request? [Open an issue](https://github.com/Copycord/Copycord/issues) on GitHub.

## Community

Join our [Discord server](https://discord.gg/ArFdqrJHBj) to chat with other contributors and users.

## Support the project

If you enjoy Copycord, consider [buying us a coffee](https://ko-fi.com/A0A41KPDX4)!
