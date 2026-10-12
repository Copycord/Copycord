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

- Docker & Docker Compose (to run Copycord from your checkout)
- Python 3.12 or newer (to run the tests)
- Node.js 20 + npm (only if you build the dashboard frontend outside Docker)

### Running locally with Docker

Copycord runs as one container (dashboard, server and client together). To build it from your local source, copy the development compose file to the repository root as `compose.yml` (it is ignored by git):

```bash
cp docs/contribute/docker-compose.yml compose.yml
```

It contains:

```yaml
services:
  copycord:
    build:
      context: .
      dockerfile: code/dockerfile
    container_name: copycord
    ports:
      - "8080:8080"
    volumes:
      - ./data:/data
```

Add an `environment:` entry with `- PASSWORD=...` if you want a login page. Then build and start:

```bash
docker compose up --build
```

This builds the image from `code/dockerfile`: it builds the dashboard frontend with Node, installs Copycord from `pyproject.toml` on Python 3.14 and runs `copycord run`. Rebuild with the same command after every change. The dashboard is at **http://localhost:8080** and your test data stays in `./data`.

The container output shows the dashboard's log plus warnings and errors from the server and client. For the full server and client logs, open the dashboard log viewers or read `./data/server.out` and `./data/client.out` (see [Where logs go](/docs/dashboard/system#where-logs-go)).

:::note No more ROLE
v4 has no `ROLE` variable and no separate `admin`, `server` and `client` services. Copycord refuses to start while `ROLE` is set.
:::

### Building the frontend

The Docker build compiles the frontend for you. To check frontend changes on their own:

```bash
cd code/admin/frontend
npm ci
npm run build
```

## Running tests

Install the test dependencies (in a virtual environment of your choice) and run the suite from the repository root:

```bash
pip install pytest pytest-asyncio -r tests/requirements-test.txt
PYTHONPATH=code python -m pytest tests/ -v
```

Run a single file while you work, for example `PYTHONPATH=code python -m pytest tests/test_cli.py -v`.

Lint and syntax checks, as CI runs them:

```bash
pip install ruff
ruff check code/
find code/ -name '*.py' -print0 | xargs -0 python -m py_compile
```

## Continuous integration

Every pull request against `main` (and every push to `main`) runs the **Tests** workflow (`.github/workflows/ci.yml`):

| Job | What it checks |
|-----|----------------|
| Lint & syntax check | `ruff check code/` and `py_compile` on every Python file |
| DB unit tests | `tests/test_db.py` on Python 3.12 and 3.14 |
| Unit tests | The rest of `tests/` on Python 3.12 and 3.14 |
| Frontend build | `npm ci` and `npm run build` in `code/admin/frontend` |
| Wheel build & smoke test | Builds the wheel with the frontend, installs it in a fresh environment and runs `copycord version` and `copycord doctor` |

Other workflows:

- **Release assets** (`release.yml`): when a GitHub release tagged `v*` is published, builds the wheel and sdist and attaches them to the release. The install scripts and `copycord update` install that wheel. Pre-releases feed the beta [update channel](/docs/cli#update-channels).
- **Build & publish Copycord container** (`build_containers.yml`): publishes the Docker image for `v*.*.*` tags.
- **Deploy Docs to GitHub Pages** (`deploy-docs.yml`): rebuilds this site when `website/` changes on `main`.
- **Docker build smoke test**, **testing containers** and the weekly **Dependency audit** run on demand or on a schedule.

## Project structure

```
copycord/
├── code/
│   ├── copycord/        # The `copycord` command
│   │   ├── __main__.py  # CLI: run, version, doctor, config
│   │   ├── supervisor.py # Runs admin, server and client in one process
│   │   ├── service.py   # copycord service (systemd, launchd, Scheduled Task)
│   │   ├── update.py    # copycord update and update channels
│   │   └── logging_setup.py # Terminal and server.out/client.out logging
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
│   ├── client/          # Self-bot client
│   │   ├── client.py    # Main client
│   │   ├── forwarding.py # External forwarding
│   │   └── sitemap.py   # Guild structure mapping
│   ├── selfclient/      # Copycord's own user-account Discord library (gateway, HTTP, models)
│   ├── common/          # Shared code
│   │   ├── config.py    # Configuration management
│   │   ├── db.py        # SQLite database manager
│   │   ├── paths.py     # Data directory per OS
│   │   └── websockets.py # Communication between the parts
│   └── dockerfile       # The single Copycord image
├── docs/contribute/     # Development compose file and build notes
├── tests/               # pytest suite
├── install.sh           # Install script for Linux and macOS
├── install.ps1          # Install script for Windows
├── pyproject.toml       # Package definition and dependencies
├── docker-compose.yml   # Example compose file for users
└── website/             # Docusaurus documentation
```

## Submitting changes

1. **Commit** your changes with clear, descriptive messages
2. **Push** to your fork
3. Open a [Pull Request](https://github.com/Copycord/Copycord/pulls) against the `main` branch
4. Describe your changes: what they do and why

## Reporting issues

Found a bug or have a feature request? [Open an issue](https://github.com/Copycord/Copycord/issues) on GitHub.

## Community

Join our [Discord server](https://discord.gg/ArFdqrJHBj) to chat with other contributors and users.

## Support the project

If you enjoy Copycord, consider [buying us a coffee](https://ko-fi.com/A0A41KPDX4)!
