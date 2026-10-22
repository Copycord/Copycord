---
sidebar_position: 8
title: Contributing
---

# Contributing

Set up a development copy of Copycord, run the tests and send a pull request with a bug fix, a feature or a docs change.

## Get the code

1. Fork the [repository](https://github.com/Copycord/Copycord).
2. Clone your fork:

   ```bash
   git clone https://github.com/YOUR_USERNAME/Copycord.git
   cd Copycord
   ```

3. Create a branch from `main`:

   ```bash
   git checkout -b my-feature
   ```

## Development setup

### Requirements

- Docker and Docker Compose, to run Copycord from your checkout
- Python 3.12 to 3.14, to run the tests
- Node.js 22 and npm, to build the dashboard outside Docker

### Run Copycord with Docker

Copycord runs as one container (dashboard, server and client together). To build it from your local source, use the development compose file `docker-compose.dev.yml` at the repository root. It contains:

```yaml
services:
  copycord:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: copycord
    ports:
      - "8080:8080"
    volumes:
      - ./data:/data
    environment:
      - PASSWORD=copycord
      - COPYCORD_ENV=dev
```

Change `PASSWORD` to your own first: the container listens on every network (`DASHBOARD_HOST=0.0.0.0`), and Copycord refuses to start there with the default password `copycord`. `COPYCORD_ENV=dev` turns on developer options such as the member scraper's debug report. Then build and start:

```bash
docker compose -f docker-compose.dev.yml up --build
```

This builds the image from the root `Dockerfile`: it builds the dashboard frontend with Node, installs Copycord from `pyproject.toml` on Python 3.14 and runs `copycord run`. Rebuild with the same command after every change. The dashboard is at **http://localhost:8080** and your test data stays in `./data`.

The container output shows the dashboard's log plus warnings and errors from the server and client. For the full server and client logs, open the dashboard's Logs page or read `./data/server.out` and `./data/client.out` (see [Where logs go](/docs/dashboard/system#where-logs-go)). Copycord does not start while the `ROLE` variable is set.

### Build the dashboard

The dashboard is a React and TypeScript app built with Vite, in `src/copycord/admin/web`. The Docker build compiles it for you. To check frontend changes on their own:

```bash
cd src/copycord/admin/web
npm ci
npm run build       # type check, then build into dist/
npm test            # the Vitest suite
```

`npm run dev` starts the Vite dev server.

## Run the tests

Install the test dependencies (in a virtual environment of your choice) and run the suite from the repository root, as CI does:

```bash
pip install -r tests/requirements-test.txt
COPYCORD_SKIP_FRONTEND=1 pip install -e .
python -m pytest tests/ -v
```

Run a single file while you work, for example `python -m pytest tests/test_cli.py -v`.

Lint and syntax checks, as CI runs them:

```bash
pip install ruff
ruff check src/
find src/ -name '*.py' -print0 | xargs -0 python -m py_compile
```

## Continuous integration

Every pull request against `main` or `v4-dev` (and every push to `main`) runs the **Tests** workflow (`.github/workflows/ci.yml`):

| Job | What it checks |
|-----|----------------|
| Lint & syntax check | `ruff check src/` and `py_compile` on every Python file |
| DB unit tests | `tests/test_db.py` on Python 3.12 and 3.14 |
| Unit tests | The rest of `tests/` on Python 3.12 and 3.14 |
| Frontend build | `npm ci` and `npm run build` in `src/copycord/admin/web` |
| Wheel build & smoke test | Builds the wheel with the frontend, installs it in a fresh environment and runs `copycord --version` and `copycord doctor` |

Other workflows:

- **Release assets** (`release.yml`): when a GitHub release tagged `v*` is published, builds the wheel and sdist with the tag as their version, checks that both carry exactly that version and attaches them to the release. The install scripts and `copycord update` install that wheel. Pre-releases feed the beta [update channel](/docs/cli#update-channels).
- **Build & publish Copycord container** (`build_containers.yml`): when a GitHub release is published, builds the Docker image from its tag and pushes it as `:<tag>`, plus `:beta` for a pre-release or `:latest` for a full release. Pushing a tag alone builds nothing. Run it by hand with a release tag to rebuild that release, for example after promoting a pre-release so `latest` picks it up.

Release versions have four numbers, and the last one goes up with every release: `v4.0.0.1`, `v4.0.0.2`, `v4.0.0.3`. Tick **Set as a pre-release** on the GitHub release to make it a beta; the version itself never carries a beta suffix. The git tag is the only place the version lives: nothing in the code is edited for a release. To cut a release, merge what goes in, then publish a GitHub release tagged with the new version (for example `v4.0.0.3`) on that commit. The release assets and the Docker image get their version from the tag.

Any build of a commit without a release tag is a dev build. Its version is the last release plus one with the number of commits since and the commit (`4.0.0.4.dev5+g1a2b3c4`), and Copycord shows it as `dev (1a2b3c4)` in `copycord --version`, the logs and the dashboard footer. A dev build never offers to update to a release on its own (`copycord update` run by hand still works) and never writes its version into the database. A local `docker compose up --build` reads the version from the checkout's `.git`, so it shows `dev (1a2b3c4)`; the CI image builds pass `COPYCORD_VERSION` instead. Building without git history, such as from a zip download, needs the version in `SETUPTOOLS_SCM_PRETEND_VERSION` (or the `COPYCORD_VERSION` build arg for Docker); otherwise the build reports `0+unknown`.
- **Deploy Docs to GitHub Pages** (`deploy-docs.yml`): rebuilds this site when `website/` changes on `main`.
- **Docker build smoke test**, **testing containers** and the weekly **Dependency audit** run on demand or on a schedule.

## Project structure

```
copycord/
├── src/
│   └── copycord/        # The one installed package
│       ├── __main__.py  # CLI: run, restart, doctor, config, backup, restore, uninstall
│       ├── supervisor.py # Starts and supervises admin, server and client
│       ├── service.py   # copycord service (systemd, launchd, Scheduled Task)
│       ├── update.py    # copycord update and update channels
│       ├── update_check.py # Update check on start and auto-update
│       ├── envfile.py   # <home>/.env
│       ├── logging_setup.py # Terminal and server.out/client.out logging
│       ├── admin/           # FastAPI web dashboard
│       │   ├── app.py       # Main FastAPI application and API
│       │   ├── auth.py      # Login and sessions
│       │   ├── tasks/       # Tasks page: backfills, purges, exports, scraping
│       │   └── web/         # React SPA source (Vite); the wheel ships the build
│       ├── server/          # Discord bot (py-cord)
│       │   ├── server.py    # Core sync engine
│       │   ├── commands.py  # Slash commands
│       │   ├── backfill.py  # History import
│       │   ├── token_sender.py # Sending cloned messages from user tokens
│       │   ├── roles.py     # Role management
│       │   ├── emojis.py    # Emoji cloning
│       │   └── stickers.py  # Sticker cloning
│       ├── client/          # Self-bot client
│       │   ├── client.py    # Main client
│       │   ├── forwarding.py # External forwarding
│       │   ├── proxy_rotator.py # Client proxies
│       │   └── sitemap.py   # Guild structure mapping
│       ├── selfclient/      # Copycord's own user-account Discord library (gateway, HTTP, models)
│       └── common/          # Shared code
│           ├── config.py    # Configuration management
│           ├── db.py        # SQLite database manager
│           ├── paths.py     # Data directory per OS
│           └── websockets.py # Communication between the parts
├── tests/               # pytest suite
├── install.sh           # Install script for Linux and macOS
├── install.ps1          # Install script for Windows
├── pyproject.toml       # Package definition and dependencies
├── Dockerfile           # The single Copycord image
├── docker-compose.yml   # Example compose file for users
├── docker-compose.dev.yml # Builds the image from your checkout
└── website/             # Docusaurus documentation
```

## Submit changes

1. Commit your changes with clear messages.
2. Push the branch to your fork.
3. Open a [pull request](https://github.com/Copycord/Copycord/pulls) against `main`.
4. In the description, say what the change does and why.

## Report issues

To report a bug or request a feature, [open an issue](https://github.com/Copycord/Copycord/issues). To talk with other contributors and users, join the [Discord server](https://discord.gg/ArFdqrJHBj). To support the project, [donate on Ko-fi](https://ko-fi.com/A0A41KPDX4).
