---
sidebar_position: 7
title: CLI Reference
---

# CLI Reference

The [install script](/docs/getting-started/manual-install) installs the `copycord` command. It runs Copycord, checks your setup, reads and writes settings, manages the background service and updates Copycord.

```
copycord run [--data-dir PATH]
copycord version [--data-dir PATH]
copycord doctor [--data-dir PATH]
copycord config get KEY [--data-dir PATH]
copycord config set KEY VALUE [--data-dir PATH]
copycord config export [FILE] [--show-secrets] [--data-dir PATH]
copycord config import FILE [--data-dir PATH]
copycord service install [--user] [--data-dir PATH]
copycord service start|stop|status|uninstall [--user]
copycord update [--check] [--channel stable|beta|dev] [--branch NAME] [--tag VERSION] [--no-install]
```

Every command has `--help`, for example `copycord service install --help`.

## The data directory {#data-dir}

Copycord keeps its database (`data.db`), logs, backups and the saved update channel in one data directory:

| Where | Data directory |
|-------|----------------|
| Linux | `~/.local/share/copycord` |
| macOS | `~/Library/Application Support/Copycord` |
| Windows | `%LOCALAPPDATA%\Copycord` |
| Docker | `/data` (your `./data` volume) |

`--data-dir PATH` uses another folder for that command. It does the same as setting the `DATA_DIR` environment variable. It is accepted by `run`, `version`, `doctor`, every `config` command and `service install`.

## copycord run {#copycord-run}

Starts Copycord: the dashboard, the server bot and the client, in one process.

```bash
copycord run
copycord run --data-dir ~/copycord-data
```

The dashboard is at **http://localhost:8080**. Press **Ctrl+C** to stop. If one part crashes, Copycord restarts that part on its own.

Useful environment variables:

| Variable | Default | What it does |
|----------|---------|--------------|
| `PASSWORD` | unset | Dashboard login password; no login page when unset |
| `ADMIN_PORT` | `8080` | Dashboard port |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG` |
| `DATA_DIR` | see above | Same as `--data-dir` |

`copycord run` refuses to start while the old v3 `ROLE` variable is set. See [Upgrading from v3](/docs/getting-started/upgrading-from-v3).

The terminal shows the dashboard's log and warnings or errors from the server and client. The full server and client logs go to files in the data directory; see [Logs](/docs/dashboard/system#where-logs-go).

## copycord version {#copycord-version}

Prints the installed version and the saved [update channel](#update-channels):

```bash
$ copycord version
4.0.0
channel stable
```

## copycord doctor {#copycord-doctor}

Checks that this machine can run Copycord. It does not use the network.

```bash
copycord doctor
copycord doctor --data-dir ~/copycord/data
```

| Check | Passes when |
|-------|-------------|
| `python` | Python is 3.12 or newer |
| `ROLE` | The `ROLE` variable is not set |
| `data dir` | The data directory is writable, or can be created |
| `database` | `data.db` opens, or does not exist yet |
| `admin port` | `ADMIN_PORT` (default 8080) is free |
| `frontend` | The dashboard files are installed |
| `import ...` | The required packages (`curl_cffi`, `py-cord`, `fastapi`, `uvicorn`) import |

Each line starts with `PASS`, `WARN` or `FAIL`. The command exits with status 1 when any check fails, so it can be used in scripts.

:::tip Port in use
`FAIL  admin port` usually means Copycord is already running (for example as a service). Stop it, or set `ADMIN_PORT` to another port.
:::

## copycord config {#copycord-config}

Reads and writes the settings Copycord stores in its database (the same values the dashboard's Global Configuration card shows, such as `COMMAND_USERS`, `LOG_LEVEL` or `AUTO_START`).

```bash
copycord config get LOG_LEVEL
copycord config set LOG_LEVEL DEBUG
copycord config export                    # print all values, secrets masked
copycord config export --show-secrets     # print all values, secrets shown
copycord config export settings.json      # write all values to a file
copycord config import settings.json
```

| Command | What it does |
|---------|--------------|
| `get KEY` | Prints the value. Exits with status 1 when the key is not set. |
| `set KEY VALUE` | Stores the value. Creates the database if needed. |
| `export` | Prints every value as JSON. Values of keys containing `TOKEN`, `PASSWORD` or `SECRET` are shown as `********` unless you add `--show-secrets`. |
| `export FILE` | Writes every value, unmasked, to `FILE` (readable only by you on Linux/macOS). |
| `import FILE` | Stores every value from a JSON object of key/value pairs (strings or numbers). Masked `********` values are skipped. |

`get` and `export` need an existing database; when there is none they say so (check `--data-dir`). Restart Copycord after `set` or `import` so it picks up the new values.

To move your settings to another machine, run `copycord config export settings.json` on the old one and `copycord config import settings.json` on the new one. Treat the file like a password: it contains your tokens.

## copycord service {#copycord-service}

Runs Copycord in the background with your operating system's service manager, so it keeps running without an open terminal and starts again automatically.

```bash
copycord service install      # install, enable and start
copycord service status
copycord service stop
copycord service start
copycord service uninstall    # stop and remove
```

| Command | What it does |
|---------|--------------|
| `install [--user] [--data-dir PATH]` | Registers `copycord run --data-dir <data dir>` with the service manager, enables it and starts it |
| `start` | Starts the service |
| `stop` | Stops the service |
| `status` | Prints whether the service is installed and running. Exits with 0 when running, 3 when not. |
| `uninstall` | Stops and removes the service. Your data is kept. |

The data directory is fixed when you install the service: the one given with `--data-dir` (or `DATA_DIR`), else the default for your user. To change it, run `copycord service install` again with the new `--data-dir`.

### How it works on each OS

| OS | Service | Starts | Restarts |
|----|---------|--------|----------|
| Linux (default) | systemd system unit `/etc/systemd/system/copycord.service` | At boot | 10 seconds after a crash |
| Linux (`--user`) | systemd user unit `~/.config/systemd/user/copycord.service` | When you log in (at boot with lingering) | 10 seconds after a crash |
| macOS | launchd agent `~/Library/LaunchAgents/com.copycord.plist` | When you log in | After a crash |
| Windows | Scheduled Task `Copycord` | When you log in | Every minute after a failure |

**Linux system service.** Installing, starting, stopping and removing it needs root. Because the install script puts `copycord` in your user's tool directory, call it by its full path:

```bash
sudo "$(command -v copycord)" service install
```

When installed with `sudo`, the service runs as your user (not root) and uses your user's data directory.

**Linux user service.** `copycord service install --user` needs no root. It only runs while you are logged in unless you enable lingering, which also starts it at boot:

```bash
copycord service install --user
sudo loginctl enable-linger "$USER"
```

Use `--user` with the other `service` commands as well. Without `--user`, `start`, `stop` and `status` use the user service when there is no system service.

**macOS.** The agent's output goes to `copycord.out.log` and `copycord.err.log` in the data directory. `--user` has no effect.

**Windows.** The task runs as the current user at logon. `--user` has no effect.

### Environment variables for the service

The service does not see variables you set in your terminal. To give it `PASSWORD`, `ADMIN_PORT` or other variables:

- **Linux:** run `systemctl --user edit copycord` (or `sudo systemctl edit copycord` for the system service), add the lines below, save, then restart with `copycord service stop` and `copycord service start`.

  ```ini
  [Service]
  Environment=PASSWORD=changeme
  Environment=ADMIN_PORT=9060
  ```

- **macOS:** add the variables to the `EnvironmentVariables` dictionary in `~/Library/LaunchAgents/com.copycord.plist`, then run `copycord service stop` and `copycord service start`. Running `copycord service install` again rewrites this file.
- **Windows:** set them as user environment variables, for example `setx PASSWORD changeme`, then sign out and back in so the task starts with them.

## copycord update {#copycord-update}

Updates a Copycord installed with the install script (or with `pipx`).

```bash
copycord update                        # newest release on the saved channel
copycord update --check                # only show what is installed and what is available
copycord update --channel beta         # switch to the beta channel and update
copycord update --channel stable       # switch back to stable
copycord update --tag 4.0.0            # install exactly this release, once
copycord update --channel dev --branch main
copycord update --channel beta --no-install
```

| Option | What it does |
|--------|--------------|
| `--check` | Prints the installed version, the channel and the newest version on that channel, then exits without installing. Network errors are printed as `WARN`. |
| `--channel stable\|beta\|dev` | Switches to this channel, installs from it and saves it once the install worked |
| `--branch NAME` | Dev channel only: the git branch to install (default `main`) |
| `--tag VERSION` | Installs this exact release (`4.0.0` or `v4.0.0`). One-off: the saved channel does not change. Cannot be combined with `--channel` or `--branch`. |
| `--no-install` | Only saves `--channel` (and `--branch`), installs nothing. The install scripts use this. |

After a successful update, a running [service](#copycord-service) is restarted so it uses the new version.

### Update channels {#update-channels}

| Channel | Installs |
|---------|----------|
| `stable` (default) | The newest [GitHub release](https://github.com/Copycord/Copycord/releases) that is not a pre-release |
| `beta` | The newest GitHub release, pre-releases included (for example `v4.0.0b1`) |
| `dev` | The newest commit of a git branch (default `main`). It builds the dashboard from source, so it needs Node.js (`npm`) on your `PATH`. |

Stable and beta install the wheel attached to the release. The channel is saved in `update.json` in the [data directory](#data-dir). Installing with `COPYCORD_CHANNEL=beta` saves `beta` the same way. `copycord update` has no `--data-dir` option, so if you use a custom data directory, set `DATA_DIR` when running it.

### Other installs

`copycord update` reinstalls Copycord with the tool that installed it: `uv tool install --force` for the install script, `pipx install --force` for pipx. For other installs it prints what to do instead:

- **Docker:** pull the new image and recreate the container (`docker compose pull && docker compose up -d`). See [Docker Install](/docs/getting-started/docker-install#update-copycord).
- **A git checkout:** update it with `git pull`, or switch to the install script.
- **pip or uv pip in your own environment:** it prints the matching `pip install --upgrade` command for you to run.
