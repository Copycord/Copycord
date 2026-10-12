---
sidebar_position: 7
title: CLI Reference
---

# CLI Reference

The [install script](/docs/getting-started/manual-install) installs the `copycord` command. It runs Copycord, checks your setup, reads and writes settings, manages the background service and updates Copycord.

```
copycord run [--data-dir PATH]
copycord doctor [--data-dir PATH]
copycord config get KEY [--data-dir PATH]
copycord config set KEY VALUE [--data-dir PATH]
copycord config export [FILE] [--show-secrets] [--data-dir PATH]
copycord config import FILE [--data-dir PATH]
copycord service install [--user] [--data-dir PATH]
copycord service start|stop|status|uninstall [--user]
copycord update [--channel stable|beta|dev] [--repo OWNER/NAME] [--ref REF] [--tag VERSION] [--no-install]
copycord update status
copycord uninstall [--purge] [--yes] [--keep-service] [--dry-run] [--data-dir PATH]
copycord --version | -V
```

Every command has `--help`, for example `copycord service install --help`. `copycord --help` lists the commands by group with examples.

## Output and colors {#output}

In a terminal the CLI shows the Copycord banner, colored check marks and crosses, a spinner while it waits for GitHub, and boxed notices for things you need to act on. Colors are turned off when the output is not a terminal (a pipe, a file, a service log), when `NO_COLOR` is set, or when `TERM=dumb`. `FORCE_COLOR=1` turns them on anyway.

Without colors every status line starts with a plain word, so scripts can match it:

```
PASS  data dir: /home/you/.copycord is writable
WARN  frontend: ... is missing, the admin UI will not load
FAIL  admin port: port 8080 is in use
```

## The data directory {#data-dir}

Copycord keeps its database (`data.db`), logs, backups and the saved update channel in one data directory (the Copycord home):

| Where | Data directory |
|-------|----------------|
| Linux, macOS | `~/.copycord` |
| Windows | `%USERPROFILE%\.copycord` |
| Docker | `/data` (your `./data` volume) |

This folder is the Copycord home. A git install keeps its checkout in `src/` and its per-user Node in `node/` inside it. Set `COPYCORD_HOME` to use another folder; `--data-dir` and `DATA_DIR` still win over it. If `~/.copycord` does not exist but an older install left a `data.db` in `~/.local/share/copycord` (Linux), `~/Library/Application Support/Copycord` (macOS) or `%LOCALAPPDATA%\Copycord` (Windows), Copycord keeps using that folder and prints a notice. It never moves data by itself; to move, stop Copycord and move the folder to `~/.copycord`.

`--data-dir PATH` uses another folder for that command. It does the same as setting the `DATA_DIR` environment variable. It is accepted by `run`, `doctor`, every `config` command, `service install` and `uninstall`.

## copycord run {#copycord-run}

Starts Copycord: the dashboard, the server bot and the client, in one process.

```bash
copycord run
copycord run --data-dir ~/copycord-data
```

The dashboard is at **http://localhost:8080**. Open it to finish the setup: the server bot token, the client token and everything else is configured there (see [Discord Setup](/docs/getting-started/discord-setup) for the tokens). Press **Ctrl+C** to stop. If one part crashes, Copycord restarts that part on its own.

Useful environment variables:

| Variable | Default | What it does |
|----------|---------|--------------|
| `PASSWORD` | unset | Dashboard login password; no login page when unset |
| `ADMIN_PORT` | `8080` | Dashboard port |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG` |
| `DATA_DIR` | see above | Same as `--data-dir` |

`copycord run` refuses to start while the old v3 `ROLE` variable is set. See [Upgrading from v3](/docs/getting-started/upgrading-from-v3).

The terminal shows the dashboard's log and warnings or errors from the server and client. The full server and client logs go to files in the data directory; see [Logs](/docs/dashboard/system#where-logs-go).

## copycord --version {#copycord-version}

`copycord --version` (or `-V`) prints the installed version and the saved [update channel](#update-channels). In a terminal it shows the banner too; piped, it prints two plain lines:

```bash
$ copycord --version | cat
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

In a terminal each check shows a green check, a yellow `!` or a red cross; piped, each line starts with `PASS`, `WARN` or `FAIL`. The command exits with status 1 when any check fails, so it can be used in scripts.

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
| `uninstall` | Stops and removes the service. Your data is kept. To remove Copycord itself too, use [`copycord uninstall`](#copycord-uninstall). |

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

## copycord uninstall {#copycord-uninstall}

Removes Copycord from this machine. The Copycord home (data, `src/`, `node/`) stays unless you add `--purge`; without it the command prints where the home is.

```bash
copycord uninstall --dry-run          # show what would be removed, change nothing
copycord uninstall                    # remove the service and the program, keep the data
copycord uninstall --purge            # also delete the Copycord home, src and node
copycord uninstall --yes --keep-service
```

It runs these steps in order:

1. Stops and removes the [service](#copycord-service), the same as `copycord service uninstall`. On Linux it checks both the user and the system service; removing the system service needs root. A service that is not installed is skipped.
2. With `--purge`, deletes the [Copycord home](#data-dir) (database, logs, backups, `src/`, `node/`) and the saved `~/.copycord-home`. It refuses when that path is `/`, your home directory or empty.
3. Removes the program with `uv tool uninstall copycord`, or `pipx uninstall copycord` when `uv` is not on your `PATH`, and deletes the saved `update.json`. When neither tool is found it prints the command to run yourself.
4. Prints a summary of what was removed and kept, and how to reinstall.

| Option | What it does |
|--------|--------------|
| `--purge` | Also deletes the Copycord home. Without `--yes` you must type `DELETE` to confirm. |
| `--yes`, `-y` | Skips the confirmation prompts |
| `--keep-service` | Leaves the OS service installed |
| `--dry-run` | Prints the steps and changes nothing |
| `--data-dir PATH` | The data directory to keep or delete |

Without a terminal (in a script or a pipe) it never asks: it exits with status 1 unless you add `--yes`. It also exits with status 1 when a step fails.

To install Copycord again:

```bash
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
```

## copycord update {#copycord-update}

Updates a Copycord installed with the install script (or with `pipx`).

```bash
copycord update                        # newest release on the saved channel
copycord update status                 # only show what is installed and what is available
copycord update --channel beta         # switch to the beta channel and update
copycord update --channel stable       # switch back to stable
copycord update --tag 4.0.0            # install exactly this release, once
copycord update --channel dev          # git install of Copycord/Copycord at main
copycord update --channel dev --ref v4-dev
copycord update --channel dev --repo you/Copycord --ref 1a2b3c4
copycord update --channel beta --no-install
```

| Option | What it does |
|--------|--------------|
| `status` | Prints the installed version, the channel, the install method and the newest version on that channel, then exits without installing. Network errors are printed as `WARN`. |
| `--channel stable\|beta\|dev` | Switches to this channel, installs from it and saves it once the install worked |
| `--ref REF` | Dev channel only: the git branch, tag or commit to install (default `main`). |
| `--repo OWNER/NAME` | Dev channel only: the GitHub repository to install from (default `Copycord/Copycord`), for example a fork |
| `--tag VERSION` | Installs this exact release (`4.0.0` or `v4.0.0`). One-off: the saved channel does not change. Cannot be combined with `--channel`, `--repo` or `--ref`. |
| `--no-install` | Only saves `--channel` (and `--repo`, `--ref`), installs nothing. The install scripts use this. |
| `--data-dir PATH` | Reads and saves the channel in this data directory |

After a successful update, a running [service](#copycord-service) is restarted so it uses the new version.

`status` also shows when Copycord last checked for an update and whether auto-update is on.

### Update check on start {#update-check}

When `copycord run` starts, and when `copycord --version` runs in a terminal, Copycord checks the saved [channel](#update-channels) in a background thread: the newest release for `stable` and `beta`, the branch head against the installed commit for `dev`. If there is something newer it prints one line and nothing else happens:

```
Update available: v4.0.1 (you have v4.0.0). Run copycord update
```

The check never delays or fails startup (it times out after a few seconds and ignores network errors), and its answer is cached for 24 hours in `update-check.json` in the [data directory](#data-dir). It never installs anything.

To turn it off, set the app config `UPDATE_CHECK_ON_START` to `false` (`copycord config set UPDATE_CHECK_ON_START false`) or set `COPYCORD_NO_UPDATE_CHECK=1` in the environment.

### Auto-update {#auto-update}

Off by default. With the app config `AUTO_UPDATE` set to `true` (`copycord config set AUTO_UPDATE true`), a Copycord running as a background [service](#copycord-service) checks every 6 hours and, when an update exists, runs the same steps as `copycord update` and restarts the service. Every check and every update is written to the log. A `copycord run` in a terminal never updates itself; it only prints the hint above. Docker and git checkouts are not updated this way.

### Update channels {#update-channels}

| Channel | Installs |
|---------|----------|
| `stable` (default) | The newest [GitHub release](https://github.com/Copycord/Copycord/releases) that is not a pre-release |
| `beta` | The newest GitHub release, pre-releases included (for example `v4.0.0b1`) |
| `dev` | A git ref of a GitHub repository, by default `main` of `Copycord/Copycord`. It runs `uv tool install --force git+https://github.com/<repo>@<ref>`, which builds the dashboard from source, so it needs Node.js (`npm`) on your `PATH`. Without `npm` the update stops with a clear error before installing anything. |

Stable and beta install the wheel attached to the release. Once you switch, plain `copycord update` keeps using the saved channel, repository and ref; `--ref` or `--repo` alone changes them while you are on dev.

The channel is saved in `update.json` in the [data directory](#data-dir). Installing with `COPYCORD_CHANNEL=beta` saves `beta` the same way. The file looks like this:

```json
{"channel": "dev", "branch": "v4-dev", "repo": "you/Copycord"}
```

`branch` holds the ref. `repo` is written only when it is not `Copycord/Copycord`, so older Copycord versions still read the file; a file without `repo` means `Copycord/Copycord`.

### Other installs

`copycord update` reinstalls Copycord with the tool that installed it: `uv tool install --force` for the install script, `pipx install --force` for pipx. For other installs it prints what to do instead:

- **Docker:** pull the new image and recreate the container (`docker compose pull && docker compose up -d`). See [Docker Install](/docs/getting-started/docker-install#update-copycord).
- **A git checkout:** update it with `git pull`, or switch to the install script.
- **pip or uv pip in your own environment:** it prints the matching `pip install --upgrade` command for you to run.
