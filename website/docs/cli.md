---
sidebar_position: 7
title: CLI Reference
---

# CLI Reference

The [installer](/docs/getting-started/quick-start) installs the `copycord` command. It runs Copycord, checks your setup, reads and writes settings, manages the background service and updates Copycord.

```
copycord run [--data-dir PATH]
copycord restart
copycord doctor [--verbose] [--fix] [--brief] [--json] [--data-dir PATH]
copycord config get KEY [--show-secrets] [--json] [--data-dir PATH]
copycord config set KEY VALUE [--data-dir PATH]
copycord config unset KEY [--data-dir PATH]
copycord config export [FILE] [--show-secrets] [--json] [--data-dir PATH]
copycord config import FILE [--data-dir PATH]
copycord config init [--data-dir PATH]
copycord service install [--user | --system] [--data-dir PATH]
copycord service status [--user | --system] [--json]
copycord service start|stop|restart [--user | --system]
copycord service uninstall [--user | --system] [--yes]
copycord backup [--json] [--data-dir PATH]
copycord restore [ARCHIVE] [--yes] [--data-dir PATH]
copycord update [--tag VERSION] [--no-backup] [--yes] [--force] [--data-dir PATH]
copycord update [--channel stable|beta|dev] [--repo OWNER/NAME] [--ref REF] [--no-install]
copycord update status [--json]
copycord uninstall [--purge|--keep-data] [--yes] [--keep-service] [--dry-run] [--data-dir PATH]
copycord help [advanced]
copycord --version | -V [--json]
copycord --no-color <command>
copycord --verbose <command>
```

`copycord` on its own, or `copycord --help`, lists the commands by group with examples. Every command has `--help`, for example `copycord service install --help`; a command group on its own, such as `copycord config`, prints its help. An unknown command suggests the closest one and exits with status 2.

## Output and colors {#output}

Every status line starts with one symbol: `✓` success, `!` warning, `✗` failure, `•` info and `?` question. Where the terminal cannot print them, they are replaced with `+ ! x -`.

Colors are off when the output is not a terminal (a pipe, a file or a service log), when `NO_COLOR` is set, when `TERM=dumb` or with `--no-color` before or after the command. `FORCE_COLOR=1` turns them on. In terminals that support it, web addresses and the folders Copycord prints can be clicked; links are off without colors, inside tmux or screen, over SSH, inside Docker and with `--json`.

### JSON output {#json}

`--json` makes a reporting command print exactly one JSON document on stdout, for scripts and monitoring. It works with `copycord --version`, `copycord doctor` (also with `--fix`), `copycord backup`, `copycord update status`, `copycord service status`, `copycord config get` and `copycord config export`, and can go before or after the command. With `--json` there is no banner, no color, no spinner and no question; any other message goes to stderr. The exit status is the same as without it.

```bash
$ copycord doctor --json
{
  "ok": true,
  "checks": [
    {
      "section": "Install",
      "status": "ok",
      "text": "Python 3.12.3",
      "hint": "",
      "path": null,
      "fixable": false,
      "fixed": false
    },
    ...
  ],
  "summary": {"ok": 11, "warn": 0, "fail": 0, "info": 2, "fixed": 0, "text": "All good"},
  "fixes": []
}
```

- `doctor`: one entry per check with its section, `status` (`ok`, `warn`, `fail` or `info`), text, hint and path, plus the counts. With `--fix`, a check a fix cleared has `"fixed": true` and `fixes` lists what was done; a fix that needs a question (moving the dashboard port) is left for a terminal.
- `update status`: the installed version and commit, the channel, how Copycord was installed, the latest version on the channel and `update_available`. When GitHub cannot be reached, `error` says why.
- `config get`: `key`, `value` (masked for a secret unless `--show-secrets`), `set`, `masked` and `source` (`env` for `.env`, else `database`). A key that is not set prints `"set": false` with an `error` and exits 1.
- `config export`: the same object as without `--json`; with a file, `{"file": ..., "keys": N}`.
- `service status`: `installed`, `running`, `type` (for example `service for your account, starts at boot`), `file` (the unit, plist or `null` for the Windows task), `started` (Linux only, else `null`) and `logs` (the command or folder with its output). Exits with 0 when running, 3 when not.
- `backup`: `ok`, `file` (the path of the new archive), `size` in bytes and `includes_key`. When no backup was made, `"ok": false` with an `error`, and the exit status is 1.
- `--version`: `version`, `commit`, `channel`, and `ref` and `repo` on the dev channel.

### Unexpected errors {#crash}

When a command hits an unexpected error, it prints three lines on stderr and exits with status 1:

```
Copycord hit an unexpected error: OSError: [Errno 28] No space left on device
The details were saved to /home/you/.copycord/logs/copycord-crash-20261013-141502-4242.log
Report it at https://github.com/Copycord/Copycord/issues
```

The log file holds the full error, the Copycord and Python versions and the command without its values, which can be secrets. It goes in the `logs` folder of the Copycord home, or in the temp folder when that is not writable. To also print the full error, add `--verbose`.

## The data directory {#data-dir}

Copycord keeps its database (`data.db`), `secret.key`, backups, exports and the saved update state in the data directory:

| Where | Data directory |
|-------|----------------|
| Linux, macOS | `~/.copycord/data` |
| Windows | `%USERPROFILE%\.copycord\data` |
| Docker | `/data` (your `./data` volume) |

The data directory is the `data` folder in the [Copycord home](/docs/getting-started/manual-install#home). The home also holds `.env` and a `logs` folder with `server.out`, `client.out`, `scraper.out` and `update.log`. In Docker, and with any other `DATA_DIR`, all of these stay in that one folder.

`--data-dir PATH` uses another data directory for one command, the same as setting `DATA_DIR`. It wins over `COPYCORD_HOME`. `run`, `doctor`, every `config` command, `service install`, `backup`, `restore`, `update` and `uninstall` accept it.

| Older layout | What Copycord does |
|--------------|--------------------|
| `data.db` or `secret.key` directly in the home, no `data/data.db` | On start, backs up the database to `backups/data-pre-update-layout-<timestamp>.db`, then moves the data files into `data` and the `*.out` files into `logs`. It never overwrites or deletes a file; if a move fails, it puts everything back. `copycord doctor` shows `data files loose in the home` until then, and `copycord doctor --fix` does the move while Copycord is stopped. Docker data is never moved. |
| No `~/.copycord`, but a `data.db` in `~/.local/share/copycord` (Linux), `~/Library/Application Support/Copycord` (macOS) or `%LOCALAPPDATA%\Copycord` (Windows) | Keeps using that folder and prints a notice. To move it, stop Copycord and move the folder to `~/.copycord`. |

## copycord run {#copycord-run}

Starts Copycord: the dashboard, the server bot and the client.

```bash
copycord run
copycord run --data-dir ~/copycord-data
```

The dashboard is at `http://localhost:8080`. **Ctrl+C** stops Copycord. If one part crashes, Copycord restarts that part.

Environment variables (with the installer, set them in [`<home>/.env`](#env-file)):

| Variable | Default | What it does |
|----------|---------|--------------|
| `PASSWORD` | a random password (set by the installer) | Dashboard password; always required |
| `ADMIN_PORT` | `8080` | Dashboard port |
| `DASHBOARD_HOST` | `127.0.0.1` | Where the dashboard listens. The default is this computer only; `0.0.0.0` opens it to your network (see [Dashboard address](#dashboard-address)) |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG`; also an app config key, which wins once it is saved |
| `DATA_DIR` | see above | Same as `--data-dir` |

`copycord run` does not start while the v3 `ROLE` variable is set. See [Upgrading from v3](/docs/getting-started/upgrading-from-v3).

The terminal shows the dashboard's log and the warnings and errors from the server and client. See [Where logs go](/docs/dashboard/system#where-logs-go).

When the dashboard port or one of the ports `9101` and `9102` is in use at start, Copycord waits up to 15 seconds for it. If it is still in use, Copycord prints `✗ Port 9101 is in use, so Copycord cannot start` with what to do and exits with status 1.

### Dashboard address {#dashboard-address}

| `DASHBOARD_HOST` | Who can open the dashboard |
|------------------|----------------------------|
| `127.0.0.1` (default) | Only this computer, at `http://localhost:8080` |
| `0.0.0.0` (Docker default) | Every network |
| An IP address of this computer | Devices that can reach that address |

With any address other than `127.0.0.1`, `copycord run` does not start while `PASSWORD` is `copycord`, and `copycord doctor` reports a failure. `COPYCORD_ALLOW_INSECURE_DASHBOARD=1` skips this check, for when something else, such as a reverse proxy with its own login, protects the dashboard. See [Security defaults](/docs/configuration/security).

## copycord --version {#copycord-version}

`copycord --version` (or `-V`) prints the installed version and the saved [update channel](#update-channels). In a terminal that is one line, in the style of the line at the top of the other commands:

```bash
$ copycord --version
Copycord v4.0.0 · stable channel
```

In a pipe or a script it prints two plain lines:

```bash
$ copycord --version | cat
4.0.0
stable channel
```

A dev install shows the commit and the ref, for example `4.0.0 (1a2b3c4)` and `dev channel · v4-dev`.

## copycord doctor {#copycord-doctor}

Checks that this machine can run Copycord and that it is set up, in five groups. A line only gets details when something is wrong; paths are shown for warnings and failures, or for every line with `--verbose`.

```bash
copycord doctor
copycord doctor --verbose
copycord doctor --fix          # fix what it can, then show the report
copycord doctor --fix --brief  # the same, printing only the result
copycord doctor --data-dir ~/copycord/data
```

| Group | Checks |
|-------|--------|
| Install | The version, commit and update channel; Python is 3.12 to 3.14; the web UI files are installed; the required packages (`curl_cffi`, `py-cord`, `fastapi`, `uvicorn`) import, as one `dependencies OK` line that names only the missing ones |
| Settings | `<home>/.env` exists; a dashboard password is set (`no dashboard password` is only info, since `copycord run` makes one) and is not still the default `copycord`; a `FAIL` when the dashboard listens beyond this computer without a real password; a `FAIL` when the removed `ROLE` variable is still set |
| Data | The Copycord home exists and is writable; the data files are in the `data` folder, not loose in the home; `data.db` opens and its schema is one this Copycord supports (a fresh install says it is created on first run); the last backup in the backups folder is under 2 days old and did not fail its check |
| Running | `ADMIN_PORT` (default 8080) is free, or in use by Copycord itself; whether the OS service is installed and running, and on Linux whether the user service starts at boot |
| Updates | The update check, cached for 24 hours. A network error is only reported, never a failure |

The report ends with a **Needs your attention** section that lists each warning and problem with what to do. A warning that `--fix` can repair says so. The command exits with status 1 when any check fails and 0 otherwise, warnings included.

`! port 8080 in use` means another program has the port: set `ADMIN_PORT` to a free one, or let `copycord doctor --fix` pick the next free port. When Copycord itself is listening, the line says `port 8080 in use by Copycord` instead.

### Fixing problems {#doctor-fix}

`copycord doctor --fix` applies the fixes, then shows the report as things are afterwards. Automatic fixes run without asking. The guided fix asks first, and only in a terminal; in a script it is skipped and listed under **Needs your attention**. `--brief` shows only the fixes applied and anything still failing.

| Check | Fix | Kind |
|-------|-----|------|
| `.env not found` | Writes `<home>/.env` with every setting at its default, the same file the installer writes | Automatic |
| `home folder missing` | Creates the Copycord home | Automatic |
| `data files loose in the home` | Moves the data files into `data` and the logs into `logs` (see [The data directory](#data-dir)); refuses while the service is running | Automatic |
| `last backup ... ago`, `no backups yet`, `last backup failed its check` | Backs up the database now, the same `.tar.gz` the daily backup makes | Automatic |
| `service installed, not running` | Starts the service | Automatic |
| `service starts only when you log in` (Linux) | Turns on lingering for your account so the user service starts at boot; when that needs root, prints the `loginctl` command to run | Automatic |
| `service runs in a console window` (Windows) | Writes the service task again so it runs without a window, with the same data directory, and restarts it | Automatic |
| `web UI missing`, `missing dependencies` | Reinstalls the version you have from your update channel (a dev install rebuilds the same ref), through `copycord update`, which backs up the database first | Automatic |
| `port 8080 in use` | Asks `y/N` to move to the next free port and writes it as `ADMIN_PORT` in `.env` | Guided |

Some things are never changed for you; they stay in Needs your attention with what to do:

- **The dashboard password:** `--fix` never changes it or asks for one. While it is still the default `copycord`, change `PASSWORD` in `<home>/.env`, then run `copycord restart`.
- **A database from a newer Copycord:** run `copycord update` to go back to a newer version, or restore a backup.
- **`ROLE` is set:** remove it from your compose file.

The installers run `copycord doctor --fix --brief` at the end of an upgrade or a reinstall (not on a first install), so the automatic repairs happen by themselves.

A recreated `.env` gets a new random `PASSWORD`. To see it, run `copycord config get PASSWORD --show-secrets`.


## copycord config {#copycord-config}

Reads and writes Copycord's settings. Most live in the database (the same values the dashboard's Global Configuration card shows, such as `COMMAND_USERS`, `COPYCORD_AUTOSTART`, `LOG_LEVEL` or `LOG_MAX_SIZE_MB`). The few that are read from the environment only, such as `PASSWORD` and `ADMIN_PORT`, live in [`<home>/.env`](#env-file); `get`, `set` and `unset` pick the right place by the key.

```bash
copycord config get LOG_LEVEL
copycord config set LOG_LEVEL DEBUG
copycord config unset COMMAND_USERS
copycord config set PASSWORD 'new-pass'   # goes to <home>/.env
copycord config export                    # print all values, secrets masked
copycord config export --show-secrets     # print all values, secrets shown
copycord config export settings.json      # write all values to a file
copycord config import settings.json
```

| Command | What it does |
|---------|--------------|
| `get KEY` | Prints the value. Values of keys containing `TOKEN`, `PASSWORD` or `SECRET` are shown as `********` unless you add `--show-secrets`. Exits with status 1 when the key is not set. |
| `set KEY VALUE` | Stores the value: an env-only key in `<home>/.env`, any other key in the database (created if needed). |
| `unset KEY` | Removes the value from the same place. Exits with status 1 when the key is not set. |
| `export` | Prints every value as JSON. Values of keys containing `TOKEN`, `PASSWORD` or `SECRET` are shown as `********` unless you add `--show-secrets`. |
| `export FILE` | Writes every value, unmasked, to `FILE` (readable only by you on Linux/macOS). |
| `import FILE` | Stores every value from a JSON object of key/value pairs (strings or numbers). Masked `********` values are skipped. |
| `init` | Writes the default [`<home>/.env`](#env-file) when it is missing and prints its path; an existing file is kept as it is. The installer runs it on first install. |

`get` and `export` need an existing database for database keys; when there is none they say so (check `--data-dir`). Restart Copycord after `set`, `unset` or `import` so it picks up the new values. On a service install that is `copycord service stop`, then `copycord service start`; the command prints this reminder.

To move your settings to another machine, run `copycord config export settings.json` on the old one and `copycord config import settings.json` on the new one. Treat the file like a password: it contains your tokens. `export` and `import` cover the database only; copy `<home>/.env` yourself.

### Settings that live in .env {#env-file}

Some settings are read from the environment only, before the database is open. In Docker you set them in the compose file. On a native install they live in `.env` in the Copycord home:

| OS | File |
|----|------|
| Linux, macOS | `~/.copycord/.env` |
| Windows | `%USERPROFILE%\.copycord\.env` |

With `COPYCORD_HOME` or `--data-dir` it is `.env` in that folder. `copycord` reads the file on every start, for `copycord run` and the service alike, so a restart applies your changes. Variables already set in the real environment win over the file.

`copycord config init`, which the installer runs on the first install, writes the file with every key at its default and a random password, and never overwrites an existing file. On Linux and macOS only your user can read it. An empty value, such as `TZ=`, means the default. For the default file and how to change a setting, see [Configuration File (.env)](/docs/getting-started/configuration).

| Key | Default | What it does |
|-----|---------|--------------|
| `PASSWORD` | a random password in the file | Dashboard password; always required. |
| `SECRET_KEY` | a key saved in `<data dir>/secret.key` | Signs the dashboard login cookie. Not in the file; add it only to use a key of your own. |
| `ADMIN_PORT` | `8080` | Dashboard port. |
| `DASHBOARD_HOST` | `127.0.0.1` | Where the dashboard listens. The file sets `127.0.0.1`, so only this computer can open it. See [Dashboard address](#dashboard-address). |
| `TZ` | `UTC` | Time zone for scheduled backups and export timestamps, e.g. `America/New_York`. |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG` for the first log lines; an app config key, so the dashboard's value wins once it is saved and `copycord config set LOG_LEVEL` writes the database. |
| `LOG_FORMAT` | `HUMAN` | `HUMAN` or `JSON` log lines. |
| `BACKUP_DIR` | `<data dir>/backups` | Where the daily database backups go. |
| `BACKUP_AT` | `03:17` | Time of the daily backup, HH:MM. |
| `BACKUP_RETAIN` | `14` | How many daily backups to keep. |
| `COPYCORD_NO_UPDATE_CHECK` | `0` | `1` turns off the update check on start; `0` keeps it on. |
| `BACKFILL_*`, `FORWARDING_*` | | Tuning knobs for backfills and forwarding; leave them unset unless asked to change them. |

`copycord config set` writes these keys to the file (creating it, readable only by you on Linux and macOS, if needed), updates a key's line in place (or a line you commented out) and keeps your comments. `copycord config get PASSWORD` prints `********`; add `--show-secrets` to see it. After `copycord config unset PASSWORD`, the next start makes a new random password.

## copycord restart {#copycord-restart}

Restarts Copycord so it applies changed settings. `<home>/.env` is read on every start.

```bash
copycord restart
```

| Copycord is running | What `copycord restart` does |
|---------------------|------------------------------|
| As a [service](#copycord-service) | Restarts the service and prints `✓ Copycord restarted`. |
| In a terminal (`copycord run`) | Prints that Copycord is not running as a service and exits with 1. Stop it with **Ctrl+C** and run `copycord run` again. |
| Not at all, service installed | Prints the service state and exits with 1; start it with `copycord service start`. |

Restarting the Linux system service (`--system`) needs root: `sudo "$(command -v copycord)" restart`. The default service for your own account needs no sudo.

On Windows, `copycord restart` and `copycord service stop` end every process the service started, then wait up to 20 seconds until it has stopped and, for a restart, until its ports are free, before starting it again. If that does not happen in time, the service stays stopped and the command says why, for example `Copycord did not stop within 20 seconds: port 9101 is still in use`, and exits with 1.

## copycord service {#copycord-service}

Runs Copycord in the background with your operating system's service manager, so it keeps running without an open terminal and starts again automatically.

```bash
copycord service install      # install, enable and start
copycord service status
copycord service stop
copycord service start
copycord service restart
copycord service uninstall    # stop and remove; your data is kept
```

`copycord service` on its own lists these commands.

| Command | What it does |
|---------|--------------|
| `install [--user \| --system] [--data-dir PATH]` | Registers `copycord run --data-dir <data dir>` with the service manager, enables it and starts it |
| `status [--json]` | Shows whether the service is installed and running, what kind of service it is, its file, when it started (Linux) and where its output goes. Exits with 0 when running, 3 when not. |
| `start` | Starts the service |
| `stop` | Stops the service |
| `restart` | Restarts the service, the same as [`copycord restart`](#copycord-restart) |
| `uninstall [--yes]` | Asks, then stops and removes the service. Your data is kept. Without a terminal it needs `--yes`. To remove Copycord itself too, use [`copycord uninstall`](#copycord-uninstall). |

When no service is installed, `start`, `stop` and `restart` say so and exit with status 1. When the service manager reports an error, the command shows the reason and exits with status 1.

The data directory is fixed when you install the service: the one given with `--data-dir` (or `DATA_DIR`), else the default for your user. To change it, run `copycord service install` again with the new `--data-dir`.

### How it works on each OS

| OS | Service | Starts | Restarts |
|----|---------|--------|----------|
| Linux (default) | systemd user unit `~/.config/systemd/user/copycord.service` | At boot (with lingering, see below) | 10 seconds after a crash |
| Linux (`--system`, or run as root) | systemd system unit `/etc/systemd/system/copycord.service` | At boot | 10 seconds after a crash |
| macOS | launchd agent `~/Library/LaunchAgents/com.copycord.plist` | When you log in | After a crash |
| Windows | Scheduled Task `Copycord` | When you log in | Every minute after a failure |

**Linux service for your account (default).** `copycord service install` needs no sudo. It installs a systemd user unit that runs as you, with your Copycord home. A user unit normally runs only while you are logged in, so the install also turns on lingering for your account, which starts Copycord at boot and keeps it running after you log out. Most systems allow this without sudo. If yours does not, the install says so; run this once:

```bash
sudo loginctl enable-linger "$USER"
```

`copycord doctor --fix` also tries to turn lingering on.

**Linux system service.** `copycord service install --system` installs `/etc/systemd/system/copycord.service` instead. It needs root, and so do `start`, `stop`, `restart` and `uninstall` for it. Because the install script puts `copycord` in your user's tool directory, call it by its full path:

```bash
sudo "$(command -v copycord)" service install --system
```

When installed with `sudo`, the system service runs as your user (not root) and uses your user's data directory. Run as root, `copycord service install` installs the system service without `--system`.

`start`, `stop`, `status` and `uninstall` use whichever service is installed. If both a user service and a system service are installed, they use the one for your account (the system service when run as root) and say which; add `--user` or `--system` to choose. On a system without systemd (for example WSL without systemd, or a container), `copycord service install` stops with the error from `systemctl`; run `copycord run` instead.

**Linux system service and backups.** The `--system` service can only write to the Copycord home, the data directory, `~/.local` and `~/.cache`. Keep `BACKUP_DIR` inside the home, or add a `ReadWritePaths=` line with `systemctl edit copycord`.

**macOS.** Logs are in the [logs folder](#data-dir). `--user` has no effect.

**Windows.** The task runs as you when you log in, without a window. Its output goes to `copycord.out` in the [logs folder](#data-dir). If Copycord runs in a console window instead, run `copycord doctor --fix` to recreate the task. `--user` has no effect.

### Environment variables for the service

The service does not see variables you set in your terminal. The simplest way to give it `PASSWORD`, `ADMIN_PORT` or the other env-only settings is [`<home>/.env`](#env-file): `copycord config set PASSWORD 'new-pass'`, then `copycord restart`. You can also set real environment variables, which win over the file:

- **Linux:** run `systemctl --user edit copycord` (or `sudo systemctl edit copycord` for the system service), add the lines below, save, then run `copycord restart`.

  ```ini
  [Service]
  Environment=PASSWORD=changeme
  Environment=ADMIN_PORT=9060
  ```

- **macOS:** add the variables to the `EnvironmentVariables` dictionary in `~/Library/LaunchAgents/com.copycord.plist`, then run `copycord service stop` and `copycord service start`. Running `copycord service install` again rewrites this file.
- **Windows:** set them as user environment variables, for example `setx PASSWORD changeme`, then sign out and back in so the task starts with them.

## copycord backup {#copycord-backup}

Backs up the database now, the same archive the [daily backup](/docs/dashboard/backups) makes, and shows where it saved it:

```bash
copycord backup
```

The archive holds `data.db`, `secret.key` (when there is one) and `manifest.json`, and goes into the backups folder (`BACKUP_DIR`, by default `backups` in the [data directory](#data-dir)). Each backup is checked for integrity before it is saved; if the check fails, nothing is kept and the command exits with status 1. It works while Copycord is running. `--json` prints the result as JSON.

In Docker, run it inside the container:

```bash
docker compose exec copycord /opt/venvs/copycord/bin/copycord backup
```

## copycord restore {#copycord-restore}

Replaces the database with a backup. Copycord must be stopped first: stop the service with `copycord service stop`, or press Ctrl+C where `copycord run` is running. Without a file it lists the 10 newest backups, including the copies made before an update or a restore, and you pick one.

Give a file to restore that one: `copycord restore ~/.copycord/data/backups/2026-10-13_03-17-00.tar.gz`. It accepts a `.tar.gz` archive (older archives that hold only `data.db` work too) or a `.db` copy, such as the ones `copycord update` makes.

Before it changes anything, `copycord restore`:

1. Refuses while Copycord is running (the service, the dashboard answering on `ADMIN_PORT`, or on Windows a Copycord in another window).
2. Checks the backup with SQLite's integrity check, and refuses a backup that fails it.
3. Refuses a backup from a newer Copycord (a higher database schema than this version supports); update Copycord first, then restore it.
4. Shows when the backup was made and what it includes, and asks `Replace the database in <data dir> with this backup? (y/N)`.

Then it backs up the current database and `secret.key` to `pre-restore-<UTC time>.tar.gz` in the backups folder, so the restore can be undone; these copies are never removed by the daily cleanup. It moves the restored `data.db` into place in one step and, when the backup has a `secret.key` that differs from the current one, restores it and keeps the current key as `secret.key.bak-<UTC time>`. A backup without a key leaves the current key alone. When it finishes, it shows the command to undo the restore.

`--yes` (`-y`) skips the question. Without a terminal (a script) it needs `--yes` and the file to restore, and exits with status 1 otherwise.

In Docker, stop the container, run the restore in a one-off container on the same `/data` volume, then start it again:

```bash
docker compose stop copycord
docker compose run --rm --entrypoint /opt/venvs/copycord/bin/copycord copycord restore
docker compose up -d
```

## copycord help advanced {#copycord-help}

```
copycord help
copycord help advanced
```

`copycord help` shows the same overview as `copycord --help`. `copycord help advanced` prints one page with every advanced option, grouped by command, and the environment variables that change how Copycord behaves. `copycord --help` ends with a line that points to it. The page is plain text, also with `--json`, and has no colors on a pipe.

It lists:

- **Update:** `--channel`, `--ref`, `--repo`, `--no-install` and `--force`, the same options `copycord update --help` shows under **Advanced**. See [channels and git installs](#update-advanced).
- **Service:** `--system` and `--user` (Linux).
- **Doctor:** `--fix` and `--brief`.
- **Config:** `--show-secrets` on `config get` and `config export`.
- **Run:** `--data-dir`, which most commands accept.
- **Backup and restore:** `backup --json` and `restore --yes`.
- **Uninstall:** `--purge`, `--keep-data`, `--keep-service` and `--dry-run`.
- **Environment:** `COPYCORD_HOME`, `COPYCORD_ALLOW_ROOT`, `COPYCORD_TASK_NAME`, `COPYCORD_SKIP_FRONTEND`, `COPYCORD_ALLOW_INSECURE_DASHBOARD`, `COPYCORD_NO_UPDATE_CHECK`, `COPYCORD_NODE_VERSION`, `COPYCORD_PLAIN_PROMPTS`, `DASHBOARD_HOST`, `TRUSTED_PROXIES`, `AUTO_UPDATE_ALL_CHANNELS` and `COPYCORD_ENV`.

## copycord uninstall {#copycord-uninstall}

Removes the Copycord program and its background service. It asks whether to keep or delete your data in the [Copycord home](#data-dir). Pressing **Enter** keeps it; **Esc** or **Ctrl+C** cancels.

Where arrow keys do not work (for example Git Bash without a console), Copycord asks you to type a number or `y`/`n` instead. `COPYCORD_PLAIN_PROMPTS=1` forces this.

```bash
copycord uninstall --dry-run          # show what would be removed, change nothing
copycord uninstall                    # remove Copycord; asks whether to delete your data
copycord uninstall --yes              # no prompts, keep the data
copycord uninstall --yes --purge      # no prompts, delete the data too
copycord uninstall --yes --keep-service
```

It runs these steps in order:

1. Stops and removes the [service](#copycord-service), the same as `copycord service uninstall`. On Linux it checks both the user and the system service; removing the system service needs root. A service that is not installed is skipped.
2. If you chose to delete everything, or gave `--purge`, deletes the [Copycord home](#data-dir) (database, logs, backups, settings, `src/`, `node/`) and the saved `~/.copycord-home`. It refuses when that path is `/`, your home directory or empty.
3. Removes the program with `uv tool uninstall copycord`, or `pipx uninstall copycord` when `uv` is not on your `PATH`, and deletes the saved `update.json`. When neither tool is found it prints the command to run yourself. On Windows it stops if another Copycord of this install is running, and the program's locked files are moved to a temporary folder that is deleted a few seconds after the command exits.
4. Prints `Copycord uninstalled`, or `Uninstall incomplete` when a step failed.

| Option | What it does |
|--------|--------------|
| `--purge` | Deletes the Copycord home. Without `--yes` the question only offers delete or cancel, and Enter cancels. |
| `--keep-data` | Keeps the Copycord home. Without `--yes` the question only offers keep or cancel. |
| `--yes`, `-y` | Skips the question |
| `--keep-service` | Leaves the OS service installed |
| `--dry-run` | Prints the steps and changes nothing |
| `--data-dir PATH` | The data directory to keep or delete |

Without a terminal (in a script or a pipe) it never asks: it exits with status 1 unless you add `--yes`. With `--yes` and neither `--purge` nor `--keep-data`, the data is kept. It also exits with status 1 when a step fails.

## copycord update {#copycord-update}

Updates a Copycord installed with the install script (or with `pipx`) to the latest version.

```bash
copycord update                        # update to the latest version
copycord update status                 # only show what is installed and what is available
copycord update --tag 4.0.0            # install exactly this release, once
```

| Option | What it does |
|--------|--------------|
| `status` | Prints the installed version, the channel, the install method and the newest version on that channel, then exits without installing. Network errors are printed as `WARN`. |
| `--tag VERSION` | Installs this exact release (`4.0.0` or `v4.0.0`). One-off: the saved channel does not change. Cannot be combined with the [advanced](#update-advanced) `--channel`, `--repo` or `--ref`. |
| `--no-backup` | Skips the [database backup](#downgrade) made before the install |
| `--yes`, `-y` | Updates without [asking first](#update-confirm), and installs an [older version](#downgrade) without asking |
| `--data-dir PATH` | Reads and saves the channel in this data directory |

After a successful update, a running [service](#copycord-service) is restarted so it uses the new version.

The full install output is saved to `logs/update.log` in the [Copycord home](#data-dir). If the install fails, the end of that log is shown.

When the install is already the newest on the saved channel (the same release version on `stable` or `beta`, the same commit as the branch head on `dev`), `copycord update` says it is up to date and changes nothing. If GitHub cannot be reached for that check, it warns and installs anyway. A channel switch, `--tag`, `--force` and `copycord doctor --fix` always install.

On Windows, `copycord update` stops a running service and moves the program files in use into `.copycord-old` in the Copycord home before it installs; the next update deletes that folder. If Copycord is running in another window, for example with `copycord run`, stop it first; the update changes nothing until you do.

`status` also shows when Copycord last checked for an update and whether auto-update is on.

### Confirming the update {#update-confirm}

When there is something to install, `copycord update` shows the installed and the new version and asks before it backs up or installs anything. A channel switch (`--channel`, `--ref`, `--repo`) asks too. Answering no changes nothing. A reinstall of the same version and a downgrade (see below) ask their own question.

It does not ask with `--yes` (`-y`), without a terminal (scripts and services), during [auto-update](#auto-update) or when `copycord doctor --fix` reinstalls Copycord.

### Downgrading and backups {#downgrade}

Before it installs anything, every `copycord update` (any channel, `--tag`, dev refs, upgrades and downgrades alike) copies the database with SQLite's online backup to `data-pre-update-<version>-<UTC timestamp>.db` in the backups directory (`BACKUP_DIR`, by default `backups` in the [data directory](#data-dir)), for example `data-pre-update-4.0.0-20261012T091500Z.db`. It prints the path. The newest 5 are kept; the daily backups have their own retention. With no database yet it says so and goes on. If the backup fails, the update stops and nothing is installed; `--no-backup` skips the backup.

When the version to install is older than the installed one (a `--tag` below it, or `--channel stable` or `beta` whose newest release is older), `copycord update` first asks `Downgrade from vX to vY? Your data is backed up first; a newer database schema will block the older version until you restore the backup. (y/N)`; `--yes` (`-y`) skips the question, and without a terminal it exits with status 1 unless you add `--yes`. Dev refs have no comparable version and are never treated as a downgrade.

Copycord refuses to start on a database written by a newer version (a higher `schema_version`), so an older version never changes it. `copycord run` prints:

```
This database was created by a newer Copycord (schema 5, this version supports 4).
Run copycord update to go back to a newer version, or restore a backup from <backups dir>.
```

and exits with status 1, naming the newest pre-update backup when there is one. `copycord doctor` reports it as `FAIL`. To go back to an older version for good, stop Copycord and restore the matching pre-update copy with [`copycord restore`](#copycord-restore), for example `copycord restore ~/.copycord/data/backups/data-pre-update-4.0.0-20261012T091500Z.db`.

### Update check on start {#update-check}

When `copycord run` starts, and when `copycord --version` runs in a terminal, Copycord checks the saved [channel](#update-channels): the newest release for `stable` and `beta`, the branch head against the installed commit for `dev`. If there is something newer it prints one line and nothing else happens:

```
Update available: v4.0.1 (you have v4.0.0). Run copycord update
```

The check never delays or fails startup (it times out after a few seconds and ignores network errors), and its answer is cached for 24 hours in `update-check.json` in the [data directory](#data-dir). It never installs anything.

To turn it off, set the app config `UPDATE_CHECK_ON_START` to `false` (`copycord config set UPDATE_CHECK_ON_START false`) or set `COPYCORD_NO_UPDATE_CHECK=1` in the environment.

### Auto-update {#auto-update}

Off by default. With the app config `AUTO_UPDATE` set to `true` (`copycord config set AUTO_UPDATE true`), a Copycord running as a background [service](#copycord-service) checks every 6 hours and, when an update exists, runs the same steps as `copycord update` and restarts the service. Every check and every update is written to the log. A `copycord run` in a terminal never updates itself; it only prints the hint above. Docker and git checkouts are not updated this way.

Auto-update only follows the `stable` channel. To let it install `beta` and `dev` updates too, run `copycord config set AUTO_UPDATE_ALL_CHANNELS true`. `copycord update` by hand works on every channel either way.

Each auto-update is checked, and undone if it does not work:

1. Before installing, Copycord writes down the version it runs now and where it came from in `update-previous.json` in the [data directory](#data-dir), and backs up the database as usual. If it cannot tell how to install the current version again, it does not update.
2. After installing, it runs the new `copycord --version` and `copycord doctor --brief`. The doctor result only counts if the doctor passed before the update.
3. After the service restarts, it waits up to 60 seconds for the dashboard to answer (on the `DASHBOARD_HOST` and `ADMIN_PORT` you set).

If any step fails, Copycord installs the previous version again, restarts, and writes an error to the log that names both versions. If the new version had already changed the database to a newer format, the backup from before the update is put back. It tries this once. After that, auto-update stays off until you run `copycord update` yourself, so a bad update is never installed again and again.

### Advanced: channels and git installs {#update-advanced}

Most installs stay on the default `stable` channel. These options switch to pre-releases or install Copycord from git; `copycord update --help` lists them under **Advanced**.

```bash
copycord update --channel beta         # switch to the beta channel and update
copycord update --channel stable       # switch back to stable
copycord update --channel dev          # git install of Copycord/Copycord at main
copycord update --channel dev --ref v4-dev
copycord update --channel dev --repo you/Copycord --ref 1a2b3c4
copycord update --channel beta --no-install
```

| Option | What it does |
|--------|--------------|
| `--channel stable\|beta\|dev` | Switches to this channel, installs from it and saves it once the install worked |
| `--ref REF` | Dev channel only: the git branch, tag or commit to install (default `main`). |
| `--repo OWNER/NAME` | Dev channel only: the GitHub repository to install from (default `Copycord/Copycord`), for example a fork |
| `--no-install` | Only saves `--channel` (and `--repo`, `--ref`), installs nothing. The install scripts use this. |
| `--force` | Reinstalls even when Copycord is already up to date |

### Update channels {#update-channels}

| Channel | Installs |
|---------|----------|
| `stable` (default) | The newest [GitHub release](https://github.com/Copycord/Copycord/releases) that is not a pre-release |
| `beta` | The newest GitHub release, pre-releases included (for example `v4.1.0b1`) |
| `dev` | A git ref of a GitHub repository, by default `main` of `Copycord/Copycord`. It runs `uv tool install --force git+https://github.com/<repo>@<ref>`, which builds the dashboard from source, so it needs Node.js (`npm`). Without `npm` on your `PATH` it reuses or downloads the official Node build (SHA-256 checked) into `<home>/node`, the same one the installer uses, so a release install can switch to a branch with no manual steps. |

Stable and beta install the wheel attached to the release. Once you switch, plain `copycord update` keeps using the saved channel, repository and ref; `--ref` or `--repo` alone changes them while you are on dev.

The channel is saved in `update.json` in the [data directory](#data-dir). Installing with `COPYCORD_CHANNEL=beta` saves `beta` the same way. The file looks like this:

```json
{"channel": "dev", "branch": "v4-dev", "repo": "you/Copycord"}
```

`branch` holds the ref. A file without `repo` means `Copycord/Copycord`.

### Other installs

`copycord update` reinstalls Copycord with the tool that installed it: `uv tool install --force` for the install script, `pipx install --force` for pipx. For other installs it prints what to do instead:

- **Docker:** pull the new image and recreate the container (`docker compose pull && docker compose up -d`). See [Docker Install](/docs/getting-started/docker-install#update-copycord).
- **A git checkout:** update it with `git pull`, or switch to the install script.
- **pip or uv pip in your own environment:** it prints the matching `pip install --upgrade` command for you to run.
