---
sidebar_position: 7
title: CLI Reference
---

# CLI Reference

The [installer](/docs/getting-started/quick-start) installs the `copycord` command. It runs Copycord, checks your setup, reads and writes settings, manages the background service and updates Copycord.

```
copycord run [--data-dir PATH]
copycord restart
copycord doctor [--verbose] [--fix] [--json] [--data-dir PATH]
copycord config get KEY [--show-secrets] [--json] [--data-dir PATH]
copycord config set KEY VALUE [--data-dir PATH]
copycord config unset KEY [--data-dir PATH]
copycord config export [FILE] [--show-secrets] [--json] [--data-dir PATH]
copycord config import FILE [--data-dir PATH]
copycord service install [--user | --system] [--data-dir PATH]
copycord service status [--user | --system] [--json]
copycord service start|stop|restart [--user | --system]
copycord service uninstall [--user | --system] [--yes]
copycord backup [--json] [--data-dir PATH]
copycord restore [ARCHIVE] [--yes] [--data-dir PATH]
copycord update [--channel stable|beta|dev] [--repo OWNER/NAME] [--ref REF] [--tag VERSION] [--no-install] [--force]
copycord update status [--json]
copycord uninstall [--purge|--keep-data] [--yes] [--keep-service] [--dry-run] [--data-dir PATH]
copycord --version | -V [--json]
copycord --no-color <command>
copycord --verbose <command>
```

`copycord` on its own (or `copycord --help`) lists the commands by group with a few examples:

```
$ copycord
Copycord v4.0.0

Usage: copycord <command> [options]

Get started:
  run        Run Copycord
  restart    Restart Copycord
  doctor     Check this machine can run Copycord

Settings:
  config     Read or write settings (get, set, unset, export, import, init)

Service:
  service    Run Copycord in the background (install, status, start, stop, restart, uninstall)

Updates:
  update     Update Copycord to the latest version

Remove:
  uninstall  Remove Copycord; asks whether to delete your data

Examples:
  copycord run                             start Copycord in this terminal
  copycord service install                 run it in the background
  copycord update                          update to the latest version
  copycord doctor                          check your setup
  copycord config set PASSWORD 'new-pass'  change the dashboard password

Options:
  -h, --help     Show this help
  -V, --version  Print the version
  --no-color     Turn off color, same as NO_COLOR=1
  --json         Print the result as JSON, for scripts
  --verbose      Show more detail, and the full error if something fails

Run copycord <command> --help for the options of one command.
Docs: https://copycord.github.io/Copycord/docs/cli
```

Every command has `--help`, for example `copycord service install --help`. Each command's help has the same layout: a heading, the usage line, its commands or options, and examples. Options most people never need come last, under their own heading. A command group on its own (`copycord config`, `copycord service`) prints its help. A mistake such as an unknown option prints one line and points at the help. An unknown command suggests the closest one and exits with status 2:

```
$ copycord updte
✗ unknown command 'updte'. Did you mean 'update'?
  Run copycord --help to see all commands.
```

```
$ copycord update --help
copycord update
Update Copycord to the latest version and restart a running service. The
database is backed up first.

Usage: copycord update [status] [options]

Arguments:
  status  Print the installed version, the channel and the latest on it

Options:
  --data-dir PATH  Data directory (sets DATA_DIR)
  --tag VERSION    Install this exact version once; the saved channel does not
                   change
  ...
  -h, --help       Show this help

Examples:
  copycord update              update to the latest version
  ...

Advanced (channels and git installs):
  --channel stable|beta|dev  Switch to this channel and save it (stable:
                             GitHub releases, beta: GitHub pre-releases too,
                             dev: a git ref, needs npm)
  ...

  copycord update --channel beta                switch to pre-releases
  ...
```

Colors follow the same rules as the rest of the output: no color when it is not a terminal, `NO_COLOR` is set or `--no-color` is given.

## Output and colors {#output}

Every status line starts with one symbol: `✓` success, `!` warning, `✗` failure, `•` info, and `?` for a question. The installers use the same set. Commands inside a message are shown in the command color. The Copycord banner appears only on the bare `copycord` overview and in the installers. In a terminal the CLI also shows a spinner while it waits for GitHub or installs an update, and boxed notices for things you need to act on. A step that takes longer than a few seconds shows the time spent next to the spinner, for example `Installing Copycord  12s`; the installers do the same. In Windows Terminal the tab and the taskbar button also show that Copycord is busy until the step ends.

Colors are turned off when the output is not a terminal (a pipe, a file, a service log), when `NO_COLOR` is set, with `--no-color` (before or after the command, for example `copycord --no-color doctor`), or when `TERM=dumb`. `FORCE_COLOR=1` turns them on anyway. Without colors the lines and symbols stay the same:

```
✓ home writable
! the default password is still set  change PASSWORD in /home/you/.copycord/.env, then run copycord restart
✗ database schema 5 is newer than this Copycord supports (4)  ...
```

Where the terminal cannot print these symbols (for example a Windows console on code page 437 or 1252), they fall back to `+ ! x -`.

Web addresses in the output (the dashboard address, the docs link) can be clicked in Windows Terminal, VS Code, iTerm2, kitty, WezTerm, Ghostty and GNOME Terminal and other VTE-based terminals. The full address is always printed, so you can still copy it anywhere else. Links are off when colors are off, and inside tmux or screen.

Folders and files in the Copycord home can be clicked the same way to open them: the `Home:` line of the overview, the paths `copycord doctor` prints (such as the `.env` in "change PASSWORD in ...") and the "Your data is in" lines of `copycord uninstall`. The path is printed as before. These links are also off over SSH and inside Docker, where the folder is not on the computer in front of you, and with `--json`.

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

If a command hits an error Copycord does not expect, it prints three plain lines on stderr instead of a Python traceback and exits with status 1:

```
Copycord hit an unexpected error: OSError: [Errno 28] No space left on device
The details were saved to /home/you/.copycord/logs/copycord-crash-20261013-141502-4242.log
Report it at https://github.com/Copycord/Copycord/issues
```

The log file holds the full traceback, the Copycord and Python versions and the command (without its values, which can be secrets). It goes in the `logs` folder of the Copycord home, or in the temp folder when that is not writable. Add `--verbose` to any command to also print the traceback. Ctrl+C stops a command as before, without a report.

## The data directory {#data-dir}

Copycord keeps its database (`data.db`), `secret.key`, backups, exports and the saved update state in the data directory:

| Where | Data directory |
|-------|----------------|
| Linux, macOS | `~/.copycord/data` |
| Windows | `%USERPROFILE%\.copycord\data` |
| Docker | `/data` (your `./data` volume) |

`~/.copycord` is the Copycord home. It holds `.env`, the `data` folder and a `logs` folder with all logs (`server.out`, `client.out`, `scraper.out`, `update.log`). A git install keeps its checkout in `src/` and its per-user Node in `node/` inside it; on Windows, `copycord update` leaves `.copycord-old` there. Set `COPYCORD_HOME` to use another home; `--data-dir` and `DATA_DIR` still win over the data directory. In Docker, and with any other `DATA_DIR`, everything stays in that one folder as before (`.env`, `*.out` and `logs/update.log` included).

Older installs kept everything loose in the home. When Copycord starts and finds `data.db` or `secret.key` directly in the home and no `data/data.db`, it first backs up the database to `backups/data-pre-update-layout-<timestamp>.db`, then moves the data files into `data` and the `*.out` files into `logs`, and logs one line: `Moved Copycord data into <home>/data and logs into <home>/logs`. It never overwrites a file that is already in the new place and never deletes anything. If a move fails, it puts everything back and keeps using the old layout. `copycord doctor` shows `data files loose in the home` until then, and `copycord doctor --fix` does the same move when Copycord is stopped. Docker data is never moved. If `~/.copycord` does not exist but an older install left a `data.db` in `~/.local/share/copycord` (Linux), `~/Library/Application Support/Copycord` (macOS) or `%LOCALAPPDATA%\Copycord` (Windows), Copycord keeps using that folder and prints a notice. It never moves data by itself; to move, stop Copycord and move the folder to `~/.copycord`.

`--data-dir PATH` uses another folder for that command. It does the same as setting the `DATA_DIR` environment variable. It is accepted by `run`, `doctor`, every `config` command, `service install` and `uninstall`.

## copycord run {#copycord-run}

Starts Copycord: the dashboard, the server bot and the client.

```bash
copycord run
copycord run --data-dir ~/copycord-data
```

The dashboard is at **http://localhost:8080**. Open it to finish the setup: the server bot token, the client token and everything else is configured there (see [Discord Setup](/docs/getting-started/discord-setup) for the tokens). Press **Ctrl+C** to stop. If one part crashes, Copycord restarts that part on its own.

Useful environment variables (on a native install, set them in [`<home>/.env`](#env-file)):

| Variable | Default | What it does |
|----------|---------|--------------|
| `PASSWORD` | a random password (set by the installer) | Dashboard password; always required |
| `ADMIN_PORT` | `8080` | Dashboard port |
| `DASHBOARD_HOST` | `127.0.0.1` | Where the dashboard listens. The default is this computer only; `0.0.0.0` opens it to your network (see [Dashboard address](#dashboard-address)) |
| `LOG_LEVEL` | `INFO` | `INFO` or `DEBUG`; also an app config key, which wins once it is saved |
| `DATA_DIR` | see above | Same as `--data-dir` |

`copycord run` refuses to start while the old v3 `ROLE` variable is set. See [Upgrading from v3](/docs/getting-started/upgrading-from-v3).

The terminal shows the dashboard's log and warnings or errors from the server and client. The full server and client logs go to files in the data directory; see [Logs](/docs/dashboard/system#where-logs-go).

When the dashboard port or one of the internal ports (`9101` and `9102`) is in use at start, Copycord waits up to 15 seconds for it, since a Copycord that just stopped can hold its ports for a moment. If the port is still in use, it prints `✗ Port 9101 is in use, so Copycord cannot start` with what to do (stop the other Copycord with `copycord service stop`, or run `copycord doctor` to check the port) and exits with 1.

### Dashboard address {#dashboard-address}

The dashboard listens on `127.0.0.1`, so only this computer can open it, at `http://localhost:8080`. To open it from other devices, set `DASHBOARD_HOST=0.0.0.0` (every network) or one IP address of this computer in [`<home>/.env`](#env-file), then run `copycord restart`. The Docker image sets `0.0.0.0` already.

With any address other than `127.0.0.1`, `copycord run` refuses to start while `PASSWORD` is still `copycord`, and `copycord doctor` shows it as a failure. Fix it by setting your own `PASSWORD`, or by setting `DASHBOARD_HOST=127.0.0.1` so the dashboard stays on this computer. If something else already protects the dashboard (a reverse proxy with its own login, for example), `COPYCORD_ALLOW_INSECURE_DASHBOARD=1` skips the check.

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
| Settings | `<home>/.env` exists; `PASSWORD` is not still the default `copycord`; a `FAIL` when the removed `ROLE` variable is still set |
| Data | The Copycord home exists and is writable; `data.db` opens and its schema is one this Copycord supports (a fresh install says it is created on first run); the data files are in the `data` folder, not loose in the home; the last backup in the backups folder is under 2 days old and did not fail its check |
| Running | `ADMIN_PORT` (default 8080) is free, or in use by Copycord itself; whether the OS service is installed and running |
| Updates | The update check, cached for 24 hours. A network error is only reported, never a failure |

Piped or redirected to a file it prints plain sections, like this on a new install:

```
Install
✓ Copycord v4.0.0 (1a2b3c4) · stable channel
✓ Python 3.12.3
✓ web UI included
✓ dependencies OK

Settings
✓ .env found
! the default password is still set  change PASSWORD in /home/you/.copycord/.env, then run copycord restart

Data
✓ home writable
• database created on first run
• no backups yet

Running
✓ admin port 8080 free
• service not installed  copycord service install runs Copycord in the background

Updates
✓ up to date

Needs your attention
! the default password is still set  change PASSWORD in /home/you/.copycord/.env, then run copycord restart

! 1 warning, 0 problems
```

In a terminal the same report hangs off a line down the left, one box per group, with the version at the top and the summary on the last line:

```
Copycord v4.0.0 (1a2b3c4) · Ultimate Discord server mirroring and utility toolkit

┌  Copycord doctor
│
◇  Install ───────────────────────────────────────╮
│                                                 │
│  ✓ Copycord v4.0.0 (1a2b3c4) · stable channel   │
│  ✓ Python 3.12.3                                │
│  ✓ web UI included                              │
│  ✓ dependencies OK                              │
│                                                 │
├─────────────────────────────────────────────────╯
│
◇  Updates ───────────────────────────────────────╮
│                                                 │
│  ✓ up to date                                   │
│                                                 │
├─────────────────────────────────────────────────╯
│
└  ! 1 warning, 0 problems
```

Every box of one command is as wide as the terminal, up to 100 columns, and long lines wrap inside it. `copycord doctor --brief` and `--json` keep their plain output.

Each line starts with `✓`, `!`, `✗` or `•`; in a terminal they are green, yellow, red and blue. The report ends with a **Needs your attention** section that lists each warning and problem once with what to do, then a last line: `✓ All good`, or the number of warnings and problems. A warning that `--fix` can repair says so there, for example `! .env not found  run copycord doctor --fix to create it with the default settings`, while its line in the group shows the path. The command exits with status 1 when any check fails and 0 otherwise (warnings included), so it can be used in scripts. When `copycord doctor --fix` can repair some of the warnings and problems, a last line says how many.

:::tip Port in use
`! port 8080 in use` means another program has the port: set `ADMIN_PORT` to a free one, or let `copycord doctor --fix` pick the next free port. When Copycord itself is listening (as a service, or `copycord run` in another terminal) the line says `port 8080 in use by Copycord` instead.
:::

### Fixing problems {#doctor-fix}

`copycord doctor --fix` applies the fixes first, then prints the report once, as things are after the fixes. Every automatic fix runs directly, without asking. The guided fix needs your input, so it asks before the report, and only when there is a terminal; without one (in a script or a pipe) it is skipped, and its entry in Needs your attention says to run `copycord doctor --fix` in a terminal.

A check that was fixed shows on its own line as fixed, for example `✓ .env created with the default settings  /home/you/.copycord/.env (fixed)`. A fix that did not work shows on the check's line as a failure with the reason. **Needs your attention** lists what is left, and the report ends with `Fixed 1 issue` (when something was fixed), then `1 thing needs your attention` or `Everything looks good`.

`--brief` prints only the result: a line for each fix applied, a line for each failed check, and `All checks passed` or the number of warnings and problems. The installers use it.

| Check | Fix | Kind |
|-------|-----|------|
| `.env not found` | Writes `<home>/.env` with every setting at its default, the same file the installer writes | Automatic |
| `home folder missing` | Creates the Copycord home | Automatic |
| `last backup ... ago`, `no backups yet`, `last backup failed its check` | Backs up the database now, the same `.tar.gz` the daily backup makes | Automatic |
| `service installed, not running` | Starts the service | Automatic |
| `service runs in a console window` (Windows) | Writes the service task again so it runs without a window, with the same data directory, and restarts it | Automatic |
| `web UI missing`, `missing dependencies` | Reinstalls the version you have from your update channel (a dev install rebuilds the same ref), through `copycord update`, which backs up the database first | Automatic |
| `port 8080 in use` | Asks `y/N` to move to the next free port and writes it as `ADMIN_PORT` in `.env` | Guided |

Some things are never changed for you; they stay in Needs your attention with what to do:

- **The dashboard password:** `--fix` never changes it or asks for one. While it is still the default `copycord`, change `PASSWORD` in `<home>/.env`, then run `copycord restart`.
- **A database from a newer Copycord:** run `copycord update` to go back to a newer version, or restore a backup.
- **`ROLE` is set:** remove it from your compose file.

The installers run `copycord doctor --fix --brief` at the end of an upgrade or a reinstall (not on a first install), so the automatic repairs happen by themselves.

With the `.env` deleted on a new install it prints:

```
Install
✓ Copycord v4.0.0 (1a2b3c4) · stable channel
✓ Python 3.12.3
✓ web UI included
✓ dependencies OK

Settings
✓ .env created with the default settings  /home/you/.copycord/.env (fixed)
! the default password is still set  change PASSWORD in /home/you/.copycord/.env, then run copycord restart

Data
✓ home writable
• database created on first run
• no backups yet

Running
✓ admin port 8080 free
• service not installed  copycord service install runs Copycord in the background

Updates
✓ up to date

Needs your attention
! the default password is still set  change PASSWORD in /home/you/.copycord/.env, then run copycord restart

✓ Fixed 1 issue
! 1 thing needs your attention
```

The recreated `.env` sets the default password `copycord`, so it is listed after the fix. Change `PASSWORD` in `<home>/.env`, then run `copycord restart`.


## copycord config {#copycord-config}

Reads and writes Copycord's settings. Most live in the database (the same values the dashboard's Global Configuration card shows, such as `COMMAND_USERS`, `LOG_LEVEL` or `AUTO_START`). The few that are read from the environment only, such as `PASSWORD` and `ADMIN_PORT`, live in [`<home>/.env`](#env-file); `get`, `set` and `unset` pick the right place by the key.

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

The installer makes sure the file exists: on first install it runs `copycord config init`, which writes the file with every key set to its default, a random password and a one-line description above each key, and prints its path. It never overwrites an existing file, and on Linux and macOS only your user can read it. `BACKUP_DIR` is the default folder, `backups` in the [data directory](#data-dir). On Linux, for a user named `alice`:

```ini
# Copycord settings. Everything else is in the dashboard.
# After a change, run: copycord restart

# Dashboard password
PASSWORD=k7Qe2Vd9sLmP0aZr4TwXbg

# Dashboard port
ADMIN_PORT=8080

# Who can open the dashboard: 127.0.0.1 = this computer, 0.0.0.0 = your network
DASHBOARD_HOST=127.0.0.1

# Time zone for backups and exports
TZ=UTC

# Log detail: INFO or DEBUG
LOG_LEVEL=INFO

# Log style: HUMAN or JSON
LOG_FORMAT=HUMAN

# Backup folder
BACKUP_DIR=/home/alice/.copycord/data/backups

# Daily backup time (HH:MM)
BACKUP_AT=03:17

# Backups to keep
BACKUP_RETAIN=14

# Skip the update check: 1 = yes, 0 = no
COPYCORD_NO_UPDATE_CHECK=0
```

An empty value, such as `TZ=`, means the default. The dashboard always asks for the password. If `PASSWORD` is empty or missing, `copycord run` makes a random one, saves it in this file and shows it once. In Docker, Copycord does not start until you set `PASSWORD`.

:::note The dashboard password
`copycord config init`, which the installers run, writes a random `PASSWORD` into a new file, and the installer shows it once. `copycord config get PASSWORD --show-secrets` prints it again. A file written by `copycord doctor --fix` has the old default `copycord`; change it.
:::

To change a setting, edit the file, then restart Copycord:

```bash
nano ~/.copycord/.env                # Linux/macOS
notepad "$HOME\.copycord\.env"       # Windows (PowerShell)
copycord restart
```

Or let `copycord config set` edit the file for you:

```bash
copycord config set PASSWORD 'new-pass'
copycord restart
```

[`copycord restart`](#copycord-restart) restarts the service. If you run Copycord in a terminal instead, stop it with **Ctrl+C** and run `copycord run` again.

| Key | Default | What it does |
|-----|---------|--------------|
| `PASSWORD` | a random password in the file | Dashboard password; always required. |
| `SECRET_KEY` | a key saved in `<data dir>/secret.key` | Signs the dashboard login cookie. Not in the file; add it only to use a key of your own. |
| `ADMIN_PORT` | `8080` | Dashboard port. |
| `DASHBOARD_HOST` | `127.0.0.1` | Where the dashboard listens. Commented out in the file, so only this computer can open it. See [Dashboard address](#dashboard-address). |
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

```text
$ copycord service install
✓ Created the service for your account  /home/mac/.config/systemd/user/copycord.service
✓ Enabled it, so it starts at boot
✓ Started Copycord
• Data  /home/mac/.copycord

Dashboard: http://localhost:8080
Manage it with copycord service status | stop | start | restart | uninstall

$ copycord service status

Copycord service
  Status   ✓ running
  Type     service for your account, starts at boot
  File     /home/mac/.config/systemd/user/copycord.service
  Started  Tue 2026-10-13 09:12:03 BST
  Logs     journalctl --user -u copycord -f

Stop it with copycord service stop, or open the dashboard at http://localhost:8080

$ copycord service stop
✓ Copycord stopped  start it again with copycord service start

$ copycord service uninstall
? Stop and remove the Copycord service? Your data is kept.  Yes / › No
```

| Command | What it does |
|---------|--------------|
| `install [--user \| --system] [--data-dir PATH]` | Registers `copycord run --data-dir <data dir>` with the service manager, enables it and starts it |
| `status [--json]` | Shows whether the service is installed and running, what kind of service it is, its file, when it started (Linux) and where its output goes. Exits with 0 when running, 3 when not. |
| `start` | Starts the service and confirms it with `✓ Copycord started` and the dashboard address |
| `stop` | Stops the service and confirms it with `✓ Copycord stopped` |
| `restart` | Restarts the service, the same as [`copycord restart`](#copycord-restart) |
| `uninstall [--yes]` | Asks, then stops and removes the service. Your data is kept. Without a terminal it needs `--yes`. To remove Copycord itself too, use [`copycord uninstall`](#copycord-uninstall). |

In a terminal `status` is a box under the version line, with the next step on the last line; `install`, `start`, `stop`, `restart` and `uninstall` show their steps on the same line down the left. Without a service:

```
$ copycord service status

Copycord v4.0.0 (1a2b3c4) · Ultimate Discord server mirroring and utility toolkit

┌  Copycord service status
│
◇  Copycord service ──────────────────────────────╮
│                                                 │
│    Status  • not installed                      │
│                                                 │
├─────────────────────────────────────────────────╯
│
└  Install it with copycord service install
```

In a pipe, `start`, `stop`, `restart` and `uninstall` print `• The Copycord service is not installed` with the install command when there is no service; `start`, `stop` and `restart` then exit with 1. When the service manager reports an error, the command prints `✗ Could not stop Copycord` (or start, restart, install, remove) with the reason and exits with 1.

The data directory is fixed when you install the service: the one given with `--data-dir` (or `DATA_DIR`), else the default for your user. To change it, run `copycord service install` again with the new `--data-dir`.

### How it works on each OS

| OS | Service | Starts | Restarts |
|----|---------|--------|----------|
| Linux (default) | systemd user unit `~/.config/systemd/user/copycord.service` | At boot (with lingering, see below) | 10 seconds after a crash |
| Linux (`--system`, or run as root) | systemd system unit `/etc/systemd/system/copycord.service` | At boot | 10 seconds after a crash |
| macOS | launchd agent `~/Library/LaunchAgents/com.copycord.plist` | When you log in | After a crash |
| Windows | Scheduled Task `Copycord` | When you log in | Every minute after a failure |

**Linux service for your account (default).** `copycord service install` needs no sudo. It installs a systemd user unit that runs as you, with your Copycord home. A user unit normally runs only while you are logged in, so the install also turns on lingering for your account, which starts Copycord at boot and keeps it running after you log out. Most systems allow this without sudo, and the install prints:

```text
✓ Enabled it, so it starts at boot
```

If your system does not allow it, the install prints `Enabled it, so it starts when you log in` and `! Starts only when you log in` with the command to run once:

```bash
sudo loginctl enable-linger "$USER"
```

`copycord doctor` shows `service starts only when you log in` while lingering is off, and `copycord doctor --fix` tries to turn it on.

**Linux system service.** `copycord service install --system` installs `/etc/systemd/system/copycord.service` instead. It needs root, and so do `start`, `stop`, `restart` and `uninstall` for it. Because the install script puts `copycord` in your user's tool directory, call it by its full path:

```bash
sudo "$(command -v copycord)" service install --system
```

When installed with `sudo`, the system service runs as your user (not root) and uses your user's data directory. Run as root, `copycord service install` installs the system service without `--system`.

`start`, `stop`, `status` and `uninstall` use whichever service is installed. If both a user service and a system service are installed, they use the one for your account (the system service when run as root) and say which; add `--user` or `--system` to choose. On a system without systemd (for example WSL without systemd, or a container), `copycord service install` stops with the error from `systemctl`; run `copycord run` instead.

**Linux sandboxing.** The unit sets `NoNewPrivileges`, `RestrictSUIDSGID` and `LockPersonality`. The `--system` unit also sets `PrivateTmp`, `ProtectSystem=strict` and `ReadWritePaths` (the Copycord home, the data directory, `~/.local` and `~/.cache`), so Copycord can only write there; put `BACKUP_DIR` inside the home, or add a `ReadWritePaths=` line with `systemctl edit copycord`. The user unit leaves those three out, because they need root.

**macOS.** The agent's console output goes to `/dev/null`: the same lines are already in the rotating log files in the [logs folder](#data-dir), and launchd never trims its own files. `--user` has no effect.

**Windows.** The task runs as the current user at logon, without a window: it starts `copycordw.exe`, which is installed next to `copycord.exe` (or `pythonw.exe` of the same install when `copycordw.exe` is missing). What a terminal would show goes to `copycord.out` in the [logs folder](#data-dir). A task created by an older version runs `copycord.exe` in a console window, and closing that window stops Copycord; `copycord doctor` shows `service runs in a console window`, and `copycord doctor --fix` writes the task again so it runs without a window, keeping its data directory (`copycord service install` does the same with the data directory you give it). `--user` has no effect.

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

Backs up the database now, the same archive the [daily backup](/docs/dashboard/backups) makes, and prints its path:

```
$ copycord backup | cat
✓ Database backed up  /home/you/.copycord/data/backups/2026-10-13_15-31-24.tar.gz
```

In a terminal the same step hangs off the line down the left, under the version line:

```
$ copycord backup

Copycord v4.0.0 (1a2b3c4) · Ultimate Discord server mirroring and utility toolkit

┌  Back up the database
│
✓  Database backed up  /home/you/.copycord/data/backups/2026-10-13_15-31-24.tar.gz
└
```

The archive holds `data.db`, `secret.key` (when there is one) and `manifest.json`, and goes into the backups folder (`BACKUP_DIR`, by default `backups` in the [data directory](#data-dir)). The copy of the database is checked with SQLite's integrity check before it is saved; when the check fails, nothing is kept, the command says why and exits with status 1, and `copycord doctor` shows `last backup failed its check`. It works while Copycord is running. With `--json` it prints `{"ok": true, "file": ..., "size": ..., "includes_key": true}`.

In Docker, run it inside the container:

```bash
docker compose exec copycord /opt/venvs/copycord/bin/copycord backup
```

## copycord restore {#copycord-restore}

Replaces the database with a backup. Copycord must be stopped first: stop the service with `copycord service stop`, or press Ctrl+C where `copycord run` is running. Without a file it lists the 10 newest backups in the backups folder, the daily archives and the copies made before an update or a restore, newest first:

```
┌  Restore a backup
│  Replaces the database with a backup. The current database is backed up first.
│
◆  Which backup do you want to restore?
│  ❯ 2026-10-13 03:17  backup, 8.4 MB  (2026-10-13_03-17-00.tar.gz)
│    2026-10-12 09:15  made before an update, 8.3 MB  (data-pre-update-4.0.0-20261012T091500Z.db)
│    2026-10-12 03:17  backup, 8.3 MB  (2026-10-12_03-17-00.tar.gz)
│    Cancel
│    ↑/↓ to move, Enter to select
```

Give a file to restore that one: `copycord restore ~/.copycord/data/backups/2026-10-13_03-17-00.tar.gz`. It accepts a `.tar.gz` archive (older archives that hold only `data.db` work too) or a `.db` copy, such as the ones `copycord update` makes.

Before it changes anything, `copycord restore`:

1. Refuses while Copycord is running (the service, the dashboard answering on `ADMIN_PORT`, or on Windows a Copycord in another window).
2. Checks the backup with SQLite's integrity check, and refuses a backup that fails it.
3. Refuses a backup from a newer Copycord (a higher database schema than this version supports); update Copycord first, then restore it.
4. Shows when the backup was made and what it includes, and asks `Replace the database in <data dir> with this backup? (y/N)`.

Then it backs up the current database and `secret.key` to `pre-restore-<UTC time>.tar.gz` in the backups folder, so the restore can be undone; these copies are never removed by the daily cleanup. It moves the restored `data.db` into place in one step and, when the backup has a `secret.key` that differs from the current one, restores it and keeps the current key as `secret.key.bak-<UTC time>`. A backup without a key leaves the current key alone. After you pick a backup it looks like this:

```
✓  backup checked  /home/you/.copycord/data/backups/2026-10-13_03-17-00.tar.gz
│  Made 2026-10-13 03:17 UTC by Copycord 4.0.0
│  Includes the database and secret.key
│
◇  Replace the database in /home/you/.copycord/data with this backup?
│  Yes
│
✓  backed up the current database  /home/you/.copycord/data/backups/pre-restore-20261013T153124Z.tar.gz
✓  database restored  /home/you/.copycord/data/data.db
✓  secret.key restored  /home/you/.copycord/data/secret.key
•  previous secret.key kept as  /home/you/.copycord/data/secret.key.bak-20261013T153124Z
│
✓  Restore complete  from 2026-10-13_03-17-00.tar.gz
│  To undo it, run copycord restore /home/you/.copycord/data/backups/pre-restore-20261013T153124Z.tar.gz
└  Start Copycord with copycord service start or copycord run
```

`--yes` (`-y`) skips the question. Without a terminal (a script) it needs `--yes` and the file to restore, and exits with status 1 otherwise.

In Docker, stop the container, run the restore in a one-off container on the same `/data` volume, then start it again:

```bash
docker compose stop copycord
docker compose run --rm --entrypoint /opt/venvs/copycord/bin/copycord copycord restore
docker compose up -d
```

## copycord uninstall {#copycord-uninstall}

Removes the Copycord program and its background service. It asks one question, about your data in the [Copycord home](#data-dir):

```
┌  Uninstall Copycord
│  Removes the Copycord program and its background service.
│
◆  Your data is in /home/you/.copycord
│  ❯ Keep my data
│    Delete everything (database, logs, backups, settings)
│    Cancel
│    ↑/↓ to move, Enter to select
```

Move with the arrow keys (or `j` and `k`) and press Enter; Enter right away keeps your data, and Esc or Ctrl-C cancels. After you pick, the question is marked `◇` with your answer under it, and the steps follow on the same line down the left:

```
┌  Uninstall Copycord
│  Removes the Copycord program and its background service.
│
◇  Your data is in /home/you/.copycord
│  Keep my data
│
✓  removed the copycord program  uv tool uninstall copycord
│
✓  Copycord uninstalled
└  Your data is still in /home/you/.copycord/data
```

Yes or no questions work the same way, for example `◆  Update Copycord now?` with `❯ Yes / No` under it: Left/Right switch, `y` or `n` answer at once, Enter takes the highlighted one. A question asked on its own, such as the port question of `copycord doctor --fix`, takes one line, `? <question>  ❯ Yes / No`, and folds into `✓ <question> Yes` once answered.

Where single keys cannot be read (input piped in, `TERM=dumb`, Git Bash without a real console, or `COPYCORD_PLAIN_PROMPTS=1`), the prompts are typed instead: a numbered list with `Enter 1, 2 or 3 [1]`, and `(y/N)` for yes or no.

Where the terminal cannot show the line characters, they are replaced with `+`, `|`, `*` and `o`. In a script or a pipe, the same lines are printed without the line down the left.

Press Enter to keep your data. It ends with `Copycord uninstalled`, and when the data is kept, the folder it is still in.

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
| `--purge` | Deletes the Copycord home. Without `--yes` the question only offers delete or cancel. |
| `--keep-data` | Keeps the Copycord home. Without `--yes` the question only offers keep or cancel. |
| `--yes`, `-y` | Skips the question |
| `--keep-service` | Leaves the OS service installed |
| `--dry-run` | Prints the steps and changes nothing |
| `--data-dir PATH` | The data directory to keep or delete |

Without a terminal (in a script or a pipe) it never asks: it exits with status 1 unless you add `--yes`. With `--yes` and neither `--purge` nor `--keep-data`, the data is kept. It also exits with status 1 when a step fails.

To install Copycord again:

```bash
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
```

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

`copycord update` shows one progress line while it installs, then `Copycord updated to <version> (<commit>)` and, if the service was running, `restarted the copycord service`. The installer output is saved to `logs/update.log` in the [Copycord home](#data-dir); if the install fails, the last 20 lines and the log path are printed.

When the install is already the newest on the saved channel (the same release version on `stable` or `beta`, the same commit as the branch head on `dev`), `copycord update` prints `Copycord is up to date (<version>)`, with the commit on `dev` (`Copycord is up to date (3.28.0, dd7c8b2)`), and exits with status 0 without a backup or an install. If GitHub cannot be reached for that check, it warns and installs as before. A channel switch, `--tag`, `--force` and `copycord doctor --fix` always install.

On Windows, a running program's files cannot be deleted, but they can be renamed. So right before the install, `copycord update` stops the service if it is running and moves the running `python.exe`, the `copycord.exe` launcher and the other executable files of the install into `.copycord-old` in the Copycord home. Then it installs in the same window and returns when the update is done, like on Linux and macOS. The next update deletes `.copycord-old`. If the install fails before replacing the old files, they are put back. If Copycord is running in another window (for example `copycord run`), `copycord update` stops with `Copycord is running in another window (process N); stop it first, then run this again` and changes nothing. `copycord doctor --fix` reinstalls the same way.

`status` also shows when Copycord last checked for an update and whether auto-update is on. In a terminal the rows are a box under the version line:

```
$ copycord update status

Copycord v4.0.0 (1a2b3c4) · Ultimate Discord server mirroring and utility toolkit

┌  Copycord update status
│
◇  Copycord ──────────────────────────────────────╮
│                                                 │
│    current  4.0.0                               │
│    channel  stable channel                      │
│    method   uv tool                             │
│    checked  2026-10-13 13:07:05                 │
│    auto     off (AUTO_UPDATE=true turns it on)  │
│    latest   4.0.0 (v4.0.0)                      │
│                                                 │
├─────────────────────────────────────────────────╯
│
✓  Copycord is up to date
└
```

### Confirming the update {#update-confirm}

When there is something to install, `copycord update` shows what will change before it backs up or installs anything, for example:

```
┌  Updating Copycord · dev channel · v4-dev
│
◇  Update available
│  3.28.0 (00f0f8d) -> v4-dev (abc1234) client: fix reconnect
│
◆  Update Copycord now?
│  ❯ Yes / No
```

Where single keys cannot be read, the question is typed: `Update Copycord now? (Y/n)`. The backup, the install and the restart then follow on the same line down the left, which ends with `└` when the update is done or stops.

For a release it shows the installed and the new version (`3.28.0 -> 4.0.0`). A channel switch (`--channel`, `--ref`, `--repo`) shows the same line and asks too. Enter or `y` updates; `n` prints `Update cancelled, nothing was changed` and exits with status 0. Reinstalling the installed version (`--force`, or `--tag` with the installed version) asks `Reinstall Copycord <version>? (Y/n)` instead. A downgrade asks only its own question (see below).

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

When `copycord run` starts, and when `copycord --version` runs in a terminal, Copycord checks the saved [channel](#update-channels) in a background thread: the newest release for `stable` and `beta`, the branch head against the installed commit for `dev`. If there is something newer it prints one line and nothing else happens:

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
| `beta` | The newest GitHub release, pre-releases included (for example `v4.0.0b1`) |
| `dev` | A git ref of a GitHub repository, by default `main` of `Copycord/Copycord`. It runs `uv tool install --force git+https://github.com/<repo>@<ref>`, which builds the dashboard from source, so it needs Node.js (`npm`). Without `npm` on your `PATH` it reuses or downloads the official Node build (SHA-256 checked) into `<home>/node`, the same one the installer uses, so a release install can switch to a branch with no manual steps. |

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
