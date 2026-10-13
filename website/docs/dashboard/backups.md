---
sidebar_position: 9
title: Backups
---

# Database Backups

Copycord backs up your SQLite database every day. You can also make a backup at any time, and restore one from the command line or the dashboard.

## Automatic backups

By default, Copycord makes a backup every day at **03:17** in the time zone set by `TZ` (UTC unless you set it) while it is running. Set `BACKUP_AT` for another time and `BACKUP_RETAIN` for how many daily backups to keep (14 by default; older ones are removed). Backups are saved as compressed `.tar.gz` archives in the `backups` folder of the data directory, or in `BACKUP_DIR` when you set it. See [Settings that live in .env](/docs/cli#env-file).

:::info Backup before the first v4 start
When Copycord v4 first opens a database created by v3, it also saves a plain copy as `backups/pre-v4-<timestamp>.db` in the data directory. See [Upgrading from v3](/docs/getting-started/upgrading-from-v3#automatic-backup-of-your-database).
:::

`copycord update` also saves a copy of the database before it installs anything, as `data-pre-update-<version>-<time>.db` in the same folder. See [Downgrading and backups](/docs/cli#downgrade).

## What a backup holds

Each archive holds:

- `data.db`: the whole database, see [below](#whats-included-in-the-database).
- `secret.key`: the key that signs dashboard logins, when there is one. With it, a restore on a new machine keeps you signed in and needs no new key.
- `manifest.json`: the Copycord version, the database schema version, when the backup was made and the files in it.

Archives made before these were added hold only `data.db`; they can still be restored.

Because an archive holds `secret.key`, it is readable only by your user on Linux and macOS. Treat your backups like a password: keep them private.

## Every backup is checked

Before an archive is saved, Copycord checks the copy of the database with SQLite's integrity check. When the check fails, the archive is not kept, the older backups stay as they are, a warning is written to the log, and `copycord doctor` shows `last backup failed its check` until the next backup passes. A failed check usually means the database file is damaged; restore a recent backup that passed (see below), or run `copycord doctor --fix` to try the backup again.

## Manual backups

From the dashboard:

1. Go to the **System** page
2. Click **Backup Now**
3. The backup is created and added to the list

From the command line, `copycord backup` makes the same archive and prints its path. It works while Copycord is running. See [copycord backup](/docs/cli#copycord-backup).

## Keep a copy off this machine

The backups folder is on the same disk as your database, so a failed disk or a lost machine takes both. Copy the backups folder (or at least the newest archive) to another place regularly: an external drive, a NAS or a cloud folder. Keep that copy private, since it holds `secret.key`.

You can also download any archive from the **System** page, see below.

## Viewing and downloading backups

The **System** page shows all available backups with their file name, creation date and size. Click **Download** next to a backup to save the `.tar.gz` archive, for storage elsewhere or to move Copycord to another machine.

## Restoring from a backup

:::warning
Restoring a backup replaces your current database. Everything that changed since the backup was made is replaced. `copycord restore` saves the current database first, so you can undo it.
:::

### With copycord restore (recommended)

1. Stop Copycord: `copycord service stop`, or Ctrl+C where `copycord run` is running.
2. Run `copycord restore` and pick a backup from the list, or give the file: `copycord restore <path to the backup>`.
3. Confirm. Copycord checks the backup, saves the current database as `pre-restore-<time>.tar.gz`, then puts the backup in place, `secret.key` included when the backup has one.
4. Start Copycord again: `copycord service start` or `copycord run`.

To move Copycord to a new machine, install it there, copy the archive over and run `copycord restore <archive>` before the first start.

In Docker:

```bash
docker compose stop copycord
docker compose run --rm --entrypoint /opt/venvs/copycord/bin/copycord copycord restore
docker compose up -d
```

See [copycord restore](/docs/cli#copycord-restore) for every step and option.

### From the dashboard

1. Go to the **System** page
2. Click **Restore** next to the backup you want to restore
3. Confirm the action

The dashboard restores only the database, not `secret.key`, and does not check the backup first; use `copycord restore` when you can.

## What's included in the database

The database holds:

- All guild mappings and their settings
- Channel, role, emoji, and sticker mappings
- Message ID mappings (for edit/delete tracking)
- Filters and blocked keywords
- Forwarding rules
- Announcement triggers and subscriptions
- All configuration settings
- Event logs
