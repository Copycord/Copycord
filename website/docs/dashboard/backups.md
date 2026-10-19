---
sidebar_position: 9
title: Backups
---

# Database backups

Copycord backs up its database every day. Use this page to make a backup yourself, keep a copy somewhere safe and restore a backup.

## Automatic backups

While Copycord runs, it makes a backup every day at 03:17 in the time zone set by `TZ`, or UTC when `TZ` is not set. It keeps the newest 14 daily backups and removes older ones.

Backups are compressed `.tar.gz` archives in the `backups` folder of the data directory.

| Setting | What it changes |
|---------|-----------------|
| `BACKUP_AT` | The time of the daily backup |
| `BACKUP_RETAIN` | How many daily backups to keep. Default: 14 |
| `BACKUP_DIR` | The folder for backups |

These settings live in `.env`; see [Settings that live in .env](/docs/cli#env-file).

## Back up now

To make a backup from the dashboard:

1. Open the **System** page and click the **Database** card's header to open it.
2. Click **Backup now**.

The new backup appears in the list.

To make a backup from the command line, run `copycord backup`. It makes the same archive and prints its path, and it works while Copycord is running. See [copycord backup](/docs/cli#copycord-backup).

## Keep a copy off this machine

The backups folder is on the same disk as your database, so a failed disk or a lost machine takes both. Copy the backups folder, or at least the newest archive, to another place regularly, such as an external drive, a NAS or a cloud folder.

To download a backup from the dashboard:

1. Open the **System** page and open the **Database** card.
2. Click **Download** next to the backup, or **Download latest** for the newest one.

Each archive holds `secret.key`, so keep every copy private, like a password.

## Restore a backup

:::warning
Restoring a backup replaces your current database. Everything that changed since the backup was made is lost, unless you restore the copy that `copycord restore` saves first.
:::

### With copycord restore

`copycord restore` checks the backup, saves your current database first and restores `secret.key` too. Use it when you can.

1. Stop Copycord: run `copycord service stop`, or press Ctrl+C where `copycord run` is running.
2. Run `copycord restore` and pick a backup from the list. To restore a specific file, run `copycord restore <path to the backup>`.
3. Confirm. Copycord checks the backup, saves the current database as `pre-restore-<time>.tar.gz`, then puts the backup in place, with `secret.key` when the backup has one.
4. Start Copycord: run `copycord service start` or `copycord run`.

In Docker, run:

```bash
docker compose stop copycord
docker compose run --rm --entrypoint /opt/venvs/copycord/bin/copycord copycord restore
docker compose up -d
```

To move Copycord to a new machine, install it there, copy the archive over and run `copycord restore <archive>` before the first start. Every option is described in [copycord restore](/docs/cli#copycord-restore).

### From the dashboard

The dashboard restores only the database, not `secret.key`, and does not check the backup first.

1. Open the **System** page and open the **Database** card.
2. Click **Restore** next to the backup. To restore an archive from your computer, click **Restore** at the top of the card and upload the `.tar.gz` file.
3. Confirm.

## Delete a backup

1. Open the **System** page and open the **Database** card.
2. Click **Delete** next to the backup and confirm.

## What a backup holds

| File | Contents |
|------|----------|
| `data.db` | The whole database; see [What's included in the database](#whats-included-in-the-database) |
| `secret.key` | The key that signs dashboard logins, when there is one. With it, a restore on a new machine keeps you signed in |
| `manifest.json` | The Copycord version, the database schema version, when the backup was made and the files in it |

An archive that holds only `data.db` can also be restored. On Linux and macOS, archives are readable only by your user.

## When a backup fails its check

Before Copycord saves an archive, it checks the copy of the database with SQLite's integrity check. When the check fails:

- The archive is not kept, and the older backups stay as they are.
- A warning is written to the log.
- `copycord doctor` shows `last backup failed its check` until a backup passes.

A failed check usually means the database file is damaged. Restore a recent backup that passed, or run `copycord doctor --fix` to try the backup again.

## Other database copies

Copycord also saves plain `.db` copies of the database at these times:

- `copycord update` saves `data-pre-update-<version>-<time>.db` in the backups folder before it installs anything; see [Downgrading and backups](/docs/cli#downgrade).
- The first start on a database from Copycord v3 saves `backups/pre-v4-<timestamp>.db` in the folder that holds `data.db`; see [Upgrading from v3](/docs/getting-started/upgrading-from-v3#automatic-backup-of-your-database).

## What's included in the database

- All guild mappings and their settings
- Channel, role, emoji and sticker mappings
- Message records, used to mirror edits and deletes
- Filters and blocked keywords
- Forwarding rules
- All configuration settings
- Event logs
- Task runs from the [Tasks](/docs/dashboard/tasks) page
