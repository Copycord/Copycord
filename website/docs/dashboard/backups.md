---
sidebar_position: 9
title: Backups
---

# Database Backups

Copycord automatically backs up your SQLite database daily. You can also trigger manual backups and restore from any backup.

## Automatic backups

By default, Copycord runs a daily backup at **03:17 UTC**. Backups are saved as compressed `.tar.gz` archives in the data directory.

## Manual backups

To create an immediate backup:

1. Go to the **System** page in the dashboard
2. Click **Backup Now**
3. The backup will be created and added to the list

## Viewing backups

The **System** page shows all available backups with:

- Filename
- Creation date
- File size

## Downloading backups

Click the **Download** button next to any backup to download the `.tar.gz` archive. This is useful for off-site storage or migration.

## Restoring from backup

:::warning
Restoring a backup replaces your current database. All data since the backup was created will be lost. Make sure to create a fresh backup before restoring if needed.
:::

1. Go to the **System** page
2. Click **Restore** next to the backup you want to restore
3. Confirm the action
4. Copycord will restart with the restored database

## What's included in backups

Backups contain the entire SQLite database, which includes:

- All guild mappings and their settings
- Channel, role, emoji, and sticker mappings
- Message ID mappings (for edit/delete tracking)
- Filters and blocked keywords
- Forwarding rules
- Announcement triggers and subscriptions
- All configuration settings
- Event logs
