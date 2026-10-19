---
title: Tasks
---

# Tasks

The **Tasks** page runs Copycord's long jobs: history imports, exports, purges, asset syncs, avatar downloads and member scrapes. Use it to start a job, follow its progress and read its report.

Each job you start is a run with its own page.

## Task types

| Task type | What it does | Needs the bots |
|-----------|--------------|----------------|
| [Backfill channels](/docs/dashboard/backfill) | Copies the message history of channels into their clones | Yes |
| [Purge messages](/docs/dashboard/purge-and-sync#purge-messages) | Deletes Copycord's stored message records older than a cutoff, or all of them. Messages on Discord are not touched | No |
| [Purge assets](/docs/dashboard/purge-and-sync#purge-assets) | Deletes emojis, stickers and roles from a clone server | Yes |
| [Sync assets](/docs/dashboard/purge-and-sync#sync-assets) | Brings a clone's roles, emojis and stickers in line with the host | Yes |
| [Export messages](/docs/dashboard/exports#export-messages) | Saves a server's messages, filtered, to a JSON file, and can post them to a webhook | Yes |
| [Export DMs](/docs/dashboard/exports#export-dms) | Saves the client account's DM history with one user to a JSON file, or posts it to a webhook | Yes |
| [Download avatars](/docs/dashboard/exports#download-avatars) | Zips the profile pictures of the members your user account can see in a server | Yes |
| [Scrape members](/docs/dashboard/scraper) | Lists a server's members with the scraper tokens and saves them to a JSON file | No |

"Needs the bots" means both the server bot and the client must be running. Until they are, **Run task** is disabled and **Start the bots first** shows beside it. If the bots stop while such a run is queued or running, the run is cancelled with the note **Bots stopped**.

## Start a run

1. In **Task types**, pick the type.
2. Click **Run task**. A form opens with the type's options.
3. Fill in the form, then click the button at the bottom of it, for example **Start** or **Export messages**. If the run deletes something, confirm.

The run page opens.

To export a server's messages from the **Guilds** page, open the menu on its card and click **Export messages**. The form opens with that server picked.

**Download avatars** and **Scrape members** run one at a time. A second run of the same type waits as **Queued** until the first one ends.

## Find a run

The run list shows the runs of the selected type, newest first, and updates live. To see every type, pick **All tasks** in **Task types**. Each row shows the run's title and number, status, how it was started, when it started and how long it took.

To narrow the list, use any of these:

- The status tabs: **All**, **Queued**, **Running**, **Success**, **Failed** and **Cancelled**, each with its count.
- **Search runs**.
- The **Task type**, **Status**, **Mapping** and **Actor** dropdowns. **Clear filters** resets them.

The filters are part of the page address, so you can bookmark or share a filtered list.

## Read a run's results

To open a run, click it in the list. The run page has these sections:

- **Summary**: the main count and stat tiles, such as messages delivered or records deleted.
- **Inputs**: the options the run was started with.
- **Steps**: one step per unit of work, for example one per channel in a backfill, with live progress.
- **Report (JSON)**: the full report, once the run ends.

Exports, avatar downloads and scrapes add a **Download** link for their file. The file stays in the data directory; if it was removed, the link reads **File no longer on disk**.

## Cancel or delete a run

- To stop a running or queued run, open **Run actions** on its page and click **Cancel run**. A run that does not stop within 30 seconds is marked cancelled anyway.
- To delete a run and its report for good, open **Run actions** and click **Delete**.

**Run actions** also has **Copy run ID** and **Download report**.

Runs are kept until you delete them. If Copycord restarts while a run is queued or running, the run is marked failed with **Interrupted by restart**. To run it again, start a new run of the same type; a backfill can [continue where it stopped](/docs/dashboard/backfill#resuming-an-unfinished-backfill).
