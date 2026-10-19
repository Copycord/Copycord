---
title: Tasks
---

# Tasks

The **Tasks** page runs Copycord's long jobs: history imports, exports, purges, asset syncs, avatar downloads and member scrapes. Each job you start is a **run** with its own page, live progress and a report.

## Task types

The **Task types** list on the left picks a type. **All tasks** shows the runs of every type.

| Task type | What it does | Needs the bots |
|-----------|--------------|----------------|
| [Backfill channels](/docs/dashboard/backfill) | Clone the message history of selected channels into their clones | Yes |
| [Purge messages](/docs/dashboard/purge-and-sync#purge-messages) | Delete Copycord's stored message records older than a cutoff, or all of them. Discord messages are not touched | No |
| [Purge assets](/docs/dashboard/purge-and-sync#purge-assets) | Delete emojis, stickers and roles from a clone server | Yes |
| [Sync assets](/docs/dashboard/purge-and-sync#sync-assets) | Bring a mapping's cloned roles, emojis and stickers in line with the host | Yes |
| [Export messages](/docs/dashboard/exports#export-messages) | Export a server's messages, filtered, to a JSON file and optionally a webhook | Yes |
| [Export DMs](/docs/dashboard/exports#export-dms) | Export a user's DM history from the client account to a JSON file or a webhook | Yes |
| [Download avatars](/docs/dashboard/exports#download-avatars) | Zip the profile pictures of every member the client account can see in one of its servers | Yes |
| [Scrape members](/docs/dashboard/scraper) | List a server's members with the scraper tokens and save them to a JSON file | No |

A type that needs the bots keeps its **Run task** button disabled, with **Start the bots first** beside it, until both the server and the client are running. If the bots stop while such a run is queued or running, the run ends as cancelled with the note **Bots stopped**.

**Download avatars** and **Scrape members** run one at a time. A second run of the same type waits as **Queued** until the first one ends.

## Starting a run

1. Pick a type in **Task types**.
2. Click **Run task**. A form opens with the type's options.
3. Fill in the form and start the run. Forms that delete something ask you to confirm first.

The run page opens as soon as the run is created.

The **Guilds** page also starts runs: **Export messages** on a guild card opens the Export messages form with that server picked.

## The run list

The list shows the runs of the selected type, newest first, and updates live. Each row shows the run title and number, its status, how it was triggered, when it started and how long it ran.

Narrow the list with:

- The status tabs: **All**, **Queued**, **Running**, **Success**, **Failed** and **Cancelled**, each with its count
- **Search runs**
- The **Task type**, **Status**, **Mapping** and **Actor** dropdowns, and **Clear filters**

The filters are kept in the page URL, so a filtered list can be bookmarked or shared.

## The run page

Click a run to open it at `/tasks/runs/<id>`. The page shows:

- **Summary**: the run's main count and stat tiles, such as messages delivered or records deleted
- **Inputs**: the options the run was started with
- **Steps**: one step per unit of work, for example one per channel in a backfill, with progress while it runs
- **Report (JSON)**: the full report, available once the run finishes

Types that produce a file (exports, avatar zips and scrapes) add a **Download** link on the run page. The file stays in the data directory; the link reads **File no longer on disk** if it was removed.

The **Run actions** menu has **Copy run ID**, **Download report**, **Cancel run** while the run is active, and **Delete**. Deleting a run removes the run and its report and cannot be undone.

## Cancelling and restarts

**Cancel run** asks the run to stop. A run that does not stop within 30 seconds is marked cancelled anyway.

Runs are kept until you delete them. If Copycord restarts while a run is queued or running, that run is marked failed with **Interrupted by restart**. Start it again from its type page; a backfill can resume where it stopped (see [Resuming](/docs/dashboard/backfill#resuming-an-unfinished-backfill)).
