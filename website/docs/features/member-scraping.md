---
sidebar_position: 4
title: Member Scraping
---

# Member Scraping

Copycord includes a member scraper that lists the members of a Discord server your scraper accounts are in and saves them to a JSON file. It runs as the **Scrape members** task on the [Tasks](/docs/dashboard/scraper) page.

## What it collects

| Data | Description |
|------|-------------|
| **User ID** | Discord snowflake ID, always included |
| **Username** | Discord username (on by default) |
| **Avatar URL** | Avatar image URL (on by default) |
| **Bio** | "About Me" text (off by default; slower) |
| **Roles** | The member's roles (off by default) |

## How to use it

### 1. Add scraper tokens

Open the **Tasks** page, pick **Scrape members** and expand the **Scraper tokens and proxies** card. Add one Discord user token per line under **Tokens**. Each token must belong to an account that is a member of the target server.

The scraper uses its own tokens and talks to Discord directly, so the server and client bots do not need to be running.

### 2. Configure proxies (optional)

Add proxies under **Proxies** in the same card, one per line. This is the same list as the **Proxy Settings** card on the Configuration page (see [Proxy support](/docs/configuration/advanced#proxy-support)).

### 3. Start a scrape

Click **Run task**, pick the server (or enter its ID), choose what to include and start the run. The scraper:

1. Checks which saved tokens can see the server; a token that fails or is not in the server is skipped
2. Fetches the member list
3. Fetches bios, if **Bio** is on
4. Saves the JSON file

### 4. Download results

The run page has a **Download** link for the JSON file. Files are saved under `scrapes/<guild_id>/` in the data directory.

## Queue

Scrape runs go one at a time. Start several and each waits as queued until the one before it ends, so you can scrape several servers without watching each one.

## Performance

Scraping speed depends on:

- **Server size**: larger servers take longer
- **Bios**: fetching bios needs one extra request per member
- **Proxies**: spread requests to avoid rate limits
- **Discord rate limits**: handled automatically

## Managing results

Each scrape is a run on the Tasks page. Delete a run from its **Run actions** menu when you no longer need it.
