---
sidebar_position: 4
title: Member scraping
---

# Member scraping

The member scraper lists the members of a Discord server and saves them to a JSON file. This page explains what it collects and how it works. To run a scrape, see [Scrape members](/docs/dashboard/scraper).

## What it collects

| Data | Included |
|------|----------|
| User ID | Always |
| Username | On by default |
| Avatar URL | On by default |
| Bio, the "About Me" text | Off by default; makes the scrape slower |
| Roles | Off by default |

## How it works

The scraper uses its own Discord user tokens, the scraper tokens, instead of the server bot and the client. It talks to Discord directly, so the bots do not need to be running. Each token's account must be a member of the server you scrape.

A scrape runs as a task on the **Tasks** page:

1. It checks which saved tokens can see the server. A token that fails or is not in the server is skipped.
2. It fetches the member list.
3. It fetches each member's bio, when **Bio** is on.
4. It saves the JSON file under `scrapes/<guild_id>/` in the data directory.

Scrapes run one at a time. You can start several, for several servers, and each waits its turn.

## How long a scrape takes

- Larger servers take longer.
- **Bio** needs one extra request per member.
- Discord limits how fast requests can be made. The scraper waits when Discord asks it to.
- Proxies spread the requests over more addresses, which helps with those limits. The scraper uses the same proxy list as the rest of Copycord; see [Proxy support](/docs/configuration/advanced#proxy-support).

Each scrape is kept as a run on the **Tasks** page until you delete it from its **Run actions** menu.
