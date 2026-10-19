---
title: Scrape Members
---

# Scrape Members

The **Scrape members** task lists a server's members with the saved scraper tokens and saves them to a JSON file. The scraper talks to Discord with its own user tokens, so the server and client bots do not need to be running.

Open the **Tasks** page and pick **Scrape members** in **Task types**.

## Scraper tokens and proxies

The **Scraper tokens and proxies** card at the top of the Scrape members page holds:

- **Tokens**: one Discord user token per line. Each token must belong to an account that is a member of the server you want to scrape
- **Proxies**: one proxy per line, `host:port` or `scheme://user:pass@host:port`. This is the same proxy list as the **Proxy Settings** card on the Configuration page (see [Proxy support](/docs/configuration/advanced#proxy-support))

Each list is saved by itself shortly after your last edit. Tokens are not validated when saved: a token that does not work or is not in the server is skipped when a scrape starts.

## Running a scrape

1. Click **Run task**
2. Pick a **Server** from the servers the scraper tokens are in (each shows how many tokens can see it), or type its ID in **Or a server ID** for a server not listed
3. Under **Include for each member**, tick what to collect
4. Leave **Use proxies** on to spread the requests over the saved proxies. With no proxies saved, none are used
5. Start the run

If no scraper tokens are saved, the form says **No scraper tokens saved** and points you at the card.

### What it collects

Each member's user ID, plus:

| Option | Default | Description |
|--------|---------|-------------|
| **Username** | On | The member's username |
| **Avatar URL** | On | Avatar image URL |
| **Bio** | Off | "About Me" text. Needs an extra call per member, so the scrape is slower |
| **Roles** | Off | The member's roles |

## Progress and results

The run page shows the steps **Check tokens**, **Scrape members**, **Fetch bios** (with **Bio** on) and **Save file**. The summary counts the members found and the tokens used and skipped.

The result is saved to `scrapes/<guild_id>/scrape_<guild_id>_<timestamp>.json` in the data directory (`/data` in Docker). Download it from the **Download** link on the run page.

**Cancel run** stops the scraper; members found before the cancel are still saved.

## Queue

Scrape members runs one at a time. Start more runs and each waits as **Queued** until the one before it ends.

## Rate limits

The scraper respects Discord's rate limits. Large servers (10,000+ members) may take some time, and **Bio** adds one request per member. Proxies spread the requests.
