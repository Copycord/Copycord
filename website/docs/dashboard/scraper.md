---
title: Scrape members
---

# Scrape members

The **Scrape members** task lists a server's members and saves them to a JSON file. It uses its own Discord user tokens, the scraper tokens, so the server bot and the client do not need to be running.

## Add scraper tokens

1. Open the **Tasks** page and pick **Scrape members** in **Task types**.
2. In the **Scraper tokens and proxies** card, under **Tokens**, enter one Discord user token per line. Each token's account must be a member of the server you want to scrape.
3. Optional: under **Proxies**, enter one proxy per line, as `host:port` or `scheme://user:pass@host:port`.

Each list saves itself shortly after your last edit. Tokens are not checked when saved; a token that does not work or is not in the server is skipped when a scrape starts.

The proxy list is the same one as the **Proxy Settings** card on the **Configuration** page; see [Proxy support](/docs/configuration/advanced#proxy-support).

## Scrape a server

1. On the **Scrape members** page, click **Run task**. If no scraper tokens are saved, the form says **No scraper tokens saved** and links to the card.
2. In **Server**, pick the server. Each server shows how many tokens can see it. For a server that is not listed, enter its ID in **Or a server ID**.
3. Under **Include for each member**, tick what to collect. See [What a scrape collects](#what-it-collects).
4. Optional: turn off **Use proxies**. When on, the requests are spread over the saved proxies; with no proxies saved, none are used.
5. Click **Scrape members**.

The run page shows the steps **Check tokens**, **Scrape members**, **Fetch bios** (when **Bio** is ticked) and **Save file**. The summary counts the members found and the tokens used and skipped.

When the run ends, download the file from the **Download** link on the run page. It is saved as `scrapes/<guild_id>/scrape_<guild_id>_<timestamp>.json` in the data directory (`/data` in Docker).

Scrapes run one at a time. Each further run waits as **Queued** until the one before it ends. **Cancel run** stops a scrape and still saves the members found so far.

## What it collects

Every scrape includes each member's user ID, plus:

| Option | Default | Data |
|--------|---------|------|
| **Username** | On | The member's username |
| **Avatar URL** | On | The address of the member's avatar image |
| **Bio** | Off | The member's "About Me" text |
| **Roles** | Off | The member's roles |

Large servers, with 10,000 members or more, can take a while. **Bio** adds one request per member, which makes the scrape slower. Copycord waits whenever Discord asks it to slow down, and proxies spread the requests over more addresses.
