---
sidebar_position: 4
title: Member Scraping
---

# Member Scraping

Copycord includes a member scraper that exports user data from any Discord server your account has access to.

## What it collects

| Data | Description |
|------|-------------|
| **User ID** | Discord snowflake ID |
| **Username** | Discord username |
| **Display Name** | Server-specific nickname or global display name |
| **Avatar** | Avatar URL |
| **Bio** | "About Me" text |
| **Join Date** | When the user joined the server |

## How to use it

### 1. Add scraper tokens

Navigate to the **Scraper** page in the dashboard and add one or more Discord user tokens. Each token must belong to an account that is a member of the target server.

Multiple tokens enable parallel scraping for faster results.

### 2. Configure proxies (optional)

For large servers or to distribute load, add proxy servers:

- HTTP proxies: `http://user:pass@host:port`
- SOCKS5 proxies: `socks5://user:pass@host:port`

### 3. Start a scrape

Select the target server and click **Start**. The scraper will:

1. Fetch the member list using the Discord API
2. Collect profile data for each member
3. Respect rate limits automatically
4. Report progress in real time

### 4. Download results

When the scrape completes, download the results as JSON.

## Scraper queue

Queue multiple scrape jobs to process sequentially. This is useful for scraping multiple servers without monitoring each one.

## Performance

Scraping speed depends on:

- **Server size** — Larger servers take longer
- **Number of tokens** — More tokens = faster parallel fetching
- **Proxies** — Distribute requests to avoid rate limits
- **Discord rate limits** — Automatically handled with backoff

A server with 10,000 members typically takes a few minutes with one token.

## Managing results

The dashboard provides tools to:

- **Browse** scrape results directly
- **Download** as JSON files
- **Delete** old scrape files
- **Clear** scraper logs
