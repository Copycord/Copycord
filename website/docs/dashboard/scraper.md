---
sidebar_position: 8
title: Member Scraper
---

# Member Scraper

The member scraper exports user data from any Discord server your account has access to.

## What it exports

For each member, the scraper collects:

- **User ID**
- **Username** and display name
- **Avatar URL**
- **Bio/About Me** text
- **Join date** (when they joined the server)

## Setting up the scraper

### Add scraper tokens

The scraper uses Discord user tokens to fetch member data. You can add multiple tokens for parallel scraping:

1. Go to the **Scraper** page in the dashboard
2. Click **Add Token**
3. Paste a Discord user token
4. Click **Validate** to verify it works
5. Repeat for additional tokens (optional — more tokens = faster scraping)

:::tip
Each token must belong to an account that is a member of the server you want to scrape.
:::

### Configure proxies (optional)

For large servers, you can add proxy servers to distribute requests:

1. On the Scraper page, click the proxy settings
2. Add proxy URLs (one per line)
3. Save

## Running a scrape

1. Select the **server** to scrape
2. Click **Start Scraper**
3. Monitor progress in real time
4. When complete, download the results

## Managing scrape results

- **View** — Browse results directly in the dashboard
- **Download** — Export results as JSON
- **Delete** — Remove old scrape files

## Scraper queue

You can queue multiple scrape jobs. The queue processes them one at a time to avoid rate limits.

- **Queue** — View pending scrape jobs
- **Cancel** — Cancel a specific job or all jobs
- **Clear** — Remove all queued jobs

## Rate limits

The scraper respects Discord's rate limits automatically. Large servers (10,000+ members) may take some time to complete. Using multiple tokens speeds up the process.
