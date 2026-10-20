---
sidebar_position: 6
title: Search
---

# Search command

Cloned messages are posted by webhooks, so Discord's own `from:` search filter can't find who wrote them. `/search` finds them by their original author instead, along with channel, date, and attachments or embeds. It searches only the clone server where you run it.

Copycord does not store message text, so `/search` can't look for words. To find words, use Discord's own search in the clone server.

## `/search`

Lists cloned messages that match every filter you set, newest first, with a link to jump to each one.

```
/search [author:<author>] [channel:<channel>] [after:<date>] [before:<date>] [has:<attachments|embeds>]
```

| Option | Required | Description |
|--------|----------|-------------|
| `author` | No | The original author. Start typing a name, username or user ID and pick from the list, which shows `Name (@username)` |
| `channel` | No | Only messages in this text channel, announcement channel or thread |
| `after` | No | Only messages sent on or after this date |
| `before` | No | Only messages sent on or before this date |
| `has` | No | `attachments` or `embeds`: only messages that have them |

Set at least one option. Dates are in UTC and take these forms:

- `YYYY-MM-DD`, such as `2026-10-01`. For `after` this is the start of that day; for `before` it is the end of that day.
- `today` or `yesterday`, read the same way as a date.
- A time back from now: a number and `m` (minutes), `h` (hours), `d` (days) or `w` (weeks), such as `30m`, `24h`, `7d` or `2w`.

```
/search author:Ann (@ann)
/search author:Ann (@ann) channel:#general after:7d
/search channel:#media has:attachments before:2026-10-01
```

Only you see the reply. It shows 10 messages per page: the author, the channel, how long ago it was sent, 📎 with the number of attachments, 🔗 with the number of embeds, and the message link, which Discord shows as a clickable link to the message. The footer shows the page number and your filters. When there are more results, use **Prev** and **Next** to move between pages. Only you can press them, and they stop working after 10 minutes; run the command again to keep going.

Messages cloned before Copycord recorded authors have no author, so an `author` search does not find them. Messages that were never posted in the clone are left out.
