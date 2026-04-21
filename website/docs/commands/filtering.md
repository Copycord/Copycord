---
sidebar_position: 3
title: Filtering
---

# Filtering Commands

Control which messages get cloned by blocking specific keywords.

## `/block_add`

Toggles a keyword in the block list for the current clone server. Messages containing blocked keywords will be silently dropped and not forwarded to the clone.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `keyword` | Yes | The keyword to block (or unblock if already blocked) |

**Usage:**
```
/block_add spoiler
/block_add giveaway
```

Running the command again with the same keyword **removes** it from the block list (toggle behavior).

:::info
Keyword matching is case-insensitive. Blocking "spoiler" will also catch "SPOILER" and "Spoiler".
:::

---

## `/block_list`

Lists all currently blocked keywords for the current clone server.

**Usage:**
```
/block_list
```

Displays all active keyword blocks for the source → clone mapping of the server where you run the command.
