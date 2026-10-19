---
sidebar_position: 3
title: Filtering
---

# Filtering commands

These commands block messages that contain a keyword. The block list belongs to the mapping of the clone server where you run the command. The same list is **Blocked Words / Phrases** in the mapping's [filters](/docs/dashboard/filters).

## `/block_add`

Adds a keyword to the block list, or removes it when it is already there. Messages that contain a blocked keyword are not cloned.

```
/block_add keyword:<keyword>
```

| Option | Required | Description |
|--------|----------|-------------|
| `keyword` | Yes | The keyword to block, or to unblock when it is already blocked |

```
/block_add keyword:spoiler
/block_add keyword:giveaway
```

Matching ignores case: blocking `spoiler` also blocks `SPOILER` and `Spoiler`.

## `/block_list`

Lists the blocked keywords of this clone server's mapping.

```
/block_list
```

This command has no options.
