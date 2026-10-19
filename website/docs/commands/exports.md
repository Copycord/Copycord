---
sidebar_position: 8
title: Exports and debugging
---

# Exports and debugging

These commands collect information for troubleshooting and bug reports. Exports are not slash commands: message exports, DM exports and avatar downloads run as tasks on the dashboard; see [Exports and downloads](/docs/dashboard/exports).

## `/mapping_debug`

Shows the stored data and settings of this clone server's guild mapping.

```
/mapping_debug
```

This command has no options.

## `/debug_report`

Makes a ZIP file with a JSON copy of every Copycord database table, one file per table, to attach to a bug report.

```
/debug_report
```

This command has no options.

Only you can see the reply. Secrets are removed: tokens, webhook URLs, passwords, API keys and similar columns, and settings whose name marks them as secret. The file still describes your servers and mappings, so share it only with people you trust.
