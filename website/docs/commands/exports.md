---
sidebar_position: 8
title: Exports and Debugging
---

# Exports and Debugging

Commands for debug information. Exports are not slash commands: message exports, DM exports and avatar downloads run as tasks on the dashboard's Tasks page. See [Exports and Downloads](/docs/dashboard/exports).

## `/mapping_debug`

Show a debug view of the current clone's guild mapping. Displays internal mapping data and settings for troubleshooting.

**Usage:**
```
/mapping_debug
```

---

## `/debug_report`

Generate a ZIP with a JSON dump of every Copycord database table, one file per table, for troubleshooting or a bug report.

**Usage:**
```
/debug_report
```

Obvious secrets (tokens, webhook URLs, passwords, API keys and similar columns, and configuration values whose key looks secret) are redacted. The reply is only visible to you. The dump still describes your servers and mappings, so share it only with people you trust.
