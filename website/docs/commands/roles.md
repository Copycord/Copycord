---
sidebar_position: 5
title: Roles
---

# Role commands

These commands give roles to new members of the clone server and alert you when someone joins a server. The commands that mention roles on cloned messages are on the [Webhooks](/docs/commands/webhooks#role-mentions) page. To stop a role from being cloned, use **Blocked roles** in the mapping's [filters](/docs/dashboard/filters#block-roles).

## `/onjoin_role`

Adds a role that new members of this server get when they join, or removes it when it is already an on-join role.

```
/onjoin_role role:<role>
```

| Option | Required | Description |
|--------|----------|-------------|
| `role` | Yes | The role to give on join |

```
/onjoin_role role:@Member
```

To add several on-join roles, run the command once per role. The bot needs the **Manage Roles** permission, and it cannot give roles managed by an integration or roles above its own highest role.

## `/onjoin_roles`

Lists this server's on-join roles, or removes all of them.

```
/onjoin_roles [clear:true]
```

| Option | Required | Description |
|--------|----------|-------------|
| `clear` | No | `true` removes every on-join role. Default: `false` |

```
/onjoin_roles
/onjoin_roles clear:true
```

## `/onjoin_sync`

Goes through the server's current members and gives each one the on-join roles they are missing.

```
/onjoin_sync [include_bots:true] [dry_run:true]
```

| Option | Required | Description |
|--------|----------|-------------|
| `include_bots` | No | `true` also gives the roles to bots. Default: `false` |
| `dry_run` | No | `true` shows what would change without changing any roles. Default: `false` |

```
/onjoin_sync
/onjoin_sync include_bots:true dry_run:true
```

## `/onjoin_dm`

Turns DM alerts on or off for one server. While on, you get a DM with the details of each new member who joins it.

```
/onjoin_dm server_id:<id>
```

| Option | Required | Description |
|--------|----------|-------------|
| `server_id` | Yes | The ID of the server to watch |

```
/onjoin_dm server_id:123456789012345678
```

Alerts work only for servers with 1,000 members or fewer, and your user account (the client) must be a member of the server.
