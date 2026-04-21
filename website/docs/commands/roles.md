---
sidebar_position: 5
title: Roles
---

# Role Commands

Commands for managing roles in your clone server — auto-assignment, blocking, and mention pings.

## On-Join Roles

### `/onjoin_role`

Toggle an on-join role. When enabled, this role is automatically assigned to new members joining the clone server.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `role` | Yes | The role to assign on join |

**Usage:**
```
/onjoin_role @Member
```

Run the command again with the same role to **remove** it.

:::note
- Multiple on-join roles are supported (run the command once per role)
- Managed roles or roles above the bot's top role cannot be assigned
- The bot must have **Manage Roles** permission
:::

---

### `/onjoin_roles`

List all configured on-join roles, or clear them all.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `clear` | No | Set to `true` to remove all on-join roles |

**Usage:**
```
/onjoin_roles
/onjoin_roles clear:true
```

---

### `/onjoin_sync`

Scan all current server members and add any missing on-join roles.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `include_bots` | No | Include bots in the sync (default: false) |
| `dry_run` | No | Show what would change without modifying roles (default: false) |

**Usage:**
```
/onjoin_sync
/onjoin_sync include_bots:true dry_run:true
```

---

### `/onjoin_dm`

Toggle DM notifications when someone joins a specific server. When enabled, you'll receive a DM with the new member's details.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `server_id` | Yes | The Discord server ID to watch |

**Usage:**
```
/onjoin_dm 123456789012345678
```

:::note
Only works for servers with 1,000 or fewer members. Your account connected to Copycord must be a member of the target server.
:::

---

## Role Blocking

### `/role_block`

Block a specific role from being cloned or updated. Use this to prevent certain roles from appearing in your clone.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `role` | No* | The cloned role to block |
| `role_id` | No* | The cloned role ID to block |

*One of `role` or `role_id` is required.

**Usage:**
```
/role_block @SomeRole
/role_block role_id:12345678987654321
```

---

### `/role_block_clear`

Clear all role blocks for the current clone server.

**Usage:**
```
/role_block_clear
```

---

## Role Mentions

Auto-mention roles at the top of cloned messages. See [Webhooks → Role Mentions](/docs/commands/webhooks#role-mentions) for the full `/role_mention` command reference.
