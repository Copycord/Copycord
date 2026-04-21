---
sidebar_position: 9
title: Rewrites
---

# Rewrite Commands

Configure simple find-and-replace rules that modify cloned messages for a specific mapping. Rewrites are applied after other sanitization and affect both message content and most embed text (title, description, fields, etc.).

## `/rewrite add`

Add or update a word/phrase rewrite for the current clone mapping.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `source_text` | Yes | Word or phrase to search for (case-insensitive) |
| `replacement_text` | Yes | Text to replace it with |

If a rewrite with the same `source_text` already exists, it will be **updated** instead of creating a duplicate.

**Examples:**
```
/rewrite add source_text:"hello" replacement_text:"yo"
/rewrite add source_text:"team rocket" replacement_text:"team valor"
```

With these rules configured, a cloned message like:
```
hello from team rocket
```
becomes:
```
yo from team valor
```

---

## `/rewrite list`

List all word/phrase rewrites for the current clone mapping.

Each entry shows a numeric **ID** and its mapping:

```
[1] `hello` → `yo`
[2] `team rocket` → `team valor`
```

Use the ID with `/rewrite remove` to delete a rule.

**Usage:**
```
/rewrite list
```

---

## `/rewrite remove`

Remove a rewrite rule by its numeric ID (shown in `/rewrite list`).

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `rewrite_id` | Yes | The ID of the rule to delete |

**Usage:**
```
/rewrite remove rewrite_id:2
```

:::tip Use cases
- **Branding** — Replace server names, URLs, or brand terms
- **Censoring** — Replace inappropriate words with alternatives
- **Localization** — Translate common phrases
- **De-linking** — Replace invite links or external URLs
:::
