---
sidebar_position: 9
title: Rewrites
---

# Rewrite commands

Rewrites replace a word or phrase in the cloned messages of one mapping, for example to replace a brand name, censor a word or remove an invite link. They apply to the message text and to most embed text, such as the title, description and fields, after Copycord's other clean-up.

## `/rewrite add`

Adds a rewrite rule for this clone server's mapping. If a rule for the same `source_text` exists, it is updated instead.

```
/rewrite add source_text:<text> replacement_text:<text>
```

| Option | Required | Description |
|--------|----------|-------------|
| `source_text` | Yes | The word or phrase to find. Matching ignores case |
| `replacement_text` | Yes | The text to put in its place |

```
/rewrite add source_text:hello replacement_text:yo
/rewrite add source_text:team rocket replacement_text:team valor
```

With these two rules, the message `hello from team rocket` is cloned as `yo from team valor`.

## `/rewrite list`

Lists the rewrite rules of this clone server's mapping, each with a number.

```
/rewrite list
```

This command has no options.

Use a rule's number with `/rewrite remove`.

## `/rewrite remove`

Deletes a rewrite rule by its number.

```
/rewrite remove rewrite_id:<number>
```

| Option | Required | Description |
|--------|----------|-------------|
| `rewrite_id` | Yes | The number shown by `/rewrite list` |

```
/rewrite remove rewrite_id:2
```
