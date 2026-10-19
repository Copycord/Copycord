---
sidebar_position: 2
title: Structure sync
---

# Structure sync

Structure sync keeps the clone server's channels, roles, emojis, stickers and server identity in line with the source server. This page explains what is synced, when, and which mapping setting controls each change.

## When the clone is synced

- At startup: when the client starts, it sends the full structure of each source server, and the clone is brought in line. Servers are synced one at a time, 3 seconds apart by default, to stay within Discord's rate limits.
- As changes happen: after startup, Copycord applies each change in a source server, such as a new channel or a renamed role, as soon as Discord reports it. There is no periodic full sync.

To bring a clone's roles, emojis and stickers back in line at any time, run a [Sync assets](/docs/dashboard/purge-and-sync#sync-assets) task.

Each change below is controlled by a mapping setting, shown in the last column. To change these settings, see [Edit a mapping's settings](/docs/dashboard/guild-mappings#edit-a-mappings-settings).

## Channel types

| Type | Structure | Messages |
|------|-----------|----------|
| Text channels | Yes | Yes, with edits and deletes |
| Voice channels | Yes (**Clone Voice Channels**) | No |
| Stage channels | Yes (**Clone Stage Channels**) | No |
| Forum channels | Yes | Yes: posts, tags and guidelines |
| Threads | Created when the source thread exists | Yes |
| Categories | Yes | Not applicable |

## Channels

| Change in the source | Change in the clone | Setting |
|----------------------|---------------------|---------|
| Channel created | Channel created | No setting |
| Channel deleted | Channel deleted | **Delete Removed Channels** |
| Channel renamed | Channel renamed | **Rename Channels** |
| Channel moved | Channel moved | **Reposition Channels** |
| Topic changed | Topic updated | **Sync Channel Topic** |
| NSFW flag changed | NSFW flag updated | **Sync NSFW Flag** |
| Slowmode changed | Slowmode updated | **Sync Slowmode** |
| Permissions changed | Permissions updated | **Mirror Channel Permissions** |
| Voice bitrate or user limit changed | Voice channel updated | **Sync Voice Properties** |
| Stage bitrate or user limit changed | Stage channel updated | **Sync Stage Properties** |
| Forum layout, tags or guidelines changed | Forum updated | **Sync Forum Properties** |

A channel you renamed on the [Channels](/docs/dashboard/channels) page keeps its custom name.

## Threads

| Change in the source | Change in the clone | Setting |
|----------------------|---------------------|---------|
| Thread created | Thread created | No setting |
| Thread deleted | Thread deleted | **Delete Removed Threads** |
| Thread renamed | Thread renamed | No setting |
| Thread archived or unarchived | Thread updated | No setting |

## Roles

| Change in the source | Change in the clone | Setting |
|----------------------|---------------------|---------|
| Role created | Role created | **Clone Roles** |
| Role deleted | Role deleted | **Delete Removed Roles** |
| Name, color or hoist changed | Role updated | **Update Role Properties** |
| Permissions changed | Permissions updated | **Mirror Role Permissions** |
| Position changed | Position updated | **Rearrange Roles** |
| Icon changed | Icon updated | **Clone Role Icons** |
| Style changed | Color style updated: solid, gradient or holographic | **Clone Role Styles** |

## Emojis and stickers

| Change in the source | Change in the clone | Setting |
|----------------------|---------------------|---------|
| Emoji added or removed | Emoji added or deleted | **Clone Emoji** |
| Sticker added or removed | Sticker added or deleted | **Clone Stickers** |

When the clone server reaches Discord's limit, for example 50 emojis on a server without boosts, Copycord stops adding emojis or stickers. It tries again on the next sync.

## Server identity

| Change in the source | Setting |
|----------------------|---------|
| Server icon | **Clone Server Icon** |
| Server banner | **Clone Server Banner** |
| Invite splash | **Clone Invite Splash** |
| Description | **Sync Server Description** |

## Messages during a sync

Messages that arrive for a channel that is not cloned yet wait in a queue. Once the channel and its webhook exist, they are posted in order, so no message is lost during a sync.

If the source structure changes while a sync is running, Copycord starts the sync again with the latest structure.

## Speed and rate limits

Channels and categories are created as fast as Discord allows. When Discord asks Copycord to slow down, it waits the time Discord gives, then continues.

With **On-Demand Webhooks** on, the default, no webhooks are created during the sync, so the structure is ready sooner; each channel gets its webhook with its first message. With it off, webhooks are created after the sync, a short time apart. Roles, emojis and stickers sync at the same time as the channels, and the sync is reported done when all of them have finished.
