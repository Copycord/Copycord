// Example body for the Custom Webhook (http) provider, shaped like
// build_http_payload in client/forwarding.py. guild.name is always null there.
// timestamp is the creation time encoded in message_id.

export const HTTP_PAYLOAD_EXAMPLE = {
  version: 1,
  rule_id: "3f9c2a7e5b1d4c8e9a6f0b2d7e4c1a95",
  message_id: "1563191102459760663",
  guild: { id: "1187340561234567890", name: null },
  channel: { id: "1187340561234567999", name: "announcements" },
  author: { id: "908172635401928374", name: "copycord_fan", bot: false },
  content: "Server maintenance starts at 18:00 UTC.",
  embeds: [],
  attachments: [{ url: "https://cdn.discordapp.com/attachments/1187340561234567999/1563191102459760663/schedule.png", filename: "schedule.png" }],
  timestamp: "2026-10-23T14:03:21.512000+00:00",
};
