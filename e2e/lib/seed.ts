import { execFileSync } from "node:child_process";
import path from "node:path";
import type { APIRequestContext } from "@playwright/test";
import { CSRF_HEADERS, E2E_DIR, HOME } from "./env.ts";

export const SEED = {
  guildId: "111111111111111111",
  clonedGuildId: "222222222222222222",
  mappingName: "E2E Mapping",
  ruleLabel: "E2E Forward",
  webhook: "https://discord.com/api/webhooks/123456789012345678/e2e-webhook-token",
  backupNote: "E2E backup",
};

/**
 * Mappings and backup tokens are checked against Discord by their API routes, so they go
 * straight into the throwaway DB through copycord's own DBManager. Everything else uses the API.
 */
export function seedDb(python: string): void {
  execFileSync(python, ["-I", path.join(E2E_DIR, "scripts", "seed_db.py")], {
    env: { ...process.env, COPYCORD_HOME: HOME },
    stdio: "inherit",
  });
}

export async function seedApi(api: APIRequestContext): Promise<void> {
  const post = async (url: string, data: unknown) => {
    const r = await api.post(url, { data, headers: CSRF_HEADERS });
    if (!r.ok()) throw new Error(`seed ${url} -> ${r.status()} ${await r.text()}`);
  };
  await post("/api/forwarding", {
    label: SEED.ruleLabel,
    guild_id: SEED.guildId,
    provider: "discord",
    enabled: true,
    config: { urls: [SEED.webhook] },
    filters: {},
  });
  const events: Array<[string, string]> = [
    ["channel_created", "Created channel #general"],
    ["channel_deleted", "Deleted channel #old-news"],
    ["role_created", "Created role Moderator"],
    ["category_created", "Created category Text Channels"],
  ];
  for (const [event_type, details] of events) {
    await post("/api/event-logs", {
      event_type,
      details,
      guild_id: SEED.clonedGuildId,
      guild_name: "E2E Clone",
      channel_name: "general",
    });
  }
}
