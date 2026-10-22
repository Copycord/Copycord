// Payloads and state logic of the row actions: category id lookup, the
// customize and blacklist saves, the orphan deletes.

import type {
  BlacklistBody,
  CategoryIds,
  ChannelRow,
  CustomizeCategoryBody,
  CustomizeChannelBody,
  DeletedResult,
  Orphans,
  OrphanDeletePayload,
} from "../../../types/channels";
import type { CategoryPins } from "../model";

const str = (v: unknown): string | null => {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
};

/** Ids of the category with this original (or customized) name, from the first row that has them. */
export function resolveCategoryIds(rows: readonly ChannelRow[], name: string, pins: CategoryPins): CategoryIds {
  const raw = name.trim();
  const out: CategoryIds = { originalCatId: null, clonedCatId: null, originalGuildId: null, clonedGuildId: null };
  if (!raw) return out;
  const want = (pins.origByEither.get(raw.toLowerCase()) ?? raw).trim().toLowerCase();
  for (const r of rows) {
    const names = [r.original_category_name, r.category_original_name, r.category_upstream_name, r.category_name];
    if (!names.some((n) => n && n.trim().toLowerCase() === want)) continue;
    out.originalCatId ??= str(r.original_category_id);
    out.clonedCatId ??= str(r.cloned_category_id);
    out.originalGuildId ??= str(r.original_guild_id);
    out.clonedGuildId ??= str(r.cloned_guild_id);
    if (out.originalCatId && out.clonedCatId && out.originalGuildId && out.clonedGuildId) break;
  }
  return out;
}

/** Empty or blank input clears the custom name. */
export function customizeChannelBody(row: ChannelRow, input: string): CustomizeChannelBody | null {
  const cgid = str(row.cloned_guild_id);
  if (!cgid) return null;
  return {
    original_channel_id: String(row.original_channel_id),
    cloned_guild_id: cgid,
    clone_channel_name: input.trim() || null,
  };
}

export function customizeCategoryBody(ids: CategoryIds, input: string): CustomizeCategoryBody | null {
  if (!ids.originalCatId || !ids.clonedGuildId) return null;
  return {
    original_category_id: ids.originalCatId,
    cloned_guild_id: ids.clonedGuildId,
    custom_category_name: input.trim() || null,
  };
}

export function blacklistBody(
  scope: BlacklistBody["scope"],
  objId: string,
  mappingId: string,
  originalGuildId: string | null | undefined,
  clonedGuildId: string | null | undefined,
): BlacklistBody {
  return {
    scope,
    obj_id: String(objId),
    mapping_id: String(mappingId),
    original_guild_id: String(originalGuildId ?? ""),
    cloned_guild_id: String(clonedGuildId ?? ""),
  };
}

export function deleteOnePayload(kind: "channel" | "category", id: string): OrphanDeletePayload {
  return { action: "delete_one", kind, id: String(id) };
}

/** Categories first, then channels. */
export function orphanIds(o: Orphans): string[] {
  return [...o.categories.map((c) => String(c.id)), ...o.channels.map((c) => String(c.id))];
}

export function deleteAllPayload(o: Orphans): OrphanDeletePayload {
  return { action: "delete_all", ids: orphanIds(o) };
}

export function unlinkPayload(row: ChannelRow): OrphanDeletePayload {
  return { action: "unlink_channel", original_channel_id: String(row.original_channel_id) };
}

/** Convert to orphan is offered on a cloned channel that is still mapped, never on a thread. */
export function canConvertToOrphan(row: ChannelRow | null | undefined): row is ChannelRow {
  return !!row && !row.is_thread && !!str(row.cloned_channel_id) && !!str(row.cloned_guild_id);
}

/** The toast text of a Convert to orphan that did not go through. */
export function unlinkFailureText(name: string, reason: string | undefined): string {
  switch (reason) {
    case "not_mapped":
      return `"${name}" is not linked to a source channel any more.`;
    case "not_found":
      return `"${name}" was not found in the clone server.`;
    case "backfilling":
      return `"${name}" is being cloned by a task. Try again when it ends.`;
    case "guild_not_found":
    case "no_mapping":
      return "The clone server of this mapping is not available.";
    default:
      return `Failed to convert "${name}" to an orphan.`;
  }
}

/** The /ws/in frame for an orphan delete. */
export function verifyEnvelope(mappingId: string, payload: OrphanDeletePayload) {
  return { kind: "verify", role: "ui", payload: { mapping_id: mappingId, ...payload } };
}

export function resultId(r: DeletedResult): string | null {
  return str(r.id ?? r.channel_id ?? r.category_id ?? r.target_id ?? r.orphan_id ?? r.original_id);
}

export function wasDeleted(r: DeletedResult): boolean {
  const s = String(r.status ?? "").toLowerCase();
  return r.deleted === true || r.ok === true || r.success === true || s === "deleted" || s === "ok";
}

/** The toast text of one result that was not deleted. */
export function failureText(r: DeletedResult): string {
  const name = r.name ?? r.channel_name ?? r.category_name ?? "Item";
  switch (r.reason) {
    case "protected":
      return `"${name}" can't be deleted. Manual action required.`;
    case "not_found":
      return `"${name}" was not found.`;
    case "not_category":
    case "not_channel":
      return `"${name}" could not be deleted (wrong type).`;
    default:
      return `Failed to delete "${name}".`;
  }
}

/** Orphans left after the deleted ids are removed. */
export function withoutDeleted(o: Orphans, deleted: ReadonlySet<string>): Orphans {
  return {
    categories: o.categories.filter((c) => !deleted.has(String(c.id))),
    channels: o.channels.filter((c) => !deleted.has(String(c.id))),
  };
}
