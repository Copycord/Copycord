import type { OrphansEvent, Orphans } from "../../../types/channels";

/** Heading line of the verify modal for the current orphan list. */
export function verifyStatusText(o: Orphans, scanned: boolean, scanning: boolean): string {
  if (scanning) return "Scanning…";
  const cats = o.categories.length;
  const chs = o.channels.length;
  if (!cats && !chs) {
    return scanned ? "All channels and categories match the last sitemap." : "Press “Fetch orphans” to scan.";
  }
  return `Found ${cats} orphan ${cats === 1 ? "category" : "categories"} and ${chs} orphan ${chs === 1 ? "channel" : "channels"}.`;
}

/** Parses an "orphans" event; null when it is for another mapping or malformed. */
export function parseOrphans(p: Partial<OrphansEvent> | undefined, mappingId: string): Orphans | null {
  if (!p || p.type !== "orphans") return null;
  const mid = p.mapping_id ? String(p.mapping_id) : "";
  if (mappingId && mid && mappingId !== mid) return null;
  const str = (v: unknown) => String(v ?? "");
  return {
    categories: Array.isArray(p.categories) ? p.categories.map((c) => ({ ...c, id: str(c.id) })) : [],
    channels: Array.isArray(p.channels) ? p.channels.map((c) => ({ ...c, id: str(c.id) })) : [],
  };
}
