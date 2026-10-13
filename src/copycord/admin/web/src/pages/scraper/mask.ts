// Token masking. The server masks before it sends (app.py _mask_token); this
// mirrors that rule so the page never shows more than 6 + 4 characters even
// if a reply ever carries a value that was not masked.

export const ELLIPSIS = "…";

/** app.py _mask_token: up to 10 chars keep 2, longer keep the first 6 and last 4. */
export function maskToken(tok: string | null | undefined): string {
  const t = String(tok ?? "");
  if (!t) return "";
  if (t.length <= 10) return `${t.slice(0, 2)}${ELLIPSIS}`;
  return `${t.slice(0, 6)}${ELLIPSIS}${t.slice(-4)}`;
}

/** The server's mask when it looks masked, otherwise masked here. */
export function displayMask(masked: string | null | undefined): string {
  const m = String(masked ?? "");
  return m.includes(ELLIPSIS) && m.length <= 11 ? m : maskToken(m);
}
