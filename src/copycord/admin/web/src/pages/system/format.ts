// Formatting copied from the old system.js so sizes and times read the same.

export const SHRUG = String.raw`¯\_(ツ)_/¯`;
/** Shown before the info has loaded or for an unreadable time. */
export const NO_VALUE = "—";

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

export function fmtBytes(n: number | null | undefined): string {
  const b = Number(n || 0);
  if (b < 1024) return `${b.toLocaleString()} B`;
  const kb = b / 1024;
  if (kb < 1024) return `${kb.toFixed(kb < 10 ? 1 : 0)} KB`;
  const mb = kb / 1024;
  if (mb < 1024) return `${mb.toFixed(mb < 10 ? 1 : 0)} MB`;
  const gb = mb / 1024;
  return `${gb.toFixed(gb < 10 ? 1 : 0)} GB`;
}

/** Today: "3:07pm"; any other day: "Oct 9 2026". Numbers are unix seconds. */
export function fmtBackupWhen(input: number | string | Date | null | undefined, now: Date = new Date()): string {
  let d: Date;
  if (typeof input === "number") d = new Date(input * 1000);
  else if (typeof input === "string") d = new Date(input);
  else if (input instanceof Date) d = input;
  else return NO_VALUE;
  if (Number.isNaN(d.getTime())) return NO_VALUE;

  const sameDay =
    d.getFullYear() === now.getFullYear() && d.getMonth() === now.getMonth() && d.getDate() === now.getDate();
  if (sameDay) {
    const h = d.getHours();
    const ampm = h >= 12 ? "pm" : "am";
    const m = d.getMinutes().toString().padStart(2, "0");
    return `${h % 12 || 12}:${m}${ampm}`;
  }
  return `${MONTHS[d.getMonth()]} ${d.getDate()} ${d.getFullYear()}`;
}

/** Archive name without the .tar.gz suffix. */
export function archiveLabel(name: string): string {
  return name.replace(/\.tar\.gz$/i, "");
}
