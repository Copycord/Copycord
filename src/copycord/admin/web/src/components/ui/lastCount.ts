// How many rows a list had when it last loaded, so its skeleton draws that many and the rows land
// without moving anything. Kept in localStorage under skel:<key>; a first visit uses the fallback.
//   const rows = readLastCount("scraper.tokens", 0, 6);   useLastCount("scraper.tokens", list.length)

import { useEffect } from "react";

const PREFIX = "skel:";

export function readLastCount(key: string, fallback: number, max: number): number {
  try {
    const raw = localStorage.getItem(PREFIX + key);
    const n = raw === null ? fallback : Number(raw);
    return Number.isInteger(n) && n >= 0 ? Math.min(n, max) : fallback;
  } catch {
    return fallback;
  }
}

/** Stores `count` once it is known (undefined while the list loads). */
export function useLastCount(key: string, count: number | undefined): void {
  useEffect(() => {
    if (count === undefined) return;
    try {
      localStorage.setItem(PREFIX + key, String(count));
    } catch {
      // Private mode or a full quota: the skeleton falls back to its default.
    }
  }, [key, count]);
}
