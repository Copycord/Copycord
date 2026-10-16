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

/** A list of counts (rows per group, as "4,2,7"), each capped at maxEach and at most maxGroups groups. */
export function readLastShape(key: string, fallback: number[], maxGroups: number, maxEach: number): number[] {
  try {
    const raw = localStorage.getItem(PREFIX + key);
    if (raw === null) return fallback;
    const parts = raw === "" ? [] : raw.split(",").map(Number);
    if (!parts.every((n) => Number.isInteger(n) && n >= 0)) return fallback;
    return parts.slice(0, maxGroups).map((n) => Math.min(n, maxEach));
  } catch {
    return fallback;
  }
}

/** Stores `shape` once it is known (undefined while the list loads). */
export function useLastShape(key: string, shape: number[] | undefined): void {
  useLastStored(key, shape?.join(","));
}

/** Stores `count` once it is known (undefined while the list loads). */
export function useLastCount(key: string, count: number | undefined): void {
  useLastStored(key, count === undefined ? undefined : String(count));
}

function useLastStored(key: string, value: string | undefined): void {
  useEffect(() => {
    if (value === undefined) return;
    try {
      localStorage.setItem(PREFIX + key, value);
    } catch {
      // Private mode or a full quota: the skeleton falls back to its default.
    }
  }, [key, value]);
}

/** Stores a count outside render, for lists that load in a callback. */
export function writeLastCount(key: string, count: number): void {
  try {
    localStorage.setItem(PREFIX + key, String(count));
  } catch {
    // As above.
  }
}
