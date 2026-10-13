// Run a save SAVE_DELAY_MS after the last call; a newer call replaces a pending one.

import { useCallback, useEffect, useRef } from "react";
import { SAVE_DELAY_MS } from "./model";

export function useDebouncedSave<T>(save: (value: T) => Promise<unknown>): (value: T) => void {
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const latest = useRef(save);
  latest.current = save;
  useEffect(() => () => clearTimeout(timer.current), []);
  return useCallback((value: T) => {
    clearTimeout(timer.current);
    timer.current = setTimeout(() => {
      latest.current(value).catch((e: unknown) => console.error(e));
    }, SAVE_DELAY_MS);
  }, []);
}
