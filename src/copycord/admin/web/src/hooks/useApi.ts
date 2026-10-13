// Loading/error state on top of lib/api.ts.
//   const { data, loading, error, reload } = useApi<Guild[]>("/api/guilds");
//   const [save, { loading }] = useApiCall((body: Cfg) => api.post<Ok>("/api/config", body));

import { useCallback, useEffect, useRef, useState } from "react";
import { api, ApiError } from "../lib/api";

export interface ApiState<T> {
  data: T | undefined;
  error: ApiError | Error | undefined;
  loading: boolean;
  reload: () => void;
  setData: (data: T | undefined) => void;
}

/** GETs `url` on mount and whenever it changes; pass null to skip. */
export function useApi<T>(url: string | null): ApiState<T> {
  const [data, setData] = useState<T | undefined>(undefined);
  const [error, setError] = useState<ApiError | Error | undefined>(undefined);
  const [loading, setLoading] = useState(url !== null);
  const [tick, setTick] = useState(0);

  useEffect(() => {
    if (url === null) {
      setLoading(false);
      return;
    }
    const ctl = new AbortController();
    setLoading(true);
    setError(undefined);
    api
      .get<T>(url, { signal: ctl.signal })
      .then((d) => {
        if (!ctl.signal.aborted) setData(d);
      })
      .catch((e: unknown) => {
        if (ctl.signal.aborted) return;
        setError(e instanceof Error ? e : new Error(String(e)));
      })
      .finally(() => {
        if (!ctl.signal.aborted) setLoading(false);
      });
    return () => ctl.abort();
  }, [url, tick]);

  const reload = useCallback(() => setTick((t) => t + 1), []);
  return { data, error, loading, reload, setData };
}

export interface ApiCallState {
  loading: boolean;
  error: ApiError | Error | undefined;
}

/** Wraps a mutation; `run` rethrows so callers can toast or ignore. */
export function useApiCall<A extends unknown[], R>(
  fn: (...args: A) => Promise<R>,
): [(...args: A) => Promise<R>, ApiCallState] {
  const [state, setState] = useState<ApiCallState>({ loading: false, error: undefined });
  const fnRef = useRef(fn);
  fnRef.current = fn;
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const run = useCallback(async (...args: A) => {
    setState({ loading: true, error: undefined });
    try {
      const out = await fnRef.current(...args);
      if (mounted.current) setState({ loading: false, error: undefined });
      return out;
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      if (mounted.current) setState({ loading: false, error: err });
      throw err;
    }
  }, []);

  return [run, state];
}
