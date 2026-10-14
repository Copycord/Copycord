// The one way the SPA calls the dashboard server: JSON in, typed JSON out,
// the CSRF header on every request and a typed error on any non 2xx reply.

export const CSRF_HEADER = "X-Copycord-CSRF";

export class ApiError extends Error {
  readonly status: number;
  readonly body: unknown;
  readonly url: string;

  constructor(status: number, url: string, body: unknown, message?: string) {
    super(message ?? `request to ${url} failed with status ${status}`);
    this.name = "ApiError";
    this.status = status;
    this.url = url;
    this.body = body;
  }
}

export type Method = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";

export interface RequestOptions {
  headers?: Record<string, string>;
  signal?: AbortSignal;
}

function errorMessage(body: unknown): string | undefined {
  if (body && typeof body === "object") {
    const b = body as Record<string, unknown>;
    for (const key of ["error", "detail", "message"]) {
      const value = b[key];
      if (typeof value === "string") return value;
    }
  }
  return typeof body === "string" && body ? body : undefined;
}

async function readBody(res: Response): Promise<unknown> {
  if (res.status === 204) return undefined;
  const text = await res.text();
  if (!text) return undefined;
  const type = res.headers.get("content-type") ?? "";
  if (type.includes("json")) {
    try {
      return JSON.parse(text) as unknown;
    } catch {
      return text;
    }
  }
  return text;
}

// An expired session shows up as a 401 or as a redirect that fetch followed to
// the login page. HTML anywhere else is an unexpected reply, not an expiry.
function sessionExpired(res: Response): boolean {
  if (res.status === 401) return true;
  if (res.redirected && res.url) {
    try {
      return new URL(res.url).pathname.startsWith("/login");
    } catch {
      return false;
    }
  }
  return false;
}

function isHtml(res: Response): boolean {
  return (res.headers.get("content-type") ?? "").includes("text/html");
}

// Several requests usually fail together when the session expires; send the
// browser to the login page only once.
let loginRedirectStarted = false;
let navigate: (url: string) => void = (url) => window.location.assign(url);

function goToLogin(): void {
  if (loginRedirectStarted) return;
  if (window.location.pathname.startsWith("/login")) return;
  loginRedirectStarted = true;
  navigate("/login");
}

// For tests: forget an earlier login redirect and optionally capture the next one.
export function resetLoginRedirect(go?: (url: string) => void): void {
  loginRedirectStarted = false;
  navigate = go ?? ((url) => window.location.assign(url));
}

export async function request<T>(
  method: Method,
  url: string,
  body?: unknown,
  opts: RequestOptions = {},
): Promise<T> {
  const headers = new Headers(opts.headers);
  headers.set(CSRF_HEADER, "1");
  headers.set("Accept", "application/json");
  let payload: BodyInit | undefined;
  if (body instanceof FormData) {
    payload = body;
  } else if (body !== undefined) {
    headers.set("Content-Type", "application/json");
    payload = JSON.stringify(body);
  }
  const res = await fetch(url, {
    method,
    headers,
    body: payload,
    credentials: "same-origin",
    signal: opts.signal,
  });
  const data = await readBody(res);
  if (sessionExpired(res)) {
    goToLogin();
    throw new ApiError(401, url, data, "session expired");
  }
  if (isHtml(res)) {
    // A page where JSON was asked for: fail the call, never navigate.
    throw new ApiError(res.status, url, data, "unexpected HTML response");
  }
  if (!res.ok) {
    throw new ApiError(res.status, url, data, errorMessage(data));
  }
  return data as T;
}

// GETs started before the code that reads them has loaded (a page chunk still downloading). The first
// api.get of the same url within PREFETCH_TTL_MS takes the response instead of asking again.
const PREFETCH_TTL_MS = 10_000;
const prefetched = new Map<string, { at: number; res: Promise<unknown> }>();

export function prefetch(urls: readonly string[]): void {
  for (const url of urls) {
    if (prefetched.has(url)) continue;
    const res = request<unknown>("GET", url);
    res.catch(() => undefined); // the reader sees the error, nobody else
    prefetched.set(url, { at: Date.now(), res });
  }
}

function takePrefetched<T>(url: string): Promise<T> | undefined {
  const hit = prefetched.get(url);
  if (!hit) return undefined;
  prefetched.delete(url);
  return Date.now() - hit.at < PREFETCH_TTL_MS ? (hit.res as Promise<T>) : undefined;
}

export const api = {
  get: <T>(url: string, opts?: RequestOptions) => takePrefetched<T>(url) ?? request<T>("GET", url, undefined, opts),
  post: <T>(url: string, body?: unknown, opts?: RequestOptions) =>
    request<T>("POST", url, body, opts),
  put: <T>(url: string, body?: unknown, opts?: RequestOptions) =>
    request<T>("PUT", url, body, opts),
  patch: <T>(url: string, body?: unknown, opts?: RequestOptions) =>
    request<T>("PATCH", url, body, opts),
  del: <T>(url: string, body?: unknown, opts?: RequestOptions) =>
    request<T>("DELETE", url, body, opts),
};

// The text an error toast should show: the server's own message when it sent
// one, else the caller's fallback with the status or "network error".
export function errorText(e: unknown, fallback: string): string {
  if (e instanceof ApiError) {
    const generic = `request to ${e.url} failed with status ${e.status}`;
    const message = e.message.trim();
    if (message && message !== generic) return message;
    return `${fallback} (${e.status})`;
  }
  if (e instanceof Error && e.message.trim()) return e.message.trim();
  return `${fallback} (network error)`;
}
