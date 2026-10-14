import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, CSRF_HEADER, api, errorText, prefetch, resetLoginRedirect } from "./api";

function mockFetch(status: number, body: unknown, type = "application/json") {
  const text = typeof body === "string" ? body : JSON.stringify(body);
  const fn = vi.fn(async () => new Response(text, { status, headers: { "content-type": type } }));
  vi.stubGlobal("fetch", fn);
  return fn;
}

function sentInit(fn: ReturnType<typeof mockFetch>): RequestInit {
  const call = fn.mock.calls[0] as unknown as [string, RequestInit];
  return call[1];
}

describe("api", () => {
  it("sends the CSRF header and same-origin credentials on GET", async () => {
    const fn = mockFetch(200, { ok: true });
    const res = await api.get<{ ok: boolean }>("/api/status");
    expect(res.ok).toBe(true);
    const init = sentInit(fn);
    expect(new Headers(init.headers).get(CSRF_HEADER)).toBe("1");
    expect(init.credentials).toBe("same-origin");
    expect(init.method).toBe("GET");
  });

  it("sends JSON bodies with the CSRF header on every write method", async () => {
    for (const call of [api.post, api.put, api.patch, api.del]) {
      const fn = mockFetch(200, {});
      await call("/api/x", { a: 1 });
      const init = sentInit(fn);
      const headers = new Headers(init.headers);
      expect(headers.get(CSRF_HEADER)).toBe("1");
      expect(headers.get("Content-Type")).toBe("application/json");
      expect(init.body).toBe('{"a":1}');
    }
  });

  it("throws ApiError with status and server message on 4xx", async () => {
    mockFetch(400, { error: "bad input" });
    const err = await api.post("/api/x", {}).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(400);
    expect((err as ApiError).message).toBe("bad input");
    expect((err as ApiError).body).toEqual({ error: "bad input" });
  });

  it("throws ApiError on 5xx with a plain text body", async () => {
    mockFetch(500, "boom", "text/plain");
    const err = await api.get("/api/x").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(500);
    expect((err as ApiError).message).toBe("boom");
  });

  it("returns undefined for an empty 204", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(null, { status: 204 })));
    await expect(api.del("/api/x")).resolves.toBeUndefined();
  });
});

describe("api session expiry", () => {
  let go = vi.fn();

  beforeEach(() => {
    go = vi.fn();
    resetLoginRedirect(go);
  });

  afterEach(() => {
    resetLoginRedirect();
  });

  async function expectExpired(call: () => Promise<unknown>) {
    const err = await call().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(401);
  }

  it("sends the browser to /login once on a 401, however many requests fail", async () => {
    mockFetch(401, { detail: "Unauthorized" });
    await expectExpired(() => api.get("/api/status"));
    await expectExpired(() => api.post("/api/x", {}));
    expect(go).toHaveBeenCalledTimes(1);
    expect(go).toHaveBeenCalledWith("/login");
  });

  it("treats a followed redirect to the login page as expired", async () => {
    const res = new Response("{}", { status: 200, headers: { "content-type": "application/json" } });
    Object.defineProperty(res, "redirected", { value: true });
    Object.defineProperty(res, "url", { value: "http://localhost/login" });
    vi.stubGlobal("fetch", vi.fn(async () => res));
    await expectExpired(() => api.get("/api/status"));
    expect(go).toHaveBeenCalledWith("/login");
  });

  it("navigates once when several requests follow a redirect to the login page", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        const res = new Response("<!doctype html>", { status: 200, headers: { "content-type": "text/html" } });
        Object.defineProperty(res, "redirected", { value: true });
        Object.defineProperty(res, "url", { value: "http://localhost/login?next=/" });
        return res;
      }),
    );
    await expectExpired(() => api.get("/api/status"));
    await expectExpired(() => api.post("/start"));
    expect(go).toHaveBeenCalledTimes(1);
    expect(go).toHaveBeenCalledWith("/login");
  });

  it("fails HTML at / with a plain ApiError and does not navigate", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        const res = new Response("<!doctype html><div id=\"root\"></div>", {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
        Object.defineProperty(res, "redirected", { value: true });
        Object.defineProperty(res, "url", { value: "http://localhost/" });
        return res;
      }),
    );
    const err = await api.post("/start").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(200);
    expect((err as ApiError).message).toBe("unexpected HTML response");
    expect(go).not.toHaveBeenCalled();
  });

  it("fails unredirected HTML where JSON was asked for without navigating", async () => {
    mockFetch(200, "<!doctype html><div id=\"root\"></div>", "text/html; charset=utf-8");
    const err = await api.get<unknown[]>("/api/guilds").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).message).toBe("unexpected HTML response");
    expect(go).not.toHaveBeenCalled();
  });

  it("leaves a normal JSON reply alone", async () => {
    mockFetch(200, [{ id: 1 }]);
    await expect(api.get<{ id: number }[]>("/api/guilds")).resolves.toEqual([{ id: 1 }]);
    expect(go).not.toHaveBeenCalled();
  });

  it("leaves other errors alone", async () => {
    mockFetch(403, { detail: "Forbidden" });
    const err = await api.post("/api/x", {}).catch((e: unknown) => e);
    expect((err as ApiError).status).toBe(403);
    expect(go).not.toHaveBeenCalled();
  });
});

describe("prefetch", () => {
  it("hands a prefetched GET to the first reader only", async () => {
    const fn = vi.fn(async () => new Response(JSON.stringify({ n: 1 }), { headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fn);
    prefetch(["/api/prefetch-test"]);
    expect(fn).toHaveBeenCalledTimes(1);
    expect(await api.get("/api/prefetch-test")).toEqual({ n: 1 });
    expect(fn).toHaveBeenCalledTimes(1);
    await api.get("/api/prefetch-test");
    expect(fn).toHaveBeenCalledTimes(2);
  });
});

describe("errorText", () => {
  afterEach(() => vi.unstubAllGlobals());

  async function caught(call: () => Promise<unknown>): Promise<unknown> {
    try {
      await call();
    } catch (e) {
      return e;
    }
    throw new Error("expected the request to fail");
  }

  it("shows a plain text 400 body, every line of it", async () => {
    mockFetch(400, "Telegram: invalid bot token. (Unauthorized)\nPushover: bad user", "text/plain");
    const e = await caught(() => api.post("/api/forwarding", {}));
    expect(errorText(e, "Failed to save forwarding rule")).toBe(
      "Telegram: invalid bot token. (Unauthorized)\nPushover: bad user",
    );
  });

  it("shows a JSON detail body", async () => {
    mockFetch(400, { detail: "missing-guild-id" });
    const e = await caught(() => api.post("/api/forwarding", {}));
    expect(errorText(e, "Failed to save forwarding rule")).toBe("missing-guild-id");
  });

  it("falls back to the caller's text and the status when the server sent no message", async () => {
    mockFetch(500, "", "text/plain");
    const e = await caught(() => api.get("/api/x"));
    expect(errorText(e, "Failed to load")).toBe("Failed to load (500)");
    expect(errorText(new ApiError(422, "/api/x", { detail: [] }), "Failed")).toBe("Failed (422)");
  });

  it("shows the message of any other Error", () => {
    expect(errorText(new TypeError("Failed to fetch"), "Failed to load")).toBe("Failed to fetch");
  });

  it("calls anything else a network error", () => {
    expect(errorText(undefined, "Failed to load")).toBe("Failed to load (network error)");
    expect(errorText(new Error(""), "Failed to load")).toBe("Failed to load (network error)");
  });
});
