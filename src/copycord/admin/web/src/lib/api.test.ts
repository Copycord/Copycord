import { describe, expect, it, vi } from "vitest";
import { ApiError, CSRF_HEADER, api } from "./api";

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
