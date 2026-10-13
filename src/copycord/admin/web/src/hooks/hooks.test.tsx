import { act, render, renderHook, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { StatusGate } from "../components/ui/StatusGate";
import { useApi, useApiCall } from "./useApi";
import { evaluateStatus, UNREACHABLE_MESSAGE, useStatusGate } from "./useStatusGate";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

describe("useApi", () => {
  it("loads data", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => jsonResponse({ n: 1 })));
    const { result } = renderHook(() => useApi<{ n: number }>("/api/x"));
    expect(result.current.loading).toBe(true);
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.data).toEqual({ n: 1 });
    expect(result.current.error).toBeUndefined();
  });

  it("exposes the error and skips when url is null", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ error: "nope" }, 500));
    vi.stubGlobal("fetch", fetchMock);
    const { result } = renderHook(() => useApi("/api/x"));
    await waitFor(() => expect(result.current.error?.message).toBe("nope"));
    renderHook(() => useApi(null));
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("useApiCall tracks loading and rethrows", async () => {
    const { result } = renderHook(() => useApiCall(async (fail: boolean) => (fail ? Promise.reject(new Error("bad")) : 7)));
    await act(async () => {
      expect(await result.current[0](false)).toBe(7);
    });
    await act(async () => {
      await expect(result.current[0](true)).rejects.toThrow("bad");
    });
    expect(result.current[1].error?.message).toBe("bad");
    expect(result.current[1].loading).toBe(false);
  });
});

describe("evaluateStatus", () => {
  it("reads the ready shapes the old gate accepted", () => {
    expect(evaluateStatus({ server: { ready: true } }).isUp).toBe(true);
    expect(evaluateStatus({ client: { status: "Logged in as bot#1" } }).isUp).toBe(true);
    expect(evaluateStatus({ server: { discord: { connected: true } } }, "both").isUp).toBe(false);
    expect(evaluateStatus({ server: { state: "READY" } }, "server").isUp).toBe(true);
    expect(evaluateStatus({}, "both").message).toBe("Waiting for Server and Client connection...");
    expect(evaluateStatus({ client: { ready: true } }, "both").message).toBe(
      "Waiting for Server to connect to Discord gateway...",
    );
  });
});

describe("useStatusGate", () => {
  it("gates while down and lifts once up, firing onDown then onUp", async () => {
    vi.useFakeTimers();
    sessionStorage.clear();
    const replies = [{ server: { ready: false } }, { server: { ready: true } }];
    vi.stubGlobal("fetch", vi.fn(async () => jsonResponse(replies.shift() ?? { server: { ready: true } })));
    const onUp = vi.fn();
    const onDown = vi.fn();
    function Page() {
      const gate = useStatusGate({ onUp, onDown });
      return (
        <StatusGate gate={gate}>
          <p>content</p>
        </StatusGate>
      );
    }
    render(<Page />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10);
    });
    expect(screen.getByTestId("status-gate")).toHaveTextContent("Bot is not connected to Discord yet...");
    expect(onDown).toHaveBeenCalledOnce();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(screen.queryByTestId("status-gate")).toBeNull();
    expect(onUp).toHaveBeenCalledOnce();
    expect(sessionStorage.getItem("bot:lastUpAt")).not.toBeNull();
  });

  it("shows the unreachable message when /api/status fails", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("fetch", vi.fn(async () => Promise.reject(new Error("down"))));
    const { result } = renderHook(() => useStatusGate());
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10);
    });
    expect(result.current.gated).toBe(true);
    expect(result.current.message).toBe(UNREACHABLE_MESSAGE);
  });
});
