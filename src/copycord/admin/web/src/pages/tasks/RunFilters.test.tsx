import { act, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { filtersFromParams, RunFilterSelects, RunSearch, SEARCH_DEBOUNCE_MS, useRunFilters } from "./RunFilters";

function Where() {
  const loc = useLocation();
  return <output aria-label="where">{loc.search}</output>;
}

function Harness() {
  const { filters, active, set, setPage, clear } = useRunFilters();
  return (
    <>
      <RunSearch value={filters.q ?? ""} onSearch={(q) => set("q", q)} />
      <RunFilterSelects
        filters={filters}
        types={[{ key: "backfill_channels", title: "Backfill channels", description: "", inputs_schema: {}, active: 0, total: 0 }]}
        actors={["admin"]}
        contexts={[{ label: "Main mapping", mapping_id: "m1" }]}
        onChange={set}
        active={active}
        onClear={clear}
      />
      <button type="button" onClick={() => setPage((filters.page ?? 1) + 1)}>
        next
      </button>
      <Where />
    </>
  );
}

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Harness />
    </MemoryRouter>,
  );
}

const where = () => new URLSearchParams(screen.getByLabelText("where").textContent ?? "");

afterEach(() => {
  vi.useRealTimers();
});

describe("filtersFromParams", () => {
  it("reads every filter and drops junk", () => {
    expect(filtersFromParams(new URLSearchParams("type=t&status=failed&context=m1&actor=admin&q=+gen+&page=3"))).toEqual({
      type: "t",
      status: "failed",
      context: "m1",
      actor: "admin",
      q: "gen",
      page: 3,
    });
    expect(filtersFromParams(new URLSearchParams("status=bogus&page=0&q=%20"))).toEqual({});
  });
});

describe("RunFilters", () => {
  it("round trips the dropdowns through the URL and resets the page", () => {
    renderAt("/tasks?page=4&actor=admin");
    expect(screen.getByRole("combobox", { name: "Actor", hidden: true })).toHaveValue("admin");
    fireEvent.change(screen.getByRole("combobox", { name: "Status", hidden: true }), { target: { value: "failed" } });
    fireEvent.change(screen.getByRole("combobox", { name: "Mapping", hidden: true }), { target: { value: "m1" } });
    fireEvent.change(screen.getByRole("combobox", { name: "Task type", hidden: true }), { target: { value: "backfill_channels" } });
    expect(Object.fromEntries(where())).toEqual({ actor: "admin", status: "failed", context: "m1", type: "backfill_channels" });

    fireEvent.click(screen.getByRole("button", { name: "next" }));
    expect(where().get("page")).toBe("2");

    fireEvent.click(screen.getByRole("button", { name: "Clear filters" }));
    expect(where().toString()).toBe("");
    expect(screen.queryByRole("button", { name: "Clear filters" })).toBeNull();
  });

  it("keeps a URL value the choices do not list", () => {
    renderAt("/tasks?actor=ghost");
    expect(screen.getByRole("combobox", { name: "Actor", hidden: true })).toHaveValue("ghost");
  });

  it("debounces the search 300 ms before writing ?q=", () => {
    vi.useFakeTimers();
    renderAt("/tasks?status=failed&page=2");
    const box = screen.getByRole("searchbox", { name: "Search runs" });
    fireEvent.change(box, { target: { value: "gen" } });
    act(() => vi.advanceTimersByTime(SEARCH_DEBOUNCE_MS - 50));
    fireEvent.change(box, { target: { value: "general" } });
    act(() => vi.advanceTimersByTime(SEARCH_DEBOUNCE_MS - 50));
    expect(where().get("q")).toBeNull();
    act(() => vi.advanceTimersByTime(60));
    expect(where().get("q")).toBe("general");
    expect(where().get("status")).toBe("failed");
    expect(where().get("page")).toBeNull();
  });

  it("searches at once on submit and follows the URL when it is cleared", () => {
    renderAt("/tasks?q=old");
    const box = screen.getByRole("searchbox", { name: "Search runs" });
    expect(box).toHaveValue("old");
    fireEvent.change(box, { target: { value: "new" } });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(where().get("q")).toBe("new");
    fireEvent.click(screen.getByRole("button", { name: "Clear filters" }));
    expect(box).toHaveValue("");
  });
});
