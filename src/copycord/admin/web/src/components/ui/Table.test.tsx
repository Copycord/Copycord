import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { Table } from "./Table";
import type { Column } from "./Table";

interface Row {
  id: number;
  name: string;
  count: number;
}

const columns: Column<Row>[] = [
  { key: "name", header: "Name" },
  { key: "count", header: "Count", align: "right", cell: (r) => `${r.count} msgs` },
];

const rows: Row[] = [
  { id: 1, name: "general", count: 3 },
  { id: 2, name: "news", count: 0 },
];

describe("Table", () => {
  it("renders headers and cells from typed columns", () => {
    render(<Table columns={columns} rows={rows} rowKey={(r) => r.id} caption="Channels" />);
    const table = screen.getByRole("table", { name: "Channels" });
    expect(within(table).getAllByRole("columnheader").map((h) => h.textContent)).toEqual(["Name", "Count"]);
    expect(screen.getByText("general")).toBeInTheDocument();
    expect(screen.getByText("3 msgs")).toBeInTheDocument();
  });

  it("shows the empty state when there are no rows", () => {
    render(<Table columns={columns} rows={[]} rowKey={(r) => r.id} empty="No channels" />);
    expect(screen.getByText("No channels")).toBeInTheDocument();
    expect(screen.getAllByRole("row")).toHaveLength(2);
  });

  it("shows skeleton rows while loading, not rows or the empty state", () => {
    render(<Table columns={columns} rows={rows} rowKey={(r) => r.id} loading skeletonRows={4} empty="No channels" />);
    expect(screen.getAllByTestId("table-skeleton")).toHaveLength(4);
    expect(screen.queryByText("general")).toBeNull();
    expect(screen.queryByText("No channels")).toBeNull();
    expect(screen.getByRole("table")).toHaveAttribute("aria-busy", "true");
  });

  it("calls onRowClick with the row", async () => {
    const onRowClick = vi.fn();
    render(<Table columns={columns} rows={rows} rowKey={(r) => r.id} onRowClick={onRowClick} />);
    await userEvent.setup().click(screen.getByText("news"));
    expect(onRowClick).toHaveBeenCalledWith(rows[1]);
  });
});
