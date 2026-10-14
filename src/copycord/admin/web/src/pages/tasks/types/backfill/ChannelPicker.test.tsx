import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { Modal } from "../../../../components/Modal";
import type { BackfillOptionCategory } from "../../../../types/tasks";
import { ChannelPicker, splitSelection } from "./ChannelPicker";

const CATS: BackfillOptionCategory[] = [
  {
    id: "c1",
    name: "Text",
    channels: [
      { id: "1", name: "general", orphan: false, busy: null, resumable: null },
      { id: "2", name: "random", orphan: false, busy: null, resumable: null },
      { id: "3", name: "locked", orphan: false, busy: { run_id: "r", number: 4 }, resumable: null },
    ],
  },
];

function Harness({ onClose = () => {} }: { onClose?: () => void }) {
  const [sel, setSel] = useState<Set<string>>(new Set());
  return (
    <Modal open onClose={onClose} title="Pick">
      <ChannelPicker categories={CATS} selected={sel} onChange={setSel} />
    </Modal>
  );
}

const box = (name: string) => screen.getByRole("checkbox", { name }) as HTMLInputElement;

describe("ChannelPicker", () => {
  it("Ctrl+A and Cmd+A select the listed free channels", () => {
    render(<Harness />);
    fireEvent.change(screen.getByRole("searchbox", { name: "Search channels" }), { target: { value: "gen" } });
    fireEvent.keyDown(box("#general"), { key: "a", ctrlKey: true });
    expect(box("#general").checked).toBe(true);
    expect(screen.queryByRole("checkbox", { name: "#random" })).toBeNull();

    fireEvent.change(screen.getByRole("searchbox", { name: "Search channels" }), { target: { value: "" } });
    fireEvent.keyDown(box("#general"), { key: "a", metaKey: true });
    expect(box("#random").checked).toBe(true);
    expect(box("#locked").checked).toBe(false);
    expect(screen.getByText("2 selected")).toBeInTheDocument();
  });

  it("Escape clears the selection first, then closes the modal", () => {
    const onClose = vi.fn();
    render(<Harness onClose={onClose} />);
    fireEvent.click(screen.getByRole("button", { name: "Select listed" }));
    expect(screen.getByText("2 selected")).toBeInTheDocument();
    fireEvent.keyDown(box("#general"), { key: "Escape" });
    expect(screen.getByText("0 selected")).toBeInTheDocument();
    expect(onClose).not.toHaveBeenCalled();
    fireEvent.keyDown(box("#general"), { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("Clear empties the selection", () => {
    render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: "Select listed" }));
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(screen.getByText("0 selected")).toBeInTheDocument();
  });

  it("sends a fully picked category by id, unless it holds a busy channel", () => {
    const free = [{ ...CATS[0]!, channels: CATS[0]!.channels.filter((c) => !c.busy) }];
    expect(splitSelection(free, new Set(["1", "2"]))).toEqual({ category_ids: ["c1"], channel_ids: [] });
    expect(splitSelection(free, new Set(["1"]))).toEqual({ category_ids: [], channel_ids: ["1"] });
    expect(splitSelection(CATS, new Set(["1", "2"]))).toEqual({ category_ids: [], channel_ids: ["1", "2"] });
  });
});
