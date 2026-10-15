// Channel picker of the Backfill form: categories as tri-state checkboxes over their channels,
// a search box, Select listed and Clear. Ctrl/Cmd+A selects the listed channels and Escape clears
// the selection before it reaches the modal. Rendered flat; not virtualised.

import { useMemo, useState } from "react";
import type { KeyboardEvent } from "react";
import { Button } from "../../../../components/ui/Button";
import { Checkbox } from "../../../../components/ui/Checkbox";
import { Input } from "../../../../components/ui/Field";
import type { BackfillOptionCategory, BackfillOptionChannel } from "../../../../types/tasks";
import { shortDate } from "./logic";

export interface ChannelPickerProps {
  categories: BackfillOptionCategory[];
  selected: ReadonlySet<string>;
  onChange: (next: Set<string>) => void;
  error?: string;
}

interface ListedCategory {
  key: string;
  cat: BackfillOptionCategory;
  channels: BackfillOptionChannel[];
}

/** Orphans hidden; a category whose name matches lists all its channels. */
export function listCategories(categories: BackfillOptionCategory[], query: string): ListedCategory[] {
  const q = query.trim().toLowerCase();
  const out: ListedCategory[] = [];
  categories.forEach((cat, i) => {
    const live = cat.channels.filter((c) => !c.orphan);
    if (live.length === 0) return;
    const channels = !q || cat.name.toLowerCase().includes(q) ? live : live.filter((c) => c.name.toLowerCase().includes(q));
    if (channels.length) out.push({ key: cat.id ?? `none-${i}`, cat, channels });
  });
  return out;
}

const selectable = (c: BackfillOptionChannel) => !c.busy;

// The list is the form's only scroller: its height leaves room in the 80vh modal for the other
// fields, so the modal body does not scroll as well.
const LIST_SCROLL =
  "max-h-[clamp(9rem,calc(80vh_-_34rem),40vh)] overflow-x-hidden overflow-y-auto overscroll-contain";

export function ChannelPicker({ categories, selected, onChange, error }: ChannelPickerProps) {
  const [query, setQuery] = useState("");
  const listed = useMemo(() => listCategories(categories, query), [categories, query]);
  const listedIds = useMemo(
    () => listed.flatMap((l) => l.channels.filter(selectable).map((c) => c.id)),
    [listed],
  );

  const selectListed = () => onChange(new Set([...selected, ...listedIds]));
  const clear = () => onChange(new Set());

  const toggle = (ids: string[], on: boolean) => {
    const next = new Set(selected);
    for (const id of ids) {
      if (on) next.add(id);
      else next.delete(id);
    }
    onChange(next);
  };

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    const inText = e.target instanceof HTMLInputElement && e.target.type !== "checkbox";
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "a" && !inText) {
      e.preventDefault();
      selectListed();
    } else if (e.key === "Escape" && selected.size > 0) {
      e.preventDefault();
      e.stopPropagation();
      clear();
    }
  };

  return (
    <div onKeyDown={onKeyDown} data-testid="channel-picker" className="flex flex-col gap-2">
      <div className="flex min-w-0 items-center gap-2">
        <Input
          type="search"
          aria-label="Search channels"
          placeholder="Search channels"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="min-w-0 flex-1"
        />
        <Button variant="ghost" size="sm" onClick={selectListed} disabled={listedIds.length === 0}>
          Select listed
        </Button>
        <Button variant="ghost" size="sm" onClick={clear} disabled={selected.size === 0}>
          Clear
        </Button>
      </div>
      <p className="m-0 text-xs text-muted">{selected.size} selected</p>
      <div
        role="group"
        aria-label="Channels"
        aria-invalid={error ? true : undefined}
        className={`${LIST_SCROLL} rounded-control border ${error ? "border-danger" : "border-border"} bg-card`}
      >
        {listed.length === 0 ? (
          <p className="m-0 px-3 py-3 text-sm text-muted">{query ? "No channels match" : "No channels in this mapping"}</p>
        ) : (
          listed.map(({ key, cat, channels }) => {
            const ids = channels.filter(selectable).map((c) => c.id);
            const on = ids.filter((id) => selected.has(id)).length;
            const all = ids.length > 0 && on === ids.length;
            return (
              <div key={key}>
                <label className="flex cursor-pointer items-center gap-2 border-b border-border bg-rail-hover/40 px-3 py-1.5 text-xs font-semibold tracking-wide text-muted uppercase">
                  <Checkbox
                    aria-label={`Category ${cat.name}`}
                    checked={all}
                    indeterminate={on > 0 && !all}
                    disabled={ids.length === 0}
                    onChange={() => toggle(ids, !all)}
                  />
                  <span className="min-w-0 truncate">{cat.name}</span>
                </label>
                {channels.map((c) => (
                  <label
                    key={c.id}
                    className={`flex items-center gap-2 px-3 py-1.5 pl-8 text-sm ${c.busy ? "cursor-not-allowed text-muted" : "cursor-pointer text-text hover:bg-rail-hover"}`}
                  >
                    <Checkbox
                      aria-label={`#${c.name}`}
                      checked={selected.has(c.id)}
                      disabled={Boolean(c.busy)}
                      onChange={(e) => toggle([c.id], e.target.checked)}
                    />
                    <span className="min-w-0 truncate">#{c.name}</span>
                    {c.busy ? <span className="ml-auto text-xs text-muted">Running in #{c.busy.number}</span> : null}
                    {!c.busy && c.resumable ? (
                      <span className="ml-auto text-xs text-muted">
                        resume from {c.resumable.checkpoint_at ? shortDate(c.resumable.checkpoint_at) : "checkpoint"}
                      </span>
                    ) : null}
                  </label>
                ))}
              </div>
            );
          })
        )}
      </div>
      {error ? (
        <p role="alert" className="m-0 text-xs text-danger">
          {error}
        </p>
      ) : null}
    </div>
  );
}

/**
 * Split a channel selection into whole categories (by id) and the remaining channels. A category
 * with a busy channel goes as its picked channels, so the run does not add a skipped step for it.
 */
export function splitSelection(
  categories: BackfillOptionCategory[],
  selected: ReadonlySet<string>,
): { category_ids: string[]; channel_ids: string[] } {
  const category_ids: string[] = [];
  const covered = new Set<string>();
  for (const cat of categories) {
    if (cat.id === null) continue;
    const live = cat.channels.filter((c) => !c.orphan);
    const ids = live.map((c) => c.id);
    if (ids.length > 1 && live.every(selectable) && ids.every((id) => selected.has(id))) {
      category_ids.push(cat.id);
      ids.forEach((id) => covered.add(id));
    }
  }
  const channel_ids = [...selected].filter((id) => !covered.has(id));
  return { category_ids, channel_ids };
}
