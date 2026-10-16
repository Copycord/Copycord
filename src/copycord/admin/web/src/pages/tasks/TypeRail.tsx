// Left column of the Tasks page: "All tasks", then one entry per registered type, each with its run
// count and a blue running badge while it has active runs.

import { Link } from "react-router";
import { cx } from "../../components/ui/cx";
import { useReveal } from "../../components/ui/Skeleton";
import type { TaskType } from "../../types/tasks";
import { FADE_IN, RAIL, RAIL_ACTIVE, RAIL_HEADING, RAIL_ITEM, RAIL_LIST, RailCounts, RailEntryLoading } from "./TasksSkeleton";

export interface TypeRailProps {
  types: TaskType[];
  /** The selected type key, or "" for All tasks. */
  selected: string;
  /** The types are still loading: one placeholder entry, as in the page skeleton. */
  loading?: boolean;
}

const item = (active: boolean) => cx(RAIL_ITEM, active ? RAIL_ACTIVE : "hover:bg-rail-hover");

/** "2 runs, 1 running": the counts in words, for the entry's title. */
export function railTitle(total: number, active: number): string {
  const runs = `${total} ${total === 1 ? "run" : "runs"}`;
  return active > 0 ? `${runs}, ${active} running` : runs;
}

export function TypeRail({ types, selected, loading = false }: TypeRailProps) {
  const placeholder = loading && types.length === 0;
  const reveal = useReveal(!placeholder);
  const total = types.reduce((n, t) => n + (t.total ?? 0), 0);
  const active = types.reduce((n, t) => n + (t.active ?? 0), 0);
  return (
    <nav aria-label="Task types" className={RAIL}>
      <Link
        to="/tasks"
        className={item(selected === "")}
        aria-current={selected === "" ? "page" : undefined}
        title={placeholder ? undefined : railTitle(total, active)}
      >
        All tasks
        <RailCounts total={placeholder ? null : total} active={active} />
      </Link>
      <div className={RAIL_HEADING}>Task types</div>
      <ul className={cx(RAIL_LIST, reveal)}>
        {placeholder ? <RailEntryLoading /> : null}
        {types.map((t) => (
          <li key={t.key} className={FADE_IN}>
            <Link
              to={`/tasks?type=${encodeURIComponent(t.key)}`}
              className={item(selected === t.key)}
              aria-current={selected === t.key ? "page" : undefined}
              title={railTitle(t.total ?? 0, t.active)}
            >
              <span className="min-w-0 truncate">{t.title}</span>
              <RailCounts total={t.total ?? 0} active={t.active} />
            </Link>
          </li>
        ))}
      </ul>
    </nav>
  );
}
