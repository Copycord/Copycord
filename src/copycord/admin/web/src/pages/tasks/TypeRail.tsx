// Left column of the Tasks page: "All tasks", then one entry per registered type with its active count.

import { Link } from "react-router";
import { cx } from "../../components/ui/cx";
import { useReveal } from "../../components/ui/Skeleton";
import type { TaskType } from "../../types/tasks";
import { FADE_IN, RAIL, RAIL_ACTIVE, RAIL_HEADING, RAIL_ITEM, RAIL_LIST, RailEntryLoading } from "./TasksSkeleton";

export interface TypeRailProps {
  types: TaskType[];
  /** The selected type key, or "" for All tasks. */
  selected: string;
  /** The types are still loading: one placeholder entry, as in the page skeleton. */
  loading?: boolean;
}

const item = (active: boolean) => cx(RAIL_ITEM, active ? RAIL_ACTIVE : "hover:bg-rail-hover");

export function TypeRail({ types, selected, loading = false }: TypeRailProps) {
  const placeholder = loading && types.length === 0;
  const reveal = useReveal(!placeholder);
  return (
    <nav aria-label="Task types" className={RAIL}>
      <Link to="/tasks" className={item(selected === "")} aria-current={selected === "" ? "page" : undefined}>
        All tasks
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
            >
              <span className="min-w-0 truncate">{t.title}</span>
              {t.active > 0 ? (
                <span className="rounded-full bg-brand-ghost px-1.5 text-xs text-accent tabular-nums" aria-label={`${t.active} active`}>
                  {t.active}
                </span>
              ) : null}
            </Link>
          </li>
        ))}
      </ul>
    </nav>
  );
}
