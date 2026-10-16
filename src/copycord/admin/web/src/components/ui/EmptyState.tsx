import type { ReactNode } from "react";
import { cx } from "./cx";

export interface EmptyStateProps {
  title: ReactNode;
  description?: ReactNode;
  icon?: ReactNode;
  action?: ReactNode;
  /** Plain centred text for use inside a frame that already has a border: no dashed card around it. */
  plain?: boolean;
  className?: string;
}

export function EmptyState({ title, description, icon, action, plain = false, className }: EmptyStateProps) {
  return (
    <div
      className={cx(
        "flex flex-col items-center justify-center text-center",
        plain ? "gap-1 px-6 py-10" : "gap-2 rounded-panel border border-dashed border-border px-6 py-10",
        className,
      )}
    >
      {icon ? <div className="text-text-2">{icon}</div> : null}
      <p className={cx("font-semibold text-text-1", plain ? "text-sm" : "text-base")}>{title}</p>
      {description ? <p className={cx("max-w-md text-sm", plain ? "text-muted" : "text-text-2")}>{description}</p> : null}
      {action ? <div className="mt-2">{action}</div> : null}
    </div>
  );
}
