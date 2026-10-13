import type { ReactNode } from "react";
import { cx } from "./cx";

export interface EmptyStateProps {
  title: ReactNode;
  description?: ReactNode;
  icon?: ReactNode;
  action?: ReactNode;
  className?: string;
}

export function EmptyState({ title, description, icon, action, className }: EmptyStateProps) {
  return (
    <div
      className={cx(
        "flex flex-col items-center justify-center gap-2 rounded-panel border border-dashed border-border px-6 py-10 text-center",
        className,
      )}
    >
      {icon ? <div className="text-accent">{icon}</div> : null}
      <p className="text-base font-semibold text-text-1">{title}</p>
      {description ? <p className="max-w-md text-sm text-text-2">{description}</p> : null}
      {action ? <div className="mt-2">{action}</div> : null}
    </div>
  );
}
