import { cx } from "./cx";

export interface SpinnerProps {
  size?: "sm" | "md" | "lg";
  label?: string;
  className?: string;
}

const SIZE = { sm: "size-4 border-2", md: "size-6 border-2", lg: "size-10 border-[3px]" } as const;

export function Spinner({ size = "md", label = "Loading", className }: SpinnerProps) {
  return (
    <span role="status" aria-label={label} className={cx("inline-flex items-center", className)}>
      <span
        aria-hidden="true"
        className={cx("animate-spin rounded-full border-line-2 border-t-accent", SIZE[size])}
      />
    </span>
  );
}
