import type { ReactNode } from "react";
import { cx } from "./cx";

export interface SpinnerProps {
  /** A named size, or a pixel box (with the sm ring) to fill a glyph slot such as a status icon. */
  size?: "sm" | "md" | "lg" | number;
  /** The status word; null when a labelled host (a status glyph) already says it, so the ring is decorative. */
  label?: string | null;
  className?: string;
}

const SIZE = { sm: "size-4 border-2", md: "size-6 border-2", lg: "size-10 border-[3px]" } as const;

export function Spinner({ size = "md", label = "Loading", className }: SpinnerProps) {
  const px = typeof size === "number";
  return (
    <span
      role={label == null ? undefined : "status"}
      aria-label={label ?? undefined}
      aria-hidden={label == null || undefined}
      className={cx("inline-flex items-center", className)}
    >
      <span
        aria-hidden="true"
        data-spinner=""
        style={px ? { width: size, height: size } : undefined}
        className={cx("animate-spin rounded-full border-line-2 border-t-accent", px ? "border-2" : SIZE[size])}
      />
    </span>
  );
}

/**
 * A busy button's content: the label keeps its place, only made see-through, so the button keeps its
 * width and its accessible name, and the spinner sits centred over it. The host button adds BUSY_HOST.
 */
export const BUSY_HOST = "relative";
export const BUSY_SPINNER = "absolute inset-0 justify-center";
export const BUSY_LABEL = "inline-flex items-center justify-center gap-[inherit] opacity-0";

export function BusyContent({ busy, children }: { busy: boolean; children?: ReactNode }) {
  if (!busy) return <>{children}</>;
  return (
    <>
      <span className={BUSY_LABEL}>{children}</span>
      <Spinner size="sm" label="Working" className={BUSY_SPINNER} />
    </>
  );
}
