// Loading placeholders: a soft light sweep over a surface tinted block (cc-shimmer in styles/tokens.css),
// a static block under prefers-reduced-motion. Size them like the content they stand in for so nothing
// moves when the data lands.
//   <Skeleton.Line width="40%" />  <Skeleton.Text className="text-xs" width={80} />  <Skeleton.Block height={118} />

import type { CSSProperties, ReactNode } from "react";
import { cx } from "./cx";

type Size = number | string;

export interface SkeletonProps {
  width?: Size;
  height?: Size;
  /** Tailwind radius class, rounded-md by default. */
  rounded?: string;
  className?: string;
  style?: CSSProperties;
}

const px = (v: Size | undefined) => (typeof v === "number" ? `${v}px` : v);

function Base({ width, height, rounded = "rounded-md", className, style }: SkeletonProps) {
  return (
    <span
      aria-hidden="true"
      data-skeleton=""
      className={cx("cc-skeleton block motion-reduce:animate-none", rounded, className)}
      style={{ width: px(width), height: px(height), ...style }}
    />
  );
}

/** One line of text; the height is the line box of a 14 px text by default. */
function Line({ width = "100%", height = 12, className, ...rest }: SkeletonProps) {
  return <Base width={width} height={height} rounded="rounded-full" className={cx("my-1", className)} {...rest} />;
}

/**
 * A bar in the line box of the text it stands for: pass the text's own classes (font size, line height,
 * truncate) and a width, so the bar sits where the text will and the row keeps its height.
 */
function Text({ className, width, style }: { className?: string; width: Size; style?: CSSProperties }) {
  return (
    <span
      aria-hidden="true"
      data-skeleton=""
      className={cx(className, "cc-skeleton inline-block rounded-md text-transparent")}
      style={{ width: px(width), ...style }}
    >
      {" "}
    </span>
  );
}

/** A filled block (thumbnail, button, chart). */
function Block({ width = "100%", height = 40, ...rest }: SkeletonProps) {
  return <Base width={width} height={height} {...rest} />;
}

/** A card outline with a block inside it, the shape of a tile in a grid. */
function Card({
  height = 118,
  className,
  testId,
  children,
}: {
  height?: Size;
  className?: string;
  testId?: string;
  children?: ReactNode;
}) {
  return (
    <div
      aria-hidden="true"
      data-testid={testId}
      data-skeleton=""
      className={cx("flex flex-col gap-2.5 rounded-[14px] border border-border bg-white/2 p-3.5", className)}
      style={{ height: px(height) }}
    >
      {children ?? (
        <>
          <Line width="45%" />
          <Line width="80%" />
          <Line width="60%" />
        </>
      )}
    </div>
  );
}

/** A table or list row: `cols` lines spread across the row. */
function Row({ cols = 3, height = 40, className }: { cols?: number; height?: Size; className?: string }) {
  const widths = ["30%", "22%", "18%", "12%", "10%"];
  return (
    <div aria-hidden="true" className={cx("flex items-center gap-4", className)} style={{ height: px(height) }}>
      {Array.from({ length: cols }, (_, i) => (
        <Line key={i} width={widths[i % widths.length]} />
      ))}
    </div>
  );
}

export const Skeleton = Object.assign(Base, { Line, Text, Block, Card, Row });

/** Card body while it loads: an optional header line, then `rows` rows. Announced once as busy. */
export function SkeletonSection({
  rows = 3,
  header = false,
  rowHeight = 40,
  label = "Loading",
  className,
}: {
  rows?: number;
  header?: boolean;
  rowHeight?: Size;
  label?: string;
  className?: string;
}) {
  return (
    <div role="status" aria-label={label} aria-busy="true" className={cx("flex flex-col gap-2", className)}>
      {header ? <Line width="35%" height={16} /> : null}
      {Array.from({ length: rows }, (_, i) => (
        <Row key={i} height={rowHeight} />
      ))}
    </div>
  );
}
