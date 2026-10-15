// Loading placeholders: a soft light sweep over a surface tinted block (cc-shimmer in styles/tokens.css),
// a static block under prefers-reduced-motion. Size them like the content they stand in for so nothing
// moves when the data lands. These are the app's only placeholders: pages build their loading rows from
// them and never style a cc-skeleton of their own (Skeleton.test.tsx scans the sources).
//   <Skeleton.Line width="40%" />  <Skeleton.Text className="text-xs" width={80} />  <Skeleton.Block height={118} />
//   <Skeleton.Box className={BADGE}>Mapping name</Skeleton.Box>   const reveal = useReveal(loaded);

import { useRef, type CSSProperties, type ReactNode } from "react";
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
 * truncate) and a width, or a sample of the text as children to size it, so the bar sits where the text
 * will and the row keeps its height. block puts the bar on a line of its own.
 */
function Text({
  className,
  width,
  style,
  block = false,
  children,
}: {
  className?: string;
  width?: Size;
  style?: CSSProperties;
  block?: boolean;
  children?: ReactNode;
}) {
  return (
    <span
      aria-hidden="true"
      data-skeleton=""
      className={cx(className, block ? "block" : "inline-block", "cc-skeleton rounded-md text-transparent")}
      style={{ width: px(width), ...style }}
    >
      {children ?? "\u00a0"}
    </span>
  );
}

/**
 * A placeholder in the box of the element it stands for (badge, pill, chip, count, icon, avatar): pass
 * that element's own classes, and its usual text as children to size it. Text and border go transparent.
 */
function Box({ className, id, style, children }: { className: string; id?: string; style?: CSSProperties; children?: ReactNode }) {
  return (
    <span id={id} aria-hidden="true" data-skeleton="" className={cx(className, "cc-skeleton border-transparent text-transparent")} style={style}>
      {children}
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

export const Skeleton = Object.assign(Base, { Line, Text, Box, Block, Card, Row });

/** Fades content in where a placeholder stood (cc-reveal in styles/tokens.css). */
export const REVEAL = "cc-reveal";

/**
 * The REVEAL class once `ready` turns true after a placeholder showed in this mount; nothing when the
 * content was there from the start. Put it on the element that holds the loaded content: the fade runs
 * once, when the class lands, and later refetches swap the content without fading again.
 */
export function useReveal(ready: boolean): string | undefined {
  const waited = useRef(!ready);
  if (!ready) waited.current = true;
  return ready && waited.current ? REVEAL : undefined;
}

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
