// The Account column avatar, always in the same 24 px box: a shimmer while its image loads, then the
// image; an image that fails falls to the Discord default avatar image, and if that fails too a muted
// circle. Which URLs loaded or failed is kept for the page, so a 5 s poll naming the same URL redraws
// the image at once instead of going back to the shimmer.

import { useRef, useState } from "react";
import { Skeleton, cx } from "../../components/ui";
import { avatarSources } from "./senders";
import { AVATAR } from "./SendersSkeleton";

const LOADED = new Set<string>();
const FAILED = new Set<string>();

/** Forgets every loaded and failed URL (tests start each case from an empty page). */
export function forgetAvatars() {
  LOADED.clear();
  FAILED.clear();
}

export function SenderAvatar({ url, id }: { url: string | null; id: string }) {
  const [, redraw] = useState(0);
  // This img element is showing a loaded image: a new src keeps it on screen until the next one lands.
  const showing = useRef(false);
  const src = avatarSources(url, id).find((u) => !FAILED.has(u));
  if (src === undefined) {
    return <span aria-hidden="true" data-avatar="none" className={cx(AVATAR, "select-none bg-line-2")} />;
  }
  const visible = LOADED.has(src) || showing.current;
  const settle = (ok: boolean) => {
    (ok ? LOADED : FAILED).add(src);
    showing.current = ok;
    redraw((n) => n + 1);
  };
  return (
    <span aria-hidden="true" data-avatar={visible ? "img" : "loading"} className={cx(AVATAR, "relative select-none")}>
      {visible ? null : <Skeleton.Box className={cx(AVATAR, "absolute inset-0")} />}
      {/* Locked in place: the browser's image drag ghost and text selection are switched off. */}
      <img
        src={src}
        alt=""
        decoding="async"
        draggable={false}
        className={cx(AVATAR, "pointer-events-none absolute inset-0 select-none object-cover", !visible && "opacity-0")}
        onDragStart={(e) => e.preventDefault()}
        onLoad={() => settle(true)}
        onError={() => settle(false)}
      />
    </span>
  );
}
