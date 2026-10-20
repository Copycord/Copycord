// The "Role claimed" gift: a Lottie animation played once in a 64 px box. lottie-web's light SVG player and the
// animation are imported on first show, so both land in their own chunks and the main bundle stays as it was.
// It always plays, prefers-reduced-motion included (Mac's call: the gift is the reward moment). If the import
// fails, `fallback` renders instead. Decorative only: the result's title carries the meaning.

import { useEffect, useRef, useState, type ReactNode } from "react";
import type { AnimationItem } from "lottie-web";

export function GiftAnimation({ fallback, className = "" }: { fallback: ReactNode; className?: string }) {
  const boxRef = useRef<HTMLDivElement | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    const container = boxRef.current;
    if (!container) return;
    let cancelled = false;
    let anim: AnimationItem | null = null;
    Promise.all([import("lottie-web/build/player/lottie_light"), import("../assets/gift.json")])
      .then(([{ default: lottie }, { default: animationData }]) => {
        if (cancelled) return;
        anim = lottie.loadAnimation({ container, renderer: "svg", loop: false, autoplay: true, animationData });
      })
      .catch(() => {
        if (!cancelled) setFailed(true);
      });
    return () => {
      cancelled = true;
      anim?.destroy();
    };
  }, []);

  if (failed) return <>{fallback}</>;
  return <div ref={boxRef} data-gift-animation aria-hidden="true" className={`size-16 ${className}`} />;
}
