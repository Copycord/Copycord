// Joins class names, skipping falsy entries.
export function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

// Stacking order of the page layers, lowest first. The status gate dims the page under the
// header, so the header and the side menu drawer (with its backdrop) sit above it and stay
// usable. Dialogs dim the whole viewport above the header and the drawer, which go inert while
// one is open (useScrollLock in Modal.tsx); toasts (z-50) sit above everything else.
export const Z = {
  gate: "z-20",
  header: "z-30",
  drawer: "z-30",
  modal: "z-40",
} as const;
