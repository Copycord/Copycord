// Toast stack shared by every page: useToast().show("Saved") or
// show("Failed", { kind: "error" }). Matches the old showToast in app.js and
// the .toast rules in main.css: top right under the header, a coloured bar on
// the left, fades and drops in, and a click anywhere on it closes it.

import { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";

export type ToastKind = "info" | "success" | "error";

export interface Toast {
  id: number;
  message: string;
  kind: ToastKind;
  shown: boolean;
}

interface ToastApi {
  show: (message: string, opts?: { kind?: ToastKind; timeout?: number }) => number;
  dismiss: (id: number) => void;
}

const ToastContext = createContext<ToastApi | null>(null);

// The old close() dropped .show, then removed the element 200 ms later.
const LEAVE_MS = 200;

// .toast-info / .toast-success / .toast-error .toast-bar
const BAR_CLASS: Record<ToastKind, string> = {
  info: "bg-[linear-gradient(180deg,var(--color-accent),var(--color-accent-deep)_80%)]",
  success: "bg-[linear-gradient(180deg,var(--color-good),var(--color-good-deep)_80%)]",
  error: "bg-[linear-gradient(180deg,var(--color-danger),var(--color-danger-deep)_80%)]",
};

const TOAST =
  "toast pointer-events-auto relative w-auto min-w-[280px] max-w-[440px] cursor-pointer overflow-hidden " +
  "rounded-[8px] border border-border border-l-transparent bg-control-top px-4 py-3.5 text-text " +
  "[transition:transform_0.18s_ease,opacity_0.18s_ease,box-shadow_0.18s_ease,border-color_0.18s_ease] " +
  "max-[520px]:w-[calc(100vw-32px)] max-[520px]:min-w-0 max-[520px]:max-w-none";
const HIDDEN = "[transform:translateY(-10px)_scale(0.96)] opacity-0";
const SHOWN = "show [transform:translateY(0)_scale(1)] opacity-100";

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const nextId = useRef(1);

  const dismiss = useCallback((id: number) => {
    setToasts((list) => list.map((t) => (t.id === id ? { ...t, shown: false } : t)));
    setTimeout(() => setToasts((list) => list.filter((t) => t.id !== id)), LEAVE_MS);
  }, []);

  const show = useCallback<ToastApi["show"]>(
    (message, opts = {}) => {
      const id = nextId.current++;
      setToasts((list) => [...list, { id, message, kind: opts.kind ?? "info", shown: false }]);
      requestAnimationFrame(() =>
        setToasts((list) => list.map((t) => (t.id === id ? { ...t, shown: true } : t))),
      );
      const timeout = opts.timeout ?? 4000;
      if (timeout > 0) setTimeout(() => dismiss(id), timeout);
      return id;
    },
    [dismiss],
  );

  const value = useMemo(() => ({ show, dismiss }), [show, dismiss]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      <div
        id="toast-root"
        className="toast-root pointer-events-none fixed top-[calc(var(--spacing-header)+16px)] right-4 z-[2147483647] flex flex-col items-end gap-2.5"
        aria-live="polite"
        aria-atomic="true"
      >
        {toasts.map((t) => (
          <div
            key={t.id}
            role={t.kind === "error" ? "alert" : "status"}
            className={`${TOAST} toast-${t.kind} ${t.shown ? SHOWN : HIDDEN}`}
            onClick={() => dismiss(t.id)}
          >
            <div
              className={`toast-bar absolute top-0 bottom-0 left-0 w-1.5 rounded-l-[8px] ${BAR_CLASS[t.kind]}`}
            />
            <div className="toast-msg pl-3 text-[14px] leading-[1.4] font-medium whitespace-pre-line [word-break:break-word] text-text">
              {t.message}
            </div>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastApi {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error("useToast needs a ToastProvider");
  return ctx;
}
