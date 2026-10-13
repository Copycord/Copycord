// Toast stack shared by every page: useToast().show("Saved") or
// show("Failed", { kind: "error" }).

import { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";

export type ToastKind = "info" | "success" | "error";

export interface Toast {
  id: number;
  message: string;
  kind: ToastKind;
}

interface ToastApi {
  show: (message: string, opts?: { kind?: ToastKind; timeout?: number }) => number;
  dismiss: (id: number) => void;
}

const ToastContext = createContext<ToastApi | null>(null);

const KIND_CLASS: Record<ToastKind, string> = {
  info: "border-border",
  success: "border-good",
  error: "border-danger",
};

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const nextId = useRef(1);

  const dismiss = useCallback((id: number) => {
    setToasts((list) => list.filter((t) => t.id !== id));
  }, []);

  const show = useCallback<ToastApi["show"]>(
    (message, opts = {}) => {
      const id = nextId.current++;
      setToasts((list) => [...list, { id, message, kind: opts.kind ?? "info" }]);
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
        className="pointer-events-none fixed right-4 bottom-4 z-50 flex flex-col gap-2"
        aria-live="polite"
        role="status"
      >
        {toasts.map((t) => (
          <div
            key={t.id}
            role={t.kind === "error" ? "alert" : undefined}
            className={`pointer-events-auto flex items-center gap-3 rounded-control border bg-card-solid px-4 py-3 text-sm text-text shadow-card ${KIND_CLASS[t.kind]}`}
          >
            <span>{t.message}</span>
            <button
              type="button"
              className="ml-auto text-muted hover:text-text"
              aria-label="Dismiss"
              onClick={() => dismiss(t.id)}
            >
              x
            </button>
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
