// Confirm dialog on top of Modal. Pages call
//   const confirm = useConfirm();
//   if (await confirm({ title: "Delete mapping?", confirmVariant: "ghost-red" })) ...
// The promise resolves true on Confirm and false on Cancel, Escape, the close
// button or an overlay click. ConfirmProvider must sit above the page.
//
// Cancel then the confirm button at the right, 8px apart, with no focus ring;
// the confirm button is "OK" in the ghost variant unless the caller passes
// confirmVariant.

import { createContext, useCallback, useContext, useRef, useState } from "react";
import type { ReactNode } from "react";
import { Modal } from "../Modal";
import { Button } from "./Button";
import type { ButtonVariant } from "./Button";

export interface ConfirmOptions {
  title: string;
  body?: ReactNode;
  confirmLabel?: string;
  cancelLabel?: string;
  /** Variant of the confirm button; default "ghost". */
  confirmVariant?: ButtonVariant;
  /** Variant of the Cancel button; default "ghost". */
  cancelVariant?: ButtonVariant;
  /** Alias for confirmVariant "ghost-red" when confirmVariant is not set. */
  danger?: boolean;
}

export interface ConfirmDialogProps extends ConfirmOptions {
  open: boolean;
  onResult: (ok: boolean) => void;
}

export function ConfirmDialog({
  open,
  onResult,
  title,
  body,
  confirmLabel = "OK",
  cancelLabel = "Cancel",
  confirmVariant,
  cancelVariant = "ghost",
  danger = false,
}: ConfirmDialogProps) {
  const okVariant = confirmVariant ?? (danger ? "ghost-red" : "ghost");
  return (
    <Modal
      open={open}
      title={title}
      onClose={() => onResult(false)}
      footer={
        <div className="flex gap-2">
          <Button variant={cancelVariant} size="confirm" flat onClick={() => onResult(false)}>
            {cancelLabel}
          </Button>
          <Button variant={okVariant} size="confirm" flat onClick={() => onResult(true)}>
            {confirmLabel}
          </Button>
        </div>
      }
    >
      {body ? <div className="text-sm text-text-2">{body}</div> : null}
    </Modal>
  );
}

type ConfirmFn = (opts: ConfirmOptions) => Promise<boolean>;

const ConfirmContext = createContext<ConfirmFn | null>(null);

export function ConfirmProvider({ children }: { children: ReactNode }) {
  const [opts, setOpts] = useState<ConfirmOptions | null>(null);
  const resolver = useRef<((ok: boolean) => void) | null>(null);

  const confirm = useCallback<ConfirmFn>((next) => {
    // A second confirm while one is open cancels the first.
    resolver.current?.(false);
    return new Promise<boolean>((resolve) => {
      resolver.current = resolve;
      setOpts(next);
    });
  }, []);

  const onResult = (ok: boolean) => {
    const resolve = resolver.current;
    resolver.current = null;
    setOpts(null);
    resolve?.(ok);
  };

  return (
    <ConfirmContext.Provider value={confirm}>
      {children}
      {opts ? <ConfirmDialog open {...opts} onResult={onResult} /> : null}
    </ConfirmContext.Provider>
  );
}

export function useConfirm(): ConfirmFn {
  const fn = useContext(ConfirmContext);
  if (!fn) throw new Error("useConfirm must be used inside ConfirmProvider");
  return fn;
}
