// main.tsx wraps the app in ToastProvider and ConfirmProvider, but the shell
// test in App.test.tsx renders the routes without them. Outside the providers
// toasts are dropped and every confirm answers no, so nothing destructive runs.

import { useConfirm } from "../../components/ui";
import type { ConfirmOptions } from "../../components/ui";
import { useToast } from "../../components/Toast";
import type { ToastKind } from "../../components/Toast";

export type Notify = (message: string, kind?: ToastKind) => void;
export type Confirm = (opts: ConfirmOptions) => Promise<boolean>;

const silent: Notify = () => undefined;
const decline: Confirm = async () => false;

// Both hooks run useContext on every render; only the missing provider throw is caught.
export function useNotify(): Notify {
  try {
    const { show } = useToast();
    return (message, kind = "info") => {
      show(message, { kind });
    };
  } catch {
    return silent;
  }
}

export function useSafeConfirm(): Confirm {
  try {
    return useConfirm();
  } catch {
    return decline;
  }
}
