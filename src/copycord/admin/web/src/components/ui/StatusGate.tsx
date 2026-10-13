// Covers a page while useStatusGate says the bot is not ready: the content is
// blurred and inert, and the gate message is announced politely.

import type { ReactNode } from "react";
import type { StatusGateState } from "../../hooks/useStatusGate";
import { cx } from "./cx";
import { Spinner } from "./Spinner";

export function StatusGate({ gate, children }: { gate: StatusGateState; children: ReactNode }) {
  return (
    <div className="relative">
      <div
        inert={gate.gated || undefined}
        aria-hidden={gate.gated || undefined}
        className={cx(gate.gated && "pointer-events-none select-none blur-[4px]")}
      >
        {children}
      </div>
      {gate.gated ? (
        <div
          data-testid="status-gate"
          className="absolute inset-0 z-30 flex min-h-48 flex-col items-center justify-center gap-3 bg-black/40 px-4"
        >
          <Spinner size="lg" label="Waiting for the bot" />
          <p role="status" aria-live="polite" className="text-center text-sm font-semibold text-[#cfd3ff]">
            {gate.message}
          </p>
        </div>
      ) : null}
    </div>
  );
}
