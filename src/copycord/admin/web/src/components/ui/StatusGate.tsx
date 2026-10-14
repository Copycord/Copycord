// Covers a page while useStatusGate says the bot is not ready: the content is
// blurred and inert, and the gate message is announced politely. Like the old #status-gate the
// dim is fixed over the whole viewport under the header, below the side menu drawer (see Z).

import type { ReactNode } from "react";
import type { StatusGateState } from "../../hooks/useStatusGate";
import { cx, Z } from "./cx";
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
          className={cx("fixed inset-x-0 top-header bottom-0 flex flex-col items-center justify-center gap-3 bg-black/40 px-4", Z.gate)}
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
