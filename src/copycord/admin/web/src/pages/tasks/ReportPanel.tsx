// "Report (JSON)": the run's structured result, collapsed by default. Open, it shows the report
// pretty printed with Copy (clipboard, toast) and Download (/api/tasks/runs/{id}/report.json).
// While the run is active and has no report yet it says the report comes at the end.

import { useMemo } from "react";
import { useToast } from "../../components/Toast";
import { cx } from "../../components/ui/cx";
import { ACTIVE_STATUSES } from "../../types/tasks";
import type { TaskRun } from "../../types/tasks";
import { runUrl } from "./runStore";
import { DISCLOSURE_BODY, DISCLOSURE_SUMMARY, DisclosureTitle, FRAME } from "./TasksSkeleton";

export function reportUrl(id: string): string {
  return `${runUrl(id)}/report.json`;
}

type RunToast = ReturnType<typeof useToast>;
const NO_TOAST: RunToast = { show: () => 0, dismiss: () => undefined };

/** useToast, or a no-op when no ToastProvider sits above (page tests render the page bare). */
export function useRunToast(): RunToast {
  try {
    return useToast();
  } catch {
    return NO_TOAST;
  }
}

export const ACTION =
  "inline-flex cursor-pointer items-center rounded-md border border-border bg-transparent px-2.5 py-1 text-xs font-semibold text-text-2 no-underline hover:bg-rail-hover hover:text-text";

export function ReportPanel({ run }: { run: TaskRun }) {
  const toast = useRunToast();
  const text = useMemo(() => (run.report == null ? null : JSON.stringify(run.report, null, 2)), [run.report]);
  const active = ACTIVE_STATUSES.includes(run.status);

  const copy = async () => {
    if (text == null) return;
    try {
      await navigator.clipboard.writeText(text);
      toast.show("Report copied", { kind: "success" });
    } catch {
      toast.show("Could not copy the report.", { kind: "error" });
    }
  };

  return (
    <section aria-label="Report">
      <details className={cx(FRAME, "group")}>
        <summary className={DISCLOSURE_SUMMARY}>
          <DisclosureTitle title="Report (JSON)" />
        </summary>
        <div className={DISCLOSURE_BODY}>
          {text == null ? (
            <p className="m-0 text-sm text-muted">{active ? "The report appears when the run finishes" : "This run has no report"}</p>
          ) : (
            <>
              <div className="mb-2 flex justify-end gap-2">
                <button type="button" onClick={() => void copy()} className={ACTION}>
                  Copy
                </button>
                <a href={reportUrl(run.id)} download className={ACTION}>
                  Download
                </a>
              </div>
              <pre
                aria-label="Report JSON"
                className="m-0 max-h-[420px] overflow-auto rounded-md border border-border bg-control p-3 font-mono text-xs leading-5 text-text-1"
              >
                {text}
              </pre>
            </>
          )}
        </div>
      </details>
    </section>
  );
}
