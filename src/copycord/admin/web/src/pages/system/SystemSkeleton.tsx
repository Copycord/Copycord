// The System page while its chunk downloads: the same collapsed Database card the page shows, with its
// real title and the collapse button as inert chrome, a bar where the last backup time goes, and the
// panel's buttons, archive table head and loading rows (hidden while collapsed, as on the page). Kept
// out of System.tsx so the app shell can show it without pulling the page chunk in; the page takes its
// shared classes, the table head and the loading rows from here.

import { Card, Skeleton, cx } from "../../components/ui";
import { readLastCount } from "../../components/ui/lastCount";
import { CHEV, CHEV_BUTTON, GhostButton, LinkButton } from "./buttons";

export const DB_CARD = "mt-[18px] max-w-full";
export const DB_HEADER = "mb-1.5 flex items-center justify-between gap-3 px-4 py-3";
export const DB_TITLE = "my-[1em] text-[1.17em] font-bold";
export const PANEL_ACTIONS = "flex flex-wrap gap-2";
export const ARCHIVES_WRAP = "overflow-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden";
export const ARCHIVES_TABLE = "mt-2 w-full border-separate border-spacing-0 text-left md:table-fixed";

// Narrow screens let the columns size to their content and stack the row buttons.
const HEAD = "px-1.5 py-2.5 font-semibold text-accent md:px-2";
export const CELL = "border-b border-border px-1.5 py-2.5 md:px-2";
export const NAME_CODE = "font-mono text-[13px] break-all";
export const NOWRAP_CELL = cx(CELL, "whitespace-nowrap");
export const ACTIONS_CELL = cx(CELL, "pr-4 text-right");
export const ROW_ACTIONS = "inline-flex flex-col items-end gap-2 md:flex-row md:whitespace-nowrap";
export const ROW_BUTTON = "max-md:min-h-[34px] max-md:px-3";

/** The last backup time until the backup info answers: a bar about as wide as a date. */
export function LastBackupPending() {
  return (
    <Skeleton.Text>Oct 9 2026 10:00</Skeleton.Text>
  );
}

/** The archive table head: Name, Size, Time and the row actions. */
export function ArchivesHead({ hidden }: { hidden: boolean }) {
  return (
    <thead hidden={hidden}>
      <tr>
        <th scope="col" className={HEAD}>Name</th>
        <th scope="col" className={HEAD}>Size</th>
        <th scope="col" className={HEAD}>Time</th>
        <th scope="col" className={cx(HEAD, "pr-4 text-right md:w-[330px]")}>
          <span className="sr-only">Actions</span>
        </th>
      </tr>
    </thead>
  );
}

// As many loading rows as the list had last time, three on a first visit.
export const ARCHIVES_COUNT_KEY = "system.archives";

/** Archive rows while the backup info loads: the row's four cells, bars on the values, the buttons as chrome. */
export function ArchiveRowsLoading({ rows = readLastCount(ARCHIVES_COUNT_KEY, 3, 5) }: { rows?: number }) {
  return (
    <>
      {Array.from({ length: rows }, (_, i) => (
        <tr key={i} data-testid="adv-archive-skeleton" aria-hidden="true">
          <td className={CELL}>
            {i === 0 ? <span role="status" aria-label="Loading backups" className="sr-only" /> : null}
            <Skeleton.Text className={NAME_CODE} width={210} />
          </td>
          <td className={NOWRAP_CELL}>
            <Skeleton.Text width={56} />
          </td>
          <td className={NOWRAP_CELL}>
            <Skeleton.Text width={84} />
          </td>
          <td className={ACTIONS_CELL}>
            <span className={ROW_ACTIONS}>
              <LinkButton tabIndex={-1} className={ROW_BUTTON}>
                Download
              </LinkButton>
              <GhostButton compact tabIndex={-1} className={ROW_BUTTON}>
                Restore
              </GhostButton>
              <GhostButton compact danger tabIndex={-1} className={ROW_BUTTON}>
                Delete
              </GhostButton>
            </span>
          </td>
        </tr>
      ))}
    </>
  );
}

export function SystemSkeleton() {
  return (
    <div role="status" aria-busy="true" aria-label="Loading page">
      <h1 className="sr-only">System</h1>
      <Card className={DB_CARD} data-testid="card-skeleton">
        <div className={DB_HEADER}>
          <div className="flex flex-col gap-1 leading-[normal]">
            <h2 className={DB_TITLE}>Database</h2>
            <div className="opacity-85">
              Last backup:{" "}
              <span>
                <LastBackupPending />
              </span>
            </div>
          </div>
          <button type="button" inert aria-label="Expand panel" aria-expanded={false} className={CHEV_BUTTON}>
            <span aria-hidden="true" className={CHEV}>
              ▸
            </span>
          </button>
        </div>
        <div inert className="mt-1.5">
          <div className={PANEL_ACTIONS} hidden>
            <GhostButton>Backup now</GhostButton>
            <LinkButton keepOpacity disabled>
              Download latest
            </LinkButton>
            <GhostButton>Restore</GhostButton>
          </div>
          <div className={ARCHIVES_WRAP}>
            <table className={ARCHIVES_TABLE}>
              <ArchivesHead hidden />
              <tbody hidden>
                <ArchiveRowsLoading />
              </tbody>
            </table>
          </div>
        </div>
      </Card>
    </div>
  );
}
