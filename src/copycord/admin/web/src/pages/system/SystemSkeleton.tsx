// The System page while its chunk downloads: the same collapsed Database card the page shows, with its
// real title and the collapse button as inert chrome, and a bar where the last backup time goes. Kept
// out of System.tsx so the app shell can show it without pulling the page chunk in; the page takes its
// shared classes and the bar from here.

import { Card } from "../../components/ui";
import { CHEV, CHEV_BUTTON } from "./buttons";

export const DB_CARD = "mt-[18px] max-w-full";
export const DB_HEADER = "mb-1.5 flex items-center justify-between gap-3 px-4 py-3";
export const DB_TITLE = "my-[1em] text-[1.17em] font-bold";
export const ARCHIVES_WRAP = "overflow-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden";
export const ARCHIVES_TABLE = "mt-2 w-full border-separate border-spacing-0 text-left md:table-fixed";

/** The last backup time until the backup info answers: a bar about as wide as a date. */
export function LastBackupPending() {
  return (
    <span data-skeleton="" className="cc-skeleton inline-block rounded-md text-transparent">
      Oct 9 2026 10:00
    </span>
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
          <button type="button" inert aria-hidden="true" className={CHEV_BUTTON}>
            <span className={CHEV}>▸</span>
          </button>
        </div>
        <div className="mt-1.5">
          <div className={ARCHIVES_WRAP}>
            <table className={ARCHIVES_TABLE} />
          </div>
        </div>
      </Card>
    </div>
  );
}
