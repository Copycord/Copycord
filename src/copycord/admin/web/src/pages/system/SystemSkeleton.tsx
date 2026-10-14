// The System page while its chunk downloads: the same collapsed Database card the page shows, with its
// real title, the "Last backup" line still at its placeholder and the collapse button. Kept out of
// System.tsx so the app shell can show it without pulling the page chunk in.

import { Card } from "../../components/ui";
import { CHEV, CHEV_BUTTON } from "./buttons";
import { NO_VALUE } from "./format";

export function SystemSkeleton() {
  return (
    <div role="status" aria-busy="true" aria-label="Loading page">
      <h1 className="sr-only">System</h1>
      <Card className="mt-[18px] max-w-full" data-testid="card-skeleton">
        <div className="mb-1.5 flex items-center justify-between gap-3 px-4 py-3">
          <div className="flex flex-col gap-1 leading-[normal]">
            <h2 className="my-[1em] text-[1.17em] font-bold">Database</h2>
            <div className="opacity-85">
              Last backup: <span>{NO_VALUE}</span>
            </div>
          </div>
          <button type="button" disabled aria-hidden="true" tabIndex={-1} className={CHEV_BUTTON}>
            <span className={CHEV}>▸</span>
          </button>
        </div>
        <div className="mt-1.5">
          <table className="mt-2 w-full border-separate border-spacing-0 text-left md:table-fixed" />
        </div>
      </Card>
    </div>
  );
}
