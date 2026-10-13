// Verify server structure (#verify-dialog): scan for orphans, list them and
// delete them. Deleting is sent by the row-actions lane (useRowActions.deleteAll);
// this modal only lists and triggers it.

import { Modal } from "../../../components/Modal";
import { Button } from "../../../components/ui";
import type { Orphans } from "../../../types/channels";
import { verifyStatusText } from "./logic";

export interface VerifyModalProps {
  open: boolean;
  onClose: () => void;
  orphans: Orphans;
  scanning: boolean;
  scanned: boolean;
  onFetch: () => void;
  onDeleteAll: () => void;
  deleteBusy: boolean;
}

export function VerifyModal({ open, onClose, orphans, scanning, scanned, onFetch, onDeleteAll, deleteBusy }: VerifyModalProps) {
  const total = orphans.categories.length + orphans.channels.length;
  return (
    <Modal
      open={open}
      onClose={onClose}
      title="Verify server structure"
      footer={
        <div className="flex w-full items-center gap-2">
          <Button id="verify-fetch" type="button" variant="secondary" onClick={onFetch} disabled={scanning}>
            Fetch orphans
          </Button>
          <div className="flex-1" />
          <Button id="verify-delall" type="button" variant="danger" disabled={total === 0 || deleteBusy} onClick={onDeleteAll}>
            Delete all
          </Button>
          <Button id="verify-cancel" type="button" variant="ghost" onClick={onClose}>
            Close
          </Button>
        </div>
      }
    >
      <div id="verify-dialog" className="flex flex-col gap-3">
        <p id="verify-status" className="m-0 text-sm text-muted">
          {verifyStatusText(orphans, scanned, scanning)}
        </p>
        <section aria-labelledby="orph-cats-title" className="flex flex-col gap-1.5">
          <h4 id="orph-cats-title" className="m-0 text-sm font-semibold">
            Orphan categories
          </h4>
          <div id="orph-cats" aria-live="polite" className="flex flex-col gap-1.5">
            {orphans.categories.map((c) => (
              <div key={c.id} data-orphan-id={c.id} className="rounded-lg border border-border bg-white/4 px-2.5 py-1.5 text-sm">
                📂 {c.name} <span className="text-muted">({c.id})</span>
              </div>
            ))}
          </div>
        </section>
        <section aria-labelledby="orph-chs-title" className="flex flex-col gap-1.5">
          <h4 id="orph-chs-title" className="m-0 text-sm font-semibold">
            Orphan channels
          </h4>
          <div id="orph-chs" aria-live="polite" className="flex flex-col gap-1.5">
            {orphans.channels.map((c) => (
              <div key={c.id} data-orphan-id={c.id} className="rounded-lg border border-border bg-white/4 px-2.5 py-1.5 text-sm">
                # {c.name} <span className="text-muted">({c.id})</span>
                {c.category_name ? <span className="text-muted"> in {c.category_name}</span> : null}
              </div>
            ))}
          </div>
        </section>
      </div>
    </Modal>
  );
}
