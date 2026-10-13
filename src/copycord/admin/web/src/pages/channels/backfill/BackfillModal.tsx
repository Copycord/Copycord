// Clone Channel History (#backfill-dialog / #bf-form): how far back to clone and
// whether to skip messages that were already cloned. One channel posts to
// /api/backfill/start, several to /api/backfill/start-batch (see useBackfill).

import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../../components/Modal";
import { Button, Input } from "../../../components/ui";
import type { BackfillMode } from "../../../types/channels";
import { EMPTY_FORM, validateForm } from "./logic";
import type { BackfillForm, FormErrors } from "./logic";

export interface BackfillModalProps {
  /** Original channel ids to clone; more than one runs the batch route. */
  channelIds: string[];
  submitting: boolean;
  onSubmit: (form: BackfillForm) => void;
  onClose: () => void;
}

const MODES: Array<{ value: BackfillMode; label: string }> = [
  { value: "all", label: "All history" },
  { value: "since", label: "Since date/time" },
  { value: "between", label: "Between dates" },
  { value: "last", label: "Last N messages" },
];

function Err({ text }: { text?: string }) {
  return text ? (
    <p role="alert" className="m-0 text-xs text-danger">
      {text}
    </p>
  ) : null;
}

export function BackfillModal({ channelIds, submitting, onSubmit, onClose }: BackfillModalProps) {
  const [f, setF] = useState<BackfillForm>(EMPTY_FORM);
  const [errors, setErrors] = useState<FormErrors>({});
  const set = <K extends keyof BackfillForm>(k: K, v: BackfillForm[K]) => {
    setF((cur) => ({ ...cur, [k]: v }));
    setErrors({});
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    const errs = validateForm(f);
    setErrors(errs);
    if (Object.keys(errs).length === 0) onSubmit(f);
  };

  const radio = (m: { value: BackfillMode; label: string }) => (
    <label key={m.value} className="flex cursor-pointer items-center gap-2 text-sm">
      <input type="radio" name="mode" value={m.value} checked={f.mode === m.value} onChange={() => set("mode", m.value)} />
      {m.label}
    </label>
  );

  return (
    <Modal
      open
      onClose={onClose}
      title={channelIds.length > 1 ? `Clone Channel History (${channelIds.length} channels)` : "Clone Channel History"}
    >
      <form id="bf-form" noValidate onSubmit={submit} className="flex flex-col gap-3">
        <fieldset className="flex flex-col gap-2 rounded-lg border border-border px-3 py-2.5">
          <legend className="px-1 text-sm font-semibold">How far back?</legend>
          {radio(MODES[0]!)}
          {radio(MODES[1]!)}
          <div className="ml-6 flex flex-wrap gap-2">
            <Input type="date" id="bf-since" aria-label="Since date" disabled={f.mode !== "since"} value={f.sinceDate} onChange={(e) => set("sinceDate", e.target.value)} aria-invalid={errors.since ? true : undefined} className="w-auto!" />
            <Input type="time" id="bf-since-time" aria-label="Since time" disabled={f.mode !== "since"} value={f.sinceTime} onChange={(e) => set("sinceTime", e.target.value)} className="w-auto!" />
            <Err text={errors.since} />
          </div>
          {radio(MODES[2]!)}
          <div className="ml-6 flex flex-col gap-2">
            <div className="flex flex-wrap gap-2">
              <Input type="date" id="bf-from" aria-label="From" disabled={f.mode !== "between"} value={f.fromDate} onChange={(e) => set("fromDate", e.target.value)} aria-invalid={errors.from ? true : undefined} className="w-auto!" />
              <Input type="time" id="bf-from-time" aria-label="From time" disabled={f.mode !== "between"} value={f.fromTime} onChange={(e) => set("fromTime", e.target.value)} className="w-auto!" />
            </div>
            <Err text={errors.from} />
            <div className="flex flex-wrap gap-2">
              <Input type="date" id="bf-to" aria-label="To" disabled={f.mode !== "between"} value={f.toDate} onChange={(e) => set("toDate", e.target.value)} aria-invalid={errors.to ? true : undefined} className="w-auto!" />
              <Input type="time" id="bf-to-time" aria-label="To time" disabled={f.mode !== "between"} value={f.toTime} onChange={(e) => set("toTime", e.target.value)} className="w-auto!" />
            </div>
            <Err text={errors.to} />
          </div>
          {radio(MODES[3]!)}
          <div className="ml-6 flex flex-col gap-1">
            <Input type="number" id="bf-lastn" aria-label="Last N" min={1} step={1} placeholder="100" disabled={f.mode !== "last"} value={f.lastN} onChange={(e) => set("lastN", e.target.value)} aria-invalid={errors.last ? true : undefined} className="w-40!" />
            <Err text={errors.last} />
          </div>
        </fieldset>

        <fieldset className="rounded-lg border border-border px-3 py-2.5">
          <legend className="px-1 text-sm font-semibold">Settings</legend>
          <label className="flex cursor-pointer items-center gap-2 text-sm" title="Filters out already-cloned messages using the local DB. Disable DB_CLEANUP_MSG in mapping settings to keep records intact.">
            <input type="checkbox" id="bf-ignore-cloned" name="ignore_cloned" checked={f.ignoreCloned} onChange={(e) => set("ignoreCloned", e.target.checked)} />
            Ignore cloned messages
          </label>
        </fieldset>

        <div className="flex justify-end">
          <Button id="bf-start" type="submit" variant="ghost" disabled={submitting}>
            Start
          </Button>
        </div>
      </form>
    </Modal>
  );
}
