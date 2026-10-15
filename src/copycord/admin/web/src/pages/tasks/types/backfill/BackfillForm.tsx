// Start form of Backfill channels: mapping, channel picker, how far back (v3 wording and
// validation), Ignore cloned with the v3 tip, and Resume previous / Start new when a picked
// channel has a checkpoint. Start posts the run and opens it.

import { useMemo, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import { Modal, TipLabel } from "../../../../components/Modal";
import { Button } from "../../../../components/ui/Button";
import { Checkbox, Radio } from "../../../../components/ui/Checkbox";
import { Field, FieldSelect, Input } from "../../../../components/ui/Field";
import { useApi } from "../../../../hooks/useApi";
import type { GuildMappingListResponse } from "../../../../types/channels";
import type { BackfillOptionsResponse } from "../../../../types/tasks";
import { useStartRun, type TaskFormProps } from "../start";
import { ChannelPicker, splitSelection } from "./ChannelPicker";
import {
  EMPTY_RANGE,
  rangeInputs,
  slotErrors,
  validateRange,
  type FormSlot,
  type RangeErrors,
  type RangeForm,
  type RangeMode,
  type RunMode,
} from "./logic";

export const IGNORE_CLONED_TIP =
  "Filters out already-cloned messages using the local DB. Disable DB_CLEANUP_MSG in mapping settings to keep records intact.";

const RANGE_OPTIONS: { value: RangeMode; label: string }[] = [
  { value: "all", label: "All history" },
  { value: "since", label: "Since date/time" },
  { value: "between", label: "Between dates" },
  { value: "last_n", label: "Last N messages" },
];

const FIELDSET = "m-0 flex min-w-0 flex-col gap-1 border-0 p-0";
const CHOICE = "flex cursor-pointer items-center gap-2 text-sm text-text";

function Indent({ children }: { children: ReactNode }) {
  return <div className="mt-1 mb-2 ml-[26px] flex min-w-0 flex-wrap gap-2">{children}</div>;
}

export function BackfillForm({ type, open, onClose }: TaskFormProps) {
  const mappings = useApi<GuildMappingListResponse>(open ? "/api/guild-mappings" : null);
  const list = mappings.data?.mappings ?? [];
  const [picked, setPicked] = useState<string | null>(null);
  const mappingId = picked ?? list[0]?.mapping_id ?? "";
  const options = useApi<BackfillOptionsResponse>(
    open && mappingId ? `/api/tasks/types/${type.key}/options?mapping_id=${encodeURIComponent(mappingId)}` : null,
  );
  const categories = useMemo(() => options.data?.categories ?? [], [options.data]);

  const [selected, setSelected] = useState<Set<string>>(() => new Set());
  const [range, setRange] = useState<RangeForm>(EMPTY_RANGE);
  const [ignoreCloned, setIgnoreCloned] = useState(false);
  const [mode, setMode] = useState<RunMode>("resume");
  const [rangeErrors, setRangeErrors] = useState<RangeErrors>({});
  const [errors, setErrors] = useState<Partial<Record<FormSlot, string>>>({});
  const { start, busy } = useStartRun(type);

  const resumable = useMemo(
    () => categories.some((cat) => cat.channels.some((c) => c.resumable && selected.has(c.id))),
    [categories, selected],
  );

  const onMapping = (id: string) => {
    if (id === mappingId) return;
    setPicked(id);
    setSelected(new Set());
    setErrors({});
  };

  const setR = (patch: Partial<RangeForm>) => {
    setRange((r) => ({ ...r, ...patch }));
    setRangeErrors({});
  };

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const rErr = validateRange(range);
    const next: Partial<Record<FormSlot, string>> = {};
    if (!mappingId) next.mapping = "Pick a mapping.";
    if (selected.size === 0) next.channels = "Select at least one channel or category.";
    setRangeErrors(rErr);
    setErrors(next);
    if (Object.keys(rErr).length || Object.keys(next).length) return;
    const body = {
      mapping_id: mappingId,
      ...splitSelection(categories, selected),
      range: rangeInputs(range),
      ignore_cloned: ignoreCloned,
      mode: resumable ? mode : "new",
    };
    const res = await start(body);
    if (res && Object.keys(res).length) setErrors(slotErrors(res));
    else if (!res) onClose();
  };

  const footer = (
    <>
      <Button variant="ghost" onClick={onClose}>
        Cancel
      </Button>
      <Button variant="primary" type="submit" form="backfill-form" loading={busy}>
        Start
      </Button>
    </>
  );

  const radio = (value: RangeMode, label: string) => (
    <label className={CHOICE}>
      <Radio name="bf-range" value={value} checked={range.mode === value} onChange={() => setR({ mode: value })} />
      {label}
    </label>
  );

  const fieldError = (msg?: string) =>
    msg ? (
      <p role="alert" className="m-0 w-full text-xs text-danger">
        {msg}
      </p>
    ) : null;

  return (
    <Modal open={open} onClose={onClose} title={`Run ${type.title}`} className="w-[min(640px,92vw)]" footer={footer}>
      <form id="backfill-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
        <Field label="Mapping" error={errors.mapping} id="bf-mapping">
          <FieldSelect
            value={mappingId}
            onChange={onMapping}
            disabled={list.length === 0}
            options={list.map((m) => ({
              value: m.mapping_id,
              label: m.mapping_name || `${m.original_guild_name ?? m.original_guild_id} to ${m.cloned_guild_name ?? m.cloned_guild_id}`,
            }))}
          />
        </Field>

        <div className="flex flex-col gap-1.5">
          <span className="ml-0.5 text-xs text-muted">Channels</span>
          {options.loading && !options.data ? (
            <p className="m-0 text-sm text-muted">Loading channels...</p>
          ) : options.data?.error ? (
            <p role="alert" className="m-0 text-sm text-danger">
              {options.data.error}
            </p>
          ) : (
            <ChannelPicker categories={categories} selected={selected} onChange={setSelected} error={errors.channels} />
          )}
        </div>

        <fieldset className={FIELDSET}>
          <legend className="mb-1.5 ml-0.5 text-xs text-muted">How far back?</legend>
          {radio("all", RANGE_OPTIONS[0]!.label)}
          {radio("since", RANGE_OPTIONS[1]!.label)}
          {range.mode === "since" ? (
            <Indent>
              <Input type="date" aria-label="Since date" className="w-auto" value={range.sinceDate} aria-invalid={rangeErrors.since ? true : undefined} onChange={(e) => setR({ sinceDate: e.target.value })} />
              <Input type="time" aria-label="Since time" className="w-auto" value={range.sinceTime} onChange={(e) => setR({ sinceTime: e.target.value })} />
              {fieldError(rangeErrors.since)}
            </Indent>
          ) : null}
          {radio("between", RANGE_OPTIONS[2]!.label)}
          {range.mode === "between" ? (
            <Indent>
              <Input type="date" aria-label="From" className="w-auto" value={range.fromDate} aria-invalid={rangeErrors.from ? true : undefined} onChange={(e) => setR({ fromDate: e.target.value })} />
              <Input type="time" aria-label="From time" className="w-auto" value={range.fromTime} onChange={(e) => setR({ fromTime: e.target.value })} />
              {fieldError(rangeErrors.from)}
              <Input type="date" aria-label="To" className="w-auto" value={range.toDate} aria-invalid={rangeErrors.to ? true : undefined} onChange={(e) => setR({ toDate: e.target.value })} />
              <Input type="time" aria-label="To time" className="w-auto" value={range.toTime} onChange={(e) => setR({ toTime: e.target.value })} />
              {fieldError(rangeErrors.to)}
            </Indent>
          ) : null}
          {radio("last_n", RANGE_OPTIONS[3]!.label)}
          {range.mode === "last_n" ? (
            <Indent>
              <Input type="number" min={1} step={1} placeholder="100" aria-label="Last N messages" className="w-40" value={range.lastN} aria-invalid={rangeErrors.last ? true : undefined} onChange={(e) => setR({ lastN: e.target.value })} />
              {fieldError(rangeErrors.last)}
            </Indent>
          ) : null}
          {fieldError(errors.range)}
        </fieldset>

        <fieldset className={FIELDSET}>
          <legend className="mb-1.5 ml-0.5 text-xs text-muted">Settings</legend>
          {/* The closed tip bubble is display none, so the hidden box does not widen or lengthen the scrolling body. */}
          <div className="flex items-center gap-2 [&_.tip-bubble[aria-hidden=true]]:hidden">
            <Checkbox id="bf-ignore-cloned" checked={ignoreCloned} onChange={(e) => setIgnoreCloned(e.target.checked)} />
            <TipLabel htmlFor="bf-ignore-cloned" text="Ignore cloned messages" tip={IGNORE_CLONED_TIP} tipLabel="What is Ignore cloned messages?" align="wrap" className="m-0! text-sm! text-text!" />
          </div>
          {resumable ? (
            <div role="radiogroup" aria-label="Mode" className="mt-2 flex flex-col gap-1">
              <span className="ml-0.5 text-xs text-muted">Some picked channels have an unfinished backfill</span>
              <label className={CHOICE}>
                <Radio name="bf-mode" checked={mode === "resume"} onChange={() => setMode("resume")} />
                Resume previous
              </label>
              <label className={CHOICE}>
                <Radio name="bf-mode" checked={mode === "new"} onChange={() => setMode("new")} />
                Start new
              </label>
            </div>
          ) : null}
        </fieldset>
        {fieldError(errors.general)}
      </form>
    </Modal>
  );
}
