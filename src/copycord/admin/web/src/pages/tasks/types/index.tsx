// SPA task type registry: key -> {icon, Form, describeInputs, summaryLabels}. A type the SPA has no
// entry for still runs: SchemaForm renders its inputs_schema (string, number, boolean, enum, date),
// describeGeneric lists its inputs as rows labelled by the schema titles, and its summary keys are
// shown in words. The run detail page reads only this registry, so it has no per type code.

import { useState } from "react";
import type { ComponentType, FormEvent, ReactNode } from "react";
import { Modal } from "../../../components/Modal";
import { Button } from "../../../components/ui/Button";
import { Checkbox } from "../../../components/ui/Checkbox";
import { Field, FieldSelect, Input } from "../../../components/ui/Field";
import type { TaskRun, TaskStep } from "../../../types/tasks";
import { formatCount } from "../StepList";
import { BackfillForm } from "./backfill/BackfillForm";
import { useStartRun, type TaskFormProps } from "./start";

export type { TaskFormProps } from "./start";

/** One row of the run detail's Inputs list. */
export interface InputRow {
  label: string;
  value: string;
  /** A list input shown as display chips ("#general", "#news"); value stays its count in words. */
  chips?: string[];
}

/** The run fields a formatter reads: the context names the mapping, the steps name the channels. */
export type InputsRun = Pick<TaskRun, "context"> & { steps?: TaskStep[] };

/** One stat tile of the run detail's summary strip. */
export interface SummaryStat {
  key: string;
  label: string;
  value: number;
}

export interface TaskTypeEntry {
  icon: ReactNode;
  Form: ComponentType<TaskFormProps>;
  /** The run's inputs as rows; the run's context names the mapping, the schema titles the fields. */
  describeInputs: (inputs: Record<string, unknown>, run?: InputsRun, schema?: Record<string, unknown>) => InputRow[];
  /** Words for the summary keys the type reports; a key without one is shown in words from its name. */
  summaryLabels?: Record<string, string>;
  /** The summary key the run page shows large at the top of the Summary card, and its words after the number. */
  hero?: { key: string; label: string };
  /** A finished step's note in the words its running label uses; null keeps the server's note. */
  stepNote?: (step: TaskStep) => string | null;
}

const DAY = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", year: "numeric" });

function day(v: unknown): string {
  if (typeof v !== "string" || !v) return "?";
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? v : DAY.format(d);
}

/** A backfill range in words: "All history", "Since Oct 3, 2026", "Oct 1, 2026 to Oct 3, 2026", "Last 500 messages". */
export function rangeWords(range: unknown): string {
  if (!range || typeof range !== "object") return String(range ?? "");
  const r = range as { mode?: string; since?: unknown; until?: unknown; last_n?: unknown };
  switch (r.mode) {
    case "all":
      return "All history";
    case "since":
      return `Since ${day(r.since)}`;
    case "between":
      return `${day(r.since)} to ${day(r.until)}`;
    case "last_n":
      return `Last ${typeof r.last_n === "number" ? formatCount(r.last_n) : String(r.last_n)} messages`;
    default:
      return JSON.stringify(range);
  }
}

function counted(v: unknown, one: string, many: string): string {
  if (!Array.isArray(v)) return formatValue(v);
  return `${formatCount(v.length)} ${v.length === 1 ? one : many}`;
}

/**
 * The backfill's channels by name: each channel is a step keyed by its id and titled "#name". Undefined
 * while no step names any of them (the count in words stands alone); an unnamed one shows its id.
 */
export function channelNames(ids: unknown, steps?: TaskStep[]): string[] | undefined {
  if (!Array.isArray(ids) || !ids.length || !steps?.length) return undefined;
  const titles = new Map(steps.map((s) => [s.key, s.title]));
  if (!ids.some((id) => titles.has(String(id)))) return undefined;
  return ids.map((id) => titles.get(String(id)) ?? String(id));
}

const BACKFILL_MODE: Record<string, string> ={ resume: "Resume previous", new: "Start new" };

function describeBackfill(inputs: Record<string, unknown>, run?: InputsRun): InputRow[] {
  const rows: InputRow[] = [];
  for (const [k, v] of Object.entries(inputs)) {
    switch (k) {
      case "mapping_id":
        rows.push({ label: "Mapping", value: run?.context?.label || formatValue(v) });
        break;
      case "channel_ids":
        rows.push({ label: "Channels", value: counted(v, "channel", "channels"), chips: channelNames(v, run?.steps) });
        break;
      case "category_ids":
        rows.push({ label: "Categories", value: counted(v, "category", "categories") });
        break;
      case "range":
        rows.push({ label: "Range", value: rangeWords(v) });
        break;
      case "ignore_cloned":
        rows.push({ label: "Ignore cloned", value: formatValue(v) });
        break;
      case "mode":
        rows.push({ label: "Mode", value: (typeof v === "string" && BACKFILL_MODE[v]) || formatValue(v) });
        break;
      default:
        rows.push({ label: humanize(k), value: formatValue(v) });
    }
  }
  return rows;
}

// The tiles use the Steps card's words with their unit, steps or messages; delivered is the hero count.
const BACKFILL_SUMMARY: Record<string, string> = {
  success: "Steps done",
  failed: "Steps failed",
  skipped: "Steps skipped",
  cancelled: "Steps cancelled",
  delivered: "Messages delivered",
};
const BACKFILL_HERO = { key: "delivered", label: "messages delivered" };

/**
 * A running channel reads "9 of 36 delivered, 36 pulled"; once it ends it reads "36 delivered" (or "9
 * delivered, then cancelled") rather than the server's "36 messages", so the step keeps one wording.
 */
export function backfillStepNote(step: TaskStep): string | null {
  if (step.done == null) return null;
  if (step.state === "success") return `${formatCount(step.done)} delivered`;
  if (step.state === "cancelled" && step.done > 0) return `${formatCount(step.done)} delivered, then cancelled`;
  return null;
}

const BackfillIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <path d="M3 12a9 9 0 1 0 3-6.7" />
    <path d="M3 4v5h5" />
  </svg>
);

const GenericIcon = (
  <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <circle cx="12" cy="12" r="9" />
    <path d="m10 8 6 4-6 4z" />
  </svg>
);

export const TASK_TYPES: Record<string, TaskTypeEntry> = {
  backfill_channels: {
    icon: BackfillIcon,
    Form: BackfillForm,
    describeInputs: describeBackfill,
    summaryLabels: BACKFILL_SUMMARY,
    hero: BACKFILL_HERO,
    stepNote: backfillStepNote,
  },
};

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "boolean") return v ? "Yes" : "No";
  if (Array.isArray(v)) return v.map(formatValue).join(", ");
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

const humanize = (key: string) => key.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase());

/**
 * The Inputs rows of a type without its own formatter: the schema's fields in schema order, labelled by
 * their titles, then any input the schema does not name.
 */
export function describeGeneric(inputs: Record<string, unknown>, _run?: InputsRun, schema?: Record<string, unknown>): InputRow[] {
  const props = (schema?.properties ?? {}) as Record<string, SchemaProp>;
  const keys = [...Object.keys(props).filter((k) => k in inputs), ...Object.keys(inputs).filter((k) => !(k in props))];
  return keys.map((k) => ({ label: props[k]?.title ?? humanize(k), value: formatValue(inputs[k]) }));
}

/** The run's inputs as rows, by its type's formatter. */
export function describeRunInputs(run: Pick<TaskRun, "type" | "inputs"> & InputsRun, schema?: Record<string, unknown>): InputRow[] {
  return entryFor(run.type).describeInputs(run.inputs ?? {}, run, schema);
}

/** The numeric keys of the run's summary (else its report's summary) as stat tiles, in the order reported. */
export function summaryStats(run: Pick<TaskRun, "type" | "summary" | "report">): SummaryStat[] {
  const report = run.report && typeof run.report === "object" ? (run.report as { summary?: unknown }).summary : undefined;
  const summary = run.summary ?? (report && typeof report === "object" ? (report as Record<string, unknown>) : {});
  const labels = entryFor(run.type).summaryLabels ?? {};
  return Object.entries(summary)
    .filter((e): e is [string, number] => typeof e[1] === "number")
    .map(([key, value]) => ({ key, label: labels[key] ?? humanize(key), value }));
}

/** The type's hero count when the run reports it: "92" and "messages delivered"; null otherwise. */
export function heroStat(run: Pick<TaskRun, "type" | "summary" | "report">): SummaryStat | null {
  const hero = entryFor(run.type).hero;
  const stat = hero ? summaryStats(run).find((s) => s.key === hero.key) : undefined;
  return hero && stat ? { ...stat, label: hero.label } : null;
}

// JSON schema subset the generic form understands.
interface SchemaProp {
  type?: string | string[];
  enum?: unknown[];
  format?: string;
  title?: string;
  description?: string;
  default?: unknown;
  anyOf?: SchemaProp[];
}

export type SchemaKind = "string" | "number" | "boolean" | "enum" | "date";

export interface SchemaField {
  key: string;
  label: string;
  kind: SchemaKind;
  required: boolean;
  options: string[];
  help?: string;
  initial: unknown;
}

function kindOf(p: SchemaProp): SchemaKind {
  const inner = p.anyOf?.find((a) => a.type !== "null") ?? p;
  if (inner.enum) return "enum";
  const t = Array.isArray(inner.type) ? inner.type.find((x) => x !== "null") : inner.type;
  if (t === "boolean") return "boolean";
  if (t === "integer" || t === "number") return "number";
  if (inner.format === "date" || inner.format === "date-time") return "date";
  return "string";
}

/** The fields of an inputs_schema the generic form can render, in schema order. */
export function schemaFields(schema: Record<string, unknown>): SchemaField[] {
  const props = (schema.properties ?? {}) as Record<string, SchemaProp>;
  const required = new Set(Array.isArray(schema.required) ? (schema.required as string[]) : []);
  return Object.entries(props).map(([key, p]) => {
    const inner = p.anyOf?.find((a) => a.type !== "null") ?? p;
    const kind = kindOf(p);
    const options = (inner.enum ?? []).map(String);
    // A required enum has no blank option, so it starts on its first choice.
    const blank = kind === "boolean" ? false : kind === "enum" && required.has(key) ? (options[0] ?? "") : "";
    return {
      key,
      label: p.title ?? humanize(key),
      kind,
      required: required.has(key),
      options,
      help: p.description,
      initial: p.default ?? blank,
    };
  });
}

export function SchemaForm({ type, open, onClose }: TaskFormProps) {
  const fields = schemaFields(type.inputs_schema ?? {});
  const [values, setValues] = useState<Record<string, unknown>>(() => Object.fromEntries(fields.map((f) => [f.key, f.initial])));
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const missing: Record<string, string> = {};
    const inputs: Record<string, unknown> = {};
    for (const f of fields) {
      const v = values[f.key];
      if (f.kind === "boolean") inputs[f.key] = Boolean(v);
      else if (v === "" || v === undefined || v === null) {
        if (f.required) missing[f.key] = "Required.";
      } else inputs[f.key] = f.kind === "number" ? Number(v) : v;
    }
    setErrors(missing);
    if (Object.keys(missing).length) return;
    const res = await start(inputs);
    if (res && Object.keys(res).length) setErrors(res);
    else if (!res) onClose();
  };

  const set = (key: string, v: unknown) => setValues((prev) => ({ ...prev, [key]: v }));

  return (
    <Modal
      open={open}
      onClose={onClose}
      title={`Run ${type.title}`}
      className="w-[min(520px,92vw)]"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" type="submit" form="schema-form" loading={busy}>
            Start
          </Button>
        </>
      }
    >
      <form id="schema-form" noValidate onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-3">
        {fields.length === 0 ? <p className="m-0 text-sm text-muted">This task takes no inputs.</p> : null}
        {fields.map((f) =>
          f.kind === "boolean" ? (
            <label key={f.key} className="flex items-center gap-2 text-sm text-text">
              <Checkbox checked={Boolean(values[f.key])} onChange={(e) => set(f.key, e.target.checked)} />
              {f.label}
              {errors[f.key] ? <span className="text-xs text-danger">{errors[f.key]}</span> : null}
            </label>
          ) : (
            <Field key={f.key} label={f.label} help={f.help} error={errors[f.key]}>
              {f.kind === "enum" ? (
                <FieldSelect
                  value={String(values[f.key] ?? "")}
                  onChange={(v) => set(f.key, v)}
                  options={[...(f.required ? [] : [{ value: "", label: "" }]), ...f.options.map((o) => ({ value: o, label: o }))]}
                />
              ) : (
                <Input
                  type={f.kind === "number" ? "number" : f.kind === "date" ? "date" : "text"}
                  value={String(values[f.key] ?? "")}
                  onChange={(e) => set(f.key, e.target.value)}
                />
              )}
            </Field>
          ),
        )}
        {errors.inputs ? (
          <p role="alert" className="m-0 text-xs text-danger">
            {errors.inputs}
          </p>
        ) : null}
      </form>
    </Modal>
  );
}

export function entryFor(key: string): TaskTypeEntry {
  return TASK_TYPES[key] ?? { icon: GenericIcon, Form: SchemaForm, describeInputs: describeGeneric };
}

export function TaskTypeForm(props: TaskFormProps) {
  const { Form } = entryFor(props.type.key);
  return <Form {...props} />;
}
