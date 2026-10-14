// SPA task type registry: key -> {icon, Form, describeInputs}. A type the SPA has no entry for
// still runs: SchemaForm renders its inputs_schema (string, number, boolean, enum, date) and
// describeGeneric lists its inputs as key/value rows.

import { useState } from "react";
import type { ComponentType, FormEvent, ReactNode } from "react";
import { Modal } from "../../../components/Modal";
import { Button } from "../../../components/ui/Button";
import { Field, Input, Select } from "../../../components/ui/Field";
import type { TaskType } from "../../../types/tasks";
import { BackfillForm } from "./backfill/BackfillForm";
import { useStartRun, type TaskFormProps } from "./start";

export type { TaskFormProps } from "./start";

/** One row of the run detail's Inputs card. */
export interface InputRow {
  label: string;
  value: string;
}

export interface TaskTypeEntry {
  icon: ReactNode;
  Form: ComponentType<TaskFormProps>;
  describeInputs: (inputs: Record<string, unknown>) => InputRow[];
}

const RANGE_WORDS: Record<string, string> = {
  all: "All history",
  since: "Since",
  between: "Between",
  last_n: "Last",
};

function describeBackfill(inputs: Record<string, unknown>): InputRow[] {
  const r = (inputs.range ?? {}) as Record<string, unknown>;
  const mode = String(r.mode ?? "all");
  let range = RANGE_WORDS[mode] ?? mode;
  if (mode === "since") range = `Since ${String(r.since ?? "")}`;
  else if (mode === "between") range = `Between ${String(r.since ?? "")} and ${String(r.until ?? "")}`;
  else if (mode === "last_n") range = `Last ${String(r.last_n ?? "")} messages`;
  const n = (k: string) => (Array.isArray(inputs[k]) ? (inputs[k] as unknown[]).length : 0);
  return [
    { label: "Mapping", value: String(inputs.mapping_id ?? "") },
    { label: "Range", value: range },
    { label: "Ignore cloned", value: inputs.ignore_cloned ? "Yes" : "No" },
    { label: "Mode", value: inputs.mode === "new" ? "Start new" : "Resume previous" },
    { label: "Channels", value: `${n("channel_ids")} channels, ${n("category_ids")} categories` },
  ];
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
  backfill_channels: { icon: BackfillIcon, Form: BackfillForm, describeInputs: describeBackfill },
};

const formatValue = (v: unknown): string => {
  if (v === null || v === undefined) return "";
  if (typeof v === "boolean") return v ? "Yes" : "No";
  if (Array.isArray(v)) return v.map(formatValue).join(", ");
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
};

const humanize = (key: string) => key.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase());

/** The key/value Inputs card of a type without its own formatter. */
export function describeGeneric(inputs: Record<string, unknown>): InputRow[] {
  return Object.entries(inputs).map(([k, v]) => ({ label: humanize(k), value: formatValue(v) }));
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
    return {
      key,
      label: p.title ?? humanize(key),
      kind,
      required: required.has(key),
      options: (inner.enum ?? []).map(String),
      help: p.description,
      initial: p.default ?? (kind === "boolean" ? false : ""),
    };
  });
}

export function SchemaForm({ type, open, onClose, prefill }: TaskFormProps) {
  const fields = schemaFields(type.inputs_schema ?? {});
  const [values, setValues] = useState<Record<string, unknown>>(() => {
    const from = (prefill?.inputs ?? {}) as Record<string, unknown>;
    return Object.fromEntries(fields.map((f) => [f.key, from[f.key] ?? f.initial]));
  });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { start, busy } = useStartRun(type, prefill);

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
      title={prefill ? `Re-run ${type.title}` : `Run ${type.title}`}
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
              <input type="checkbox" checked={Boolean(values[f.key])} onChange={(e) => set(f.key, e.target.checked)} />
              {f.label}
              {errors[f.key] ? <span className="text-xs text-danger">{errors[f.key]}</span> : null}
            </label>
          ) : (
            <Field key={f.key} label={f.label} help={f.help} error={errors[f.key]}>
              {f.kind === "enum" ? (
                <Select
                  value={String(values[f.key] ?? "")}
                  onChange={(e) => set(f.key, e.target.value)}
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
