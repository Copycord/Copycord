// Wire shapes of /api/tasks/* and the task.run.updated frame on /ws/out. Every id is a string.

export type RunStatus = "queued" | "running" | "success" | "failed" | "cancelled";
export type StepState = "queued" | "running" | "success" | "failed" | "skipped" | "cancelled";
export type RunTrigger = "manual" | "api";

export interface TaskType {
  key: string;
  title: string;
  description: string;
  inputs_schema: Record<string, unknown>;
  /** The type runs only while the server and client bots are both running. */
  requires_bots?: boolean;
  /** Dev mode (COPYCORD_ENV=dev): forms show their developer options. */
  dev?: boolean;
  /** Queued plus running runs of this type. */
  active: number;
  total: number;
}

export interface TaskContext {
  label: string;
  mapping_id?: string | null;
  guild_id?: string | null;
}

export interface TaskStep {
  key: string;
  title: string;
  state: StepState;
  label?: string | null;
  done?: number | null;
  total?: number | null;
  queue_position?: number | null;
  started_at?: string | null;
  finished_at?: string | null;
  note?: string | null;
}

/** One friendly log line in the run detail's log_lines; seq numbers the line, step names a step key. */
export interface TaskLogLine {
  seq: number;
  ts: string;
  level: "info" | "warn" | "error" | string;
  step?: string | null;
  msg: string;
}

export interface TaskRun {
  id: string;
  type: string;
  number: number;
  title: string;
  context: TaskContext | null;
  status: RunStatus;
  trigger: RunTrigger | string;
  actor: string;
  inputs?: Record<string, unknown>;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error: string | null;
  summary: Record<string, number> | null;
  /** How many steps the run has; list rows and frames carry it without the steps themselves. */
  step_count?: number;
  /** The detail response carries the friendly log lines; list rows may carry a count or nothing. */
  log_lines?: unknown;
  steps?: TaskStep[];
  report?: unknown;
  /** Monotonic per run; a frame with a lower seq than the one held is stale. */
  seq: number;
}

export interface TaskTypesResponse {
  types: TaskType[];
}

/** Runs per status under every filter but the status itself, and all of them: the quick status tabs. */
export type RunCounts = Record<RunStatus | "all", number>;

export interface TaskRunsResponse {
  runs: TaskRun[];
  total: number;
  counts?: RunCounts;
  page: number;
  actors: string[];
  contexts: TaskContext[];
}

export interface TaskRunResponse {
  run: TaskRun;
}

/** The payload of a task.run.updated frame: the run without inputs and only the steps that changed. */
export interface TaskRunUpdated {
  run: TaskRun;
  steps?: TaskStep[];
}

export interface RunFilters {
  type?: string;
  status?: RunStatus;
  actor?: string;
  context?: string;
  q?: string;
  page?: number;
}

export const ACTIVE_STATUSES: readonly RunStatus[] = ["queued", "running"];

/** One channel of GET /api/tasks/types/backfill_channels/options. */
export interface BackfillOptionChannel {
  id: string;
  name: string;
  orphan: boolean;
  /** Set while another run holds this channel. */
  busy: { run_id: string; number: number } | null;
  /** Set when an unfinished checkpoint exists. */
  resumable: { checkpoint_at: string | null } | null;
}

export interface BackfillOptionCategory {
  /** null for "Uncategorized". */
  id: string | null;
  name: string;
  channels: BackfillOptionChannel[];
}

export interface BackfillOptionsResponse {
  mapping_id: string;
  categories: BackfillOptionCategory[];
  error?: string;
}

/** GET /api/tasks/types/purge_messages/options: what a purge with these inputs would delete. */
export interface PurgePreviewResponse {
  matched: number;
  mappings: { mapping_id: string | null; name: string; matched: number }[];
  errors?: Record<string, string>;
  error?: string;
}

/** A 422 from POST /api/tasks/runs: one message per input field. */
export interface TaskInputErrors {
  errors: Record<string, string>;
}
