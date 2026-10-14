// Clone history of the Channels page: the "Clone messages" menu item, the
// resume prompt, the backfill modal and the live progress over /ws/out. The
// result plugs into the channelStatus slot and into useRowActions.extraItems.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useToast } from "../../../components/Toast";
import { useConfirm } from "../../../components/ui";
import { useWsMessages } from "../../../hooks/useWsMessages";
import { api, ApiError } from "../../../lib/api";
import type { BusMessage } from "../../../types/api";
import type {
  BackfillBatchResponse,
  BackfillEvent,
  BackfillStartResponse,
  CloneState,
  InflightResponse,
  QueueResponse,
  ResumeInfo,
  ResumeInfoResponse,
} from "../../../types/channels";
import type { MenuItem } from "../actions/RowMenu";
import type { TreeChannel } from "../model";
import { BackfillModal } from "./BackfillModal";
import { ChannelStatus } from "./ChannelStatus";
import { createCloneStore } from "./cloneStore";
import {
  batchBody,
  batchSummary,
  cancelText,
  canResume,
  claimToast,
  eventChannelId,
  loadDoneTasks,
  loadTaskMap,
  progressOf,
  queueStates,
  resumeBody,
  resumeInfoUrl,
  saveDoneTasks,
  saveSet,
  saveTaskMap,
  scopedUrl,
  stageForProgress,
  startBody,
  TOAST_TTL_MS,
} from "./logic";
import type { BackfillForm } from "./logic";

const POLL_MS = 5000;
const NOTE_TTL_MS = 15000;
const WS_LEAD_MS = 8000;

export interface BackfillInput {
  mappingId: string;
  /** Display name of an original channel id, for the done toast. */
  nameOf: (cid: string) => string;
}

export interface Backfill {
  /** Menu items to add to a channel's menu (#ctx-backfill). */
  items: (c: TreeChannel) => MenuItem[];
  channelStatus: (c: TreeChannel) => ReactNode;
  dialogs: ReactNode;
  /** Opens the modal for several channels (batch route). */
  openBatch: (ids: string[]) => void;
}

export function useBackfill({ mappingId, nameOf }: BackfillInput): Backfill {
  const toast = useToast();
  const confirm = useConfirm();
  const store = useMemo(() => createCloneStore(), []);
  const [target, setTarget] = useState<string[] | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const taskMap = useRef(loadTaskMap());
  const doneTasks = useRef(loadDoneTasks());
  const startedHere = useRef(new Set<string>());
  const launchKey = useRef(new Map<string, string>());
  const nameRef = useRef(nameOf);
  nameRef.current = nameOf;
  const statusEls = useRef(new Map<string, ReactNode>());
  /** When the websocket last spoke for a channel; the poll does not override it for a few seconds. */
  const wsAt = useRef(new Map<string, number>());

  const note = useCallback(
    (key: string, msg: string, kind: "info" | "success" | "error", ttl = NOTE_TTL_MS) => {
      if (claimToast(key, ttl)) toast.show(msg, { kind });
    },
    [toast],
  );

  // Mirror the store into the bf:* sets the old page kept.
  useEffect(
    () =>
      store.subscribe(() => {
        const by = (...stages: CloneState["stage"][]) =>
          [...store.entries()].filter(([, s]) => stages.includes(s.stage)).map(([k]) => k);
        saveSet("running", by("pulling", "cloning", "cleaning"));
        saveSet("launching", by("launching"));
        saveSet("pulling", by("pulling"));
        saveSet("queued", by("queued"));
        saveSet("cleaning", by("cleaning"));
      }),
    [store],
  );

  const rememberTask = (tid: unknown, cid: string) => {
    if (tid == null) return;
    taskMap.current.set(String(tid), cid);
    saveTaskMap(taskMap.current);
  };

  const refresh = useCallback(async () => {
    try {
      const inflight = await api.get<InflightResponse>(scopedUrl("/api/backfills/inflight", mappingId));
      const next = new Map<string, CloneState>();
      // Keep local launches and cleanups the server does not list yet.
      const fresh = (k: string) => Date.now() - (wsAt.current.get(k) ?? 0) < WS_LEAD_MS;
      for (const [k, s] of store.entries()) if (s.stage === "launching" || s.stage === "cleaning" || fresh(k)) next.set(k, s);
      for (const [cid, info] of Object.entries(inflight.items ?? {})) {
        const cur = store.get(cid);
        if (cur?.stage === "cleaning" || fresh(cid)) continue;
        const delivered = info?.delivered ?? null;
        const total = info?.expected_total ?? null;
        next.set(cid, { stage: stageForProgress(delivered, total), delivered, total });
      }
      const queue = await api.get<QueueResponse>(scopedUrl("/api/backfills/queue", mappingId)).catch(() => undefined);
      if (queue?.items) {
        const q = queueStates(queue.items, (cid) => (next.get(cid)?.delivered ?? 0) > 0);
        for (const [cid, s] of q) if (!next.has(cid)) next.set(cid, s);
      }
      store.replace(next);
    } catch {
      // The websocket keeps the cards current; the next poll retries.
    }
  }, [mappingId, store]);

  const finalize = useCallback(
    (cid: string, taskId: string | null, announce = true) => {
      const initiated = startedHere.current.has(cid) || !!store.get(cid);
      store.clear(cid);
      startedHere.current.delete(cid);
      launchKey.current.delete(cid);
      if (taskId) {
        doneTasks.current.add(taskId);
        saveDoneTasks(doneTasks.current);
      }
      if (announce && initiated) {
        const name = nameRef.current(cid);
        note(
          taskId ? `bf:done-task:${taskId}` : `bf:done:${cid}`,
          name ? `Clone completed for #${name}.` : `Clone completed (channel ${cid}).`,
          "success",
          TOAST_TTL_MS,
        );
        store.markSynced(cid);
      }
    },
    [note, store],
  );

  const onMessage = useCallback(
    (msg: BusMessage) => {
      const raw = msg as { kind?: string; payload?: BackfillEvent };
      const p = (raw.payload ?? msg) as BackfillEvent;
      const kind = raw.kind ?? (p as { kind?: string }).kind ?? "client";
      if (kind !== "client" || !p.type?.startsWith("backfill_")) return;
      const mid = p.data?.mapping_id ? String(p.data.mapping_id) : "";
      if (mappingId && mid && mid !== mappingId) return;
      const tid = p.task_id != null ? String(p.task_id) : null;
      if (tid && doneTasks.current.has(tid)) return;
      const cid = eventChannelId(p, taskMap.current);
      if (!cid) return;
      const cur = store.get(cid);
      const state = String(p.data?.state ?? "").toLowerCase();
      wsAt.current.set(cid, Date.now());

      switch (p.type) {
        case "backfill_started":
        case "backfill_ack":
        case "backfill_busy": {
          if (!cur && !startedHere.current.has(cid) && !(tid && taskMap.current.has(tid))) return;
          rememberTask(tid, cid);
          const wasLaunching = cur?.stage === "launching";
          startedHere.current.add(cid);
          store.set(cid, { stage: "pulling", delivered: null, total: null });
          if (wasLaunching) {
            const key = tid ? `bf:started:${cid}:${tid}` : `bf:started:${cid}:${launchKey.current.get(cid) ?? Date.now()}`;
            note(key, p.type === "backfill_busy" ? "A clone for this channel is already running or finishing up." : "Clone started…", "success");
          }
          setTarget(null);
          return;
        }
        case "backfill_progress": {
          if (cur?.stage === "cleaning") return;
          rememberTask(tid, cid);
          const { delivered, total } = progressOf(p, { delivered: cur?.delivered ?? null, total: cur?.total ?? null });
          store.set(cid, { stage: stageForProgress(delivered, total), delivered, total });
          return;
        }
        case "backfill_cleanup":
        case "backfill_done": {
          if (state === "starting") {
            store.set(cid, { stage: "cleaning", delivered: cur?.delivered ?? null, total: cur?.total ?? null });
            return;
          }
          if (state === "finished" || (p.type === "backfill_done" && p.data?.no_work === true)) finalize(cid, tid);
          return;
        }
        case "backfill_cancelled": {
          store.clear(cid);
          startedHere.current.delete(cid);
          if (tid) {
            taskMap.current.delete(tid);
            saveTaskMap(taskMap.current);
          }
          try {
            sessionStorage.setItem(`bf:cancelled:${cid}`, String(Date.now()));
          } catch {
            // ignore
          }
          note(`bf:cancel:${cid}`, cancelText(String(p.data?.reason ?? "")), "info");
        }
      }
    },
    [finalize, mappingId, note, store],
  );

  const { connected } = useWsMessages("/ws/out", onMessage);

  // Inflight and queue on connect and on mapping change, then while anything runs.
  useEffect(() => {
    if (connected) void refresh();
  }, [connected, refresh]);
  useEffect(() => {
    const t = setInterval(() => {
      if (store.entries().size > 0) void refresh();
    }, POLL_MS);
    return () => clearInterval(t);
  }, [refresh, store]);

  const launch = useCallback(
    async (ids: string[], form: BackfillForm | null, resume?: ResumeInfo) => {
      for (const id of ids) {
        launchKey.current.set(id, String(Date.now()));
        store.set(id, { stage: "launching", delivered: null, total: null });
      }
      const unlock = () => ids.forEach((id) => store.clear(id));
      setSubmitting(true);
      try {
        if (ids.length > 1 && form) {
          const res = await api.post<BackfillBatchResponse>("/api/backfill/start-batch", batchBody(ids, mappingId, form));
          if (res?.ok === false) throw new Error(res.error || "Failed to start batch clone.");
          (res.results ?? []).forEach((r, i) => {
            if (r?.ok && ids[i]) startedHere.current.add(ids[i]);
            else if (ids[i]) store.clear(ids[i]);
          });
          const sum = batchSummary(res.counts);
          toast.show(sum.text, { kind: sum.ok ? "success" : "info" });
          setTarget(null);
          void refresh();
          return;
        }
        const id = ids[0]!;
        const body = resume ? resumeBody(id, mappingId, resume) : startBody(id, mappingId, form!);
        const res = await api.post<BackfillStartResponse>("/api/backfill/start", body);
        if (res?.ok === false) throw new Error(res.error || "Failed to start clone.");
        startedHere.current.add(id);
        setTarget(null);
      } catch (e) {
        unlock();
        if (e instanceof ApiError && e.status === 409) {
          const state = (e.body as BackfillStartResponse | undefined)?.state;
          note(
            `bf:already:${ids[0]}`,
            state === "running" ? "A clone for this channel is already running or finishing up." : "A clone launch is already in progress.",
            "info",
          );
          setTarget(null);
        } else {
          const bodyError = e instanceof ApiError ? (e.body as { error?: unknown } | undefined)?.error : undefined;
          const text = typeof bodyError === "string" ? bodyError : e instanceof ApiError || !(e instanceof Error) ? "" : e.message;
          const network = !(e instanceof ApiError) && e instanceof TypeError;
          toast.show(
            resume
              ? "Couldn't resume the clone. You can start a new backfill."
              : network
                ? "Network error starting clone."
                : text || (ids.length > 1 ? "Failed to start batch clone." : "Failed to start clone."),
            { kind: "error" },
          );
          if (resume) setTarget(ids);
        }
      } finally {
        setSubmitting(false);
      }
    },
    [mappingId, note, refresh, store, toast],
  );

  const open = useCallback(
    async (cid: string) => {
      if (store.get(cid)) {
        toast.show("A clone for this channel is already in progress.", { kind: "info" });
        return;
      }
      let info: ResumeInfo | null = null;
      try {
        const res = await api.get<ResumeInfoResponse>(resumeInfoUrl(cid, mappingId));
        info = res.resume ?? res.data ?? null;
      } catch {
        toast.show("Couldn’t check resume status. You can start a new backfill.", { kind: "info" });
      }
      if (info && canResume(info)) {
        const fmt = (s?: string | null) => (s ? new Date(s).toLocaleString() : "—");
        const n = (v?: number | null) => (v != null ? v.toLocaleString() : "—");
        const ok = await confirm({
          title: "Resume previous backfill?",
          confirmLabel: "Continue",
          cancelLabel: "Start Over",
          confirmVariant: "ghost",
          cancelVariant: "ghost-red",
          body: (
            <div>
              <p className="mb-2">A previous backfill for this channel was not finished.</p>
              <dl className="m-0 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-sm">
                <dt className="text-muted">Backfill ID:</dt>
                <dd className="m-0">
                  <code>{info.run_id || "—"}</code>
                </dd>
                <dt className="text-muted">Started At:</dt>
                <dd className="m-0">
                  <code>{fmt(info.started_at ?? info.started_dt)}</code>
                </dd>
                <dt className="text-muted">Last Updated:</dt>
                <dd className="m-0">
                  <code>{fmt(info.updated_at ?? info.checkpoint?.last_orig_timestamp)}</code>
                </dd>
                <dt className="text-muted">Messages Sent:</dt>
                <dd className="m-0">
                  <code>{n(info.delivered)}</code> / <code>{n(info.expected_total)}</code>
                </dd>
              </dl>
            </div>
          ),
        });
        if (ok) {
          await launch([cid], null, info);
          return;
        }
      }
      setTarget([cid]);
    },
    [confirm, launch, mappingId, store, toast],
  );

  const items = useCallback(
    (c: TreeChannel): MenuItem[] =>
      c.orphan || !c.clonedId ? [] : [{ act: "clone", id: "ctx-backfill", label: "Clone messages", onSelect: () => void open(c.id) }],
    [open],
  );

  const channelStatus = useCallback(
    (c: TreeChannel): ReactNode => {
      let el = statusEls.current.get(c.id);
      if (!el) {
        el = <ChannelStatus key={c.id} store={store} cid={c.id} />;
        statusEls.current.set(c.id, el);
      }
      return el;
    },
    [store],
  );

  const dialogs = target ? (
    <BackfillModal
      key={target.join(",")}
      channelIds={target}
      submitting={submitting}
      onSubmit={(f) => void launch(target, f)}
      onClose={() => setTarget(null)}
    />
  ) : null;

  return { items, channelStatus, dialogs, openBatch: (ids) => ids.length && setTarget(ids) };
}
