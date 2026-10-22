// Row actions of the Channels page: the channel and category menus, the
// customize dialogs, blacklist, id copy, Convert to orphan, and orphan delete
// (one and all).
// The result plugs into the ChannelTree slots and the #orph-delall button.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useToast } from "../../../components/Toast";
import { useConfirm } from "../../../components/ui";
import { useWsMessages } from "../../../hooks/useWsMessages";
import { api } from "../../../lib/api";
import { wsUrl } from "../../../lib/ws";
import type { BusMessage } from "../../../types/api";
import type {
  ChannelRow,
  CustomizeResponse,
  DeletedResult,
  Orphans,
  OrphanDeletePayload,
  UnlinkedEvent,
} from "../../../types/channels";
import type { CategoryPins, TreeChannel, TreeSection } from "../model";
import { CustomizeModal } from "./CustomizeModal";
import {
  blacklistBody,
  canConvertToOrphan,
  customizeCategoryBody,
  customizeChannelBody,
  deleteAllPayload,
  deleteOnePayload,
  failureText,
  orphanIds,
  resolveCategoryIds,
  resultId,
  unlinkFailureText,
  unlinkPayload,
  verifyEnvelope,
  wasDeleted,
  withoutDeleted,
} from "./logic";
import { RowMenu } from "./RowMenu";
import type { MenuItem } from "./RowMenu";

export interface RowActionsInput {
  mappingId: string;
  rows: readonly ChannelRow[];
  pins: CategoryPins;
  orphans: Orphans;
  setOrphans: (next: (cur: Orphans) => Orphans) => void;
  /** Reloads the channel list after a save. */
  reload: () => void;
}

export interface RowActions {
  channelActions: (channel: TreeChannel, section: TreeSection) => ReactNode;
  categoryActions: (section: TreeSection) => ReactNode;
  /** The customize dialog, or null; render it once in the page. */
  dialogs: ReactNode;
  deleteAll: { disabled: boolean; busy: boolean; onClick: () => void };
}

type Dialog =
  | { kind: "channel"; row: ChannelRow }
  | { kind: "category"; name: string; initial: string }
  | null;

export function useRowActions({ mappingId, rows, pins, orphans, setOrphans, reload }: RowActionsInput): RowActions {
  const toast = useToast();
  const confirm = useConfirm();
  const [dialog, setDialog] = useState<Dialog>(null);
  const [saving, setSaving] = useState(false);
  const [busy, setBusy] = useState(false);
  const pending = useRef(new Set<string>());
  // Convert to orphan: original channel id -> card name, until the "unlinked" reply.
  const unlinks = useRef(new Map<string, string>());
  const [unlinking, setUnlinking] = useState(0);
  // Frames that wait for /ws/out to connect, so the reply is not missed.
  const queued = useRef<OrphanDeletePayload[]>([]);
  const sock = useRef<WebSocket | null>(null);

  useEffect(() => () => sock.current?.close(), []);

  const sendVerify = useCallback(
    (payload: OrphanDeletePayload) => {
      let s = sock.current;
      if (!s || s.readyState > WebSocket.OPEN) {
        s = new WebSocket(wsUrl("/ws/in"));
        sock.current = s;
      }
      const frame = JSON.stringify(verifyEnvelope(mappingId, payload));
      if (s.readyState === WebSocket.OPEN) s.send(frame);
      else s.addEventListener("open", () => s.send(frame), { once: true });
    },
    [mappingId],
  );

  const total = orphans.categories.length + orphans.channels.length;
  const out = useWsMessages(
    "/ws/out",
    (msg: BusMessage) => {
      const p = (msg.payload ?? msg) as {
        type?: string;
        mapping_id?: string | number;
        results?: DeletedResult[];
        ids?: Array<string | number>;
      } & Omit<Partial<UnlinkedEvent>, "type">;
      if (p.type !== "deleted" && p.type !== "unlinked") return;
      if (p.mapping_id != null && String(p.mapping_id) !== mappingId) return;
      if (p.type === "unlinked") {
        const id = p.id != null ? String(p.id) : "";
        const name = unlinks.current.get(id);
        if (name === undefined) return;
        unlinks.current.delete(id);
        setUnlinking(unlinks.current.size);
        if (p.ok) toast.show(`Converted "${name}" to an orphan.`, { kind: "success" });
        else toast.show(unlinkFailureText(name, p.reason), { kind: "error" });
        reload();
        sendVerify({ action: "list" });
        return;
      }
      let deleted: string[] = [];
      if (Array.isArray(p.results)) {
        for (const r of p.results) {
          const id = resultId(r);
          const mine = id != null && pending.current.delete(id);
          if (wasDeleted(r)) {
            if (id) deleted.push(id);
            if (mine) toast.show(`Deleted "${r.name ?? r.channel_name ?? r.category_name ?? "Item"}"`, { kind: "success" });
          } else if (mine) {
            toast.show(failureText(r), { kind: "error" });
          }
        }
      } else if (Array.isArray(p.ids)) {
        deleted = p.ids.map(String);
        const mine = deleted.some((id) => pending.current.delete(id));
        if (mine) toast.show(`Deleted ${deleted.length} item(s).`, { kind: "success" });
      } else {
        return;
      }
      if (deleted.length) setOrphans((cur) => withoutDeleted(cur, new Set(deleted)));
      setBusy(false);
      sendVerify({ action: "list" });
    },
    { types: ["verify"], enabled: total > 0 || pending.current.size > 0 || unlinking > 0 },
  );

  useEffect(() => {
    if (!out.connected) return;
    for (const frame of queued.current.splice(0)) sendVerify(frame);
  }, [out.connected, unlinking, sendVerify]);

  const copy = useCallback(
    async (text: string, done: string, fail: string) => {
      try {
        await navigator.clipboard.writeText(text);
        toast.show(done, { kind: "success" });
      } catch {
        toast.show(fail, { kind: "error" });
      }
    },
    [toast],
  );

  const blacklist = useCallback(
    async (scope: "channel" | "category", id: string, name: string, og: string | null, cg: string | null) => {
      const ok = await confirm({
        title: `Add ${scope} to blacklist?`,
        body: (
          <>
            This will blacklist <b>{scope === "channel" ? `#${name}` : name}</b> <span className="text-muted">({id})</span>.
          </>
        ),
        confirmLabel: "Add to blacklist",
        confirmVariant: "ghost-red",
      });
      if (!ok) return;
      if (!mappingId) {
        toast.show("Select a mapping first.", { kind: "error" });
        return;
      }
      try {
        const res = await api.post<CustomizeResponse>("/api/filters/blacklist", blacklistBody(scope, id, mappingId, og, cg));
        if (res?.ok === false) throw new Error(res.error);
        toast.show(`${scope === "channel" ? "Channel" : "Category"} added to blacklist.`, { kind: "success" });
        if (scope === "channel") reload();
      } catch {
        toast.show("Failed to add to blacklist.", { kind: "error" });
      }
    },
    [confirm, mappingId, reload, toast],
  );

  const deleteOrphan = useCallback(
    async (kind: "channel" | "category", id: string, name: string) => {
      const ok = await confirm({
        title: `Delete orphan ${kind}?`,
        body: (
          <>
            This will delete <b>{name}</b> <span className="text-muted">({id})</span>.
          </>
        ),
        confirmLabel: "Delete",
        confirmVariant: "ghost-red",
      });
      if (!ok) return;
      pending.current.add(id);
      sendVerify(deleteOnePayload(kind, id));
    },
    [confirm, sendVerify],
  );

  const convertToOrphan = useCallback(
    async (row: ChannelRow, name: string) => {
      const ok = await confirm({
        title: "Convert to orphan?",
        body: (
          <>
            <b>#{name}</b> <span className="text-muted">({row.cloned_channel_id})</span> stays in the clone server as it is,
            but it is no longer linked to its source channel: Copycord stops posting, editing and deleting in it. On the next
            sync the source channel is cloned again as a new channel.
          </>
        ),
        confirmLabel: "Convert to orphan",
      });
      if (!ok) return;
      const id = String(row.original_channel_id);
      if (unlinks.current.has(id)) return;
      unlinks.current.set(id, name);
      queued.current.push(unlinkPayload(row));
      setUnlinking(unlinks.current.size);
    },
    [confirm],
  );

  const channelActions = useCallback(
    (c: TreeChannel): ReactNode => {
      const items: MenuItem[] = [];
      const row = c.row;
      if (c.orphan) {
        items.push({ act: "delete-orphan", label: "Delete orphan", danger: true, onSelect: () => void deleteOrphan("channel", c.id, c.displayName) });
      } else if (row) {
        if (row.cloned_channel_id) items.push({ act: "customize", label: "Customize", onSelect: () => setDialog({ kind: "channel", row }) });
        if (canConvertToOrphan(row)) {
          items.push({ act: "convert-orphan", label: "Convert to orphan", onSelect: () => void convertToOrphan(row, c.displayName) });
        }
        items.push({
          act: "bl-channel",
          label: "Add channel to blacklist",
          onSelect: () =>
            void blacklist("channel", c.id, c.displayName, row.original_guild_id ?? null, row.cloned_guild_id ?? null),
        });
      }
      items.push({
        act: "copy-orig-id",
        label: "Copy original channel ID",
        onSelect: () => void copy(c.id, "Copied original channel ID to clipboard.", "Could not copy channel ID."),
      });
      if (c.clonedId) {
        items.push({
          act: "copy-clone-id",
          label: "Copy clone channel ID",
          onSelect: () => void copy(c.clonedId, "Copied clone channel ID to clipboard.", "Could not copy channel ID."),
        });
      }
      return <RowMenu kind="channel" items={items} />;
    },
    [blacklist, convertToOrphan, copy, deleteOrphan],
  );

  const categoryActions = useCallback(
    (s: TreeSection): ReactNode => {
      const items: MenuItem[] = [];
      if (s.orphanCategory) {
        const id = s.orphanCategoryId;
        if (id) {
          items.push({ act: "delete-orphan-cat", label: "Delete orphan category", danger: true, onSelect: () => void deleteOrphan("category", id, s.displayName) });
          items.push({
            act: "copy-cat-orig-id",
            label: "Copy original category ID",
            onSelect: () => void copy(id, "Copied original category ID.", "Could not copy ID."),
          });
        }
        return <RowMenu kind="category" items={items} />;
      }
      const ids = resolveCategoryIds(rows, s.originalName, pins);
      items.push({
        act: "customize-category",
        label: "Customize category",
        onSelect: () => {
          if (!ids.originalCatId) toast.show("Unable to resolve the original category id.", { kind: "error" });
          else setDialog({ kind: "category", name: s.originalName, initial: s.customized ? s.displayName : "" });
        },
      });
      items.push({
        act: "bl-category",
        label: "Add category to blacklist",
        onSelect: () => {
          if (!ids.originalCatId) toast.show("Could not resolve category ID.", { kind: "error" });
          else void blacklist("category", ids.originalCatId, s.originalName, ids.originalGuildId, ids.clonedGuildId);
        },
      });
      items.push({
        act: "copy-cat-orig-id",
        label: "Copy original category ID",
        onSelect: () => {
          if (!ids.originalCatId) toast.show("No original category ID found.", { kind: "error" });
          else void copy(ids.originalCatId, "Copied original category ID.", "Could not copy ID.");
        },
      });
      items.push({
        act: "copy-cat-clone-id",
        label: "Copy clone category ID",
        onSelect: () => {
          if (!ids.clonedCatId) toast.show("No clone category ID found.", { kind: "error" });
          else void copy(ids.clonedCatId, "Copied clone category ID.", "Could not copy ID.");
        },
      });
      return <RowMenu kind="category" items={items} />;
    },
    [blacklist, copy, deleteOrphan, pins, rows, toast],
  );

  const save = async (name: string) => {
    if (!dialog) return;
    const isChannel = dialog.kind === "channel";
    const body = isChannel
      ? customizeChannelBody(dialog.row, name)
      : customizeCategoryBody(resolveCategoryIds(rows, dialog.name, pins), name);
    if (!body) {
      toast.show(isChannel ? "Missing cloned guild id for this channel." : "Unable to resolve the original category id.", { kind: "error" });
      return;
    }
    setSaving(true);
    try {
      const res = await api.post<CustomizeResponse>(isChannel ? "/api/channels/customize" : "/api/categories/customize", body);
      if (res?.ok === false) {
        toast.show(res.error || "Failed to save.", { kind: "error" });
        return;
      }
      toast.show(isChannel ? "Saved channel customization." : "Saved category customization.", { kind: "success" });
      setDialog(null);
      reload();
    } catch {
      toast.show("Network error saving customization.", { kind: "error" });
    } finally {
      setSaving(false);
    }
  };

  const deleteAll = useMemo(
    () => ({
      disabled: total === 0 || busy,
      busy,
      onClick: () => {
        void (async () => {
          const cats = orphans.categories.length;
          const chans = orphans.channels.length;
          if (!cats && !chans) return;
          const ok = await confirm({
            title: "Delete all orphans?",
            body: (
              <>
                This will delete <b>{cats}</b> orphan {cats === 1 ? "category" : "categories"} and <b>{chans}</b> orphan{" "}
                {chans === 1 ? "channel" : "channels"} that are <em>not part of the original structure</em>.
              </>
            ),
            confirmLabel: "Delete all",
            confirmVariant: "ghost-red",
          });
          if (!ok) return;
          for (const id of orphanIds(orphans)) pending.current.add(id);
          setBusy(true);
          sendVerify(deleteAllPayload(orphans));
        })();
      },
    }),
    [busy, confirm, orphans, sendVerify, total],
  );

  const dialogs = dialog ? (
    <CustomizeModal
      key={dialog.kind === "channel" ? dialog.row.original_channel_id : dialog.name}
      kind={dialog.kind}
      initial={dialog.kind === "channel" ? dialog.row.clone_channel_name : dialog.initial}
      saving={saving}
      onSave={(n) => void save(n)}
      onClose={() => setDialog(null)}
    />
  ) : null;

  return { channelActions, categoryActions, dialogs, deleteAll };
}
