// The two browse dialogs of the Filters modal: #filterObjectsModal (categories,
// channels and threads of the host guild) and #roleBlocksModal (its roles).
// Both load on open, search, select all over the visible rows, and hand the
// selection back on Apply.

import { useEffect, useMemo, useRef, useState } from "react";
import { Button } from "../../../components/ui";
import type { GuildObject, GuildRole, PickerKey } from "../../../types/dashboard-filters";
import {
  categoryNames,
  fetchGuildTree,
  fetchRoles,
  filterObjects,
  filterRoles,
  parentLabel,
  PICKER_KIND,
  pickerHelp,
  pickerTitle,
  selectState,
  setMany,
  toggleOne,
} from "./model";
import { Shell } from "./Shell";

const TH = "px-3 py-2 text-left text-xs font-semibold uppercase tracking-wide text-muted";
const TD = "px-3 py-2 text-sm";
const SEARCH =
  "log-search-input w-full rounded-control border border-border bg-bg-2 px-3 py-2 text-sm text-text outline-none focus:border-accent";

function SelectAll({ id, state, onChange, label }: { id: string; state: "none" | "some" | "all"; onChange: (on: boolean) => void; label: string }) {
  const ref = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (ref.current) ref.current.indeterminate = state === "some";
  }, [state]);
  return <input ref={ref} id={id} type="checkbox" aria-label={label} checked={state === "all"} onChange={(e) => onChange(e.target.checked)} />;
}

function Message({ cols, children, danger }: { cols: number; children: string; danger?: boolean }) {
  return (
    <tr>
      <td colSpan={cols} className={`px-3 py-4 text-center text-sm ${danger ? "text-danger" : "text-muted"}`}>
        {children}
      </td>
    </tr>
  );
}

function Footer({ cancelId, applyId, onCancel, onApply }: { cancelId: string; applyId: string; onCancel: () => void; onApply: () => void }) {
  return (
    <div className="modal-footer flex justify-end gap-2 border-t border-border px-5 py-3">
      <Button id={cancelId} variant="ghost" onClick={onCancel}>
        Cancel
      </Button>
      <Button id={applyId} variant="ghost" onClick={onApply}>
        Apply
      </Button>
    </div>
  );
}

export interface ObjectPickerProps {
  mappingId: string;
  listKey: PickerKey;
  initial: readonly string[];
  onApply: (key: PickerKey, ids: string[]) => void;
  onClose: () => void;
}

export function ObjectPicker({ mappingId, listKey, initial, onApply, onClose }: ObjectPickerProps) {
  const kind = PICKER_KIND[listKey];
  const [items, setItems] = useState<GuildObject[] | null>(null);
  const [cats, setCats] = useState<Map<string, string>>(new Map());
  const [failed, setFailed] = useState(false);
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState<Set<string>>(() => new Set(initial.map(String)));

  useEffect(() => {
    let live = true;
    fetchGuildTree(mappingId)
      .then((tree) => {
        if (!live) return;
        const categories = Array.isArray(tree.categories) ? tree.categories : [];
        const channels = Array.isArray(tree.channels) ? tree.channels : [];
        setCats(categoryNames(categories));
        setItems(kind === "category" ? categories : channels);
      })
      .catch(() => live && setFailed(true));
    return () => {
      live = false;
    };
  }, [mappingId, kind]);

  const shown = useMemo(() => (items ? filterObjects(items, query, kind, cats) : []), [items, query, kind, cats]);
  const ids = shown.map((o) => String(o.id));
  const cols = kind === "channel" ? 4 : 3;
  const isChannel = kind === "channel";

  return (
    <Shell id="filterObjectsModal" closeId="filterObjectsCloseBtn" titleId="filterObjectsTitle" title={pickerTitle(listKey)} onClose={onClose} wide layer={35}>
      <div className="modal-body flex flex-col gap-3 p-5">
        <p id="filterObjectsHelp" className="text-xs text-muted">
          {pickerHelp(listKey)}
        </p>
        <div className="filter-objects-search">
          <input
            id="filterObjectsSearch"
            type="text"
            className={SEARCH}
            placeholder="Search by name, cat, or ID…"
            autoComplete="off"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
        <div className="role-table-wrap max-h-[50vh] overflow-y-auto rounded-control border border-border">
          <table id="filterObjectsTable" className="role-table w-full border-collapse">
            <thead className="sticky top-0 bg-card-solid">
              <tr>
                <th className={`${TH} w-10`}>
                  <SelectAll
                    id="filterObjectsSelectAll"
                    label="Select or deselect all items"
                    state={selectState(selected, ids)}
                    onChange={(on) => setSelected((s) => setMany(s, ids, on))}
                  />
                </th>
                <th id="filterObjectsNameHeader" className={TH}>
                  Name
                </th>
                {isChannel ? (
                  <th id="filterObjectsCategoryHeader" className={TH}>
                    Category
                  </th>
                ) : null}
                <th className={TH}>ID</th>
              </tr>
            </thead>
            <tbody id="filterObjectsTableBody">
              {failed ? (
                <Message cols={cols} danger>
                  Failed to fetch channels/categories from Discord. Try again later.
                </Message>
              ) : items === null ? (
                <Message cols={cols}>Loading…</Message>
              ) : items.length === 0 ? (
                <Message cols={cols}>
                  {`No ${kind === "category" ? "categories" : "channels or active threads"} found for this guild.`}
                </Message>
              ) : (
                shown.map((o) => {
                  const id = String(o.id);
                  return (
                    <tr
                      key={id}
                      data-obj-id={id}
                      className="cursor-pointer border-t border-border hover:bg-white/5"
                      onClick={(e) => {
                        if ((e.target as HTMLElement).closest("input[type='checkbox']")) return;
                        setSelected((s) => toggleOne(s, id));
                      }}
                    >
                      <td className={TD}>
                        <input
                          type="checkbox"
                          className="filter-obj-toggle"
                          data-obj-id={id}
                          aria-label={`Select ${o.name || id}`}
                          checked={selected.has(id)}
                          onChange={() => setSelected((s) => toggleOne(s, id))}
                        />
                      </td>
                      <td className={`${TD} filter-obj-name-col`}>
                        <span className="filter-obj-name">{o.name || `ID ${id}`}</span>
                        {o.is_thread ? <span className="filter-obj-thread-tag ml-1.5 rounded bg-white/10 px-1.5 text-[10px] uppercase">thread</span> : null}
                      </td>
                      {isChannel ? <td className={`${TD} filter-obj-cat text-muted`}>{parentLabel(o, cats)}</td> : null}
                      <td className={`${TD} filter-obj-id-col`}>
                        <code>{id}</code>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
      <Footer cancelId="filterObjectsCancel" applyId="filterObjectsApply" onCancel={onClose} onApply={() => onApply(listKey, Array.from(selected))} />
    </Shell>
  );
}

export interface RolePickerProps {
  mappingId: string;
  initial: readonly string[];
  onApply: (ids: string[]) => void;
  onClose: () => void;
  onStatus: (text: string) => void;
}

export function RolePicker({ mappingId, initial, onApply, onClose, onStatus }: RolePickerProps) {
  const [roles, setRoles] = useState<GuildRole[] | null>(null);
  const [failed, setFailed] = useState(false);
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState<Set<string>>(() => new Set(initial.map(String)));
  const statusRef = useRef(onStatus);
  statusRef.current = onStatus;

  useEffect(() => {
    let live = true;
    statusRef.current("");
    fetchRoles(mappingId)
      .then((r) => live && setRoles(r))
      .catch(() => {
        if (!live) return;
        setFailed(true);
        statusRef.current("Failed to load roles.");
      });
    return () => {
      live = false;
    };
  }, [mappingId]);

  const shown = useMemo(() => (roles ? filterRoles(roles, query) : []), [roles, query]);
  const ids = shown.map((r) => String(r.id));

  return (
    <Shell id="roleBlocksModal" closeId="roleBlocksCloseBtn" titleId="roleBlocksTitle" title="Role Blocks" onClose={onClose} wide layer={35}>
      <div className="modal-body flex flex-col gap-3 p-5">
        <p className="text-xs text-muted">Select which roles you want to block for this mapping.</p>
        <div className="role-blocks-search">
          <input
            id="roleBlocksSearch"
            type="text"
            className={SEARCH}
            placeholder="Search roles by name or ID…"
            autoComplete="off"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
        <div className="role-table-wrap max-h-[50vh] overflow-y-auto rounded-control border border-border">
          <table className="role-table w-full border-collapse">
            <thead className="sticky top-0 bg-card-solid">
              <tr>
                <th className={`${TH} w-10`}>
                  <SelectAll
                    id="roleBlocksSelectAll"
                    label="Select or deselect all roles"
                    state={selectState(selected, ids)}
                    onChange={(on) => setSelected((s) => setMany(s, ids, on))}
                  />
                </th>
                <th className={TH}>Role</th>
                <th className={TH}>ID</th>
              </tr>
            </thead>
            <tbody id="roleBlocksTableBody">
              {failed ? (
                <Message cols={3}>Failed to load roles.</Message>
              ) : roles === null ? (
                <Message cols={3}>Loading…</Message>
              ) : roles.length === 0 ? (
                <Message cols={3}>No roles found for this guild.</Message>
              ) : (
                shown.map((r) => {
                  const id = String(r.id);
                  const color = (r.color_hex || "").trim() || "#99AAB5";
                  return (
                    <tr
                      key={id}
                      data-role-id={id}
                      className="cursor-pointer border-t border-border hover:bg-white/5"
                      onClick={(e) => {
                        if ((e.target as HTMLElement).closest("input[type='checkbox']")) return;
                        setSelected((s) => toggleOne(s, id));
                      }}
                    >
                      <td className={TD}>
                        <input
                          type="checkbox"
                          className="role-block-toggle"
                          data-role-id={id}
                          aria-label={`Block ${r.name || id}`}
                          checked={selected.has(id)}
                          onChange={() => setSelected((s) => toggleOne(s, id))}
                        />
                      </td>
                      <td className={TD}>
                        <span className="role-pill inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs" style={{ borderColor: color, color }}>
                          <span className="role-pill-text">{r.name || ""}</span>
                        </span>
                      </td>
                      <td className={TD}>
                        <code>{id}</code>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
      <Footer cancelId="roleBlocksCancel" applyId="roleBlocksApply" onCancel={onClose} onApply={() => onApply(Array.from(selected))} />
    </Shell>
  );
}
