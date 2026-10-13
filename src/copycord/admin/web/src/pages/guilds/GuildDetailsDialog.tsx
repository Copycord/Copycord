// "View details" (#guild-details-modal): GET /api/guilds/{id} and show the
// name, member count, owner, creation date and description.

import { useEffect, useState } from "react";
import { Button } from "../../components/ui";
import { api, ApiError } from "../../lib/api";
import type { GuildDetails, GuildDetailsResponse, GuildSummary } from "../../types/guilds";
import { GuildDialog } from "./GuildDialog";
import { FALLBACK_ICON, formatMembers } from "./model";

function detailsError(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.status === 404) return "This guild is not in the database any more.";
    return `The server could not load this guild (status ${e.status}).`;
  }
  if (e instanceof TypeError) return "The server could not be reached.";
  return "The server could not load this guild.";
}

type Load ={ state: "loading" } | { state: "ok"; item: GuildDetails } | { state: "error"; message: string };

export function GuildDetailsDialog({ guild, onClose }: { guild: GuildSummary; onClose: () => void }) {
  const [load, setLoad] = useState<Load>({ state: "loading" });
  const [iconBroken, setIconBroken] = useState(false);

  useEffect(() => {
    const ctl = new AbortController();
    setLoad({ state: "loading" });
    api
      .get<GuildDetailsResponse>(`/api/guilds/${encodeURIComponent(guild.id)}`, { signal: ctl.signal })
      .then((res) => {
        if (!res?.ok || !res.item) throw new Error(res?.error || "Failed");
        setLoad({ state: "ok", item: res.item });
      })
      .catch((e: unknown) => {
        if (ctl.signal.aborted) return;
        setLoad({ state: "error", message: detailsError(e) });
      });
    return () => ctl.abort();
  }, [guild.id]);

  const item = load.state === "ok" ? load.item : null;
  const iconUrl = (item ? item.icon_url : guild.icon_url) || FALLBACK_ICON;
  const name =
    load.state === "error" ? "Error loading details" : item ? item.name || "Unknown guild" : guild.name || "Loading…";

  const extras: Array<[string, string]> = [];
  if (item?.owner_id) extras.push(["Owner ID", String(item.owner_id)]);
  if (item?.created_at) extras.push(["Created", String(item.created_at)]);

  return (
    <GuildDialog
      title="Guild details"
      icon={iconBroken ? FALLBACK_ICON : iconUrl}
      onIconError={() => setIconBroken(true)}
      onClose={onClose}
      testId="guild-details"
      footer={
        <Button variant="ghost" onClick={onClose}>
          Close
        </Button>
      }
    >
      <div className="mb-3">
        <div id="gd-name" title={name} className="truncate font-semibold">
          {name}
        </div>
        <div id="gd-sub" className="text-[13px] text-muted">
          {item ? formatMembers(item.member_count) : ""}
        </div>
      </div>
      {load.state === "loading" ? null : (
        <div id="gd-extra" className="grid grid-cols-2 gap-3">
          {load.state === "error" ? (
            <div className="text-[13px] text-muted">{load.message}</div>
          ) : extras.length ? (
            extras.map(([label, value]) => (
              <div key={label} className="min-w-0">
                <span className="text-[13px] text-muted">{label}</span>
                <div className="break-words">{value}</div>
              </div>
            ))
          ) : (
            <div className="text-[13px] text-muted">No extra fields.</div>
          )}
        </div>
      )}
      {item?.description ? (
        <div id="gd-desc" className="mt-2">
          <div className="mb-1 text-[13px] text-muted">Description</div>
          <div className="break-words whitespace-pre-line">{item.description}</div>
        </div>
      ) : null}
    </GuildDialog>
  );
}
