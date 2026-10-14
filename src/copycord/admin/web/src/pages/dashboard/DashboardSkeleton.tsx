// The Configuration page while its chunk downloads: the same cards in the same order with their real
// titles, buttons and table heads, shimmer only where a value goes. Kept out of Dashboard.tsx so the app
// shell can show it without pulling the page chunk in. The sections share the loading bodies below so the
// page's own loading state and this one are the same boxes.

import { Skeleton, cx } from "../../components/ui";
import { FIELD_TIPS } from "./config/model";
import { PromoCards } from "./config/PromoCards";
import { FIELD_INPUT } from "./config/TokenInput";
import { NOTIF_HINT, NOTIF_ROWS } from "./notify/model";
import {
  BOT_ROLES,
  BOTS_CELL,
  BOTS_HEAD,
  BOTS_STATE,
  CARD,
  CHECK,
  CHECK_LABEL,
  Chevron,
  EVENT_ROW,
  GC_FIELD,
  GC_HINT,
  GC_LABEL,
  GHOST,
  GHOST_XS_GREEN,
  GHOST_XS_RED,
  PROXY_CHIPS,
  PROXY_HINT,
  PS_UNIT,
  SECTION_HINT,
  SETTING_LABEL,
  SETTING_ROW,
  SETTING_SUB,
  SettingRow,
  SUMMARY,
  SUMMARY_TITLE,
  WEBHOOK_INPUT,
} from "./parts";

/** One placeholder tile while the list loads; the count is unknown until it lands. */
export const MAPPING_SKELETONS = 1;

/** A shimmer bar standing in for a control while the config loads. */
function Bar({ className }: { className: string }) {
  return <span aria-hidden="true" data-skeleton="" className={cx("cc-skeleton block shrink-0", className)} />;
}

/** Global Configuration body while the config loads: the loaded form's grid, field boxes, labels and hints,
 * with empty boxes and bars where the values go, so each box sits where the form puts it. */
export function GlobalConfigLoading() {
  return (
    <div role="status" aria-busy="true" aria-label="Loading configuration" className="grid grid-cols-2 gap-x-[22px] gap-y-3 max-[900px]:grid-cols-1">
      <div className={GC_FIELD}>
        <span className={GC_LABEL}>Server Token</span>
        <span className={GC_HINT}>{FIELD_TIPS.SERVER_TOKEN}</span>
        <div className="relative w-full">
          <input type="password" disabled tabIndex={-1} aria-hidden="true" className={cx(FIELD_INPUT, "border-border")} />
        </div>
      </div>
      <div className={GC_FIELD}>
        <span className={cx(GC_LABEL, "mt-1 mb-2.5 w-fit")}>Client Token</span>
        <span className={GC_HINT}>{FIELD_TIPS.CLIENT_TOKEN}</span>
        <div className="relative w-full">
          <input type="password" disabled tabIndex={-1} aria-hidden="true" className={cx(FIELD_INPUT, "border-border")} />
        </div>
      </div>
      <div className="col-span-full -mt-1 grid grid-cols-2 gap-x-[22px] gap-y-2 max-[900px]:grid-cols-1">
        <div className={cx(SETTING_ROW, "flex-col items-stretch")}>
          <div>
            <div className={SETTING_LABEL}>
              <span className="text-[13px] font-medium">Command Users</span>
              <span className={SETTING_SUB}>{FIELD_TIPS.COMMAND_USERS}</span>
            </div>
            <div className="mt-1.5">
              <div className="flex min-h-[42px] items-center rounded-control border border-border bg-[#140b26] px-3 py-2" />
            </div>
          </div>
        </div>
        <SettingRow label="Auto Start" hint={FIELD_TIPS.COPYCORD_AUTOSTART}>
          <Bar className="ml-auto h-[22px] w-[38px] rounded-[999px]" />
        </SettingRow>
        <SettingRow label="Log Level" hint={FIELD_TIPS.LOG_LEVEL}>
          <Bar className="ml-auto h-[34px] w-[100px] rounded-lg" />
        </SettingRow>
        <SettingRow label="Max Log Size" hint={FIELD_TIPS.LOG_MAX_SIZE_MB}>
          <div className="ml-auto flex shrink-0 items-center gap-1.5">
            <Bar className="h-8 w-16 rounded-lg" />
            <span className={PS_UNIT}>MB</span>
          </div>
        </SettingRow>
      </div>
      <div className="col-span-full mt-3 flex flex-wrap gap-2.5 max-sm:flex-col">
        <span className={cx(GHOST, "invisible flex")}>Save</span>
        <span className="flex-1 max-sm:hidden" />
        <span className="inline-flex items-center gap-[5px] text-xs font-medium text-accent opacity-30 max-sm:self-start">Add Bot to Server</span>
      </div>
    </div>
  );
}

/** One mapping tile while the list loads. */
export function MappingTileSkeleton() {
  return <Skeleton.Card height="auto" testId="mapping-skeleton" className="min-h-[150px]" />;
}

function DocIcon() {
  return <span aria-hidden="true" className="inline-flex size-3.5 opacity-50" />;
}

function Summary({ title }: { title: string }) {
  return (
    <summary className={SUMMARY} onClick={(e) => e.preventDefault()}>
      <h3 className={SUMMARY_TITLE}>{title}</h3>
      <DocIcon />
      <Chevron />
    </summary>
  );
}

export function DashboardSkeleton() {
  return (
    <div className="flex flex-col" role="status" aria-busy="true" aria-label="Loading page" data-testid="dashboard-skeleton">
      <h1 className="sr-only">Configuration</h1>
      <PromoCards />
      <section data-testid="card-skeleton" className="mt-[18px] rounded-card border border-border bg-(image:--card-gradient) p-[18px] text-text">
        <header>
          <h2 className="my-[0.83em] text-[24px] leading-[normal] font-bold">Bots</h2>
        </header>
        <div className="w-full overflow-x-hidden">
          <table className="mt-2 w-full table-fixed border-collapse text-left">
            <thead>
              <tr>
                <th className={cx(BOTS_HEAD, "text-left")}>Component</th>
                <th className={cx(BOTS_HEAD, "w-[110px] text-center")}>State</th>
                <th className={cx(BOTS_HEAD, "w-[120px] text-center")}>Uptime</th>
                <th className={cx(BOTS_HEAD, "text-center")}>Status</th>
              </tr>
            </thead>
            <tbody>
              {BOT_ROLES.map(({ role, label }) => (
                <tr key={role}>
                  <td className={cx(BOTS_CELL, "text-left")}>{label}</td>
                  <td className={cx(BOTS_CELL, "text-center")}>
                    <span data-skeleton="" className={cx(BOTS_STATE, "cc-skeleton border-transparent text-transparent")}>
                      stopped
                    </span>
                  </td>
                  <td className={cx(BOTS_CELL, "text-center")} />
                  <td className={cx(BOTS_CELL, "text-center")} />
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="mt-3 flex flex-wrap gap-2.5 max-sm:grid max-sm:grid-cols-1">
          <span className="inline max-sm:block">
            <button type="button" disabled className={cx(GHOST, "inline-flex max-sm:w-full")}>
              Start
            </button>
          </span>
          <span className="flex-1 max-sm:hidden" />
          <button type="button" disabled className={cx(GHOST, "flex max-sm:w-full")}>
            View server logs
          </button>
          <button type="button" disabled className={cx(GHOST, "flex max-sm:w-full")}>
            View client logs
          </button>
        </div>
      </section>
      <details open data-testid="card-skeleton" className={CARD}>
        <Summary title="Global Configuration" />
        <div className="mt-3.5">
          <GlobalConfigLoading />
        </div>
      </details>
      <details open data-testid="card-skeleton" className={CARD}>
        <Summary title="Cloning Configuration" />
        <div className="mt-3.5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="m-0 max-w-[480px] min-w-[200px] flex-[1_1_auto] text-xs leading-[1.4] text-muted opacity-90">
              Map a source server to a target server to enable cloning for that pair
            </p>
            <div className="ml-auto">
              <input
                type="text"
                disabled
                placeholder="Search mappings…"
                aria-label="Search mappings"
                className="ml-0 inline-block min-w-[220px] rounded-lg border border-[#3b365a] bg-[#1c1830] px-2.5 py-1.5 text-[13.3333px] text-[#e6e1ff] outline-none placeholder:text-[#e6e1ff]/55"
              />
            </div>
          </div>
          <div className="mt-4 grid max-h-80 min-h-[180px] grid-cols-[repeat(auto-fill,minmax(220px,1fr))] gap-4 overflow-y-auto py-2 pr-1 max-[600px]:grid-cols-[repeat(auto-fill,minmax(180px,1fr))] max-[600px]:gap-3">
            <span className="flex rounded-xl border border-dashed border-border bg-[rgba(255,255,255,0.02)] p-4 max-[600px]:min-h-[120px]" />
            {Array.from({ length: MAPPING_SKELETONS }, (_, i) => (
              <MappingTileSkeleton key={i} />
            ))}
          </div>
        </div>
      </details>
      <details data-testid="card-skeleton" className={CARD}>
        <Summary title="Notifications" />
        <div className="srv-proxy-body mt-3.5">
          <p className={SECTION_HINT}>{NOTIF_HINT}</p>
          <div className="mb-3.5 flex gap-2">
            <input
              type="text"
              disabled
              tabIndex={-1}
              aria-hidden="true"
              placeholder="https://discord.com/api/webhooks/..."
              className={WEBHOOK_INPUT}
            />
            <span className={GHOST_XS_GREEN}>Test</span>
          </div>
          <div className="flex flex-col gap-1.5">
            {NOTIF_ROWS.map((r) => (
              <div key={r.event} className={EVENT_ROW}>
                <div className={SETTING_LABEL}>
                  <span className="text-[13px] font-medium">{r.label}</span>
                  <span className={SETTING_SUB}>{r.hint}</span>
                </div>
                <Bar className="h-[22px] w-[38px] rounded-[999px]" />
              </div>
            ))}
          </div>
        </div>
      </details>
      <details data-testid="card-skeleton" className={CARD}>
        <Summary title="Sync Settings" />
      </details>
      <details data-testid="card-skeleton" className={CARD}>
        <Summary title="Proxy" />
        <div className="srv-proxy-body mt-3.5">
          <div className="mb-3">
            <span className={CHECK_LABEL}>
              <span className={cx(CHECK, "inline-block")} />
              <span>Enable proxies for client</span>
            </span>
          </div>
          <div className="mb-2.5 flex items-start justify-between gap-3">
            <p className={PROXY_HINT}>
              Paste one or more proxies &mdash; <code>host:port</code> &middot; <code>host:port:user:pass</code>{" "}
              &middot; <code>user:pass@host:port</code>
            </p>
            <div className="flex shrink-0 gap-1.5">
              <span className={GHOST_XS_GREEN}>Test All</span>
              <span className={GHOST_XS_RED}>Clear</span>
            </div>
          </div>
          <div className={PROXY_CHIPS} />
        </div>
      </details>
    </div>
  );
}
