// The Configuration page while its chunk downloads: the same cards in the same order with their real
// titles, docs links, buttons, inputs, hints and table heads as inert chrome, shimmer only where a value
// from the API goes. Kept out of Dashboard.tsx so the app shell can show it without pulling the page
// chunk in. The cards share the classes, bodies and loading pieces below so the page's own loading
// state and this one are the same boxes.

import type { ReactNode } from "react";
import { Skeleton, cx } from "../../components/ui";
import { readLastCount } from "../../components/ui/lastCount";
import { ToolbarSelect } from "../channels/ToolbarSelect";
import { FIELD_TIPS } from "./config/model";
import { PromoCards } from "./config/PromoCards";
import { EyeIcon, FIELD_INPUT, FIELD_REVEAL } from "./config/TokenInput";
import { CloneIcon, FiltersIcon, NewMappingIcon, PauseIcon, SettingsIcon, TrashIcon } from "./mappings/icons";
import { NotifFields } from "./notify/NotifFields";
import { SyncFields } from "./notify/SyncFields";
import {
  BOT_ROLES,
  BotStatePending,
  BOTS_CARD,
  BOTS_CELL,
  BOTS_HEAD,
  BOTS_TABLE,
  BOTS_TITLE,
  CARD,
  CARD_DOCS,
  CARD_OPEN_KEYS,
  Chevron,
  DocLink,
  GC_FIELD,
  GC_HINT,
  GC_LABEL,
  GHOST,
  GHOST_XS_GREEN,
  GHOST_XS_RED,
  PS_INPUT,
  PS_UNIT,
  readCardOpen,
  SETTING_LABEL,
  SETTING_ROW,
  SETTING_SUB,
  SettingRow,
  SUMMARY,
  SUMMARY_TITLE,
  SwitchPending,
} from "./parts";
import {
  CheckPending,
  CheckRow,
  HintRow,
  IntervalSection,
  NUM,
  RECHECK_TEXT,
  RecheckControls,
  ROTATION_TEXT,
  RotationControls,
  SummaryTools,
  UnitSelect,
} from "./proxy/chrome";
import { ProxyChips } from "./proxy/ProxyChips";
import { PROXY_CARD_OPEN_KEY } from "./proxy/model";

const noop = () => undefined;

/** The 28px backup tokens button at the top right of the Client Token box. */
export const BACKUP_BTN =
  "absolute top-2 right-2.5 flex size-7 cursor-pointer items-center justify-center rounded-[10px] border border-border bg-wash-4 px-1.5 py-px text-accent opacity-90 outline-none hover:border-accent hover:opacity-100 focus-visible:border-accent focus-visible:opacity-100";

export function BackupIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24" aria-hidden="true" className="shrink-0">
      <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
      <circle cx="12" cy="7" r="4" />
    </svg>
  );
}

/** The Add Bot to Server link; INVITE_OFF until the bot's client id is known. */
export const INVITE_LINK = "inline-flex items-center gap-[5px] text-xs font-medium text-accent no-underline transition-opacity max-sm:self-start";
export const INVITE_OFF = "pointer-events-none cursor-default opacity-30";

export function InviteIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" className="shrink-0">
      <path d="M12 8V4H8" />
      <rect width="16" height="12" x="4" y="8" rx="2" />
      <path d="M2 14h2" />
      <path d="M20 14h2" />
      <path d="M15 13v2" />
      <path d="M9 13v2" />
    </svg>
  );
}

/** The Log Level dropdown's box at the right of its row. */
export const LOG_LEVEL_BOX = "ml-auto w-[100px] shrink-0";

/** A token box: the empty password field with its eye button. */
function TokenBox() {
  return (
    <div className="relative w-full">
      <input type="password" className={cx(FIELD_INPUT, "border-border-input")} />
      <button type="button" className={FIELD_REVEAL}>
        <EyeIcon />
      </button>
    </div>
  );
}

/** Global Configuration body while the config loads: the loaded form's grid, field boxes, labels, hints
 * and buttons as inert chrome, empty boxes where typed values go and bars where a switch or choice goes. */
export function GlobalConfigLoading() {
  return (
    <div role="status" aria-busy="true" aria-label="Loading configuration" className="mt-3.5">
      <div inert className="grid grid-cols-2 gap-x-[22px] gap-y-3 max-[900px]:grid-cols-1">
        <div className={GC_FIELD}>
          <span className={GC_LABEL}>Server Token</span>
          <span className={GC_HINT}>{FIELD_TIPS.SERVER_TOKEN}</span>
          <TokenBox />
        </div>
        <div className={GC_FIELD}>
          <span className={cx(GC_LABEL, "mt-1 mb-2.5 w-fit")}>Client Token</span>
          <button type="button" className={BACKUP_BTN}>
            <BackupIcon />
          </button>
          <span className={GC_HINT}>{FIELD_TIPS.CLIENT_TOKEN}</span>
          <TokenBox />
        </div>
        <div className="col-span-full -mt-1 grid grid-cols-2 gap-x-[22px] gap-y-2 max-[900px]:grid-cols-1">
          <div className={cx(SETTING_ROW, "flex-col items-stretch")}>
            <div>
              <div className={SETTING_LABEL}>
                <span className="text-[13px] font-medium">Command Users</span>
                <span className={SETTING_SUB}>{FIELD_TIPS.COMMAND_USERS}</span>
              </div>
              <div className="mt-1.5">
                <div className="flex min-h-[42px] items-center rounded-control border border-border-input bg-control px-3 py-2" />
              </div>
            </div>
          </div>
          <SettingRow label="Auto Start" hint={FIELD_TIPS.COPYCORD_AUTOSTART}>
            <div className="ml-auto flex shrink-0 items-center">
              <SwitchPending />
            </div>
          </SettingRow>
          <SettingRow label="Log Level" hint={FIELD_TIPS.LOG_LEVEL}>
            <ToolbarSelect id="LOG_LEVEL-skeleton" value="" options={[]} onChange={noop} pending pendingLabel="INFO" className={LOG_LEVEL_BOX} />
          </SettingRow>
          <SettingRow label="Max Log Size" hint={FIELD_TIPS.LOG_MAX_SIZE_MB}>
            <div className="ml-auto flex shrink-0 items-center gap-1.5">
              <input type="number" className={PS_INPUT} />
              <span className={PS_UNIT}>MB</span>
            </div>
          </SettingRow>
        </div>
        <div className="col-span-full mt-3 flex flex-wrap gap-2.5 max-sm:flex-col">
          <button type="button" className={cx(GHOST, "invisible flex")}>
            Save
          </button>
          <button type="button" className={cx(GHOST, "invisible flex")}>
            Cancel
          </button>
          <span className="flex-1 max-sm:hidden" />
          <span className={cx(INVITE_LINK, INVITE_OFF)}>
            <InviteIcon />
            Add Bot to Server
          </span>
        </div>
      </div>
    </div>
  );
}

// One mapping tile (.guild-card): avatar, name and host line, then the Settings, Filters, status, Clone and
// Delete buttons. MappingCard and the loading tile share these, so both have the same boxes.
export const MAPPING_CARD =
  "relative flex min-w-0 flex-col items-stretch overflow-hidden rounded-xl border p-3 text-left text-text [box-shadow:0_4px_12px_var(--color-shadow-40)] transition max-[600px]:min-h-[110px]";
export const MAPPING_INNER = "flex h-full w-full flex-col items-stretch gap-0.5";
export const MAPPING_HEADER = "flex items-center gap-2.5";
export const MAPPING_AVATAR = "size-9 min-w-9 shrink-0 rounded-[50%] object-cover";
export const MAPPING_INFO = "min-w-0 flex-1";
export const MAPPING_NAME =
  "line-clamp-2 w-full min-w-0 flex-auto text-[13px] leading-[1.3] font-semibold wrap-break-word whitespace-normal";
export const MAPPING_ROUTE = "mt-px truncate text-[11px] text-muted opacity-70";
export const MAPPING_ACTIONS = "mt-auto flex gap-1.5 border-t border-t-border pt-2";
export const MAPPING_ICON_BTN =
  "inline-flex size-7 min-h-7 cursor-pointer items-center justify-center rounded-md border-0 bg-wash-4 p-0 text-xs leading-none " +
  "text-muted transition hover:bg-accent-a12 hover:text-accent active:translate-y-px disabled:cursor-default disabled:opacity-50 [&_svg]:block";

/** As many tiles as the list had last time; one on a first visit, the common case. */
export const MAPPINGS_COUNT_KEY = "dashboard.mappings";
export const mappingSkeletons = () => readLastCount(MAPPINGS_COUNT_KEY, 1, 12);

/** One mapping tile while the list loads: MappingCard's boxes and buttons, bars on the name and host line. */
export function MappingTileSkeleton() {
  return (
    <div data-testid="mapping-skeleton" aria-hidden="true" data-skeleton-row="" className={cx(MAPPING_CARD, "border-border bg-card")}>
      <div className={MAPPING_INNER}>
        <div className={MAPPING_HEADER}>
          <Skeleton.Box className={MAPPING_AVATAR} />
          <div className={MAPPING_INFO}>
            <div className={MAPPING_NAME}>
              <Skeleton.Text width={110} />
            </div>
            <div className={MAPPING_ROUTE}>
              <Skeleton.Text width={130} />
            </div>
          </div>
        </div>
        <div className={MAPPING_ACTIONS}>
          {[SettingsIcon, FiltersIcon, PauseIcon, CloneIcon].map((Icon, i) => (
            <button key={i} type="button" tabIndex={-1} className={MAPPING_ICON_BTN}>
              <Icon />
            </button>
          ))}
          <span className="flex-1" />
          <button type="button" tabIndex={-1} className={MAPPING_ICON_BTN}>
            <TrashIcon />
          </button>
        </div>
      </div>
    </div>
  );
}

/** The Cloning Configuration hint, search box, tile grid and Add tile, shared with the loaded card. */
export const MAPPINGS_HINT = "m-0 max-w-[480px] min-w-[200px] flex-[1_1_auto] text-xs leading-[1.4] text-muted opacity-90";
export const MAPPINGS_HINT_TEXT = "Map a source server to a target server to enable cloning for that pair";
export const MAPPINGS_SEARCH =
  "ml-0 inline-block min-w-[220px] rounded-lg border border-log-input-edge bg-log-input-bg px-2.5 py-1.5 text-[13.3333px] text-text-soft caret-log-input-focus outline-none transition placeholder:text-text-soft/55 focus-visible:border-log-input-focus";
export const MAPPING_GRID =
  "mt-4 grid max-h-80 min-h-[180px] grid-cols-[repeat(auto-fill,minmax(min(320px,100%),1fr))] gap-4 overflow-y-auto py-2 pr-1 max-[600px]:gap-3";
export const NEW_MAPPING_TILE =
  "group/new relative flex cursor-pointer items-center justify-center rounded-xl border border-dashed border-border bg-wash-2 p-4 text-left text-muted transition max-[600px]:min-h-[120px] max-[600px]:p-3.5";

export function NewMappingPlus() {
  return (
    <span className="new-card-inner flex flex-col items-center text-center leading-none select-none">
      <span className="new-card-plus leading-[0] text-accent select-none group-hover/new:text-text group-focus/new:text-text">
        <NewMappingIcon />
      </span>
    </span>
  );
}

/** A card summary: the title, its docs link and the chevron, with any extra tools between. */
function Summary({ title, href, label, children }: { title: string; href: string; label?: string; children?: ReactNode }) {
  return (
    <summary className={SUMMARY} onClick={(e) => e.preventDefault()}>
      <h3 className={SUMMARY_TITLE}>{title}</h3>
      <DocLink href={href} label={label} inert />
      {children ? <span inert className="contents">{children}</span> : null}
      <Chevron />
    </summary>
  );
}

export function DashboardSkeleton() {
  return (
    <div className="flex flex-col" role="status" aria-busy="true" aria-label="Loading page" data-testid="dashboard-skeleton">
      <h1 className="sr-only">Configuration</h1>
      <PromoCards />
      <section data-testid="card-skeleton" className={BOTS_CARD}>
        <header>
          <h2 className={BOTS_TITLE}>Bots</h2>
        </header>
        <div className="w-full overflow-x-hidden">
          <table className={BOTS_TABLE}>
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
                    <BotStatePending />
                  </td>
                  <td className={cx(BOTS_CELL, "text-center")} />
                  <td className={cx(BOTS_CELL, "text-center")} />
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div inert className="mt-3 flex flex-wrap gap-2.5 max-sm:grid max-sm:grid-cols-1">
          <span className="inline max-sm:block">
            <button type="button" className={cx(GHOST, "inline-flex max-sm:w-full")}>
              Start
            </button>
          </span>
          <span className="flex-1 max-sm:hidden" />
          <button type="button" className={cx(GHOST, "flex max-sm:w-full")}>
            View server logs
          </button>
          <button type="button" className={cx(GHOST, "flex max-sm:w-full")}>
            View client logs
          </button>
        </div>
      </section>
      <details open data-testid="card-skeleton" className={CARD}>
        <Summary title="Global Configuration" href={CARD_DOCS.global} />
        <GlobalConfigLoading />
      </details>
      <details open data-testid="card-skeleton" className={CARD}>
        <Summary title="Cloning Configuration" href={CARD_DOCS.mappings} />
        <div className="mt-3.5">
          <div inert className="flex flex-wrap items-center justify-between gap-3">
            <p className={MAPPINGS_HINT}>{MAPPINGS_HINT_TEXT}</p>
            <div className="ml-auto">
              <input type="text" placeholder="Search mappings…" aria-label="Search mappings" className={MAPPINGS_SEARCH} />
            </div>
          </div>
          <div className={MAPPING_GRID}>
            <button type="button" inert aria-label="Add new mapping" title="Add new mapping" className={NEW_MAPPING_TILE}>
              <NewMappingPlus />
            </button>
            {Array.from({ length: mappingSkeletons() }, (_, i) => (
              <MappingTileSkeleton key={i} />
            ))}
          </div>
        </div>
      </details>
      <details open={readCardOpen(CARD_OPEN_KEYS.notifications)} data-testid="card-skeleton" className={CARD}>
        <Summary title="Notifications" href={CARD_DOCS.notifications} label="Notifications docs" />
        <div inert className="srv-proxy-body mt-3.5">
          <NotifFields url="" events={null} />
        </div>
      </details>
      <details open={readCardOpen(CARD_OPEN_KEYS.sync)} data-testid="card-skeleton" className={CARD}>
        <Summary title="Sync Settings" href={CARD_DOCS.sync} label="Sync Settings docs" />
        <div inert className="srv-proxy-body mt-3.5">
          <SyncFields form={null} />
        </div>
      </details>
      <details open={readCardOpen(PROXY_CARD_OPEN_KEY)} data-testid="card-skeleton" className={CARD}>
        <Summary title="Proxy" href={CARD_DOCS.proxy} label="Proxy docs">
          <SummaryTools chip={null} />
        </Summary>
        <div inert className="srv-proxy-body mt-3.5">
          <div className="srv-proxy-toggle-row mb-3">
            <CheckRow label="Enable proxies for client">
              <CheckPending />
            </CheckRow>
          </div>
          <HintRow>
            <button type="button" className={GHOST_XS_GREEN}>
              Test All
            </button>
            <button type="button" className={GHOST_XS_RED}>
              Clear
            </button>
          </HintRow>
          <ProxyChips proxies={[]} favorites={new Set()} results={{}} onAdd={noop} onRemove={noop} onRemoveLast={noop} onToggleFavorite={noop} />
          <IntervalSection locked hint={ROTATION_TEXT} check={<CheckRow label="Rotate every"><CheckPending /></CheckRow>} controls={<RotationControls on={false} />} />
          <IntervalSection
            locked
            hint={RECHECK_TEXT}
            check={
              <CheckRow label="Return to a favorite every">
                <CheckPending />
              </CheckRow>
            }
            controls={
              <RecheckControls off>
                <input type="number" disabled className={NUM} />
                <UnitSelect id="srv-proxy-fav-recheck-unit-skeleton" value="" disabled pending onChange={noop} />
              </RecheckControls>
            }
          />
        </div>
      </details>
    </div>
  );
}
