// The mappings above the sending accounts: one card per mapping with its Active or Paused chip, how
// many of its sending accounts are online ("53 online"), the other non-zero states in small muted
// counts and a thin bar of the proportions. Fed by the senders poll, so the numbers change in place
// every 5 s and a failed poll keeps the last ones.

import { useReveal, cx } from "../../components/ui";
import { useLastCount } from "../../components/ui/lastCount";
import type { SenderMapping } from "../../types/system";
import {
  MAPPING_BAR,
  MAPPING_CARD,
  MAPPING_LINE,
  MAPPING_NAME,
  MAPPING_TOP,
  MAPPINGS_COUNT_KEY,
  MAPPINGS_LIST,
  MAPPINGS_NOTE,
  MAPPINGS_SECTION,
  MappingChip,
  MappingsCountBar,
  MappingsHeader,
  MappingsLoadingCards,
} from "./MappingsSkeleton";

type Part = "online" | "connecting" | "offline" | "disabled" | "revoked";

const DOT = "inline-block size-2 flex-none rounded-full";
// One colour per state, shared by the bar segments and the dots before the counts.
export const PART_COLOR: Record<Part, string> = {
  online: "bg-good",
  connecting: "bg-warn",
  offline: "bg-wash-20",
  disabled: "bg-danger-a35",
  revoked: "bg-danger",
};
const PARTS: readonly Part[] = ["online", "connecting", "offline", "disabled", "revoked"];
const REST: readonly Part[] = ["connecting", "offline", "disabled", "revoked"];

const num = (n: number) => n.toLocaleString();

/** "53 online, 3 connecting, 17 offline of 73 tokens": every non-zero state, online always. */
export function mappingSummary(m: SenderMapping): string {
  const parts = PARTS.filter((p) => p === "online" || m[p] > 0).map((p) => `${num(m[p])} ${p}`);
  return `${parts.join(", ")} of ${num(m.total)} ${m.total === 1 ? "token" : "tokens"}`;
}

/** The proportions bar: each state grows by its count, a non-zero one stays at least 2 px wide. */
function Bar({ m }: { m: SenderMapping }) {
  if (!m.tokens_enabled || m.total === 0) return <div aria-hidden="true" className={MAPPING_BAR} />;
  return (
    <div role="img" aria-label={mappingSummary(m)} className={MAPPING_BAR}>
      {PARTS.map((p) =>
        m[p] > 0 ? (
          <span
            key={p}
            data-segment={p}
            className={cx("h-full min-w-[2px] transition-[flex-grow] duration-500 motion-reduce:transition-none", PART_COLOR[p])}
            style={{ flex: `${m[p]} 1 0` }}
          />
        ) : null,
      )}
    </div>
  );
}

function StatusLine({ m }: { m: SenderMapping }) {
  if (!m.tokens_enabled) {
    return (
      <div className={cx(MAPPING_LINE, "text-muted")}>
        <span>Token cloning off</span>
        {m.total > 0 ? <span className="tabular-nums">{`${num(m.total)} ${m.total === 1 ? "token" : "tokens"}`}</span> : null}
      </div>
    );
  }
  if (m.total === 0) {
    return (
      <div className={cx(MAPPING_LINE, "text-muted")}>
        <span>No tokens</span>
      </div>
    );
  }
  return (
    <div className={MAPPING_LINE} title={mappingSummary(m)}>
      <span className="flex h-5 flex-none items-center gap-1.5">
        <span
          aria-hidden="true"
          className={m.online > 0 ? cx(DOT, "bg-good") : cx(DOT, "border-[1.5px] border-muted")}
        />
        <span data-count="online" className="font-semibold text-text tabular-nums">
          {`${num(m.online)} online`}
        </span>
      </span>
      {REST.filter((p) => m[p] > 0).map((p) => (
        <span key={p} className="flex h-5 flex-none items-center gap-1 text-[12px] text-muted">
          <span aria-hidden="true" className={cx("inline-block size-1.5 rounded-full", PART_COLOR[p])} />
          <span data-count={p} className="tabular-nums">{`${num(m[p])} ${p}`}</span>
        </span>
      ))}
    </div>
  );
}

function MappingCard({ m }: { m: SenderMapping }) {
  const paused = m.status === "paused";
  return (
    <li data-mapping={m.id} className={MAPPING_CARD}>
      <div className={MAPPING_TOP}>
        <span className={MAPPING_NAME} title={m.name}>
          {m.name}
        </span>
        <MappingChip paused={paused} />
      </div>
      <StatusLine m={m} />
      <Bar m={m} />
    </li>
  );
}

const count = (n: number) => `${num(n)} ${n === 1 ? "mapping" : "mappings"}`;

export function MappingsSection({ mappings, error }: { mappings: SenderMapping[] | undefined; error: boolean }) {
  const reveal = useReveal(mappings !== undefined || error);
  useLastCount(MAPPINGS_COUNT_KEY, mappings?.length);
  const loading = mappings === undefined && !error;
  const note =
    mappings === undefined ? (error ? "Could not load mappings" : null) : mappings.length === 0 ? "No mappings yet" : null;

  return (
    <section aria-labelledby="system-mappings-title" className={MAPPINGS_SECTION}>
      <MappingsHeader count={mappings === undefined ? (error ? null : <MappingsCountBar />) : count(mappings.length)} />
      {note ? (
        <p className={MAPPINGS_NOTE}>{note}</p>
      ) : (
        <ul id="system-mappings" aria-label="Mappings" className={cx(MAPPINGS_LIST, !loading && reveal)}>
          {loading ? <MappingsLoadingCards /> : mappings?.map((m) => <MappingCard key={m.id} m={m} />)}
        </ul>
      )}
    </section>
  );
}
