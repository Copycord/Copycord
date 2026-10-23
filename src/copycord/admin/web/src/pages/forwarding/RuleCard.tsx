// One rule card: title, provider badge, status pill, the Edit, Enable/Disable,
// Duplicate and Delete buttons and the per rule forwarded count. The plain
// class names are stable hooks that tests select by.

import { CARD_HOVER, cx } from "../../components/ui";
import type { ForwardingRule } from "../../types/forwarding";
import { describeProvider, ruleSummary } from "./forwardingApi";
import {
  FooterButton,
  PROVIDER_BADGE,
  PROVIDER_ICON,
  RULE_BADGES,
  RULE_CARD,
  RULE_FOOTER,
  RULE_HEADER,
  RULE_STAT,
  RULE_TITLE,
  RULE_TITLE_BOX,
  STATUS_PILL,
} from "./ForwardingSkeleton";

const BADGE_TONE: Record<string, string> = {
  pushover: "bg-danger-a12 text-danger-soft",
  discord: "bg-primary-a12 text-accent",
  telegram: "bg-info-a12 text-info-soft",
  http: "bg-warn-a12 text-warn",
};

const ICON = PROVIDER_ICON;

export function ProviderIcon({ provider }: { provider: string }) {
  switch (provider) {
    case "telegram":
      return (
        <svg className={ICON} viewBox="0 0 48 48" aria-hidden="true">
          <path
            d="M0 24c0 13.255 10.745 24 24 24s24-10.745 24-24S37.255 0 24 0 0 10.745 0 24zm19.6 11 .408-6.118 11.129-10.043c.488-.433-.107-.645-.755-.252l-13.735 8.665-5.933-1.851c-1.28-.393-1.29-1.273.288-1.906l23.118-8.914c1.056-.48 2.075.254 1.672 1.87l-3.937 18.553c-.275 1.318-1.072 1.633-2.175 1.024l-5.998-4.43L20.8 34.4l-.027.027c-.323.314-.59.573-1.173.573z"
            fill="currentColor"
            fillRule="evenodd"
            clipRule="evenodd"
          />
        </svg>
      );
    case "pushover":
      return (
        <svg className={ICON} viewBox="0 0 512 512" aria-hidden="true">
          <path
            d="m280.95 172.51 74.48-9.8-72.52 163.66c12.74-.98 25.233-5.307 37.48-12.98 12.253-7.68 23.527-17.317 33.82-28.91 10.287-11.6 19.187-24.503 26.7-38.71 7.513-14.213 12.903-28.18 16.17-41.9 1.96-8.493 2.86-16.66 2.7-24.5-.167-7.84-2.21-14.7-6.13-20.58s-9.883-10.617-17.89-14.21c-8-3.593-18.86-5.39-32.58-5.39-16.007 0-31.77 2.613-47.29 7.84-15.513 5.227-29.887 12.823-43.12 22.79-13.227 9.96-24.74 22.373-34.54 37.24-9.8 14.86-16.823 31.763-21.07 50.71-1.633 6.207-2.613 11.187-2.94 14.94-.327 3.76-.407 6.863-.24 9.31.16 2.453.483 4.333.97 5.64a35.55 35.55 0 0 1 1.23 3.92c-16.66 0-28.83-3.35-36.51-10.05-7.673-6.693-9.55-18.37-5.63-35.03 3.92-17.313 12.823-33.81 26.71-49.49 13.88-15.68 30.373-29.483 49.48-41.41 19.113-11.92 40.02-21.39 62.72-28.41 22.707-7.027 44.84-10.54 66.4-10.54 18.947 0 34.87 2.693 47.77 8.08 12.907 5.393 22.953 12.5 30.14 21.32s11.677 19.11 13.47 30.87c1.8 11.76 1.23 24.01-1.71 36.75-3.593 15.353-10.373 30.79-20.34 46.31-9.96 15.513-22.453 29.56-37.48 42.14-15.027 12.573-32.26 22.78-51.7 30.62-19.433 7.84-40.093 11.76-61.98 11.76h-2.45l-62.23 139.65h-70.56z"
            fill="currentColor"
          />
        </svg>
      );
    case "discord":
      return (
        <svg className={ICON} viewBox="-6 -4 76 76" aria-hidden="true">
          <path
            d="M60.104 12.927a58.55 58.55 0 0 0-14.452-4.482.22.22 0 0 0-.232.11 40.783 40.783 0 0 0-1.8 3.696c-5.457-.817-10.886-.817-16.232 0-.484-1.164-1.2-2.586-1.827-3.696a.228.228 0 0 0-.233-.11 58.39 58.39 0 0 0-14.452 4.482.207.207 0 0 0-.095.082C1.577 26.759-.945 40.174.292 53.42a.244.244 0 0 0 .093.166c6.073 4.46 11.956 7.167 17.729 8.962a.23.23 0 0 0 .249-.082 42.08 42.08 0 0 0 3.627-5.9.225.225 0 0 0-.123-.312 38.772 38.772 0 0 1-5.539-2.64.228.228 0 0 1-.022-.377c.372-.28.744-.57 1.1-.862a.22.22 0 0 1 .23-.031c11.62 5.305 24.198 5.305 35.681 0a.219.219 0 0 1 .232.028c.356.293.728.586 1.103.865a.228.228 0 0 1-.02.377 36.384 36.384 0 0 1-5.54 2.637.227.227 0 0 0-.12.316 47.249 47.249 0 0 0 3.623 5.897.225.225 0 0 0 .25.084c5.8-1.795 11.683-4.502 17.756-8.962a.228.228 0 0 0 .093-.163c1.48-15.315-2.48-28.618-10.498-40.412a.18.18 0 0 0-.093-.085zM23.725 45.355c-3.498 0-6.38-3.212-6.38-7.156s2.826-7.156 6.38-7.156c3.582 0 6.437 3.24 6.38 7.156 0 3.944-2.826 7.156-6.38 7.156zm23.592 0c-3.498 0-6.38-3.212-6.38-7.156s2.826-7.156 6.38-7.156c3.582 0 6.437 3.24 6.38 7.156 0 3.944-2.798 7.156-6.38 7.156z"
            fill="currentColor"
          />
        </svg>
      );
    case "http":
      return (
        <svg className={ICON} viewBox="0 0 24 24" aria-hidden="true">
          <path
            d="M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20zm6.9 6h-2.95a15.6 15.6 0 0 0-1.38-3.56A8.03 8.03 0 0 1 18.9 8zM12 4.04c.83 1.2 1.48 2.53 1.91 3.96h-3.82c.43-1.43 1.08-2.76 1.91-3.96zM4.26 14a8.2 8.2 0 0 1 0-4h3.38a16.5 16.5 0 0 0 0 4H4.26zm.82 2h2.95c.32 1.25.78 2.45 1.38 3.56A7.99 7.99 0 0 1 5.08 16zm2.95-8H5.08a7.99 7.99 0 0 1 4.33-3.56A15.6 15.6 0 0 0 8.03 8zM12 19.96A14.1 14.1 0 0 1 10.09 16h3.82A14.1 14.1 0 0 1 12 19.96zM14.34 14H9.66a14.7 14.7 0 0 1 0-4h4.68a14.7 14.7 0 0 1 0 4zm.25 5.56c.6-1.11 1.06-2.31 1.38-3.56h2.95a8.03 8.03 0 0 1-4.33 3.56zM16.36 14a16.5 16.5 0 0 0 0-4h3.38a8.2 8.2 0 0 1 0 4h-3.38z"
            fill="currentColor"
          />
        </svg>
      );
    default:
      return null;
  }
}

export interface RuleCardProps {
  rule: ForwardingRule;
  count: number;
  busy: "toggle" | "delete" | null;
  onEdit: () => void;
  onToggle: () => void;
  onDuplicate: () => void;
  onDelete: () => void;
  /** The fade in as the tile replaces a loading tile (useReveal). */
  className?: string;
}

export function RuleCard({ rule, count, busy, onEdit, onToggle, onDuplicate, onDelete, className }: RuleCardProps) {
  const enabled = Boolean(rule.enabled);
  const provider = (rule.provider || "").toLowerCase();
  const summary = ruleSummary(rule);
  return (
    <article data-id={rule.rule_id} className={cx("fwd-card", RULE_CARD, CARD_HOVER, className)}>
      <header className={RULE_HEADER}>
        <div className={RULE_TITLE_BOX}>
          <h3 className={cx("fwd-card-title", RULE_TITLE)}>{rule.label}</h3>
        </div>
        <div className={RULE_BADGES}>
          <span className={cx(PROVIDER_BADGE, BADGE_TONE[provider] ?? "bg-white/4 text-text-2")}>
            <ProviderIcon provider={provider} />
            <span>{describeProvider(provider)}</span>
          </span>
          <span
            className={cx(
              STATUS_PILL,
              enabled
                ? "bg-good-a12 text-good"
                : "bg-slate-a10 text-muted",
            )}
          >
            {enabled ? "Enabled" : "Disabled"}
          </span>
        </div>
      </header>
      {summary ? <p className="fwd-card-summary m-0 mt-1.5 truncate text-xs text-muted">{summary}</p> : null}

      <footer className={RULE_FOOTER}>
        <FooterButton className="fwd-edit-btn" onClick={onEdit}>
          Edit
        </FooterButton>
        <FooterButton className="fwd-toggle-btn" disabled={busy !== null} onClick={onToggle}>
          {enabled ? "Disable" : "Enable"}
        </FooterButton>
        <FooterButton className="fwd-duplicate-btn" onClick={onDuplicate}>
          Duplicate
        </FooterButton>
        <FooterButton danger className="fwd-delete-btn" disabled={busy !== null} onClick={onDelete}>
          {busy === "delete" ? "Deleting…" : "Delete"}
        </FooterButton>
        <div className={RULE_STAT}>
          <span aria-hidden="true">Forwarded:</span>
          <span
            className="fwd-card-stat-value"
            title="Total messages forwarded by this rule"
            aria-label="Total messages forwarded by this rule"
            data-rule-count-id={rule.rule_id}
          >
            {count.toLocaleString()}
          </span>
        </div>
      </footer>
    </article>
  );
}
