// The old page's top row of three link cards: Star on GitHub, Join the
// Discord and Donate.

import type { ReactNode } from "react";
import { cx } from "../../../components/ui";
import { PROMO_LINKS } from "./model";

const CARD =
  "group relative flex cursor-pointer items-center gap-3.5 overflow-hidden rounded-[14px] border border-[rgba(255,255,255,0.08)] " +
  "bg-[linear-gradient(rgba(255,255,255,0.04),rgba(255,255,255,0.02))] px-[18px] py-4 text-inherit no-underline " +
  "[box-shadow:0_8px_24px_rgba(0,0,0,0.25)] transition duration-200 outline-none " +
  "hover:-translate-y-0.5 hover:border-[rgba(214,188,250,0.35)] hover:[box-shadow:0_12px_28px_rgba(0,0,0,0.32)] " +
  "focus-visible:-translate-y-0.5 focus-visible:border-[rgba(214,188,250,0.35)] focus-visible:[box-shadow:0_12px_28px_rgba(0,0,0,0.32)]";

const ICON =
  "relative grid size-[46px] flex-none place-items-center rounded-[9999px] text-white [box-shadow:inset_0_0_1px_rgba(255,255,255,0.35)]";

interface PromoProps {
  className: string;
  href: string;
  title: string;
  sub: string;
  icon: ReactNode;
  iconClass: string;
  ariaLabel?: string;
}

function Promo({ className, href, title, sub, icon, iconClass, ariaLabel }: PromoProps) {
  return (
    <a className={cx("promo-card", className, CARD)} href={href} target="_blank" rel="noopener noreferrer" aria-label={ariaLabel}>
      <span className={cx("promo-icon", ICON, iconClass)} aria-hidden="true">
        {icon}
      </span>
      <span className="grid gap-0.5">
        <span className="font-semibold leading-[1.15]">{title}</span>
        <span className="text-[0.92rem] opacity-80">{sub}</span>
      </span>
    </a>
  );
}

const GITHUB =
  "M12 .296a12 12 0 0 0-3.794 23.39c.6.111.82-.261.82-.58v-2.028c-3.338.726-4.042-1.61-4.042-1.61-.546-1.387-1.333-1.757-1.333-1.757-1.089-.745.083-.729.083-.729 1.205.085 1.84 1.237 1.84 1.237 1.07 1.835 2.807 1.305 3.492.998.108-.775.418-1.305.762-1.605-2.665-.303-5.466-1.334-5.466-5.931 0-1.31.469-2.381 1.236-3.221-.125-.303-.536-1.523.117-3.176 0 0 1.008-.322 3.3 1.23a11.5 11.5 0 0 1 6.002 0c2.291-1.552 3.297-1.23 3.297-1.23.655 1.653.244 2.873.12 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.804 5.625-5.476 5.921.43.372.814 1.102.814 2.222v3.293c0 .322.219.694.825.576A12 12 0 0 0 12 .296z";
const DISCORD =
  "M20.317 4.369a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.078.037 13.965 13.965 0 0 0-.607 1.248 19.607 19.607 0 0 0-5.89 0 12.64 12.64 0 0 0-.617-1.248.077.077 0 0 0-.078-.037c-1.71.3-3.36.82-4.885 1.515a.07.07 0 0 0-.032.027C.533 9.043-.32 13.579.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.04.078.078 0 0 0 .084-.027c.461-.63.873-1.295 1.226-1.994a.076.076 0 0 0-.041-.105 13.097 13.097 0 0 1-1.866-.9.077.077 0 0 1-.008-.127c.125-.094.25-.192.368-.291a.074.074 0 0 1 .078-.01c3.927 1.796 8.18 1.796 12.062 0a.074.074 0 0 1 .079.009c.12.099.243.197.368.291a.077.077 0 0 1-.006.127 12.298 12.298 0 0 1-1.867.9.076.076 0 0 0-.04.106c.36.699.773 1.364 1.225 1.994a.078.078 0 0 0 .084.028 19.876 19.876 0 0 0 5.994-3.04.078.078 0 0 0 .031-.057c.5-5.177-.838-9.674-3.548-13.661a.06.06 0 0 0-.032-.027zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.095 2.157 2.419 0 1.334-.955 2.419-2.157 2.419zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.095 2.157 2.419 0 1.334-.947 2.419-2.157 2.419z";
const HEART =
  "M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 6.5 3.99 5 6.5 5c1.54 0 3.04.81 3.85 2.09C11.16 5.81 12.66 5 14.2 5 16.71 5 18.7 6.5 18.7 8.5c0 3.78-3.4 6.86-8.25 11.53L12 21.35z";
const STAR = "M12 17.27 18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z";

const svg = (size: number, d: string) => (
  <svg viewBox="0 0 24 24" width={size} height={size} fill="currentColor" aria-hidden="true">
    <path d={d} />
  </svg>
);

export function PromoCards() {
  return (
    <section className="mb-4 grid grid-cols-[repeat(auto-fit,minmax(260px,1fr))] gap-4" aria-label="Quick actions">
      <Promo
        className="promo-star"
        href={PROMO_LINKS.github}
        title="Star on GitHub"
        sub="Support development & features"
        iconClass="bg-[radial-gradient(105%_105%_at_20%_20%,#24292f,#0d1117)]"
        icon={
          <>
            {svg(22, GITHUB)}
            <span className="promo-badge badge-star pointer-events-none absolute -top-[3px] -right-[3px] z-1">
              <svg
                viewBox="0 0 24 24"
                width="16"
                height="16"
                aria-hidden="true"
                className="block overflow-visible text-[#ffd54f] drop-shadow-[0_0_6px_rgba(255,213,79,0.55)]"
              >
                <path d={STAR} fill="none" stroke="currentColor" strokeWidth="2" strokeLinejoin="round" />
              </svg>
            </span>
          </>
        }
      />
      <Promo
        className="promo-discord"
        href={PROMO_LINKS.discord}
        title="Join the Discord"
        sub="Chat with the community"
        ariaLabel="Join the Discord"
        iconClass="bg-[radial-gradient(130%_130%_at_25%_20%,#8ea1f2_0%,#5865f2_55%,#404eed_100%)]"
        icon={svg(22, DISCORD)}
      />
      <Promo
        className="promo-donate"
        href={PROMO_LINKS.kofi}
        title="Donate"
        sub="Thank you for your support!"
        iconClass="bg-[radial-gradient(105%_105%_at_20%_20%,#fca5a5,#ec4899)]"
        icon={svg(24, HEART)}
      />
    </section>
  );
}
