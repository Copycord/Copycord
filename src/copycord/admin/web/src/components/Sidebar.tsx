// Side menu with the same items, icons, order and footer as templates/_sidebar.html.

import { useState } from "react";
import { NavLink } from "react-router";
import { api } from "../lib/api";
import { PROMO_LINKS } from "../pages/dashboard/config/model";
import type { SidebarItem } from "../types/api";
import { DISCORD_PATH, MenuIcon } from "./icons";

/** A menu entry; Tasks and Messages are v4 pages the old sidebar never had, so their keys sit outside SidebarItem. */
export type MenuItem = Omit<SidebarItem, "key"> & { key: SidebarItem["key"] | "tasks" | "messages" };

export const SIDEBAR_ITEMS: MenuItem[] = [
  { key: "config", label: "Configuration", to: "/" },
  { key: "channels", label: "Channels", to: "/channels" },
  { key: "messages", label: "Messages", to: "/messages" },
  { key: "guilds", label: "Guilds", to: "/guilds" },
  { key: "tasks", label: "Tasks", to: "/tasks" },
  { key: "forwarding", label: "Forwarding", to: "/forwarding" },
  { key: "system", label: "System", to: "/system" },
  { key: "logs", label: "Logs", to: "/logs" },
];

// .side-menu .menu-list li a
const ITEM =
  "relative flex w-full items-center gap-3 rounded-rail px-4 py-2.5 text-white no-underline transition-colors " +
  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent/70";

// .sm-footer-link
const FOOTER_LINK = "inline-flex items-center gap-2 leading-none";

const GITHUB_PATH =
  "M12 .3A12 12 0 0 0 8.21 23.7c.6.11.79-.26.79-.58v-2.06c-3.35.73-4.1-1.62-4.1-1.62-.55-1.39-1.34-1.76-1.34-1.76-1.09-.74.08-.73.08-.73 1.2.09 1.85 1.24 1.85 1.24 1.06 1.84 2.8 1.3 3.48.99.11-.78.42-1.31.77-1.61-2.68-.3-5.5-1.34-5.5-5.94 0-1.31.47-2.38 1.24-3.22-.13-.3-.54-1.52.12-3.18 0 0 1.02-.32 3.31 1.23 1.95-.53 4.06-.53 6.01 0 2.29-1.55 3.31-1.23 3.31-1.23.66 1.66.25 2.88.12 3.18.77.84 1.24 1.91 1.24 3.22 0 4.61-2.82 5.64-5.5 5.94.43.37.83 1.1.83 2.22v3.31c0 .32.2.69.8.58A12 12 0 0 0 12 .3z";

export async function logout(go: (url: string) => void = (url) => window.location.assign(url)) {
  try {
    await api.post("/logout");
  } catch {
    // Go to the login page whatever the reply.
  }
  go("/login");
}

export interface SidebarProps {
  onNavigate?: () => void;
  /** Items to leave out, like the old sidebar_hide. */
  hide?: MenuItem["key"][];
  /** "social" shows GitHub and Discord links in the footer instead of GitHub and the version. */
  footer?: "version" | "social";
  version?: string | null;
}

export function Sidebar({ onNavigate, hide = [], footer = "version", version }: SidebarProps) {
  const [busy, setBusy] = useState(false);
  return (
    <>
      <nav aria-label="Site menu" className="min-h-0 flex-auto overflow-auto py-3">
        <ul className="m-0 flex flex-col gap-1.5 py-1.5">
          {SIDEBAR_ITEMS.filter((item) => !hide.includes(item.key)).map((item) => (
            <li key={item.key}>
              <NavLink
                to={item.to}
                // Tasks stays lit on its run pages (/tasks/runs/:id).
                end={item.key !== "tasks"}
                onClick={onNavigate}
                className={({ isActive }) => `${ITEM} ${isActive ? "bg-rail-active" : "hover:bg-rail-hover"}`}
              >
                <span aria-hidden="true">
                  <MenuIcon name={item.key} className="block size-[18px]" />
                </span>
                <span>{item.label}</span>
              </NavLink>
            </li>
          ))}
          <li>
            <a
              href="/login"
              role="button"
              data-logout
              className={`${ITEM} hover:bg-rail-hover`}
              onClick={(e) => {
                e.preventDefault();
                if (busy) return;
                setBusy(true);
                void logout();
              }}
            >
              <span aria-hidden="true">
                <MenuIcon name="logout" className="block size-[18px]" />
              </span>
              <span>Log out</span>
            </a>
          </li>
        </ul>
      </nav>

      <footer
        role="contentinfo"
        aria-label="About"
        className="mt-auto flex items-center justify-between gap-3 border-t border-t-border bg-[linear-gradient(180deg,transparent,var(--color-shadow-18))] px-3 py-2.5"
      >
        {footer === "social" ? (
          <>
            <a className={FOOTER_LINK} href={PROMO_LINKS.github} target="_blank" rel="noopener noreferrer" aria-label="Open GitHub">
              <svg viewBox="0 0 24 24" className="block size-[18px] shrink-0" fill="currentColor" aria-hidden="true">
                <path d={GITHUB_PATH} />
              </svg>
              GitHub
            </a>
            <a className={FOOTER_LINK} href={PROMO_LINKS.discord} target="_blank" rel="noopener noreferrer" aria-label="Open Discord">
              <svg viewBox="0 0 127.14 96.36" className="block size-[18px] shrink-0" fill="currentColor" aria-hidden="true">
                <path d={DISCORD_PATH} />
              </svg>
              Discord
            </a>
          </>
        ) : (
          <>
            <a className={FOOTER_LINK} href={PROMO_LINKS.github} target="_blank" rel="noopener noreferrer" aria-label="Open GitHub">
              <svg viewBox="0 0 24 24" className="block size-[18px] shrink-0" fill="currentColor" aria-hidden="true">
                <path d={GITHUB_PATH} />
              </svg>
              GitHub
            </a>
            {version ? (
              // Same target as the page footer's version link: the release notes for this tag.
              <a
                id="sidebar-version-link"
                className="text-text-2 tabular-nums opacity-90 transition-colors duration-200 hover:text-accent hover:opacity-100"
                href={`${PROMO_LINKS.github}/releases/tag/${version}`}
                target="_blank"
                rel="noopener noreferrer"
                aria-label={`Copycord ${version} release notes`}
              >
                {version}
              </a>
            ) : (
              <span className="text-text-2 tabular-nums opacity-90">dev</span>
            )}
          </>
        )}
      </footer>
    </>
  );
}
