// Side menu with the same items, icons and order as templates/_sidebar.html.

import { useState } from "react";
import { NavLink } from "react-router";
import { api } from "../lib/api";
import type { SidebarItem } from "../types/api";
import { MenuIcon } from "./icons";

export const SIDEBAR_ITEMS: SidebarItem[] = [
  { key: "config", label: "Configuration", to: "/" },
  { key: "channels", label: "Channels", to: "/channels" },
  { key: "guilds", label: "Guilds", to: "/guilds" },
  { key: "forwarding", label: "Forwarding", to: "/forwarding" },
  { key: "scraper", label: "Scraper", to: "/scraper" },
  { key: "system", label: "System", to: "/system" },
  { key: "logs", label: "Logs", to: "/logs" },
];

const ITEM =
  "flex items-center gap-3 rounded-control px-3 py-2 text-sm text-muted no-underline transition-colors hover:bg-brand-ghost hover:text-text";

export async function logout(go: (url: string) => void = (url) => window.location.assign(url)) {
  try {
    await api.post("/logout");
  } catch {
    // Go to the login page whatever the reply.
  }
  go("/login");
}

export function Sidebar({ onNavigate }: { onNavigate?: () => void }) {
  const [busy, setBusy] = useState(false);
  return (
    <nav aria-label="Site menu" className="flex h-full flex-col gap-1 p-3">
      <ul className="flex flex-col gap-1">
        {SIDEBAR_ITEMS.map((item) => (
          <li key={item.key}>
            <NavLink
              to={item.to}
              end
              onClick={onNavigate}
              className={({ isActive }) => `${ITEM}${isActive ? " bg-brand-ghost-2 text-text" : ""}`}
            >
              <MenuIcon name={item.key} />
              <span>{item.label}</span>
            </NavLink>
          </li>
        ))}
        <li>
          <a
            href="/login"
            role="button"
            className={ITEM}
            onClick={(e) => {
              e.preventDefault();
              if (busy) return;
              setBusy(true);
              void logout();
            }}
          >
            <MenuIcon name="logout" />
            <span>Log out</span>
          </a>
        </li>
      </ul>
    </nav>
  );
}
