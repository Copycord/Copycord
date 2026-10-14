// Page frame from the old templates: a fixed header at every width, the side menu as a drawer under it,
// the content column (.container and its per-page width variants) and the page footer.

import { useEffect, useState } from "react";
import { Link, Outlet, useLocation } from "react-router";
import { api } from "../lib/api";
import type { SidebarItem } from "../types/api";
import { ErrorBoundary } from "./ErrorBoundary";
import { Footer } from "./Footer";
import { Z } from "./ui/cx";
import { Sidebar } from "./Sidebar";

interface Frame {
  /** Classes for <main>, the old .container plus the page's variant class. */
  main: string;
  /** The old page put #fixed-footer inside the container rather than after it. */
  footerInside?: boolean;
  /** The old 404 page had no page footer and a shorter menu. */
  bare?: boolean;
  sidebarFooter?: "version" | "social";
}

// .container: centered, a flex column that grows to push the footer down, 1100px wide at most.
const COLUMN = "mx-auto flex w-full flex-1 flex-col";
const CONTAINER = `${COLUMN} max-w-[1100px]`;
// .channels-page / .guilds-page .container.wide and body.page-forwarding .container: full width, 20px sides.
const WIDE = "flex w-full flex-1 flex-col px-5";

// The Tasks list and its run pages (/tasks/runs/:id) share one frame.
const TASKS_FRAME: Frame = { main: WIDE };

const FRAMES: Record<string, Frame> = {
  "/": { main: CONTAINER, footerInside: true },
  "/channels": { main: `${WIDE} max-sm:px-3` },
  "/guilds": { main: WIDE },
  "/forwarding": { main: WIDE, footerInside: true },
  "/tasks": TASKS_FRAME,
  // .scraper-wide
  "/scraper": { main: `${COLUMN} max-w-[1200px] px-5 pb-20`, sidebarFooter: "social" },
  // .adv-wide
  "/system": { main: `${COLUMN} max-w-[1120px] xl:max-w-[1240px]` },
  // .logs-wide
  "/logs": { main: `${COLUMN} max-w-[1200px]` },
  "/_kit": { main: CONTAINER },
};

// 404.html: main.page-main fills the width and the page has no footer.
const NOT_FOUND: Frame = { main: "w-full flex-[1_0_auto]", bare: true };
const NOT_FOUND_HIDE: SidebarItem["key"][] = ["forwarding", "scraper"];

const DOCS_PATH =
  "M21 5c-1.11-.35-2.33-.5-3.5-.5-1.95 0-4.05.4-5.5 1.5-1.45-1.1-3.55-1.5-5.5-1.5S2.45 4.9 1 6v14.65c0 .25.25.5.5.5.1 0 .15-.05.25-.05C3.1 20.45 5.05 20 6.5 20c1.95 0 4.05.4 5.5 1.5 1.35-.85 3.8-1.5 5.5-1.5 1.65 0 3.35.3 4.75 1.05.1.05.15.05.25.05.25 0 .5-.25.5-.5V6c-.6-.45-1.25-.75-2-1zm0 13.5c-1.1-.35-2.3-.5-3.5-.5-1.7 0-4.15.65-5.5 1.5V8c1.35-.85 3.8-1.5 5.5-1.5 1.2 0 2.4.15 3.5.5v11.5z";

// .menu-btn .bar; the three bars turn into an X while the menu is open.
const BAR = "block h-0.5 w-5 rounded-[2px] bg-accent transition duration-200 group-hover:bg-current";

export function Layout() {
  const [drawerOpen, setDrawerOpen] = useState(false);
  // undefined while /version loads, null when it has no version or fails.
  const [version, setVersion] = useState<string | null | undefined>(undefined);
  const location = useLocation();
  const frame = FRAMES[location.pathname] ?? (location.pathname.startsWith("/tasks/") ? TASKS_FRAME : NOT_FOUND);

  useEffect(() => setDrawerOpen(false), [location.pathname]);

  useEffect(() => {
    if (!drawerOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setDrawerOpen(false);
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [drawerOpen]);

  useEffect(() => {
    let live = true;
    api
      .get<{ current?: string }>("/version")
      .then((v) => live && setVersion(v.current || null))
      .catch(() => live && setVersion(null));
    return () => {
      live = false;
    };
  }, []);

  const footer = frame.bare ? null : <Footer version={version} />;

  return (
    <>
      <header
        role="banner"
        className={`fixed inset-x-0 top-0 ${Z.header} flex h-header items-center gap-2.5 border-b border-b-border bg-page px-4`}
      >
        <button
          type="button"
          className="group inline-flex size-menu-btn shrink-0 cursor-pointer flex-col items-center justify-center gap-[5px] rounded-btn border border-border bg-[rgba(255,255,255,0.04)] px-1.5 py-px text-text transition duration-200 hover:bg-border hover:text-white"
          id="menu-toggle"
          aria-haspopup="true"
          aria-label="Open menu"
          aria-expanded={drawerOpen}
          aria-controls="side-menu"
          onClick={() => setDrawerOpen((open) => !open)}
        >
          <span className={`${BAR}${drawerOpen ? " translate-y-[7px] rotate-45" : ""}`} />
          <span className={`${BAR}${drawerOpen ? " opacity-0" : ""}`} />
          <span className={`${BAR}${drawerOpen ? " -translate-y-[7px] -rotate-45" : ""}`} />
        </button>

        <Link
          to="/"
          className="ml-1.5 flex items-center gap-2 rounded-lg px-1.5 py-1 text-inherit no-underline focus-visible:outline-2 focus-visible:outline-offset-[3px] focus-visible:outline-accent"
        >
          <img
            src="/static/logo.png"
            alt=""
            aria-hidden="true"
            draggable={false}
            className="pointer-events-none block size-8 rounded-md select-none"
          />
          <span className="block text-[1.1rem] font-bold tracking-[0.3px]">Copycord</span>
        </Link>

        <div className="ml-auto">
          <a
            href="https://copycord.github.io/Copycord/"
            target="_blank"
            rel="noopener noreferrer"
            aria-label="Documentation"
            className="inline-flex items-center gap-1.5 rounded-input bg-[rgba(139,92,246,0.15)] px-3.5 py-1.5 text-[0.82rem] font-semibold text-accent no-underline transition-colors hover:bg-[rgba(139,92,246,0.28)] hover:text-[#c4b5fd]"
          >
            <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true">
              <path d={DOCS_PATH} />
            </svg>
            <span>Docs</span>
          </a>
        </div>
      </header>

      <div
        id="menu-backdrop"
        data-testid="menu-backdrop"
        aria-hidden="true"
        hidden={!drawerOpen}
        className={`fixed inset-x-0 top-header bottom-0 ${Z.drawer} bg-[rgba(0,0,0,0.6)]`}
        onClick={() => setDrawerOpen(false)}
      />

      <aside
        id="side-menu"
        aria-label="Site menu"
        className={`fixed top-header bottom-0 left-0 ${Z.drawer} flex w-drawer flex-col overflow-hidden border-r border-r-border bg-(image:--card-gradient) transition-transform duration-[220ms] ${
          drawerOpen ? "visible translate-x-0" : "invisible -translate-x-full"
        }`}
        data-open={drawerOpen ? "true" : "false"}
      >
        <Sidebar
          onNavigate={() => setDrawerOpen(false)}
          hide={frame.bare ? NOT_FOUND_HIDE : undefined}
          footer={frame.sidebarFooter}
          version={version}
        />
      </aside>

      <main className={frame.main}>
        <ErrorBoundary key={location.pathname}>
          <Outlet />
        </ErrorBoundary>
        {frame.footerInside ? footer : null}
      </main>
      {frame.footerInside ? null : footer}
    </>
  );
}
