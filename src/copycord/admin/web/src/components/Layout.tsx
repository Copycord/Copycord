// Page frame: fixed sidebar on wide screens, a drawer behind a menu button
// on narrow ones.

import { useEffect, useState } from "react";
import { Outlet, useLocation } from "react-router";
import { Footer } from "./Footer";
import { Sidebar } from "./Sidebar";

export function Layout() {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const location = useLocation();

  useEffect(() => setDrawerOpen(false), [location.pathname]);

  useEffect(() => {
    if (!drawerOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setDrawerOpen(false);
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [drawerOpen]);

  return (
    <div className="min-h-screen bg-page font-sans text-text">
      <header className="sticky top-0 z-30 flex h-header items-center gap-3 border-b border-border bg-page/90 px-4 backdrop-blur md:hidden">
        <button
          type="button"
          className="flex size-menu-btn items-center justify-center rounded-control border border-border text-muted hover:text-text"
          id="menu-toggle"
          aria-label="Open menu"
          aria-expanded={drawerOpen}
          aria-controls="side-menu"
          onClick={() => setDrawerOpen(true)}
        >
          <svg viewBox="0 0 24 24" className="size-5" fill="none" stroke="currentColor" strokeWidth={2} aria-hidden="true">
            <path strokeLinecap="round" d="M4 6h16M4 12h16M4 18h16" />
          </svg>
        </button>
        <span className="font-bold tracking-wide">Copycord</span>
      </header>

      {drawerOpen ? (
        <div
          className="fixed inset-0 z-40 bg-black/50 md:hidden"
          id="menu-backdrop"
          data-testid="menu-backdrop"
          onClick={() => setDrawerOpen(false)}
        />
      ) : null}

      <aside
        id="side-menu"
        aria-label="Site menu"
        className={`fixed inset-y-0 left-0 z-50 w-64 border-r border-border bg-card-solid transition-transform md:translate-x-0 ${
          drawerOpen ? "translate-x-0" : "-translate-x-full"
        }`}
        data-open={drawerOpen ? "true" : "false"}
      >
        <Sidebar onNavigate={() => setDrawerOpen(false)} />
      </aside>

      <main className="p-4 pb-14 md:ml-64 md:p-page md:pb-14">
        <Outlet />
      </main>
      <Footer />
    </div>
  );
}
