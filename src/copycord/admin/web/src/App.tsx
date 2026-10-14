import { useEffect, useState, type ComponentType, type ReactNode } from "react";
import { Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { SkeletonSection } from "./components/ui/Skeleton";
import { prefetch } from "./lib/api";
import { ChannelsSkeleton } from "./pages/channels/ChannelsSkeleton";
import { DashboardSkeleton } from "./pages/dashboard/DashboardSkeleton";
import { ForwardingSkeleton } from "./pages/forwarding/ForwardingSkeleton";
import { GuildsSkeleton } from "./pages/guilds/GuildsSkeleton";
import { LogsSkeleton } from "./pages/logs/LogsSkeleton";
import { ScraperSkeleton } from "./pages/scraper/ScraperSkeleton";
import { SystemSkeleton } from "./pages/system/SystemSkeleton";
import { Login } from "./pages/Login";
import { NotFound } from "./pages/NotFound";

// Each page is its own chunk, so the first paint needs only the shell and the page it lands on. The
// skeleton shows until the chunk is in. React.lazy with a Suspense fallback would hold the page back
// about 300 ms after its chunk arrived (React 19 throttles fallback reveals), so a plain effect loads it.
function lazyPage(load: () => Promise<ComponentType>, Skeleton: ComponentType = PageSkeleton): ComponentType {
  let loaded: ComponentType | null = null;
  let pending: Promise<ComponentType> | null = null;
  const get = () => (pending ??= load().then((c) => (loaded = c)));
  return function LazyPage() {
    // Start the chunk download during the first render rather than after the commit.
    const [Page, setPage] = useState<ComponentType | null>(() => loaded ?? (void get(), null));
    useEffect(() => {
      if (Page) return;
      let live = true;
      void get().then((c) => live && setPage(() => c));
      return () => {
        live = false;
      };
    }, [Page]);
    return Page ? <Page /> : <Skeleton />;
  };
}

// The Configuration page asks for these on mount; start them while its chunk is still downloading.
const DASHBOARD_GETS = [
  "/api/status",
  "/api/config",
  "/api/validate-tokens",
  "/api/guild-mappings",
  "/api/notifications/settings",
  "/api/sync/settings",
  "/api/server/proxies",
  "/api/server/proxies/test/status",
  "/version",
] as const;

const Dashboard = lazyPage(() => {
  prefetch(DASHBOARD_GETS);
  return import("./pages/Dashboard").then((m) => m.Dashboard);
}, DashboardSkeleton);
const Channels = lazyPage(() => import("./pages/Channels").then((m) => m.Channels), ChannelsSkeleton);
const Guilds = lazyPage(() => import("./pages/Guilds").then((m) => m.Guilds), GuildsSkeleton);
const Forwarding = lazyPage(() => import("./pages/Forwarding").then((m) => m.Forwarding), ForwardingSkeleton);
const Scraper = lazyPage(() => import("./pages/Scraper").then((m) => m.Scraper), ScraperSkeleton);
const System = lazyPage(() => import("./pages/System").then((m) => m.System), SystemSkeleton);
const Logs = lazyPage(() => import("./pages/Logs").then((m) => m.Logs), LogsSkeleton);
const Kit = lazyPage(() => import("./pages/Kit").then((m) => m.Kit));

// The server serves index.html at each of these paths (admin/app.py SPA_PAGES).
export const PAGES = [
  { path: "/", title: "Configuration" },
  { path: "/channels", title: "Channels" },
  { path: "/guilds", title: "Guilds" },
  { path: "/forwarding", title: "Forwarding" },
  { path: "/scraper", title: "Scraper" },
  { path: "/system", title: "System" },
  { path: "/logs", title: "Logs" },
] as const;

/** Shown while a page chunk downloads: card shaped skeletons in the page column. */
function PageSkeleton() {
  return (
    <div className="flex flex-col gap-4">
      {[4, 3, 3].map((rows, i) => (
        <div key={i} className="rounded-card border border-border bg-(image:--card-gradient) p-4 shadow-card">
          <SkeletonSection header rows={rows} label="Loading page" />
        </div>
      ))}
    </div>
  );
}

const PAGE_ELEMENTS: Record<(typeof PAGES)[number]["path"], ReactNode> = {
  "/": <Dashboard />,
  "/channels": <Channels />,
  "/guilds": <Guilds />,
  "/forwarding": <Forwarding />,
  "/scraper": <Scraper />,
  "/system": <System />,
  "/logs": <Logs />,
};

export function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route element={<Layout />}>
        {PAGES.map((p) =>
          p.path === "/" ? (
            <Route key={p.path} index element={PAGE_ELEMENTS[p.path]} />
          ) : (
            <Route key={p.path} path={p.path} element={PAGE_ELEMENTS[p.path]} />
          ),
        )}
        <Route path="/_kit" element={<Kit />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
