import { lazy, Suspense, type ReactNode } from "react";
import { Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { SkeletonSection } from "./components/ui/Skeleton";
import { Login } from "./pages/Login";
import { NotFound } from "./pages/NotFound";

// Each page is its own chunk, so the first paint needs only the shell and the page it lands on.
const Dashboard = lazy(() => import("./pages/Dashboard").then((m) => ({ default: m.Dashboard })));
const Channels = lazy(() => import("./pages/Channels").then((m) => ({ default: m.Channels })));
const Guilds = lazy(() => import("./pages/Guilds").then((m) => ({ default: m.Guilds })));
const Forwarding = lazy(() => import("./pages/Forwarding").then((m) => ({ default: m.Forwarding })));
const Scraper = lazy(() => import("./pages/Scraper").then((m) => ({ default: m.Scraper })));
const System = lazy(() => import("./pages/System").then((m) => ({ default: m.System })));
const Logs = lazy(() => import("./pages/Logs").then((m) => ({ default: m.Logs })));
const Kit = lazy(() => import("./pages/Kit").then((m) => ({ default: m.Kit })));

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

const page = (el: ReactNode) => <Suspense fallback={<PageSkeleton />}>{el}</Suspense>;

const PAGE_ELEMENTS: Record<(typeof PAGES)[number]["path"], ReactNode> = {
  "/": page(<Dashboard />),
  "/channels": page(<Channels />),
  "/guilds": page(<Guilds />),
  "/forwarding": page(<Forwarding />),
  "/scraper": page(<Scraper />),
  "/system": page(<System />),
  "/logs": page(<Logs />),
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
        <Route path="/_kit" element={page(<Kit />)} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
