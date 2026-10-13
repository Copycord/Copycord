import type { ReactNode } from "react";
import { Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { Dashboard } from "./pages/Dashboard";
import { Guilds } from "./pages/Guilds";
import { Forwarding } from "./pages/Forwarding";
import { Scraper } from "./pages/Scraper";
import { Kit } from "./pages/Kit";
import { Logs } from "./pages/Logs";
import { Placeholder } from "./pages/Placeholder";
import { System } from "./pages/System";

// Paths are relative to the router basename /app.
export const PAGES = [
  { path: "/", title: "Configuration", legacy: "/" },
  { path: "/channels", title: "Channels", legacy: "/channels" },
  { path: "/guilds", title: "Guilds", legacy: "/guilds" },
  { path: "/forwarding", title: "Forwarding", legacy: "/forwarding" },
  { path: "/scraper", title: "Scraper", legacy: "/scraper" },
  { path: "/system", title: "System", legacy: "/system" },
  { path: "/logs", title: "Logs", legacy: "/event-logs" },
] as const;

// Pages already ported to the SPA, one line each; the rest render a Placeholder linking to the old page.
const PAGE_ELEMENTS: Partial<Record<(typeof PAGES)[number]["path"], ReactNode>> = {
  "/": <Dashboard />,
  "/guilds": <Guilds />,
  "/forwarding": <Forwarding />,
  "/scraper": <Scraper />,
  "/system": <System />,
  "/logs": <Logs />,
};

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        {PAGES.map((p) => {
          const element = PAGE_ELEMENTS[p.path] ?? <Placeholder title={p.title} legacyHref={p.legacy} />;
          return p.path === "/" ? (
            <Route key={p.path} index element={element} />
          ) : (
            <Route key={p.path} path={p.path} element={element} />
          );
        })}
        <Route path="/_kit" element={<Kit />} />
        <Route path="*" element={<Placeholder title="Not found" legacyHref="/" />} />
      </Route>
    </Routes>
  );
}
