import type { ReactNode } from "react";
import { Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { Dashboard } from "./pages/Dashboard";
import { Guilds } from "./pages/Guilds";
import { Forwarding } from "./pages/Forwarding";
import { Scraper } from "./pages/Scraper";
import { Channels } from "./pages/Channels";
import { Kit } from "./pages/Kit";
import { Login } from "./pages/Login";
import { Logs } from "./pages/Logs";
import { NotFound } from "./pages/NotFound";
import { System } from "./pages/System";

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
