import { Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { Placeholder } from "./pages/Placeholder";

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

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        {PAGES.map((p) =>
          p.path === "/" ? (
            <Route key={p.path} index element={<Placeholder title={p.title} legacyHref={p.legacy} />} />
          ) : (
            <Route key={p.path} path={p.path} element={<Placeholder title={p.title} legacyHref={p.legacy} />} />
          ),
        )}
        <Route path="*" element={<Placeholder title="Not found" legacyHref="/" />} />
      </Route>
    </Routes>
  );
}
