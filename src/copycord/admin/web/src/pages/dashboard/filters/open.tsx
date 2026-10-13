// Mounts the Filters modal from outside the React tree: the mapping cards call
// openFilters(mapping) and the modal lives in its own root until it closes.

import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import { ToastProvider } from "../../../components/Toast";
import type { DashboardMapping } from "../../../types/dashboard-mappings";
import { FiltersModal } from "./FiltersModal";

let current: { root: Root; host: HTMLElement } | null = null;

function unmount(): void {
  if (!current) return;
  const { root, host } = current;
  current = null;
  root.unmount();
  host.remove();
}

export function mountFilters(mapping: DashboardMapping | null): void {
  if (!mapping || !mapping.mapping_id) return;
  unmount();
  const host = document.createElement("div");
  document.body.appendChild(host);
  const root = createRoot(host);
  current = { root, host };
  root.render(
    <ToastProvider>
      <FiltersModal mapping={mapping} onClosed={() => setTimeout(unmount, 0)} />
    </ToastProvider>,
  );
}
