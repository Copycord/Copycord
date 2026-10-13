// Configuration page (the old index.html): the update notice, the Bots card
// with its log modal, then one slot per remaining card in the old order. Each
// slot lives in its own file under dashboard/ so later lanes port one card
// without touching the others.

import { useState } from "react";
import type { UseWsOptions } from "../hooks/useWsMessages";
import type { SseOptions } from "../lib/sse";
import type { LogStream } from "../types/api";
import { BotsPanel } from "./dashboard/BotsPanel";
import { PromoCards } from "./dashboard/config/PromoCards";
import { GlobalConfigSection } from "./dashboard/GlobalConfigSection";
import { LogModal } from "./dashboard/LogModal";
import { MappingsSection } from "./dashboard/MappingsSection";
import { NotificationsSection } from "./dashboard/NotificationsSection";
import { ProxySection } from "./dashboard/ProxySection";
import { SyncSettingsSection } from "./dashboard/SyncSettingsSection";
import { UpdateNotice } from "./dashboard/UpdateNotice";

export interface DashboardProps {
  /** For tests. */
  WebSocketImpl?: UseWsOptions["WebSocketImpl"];
  EventSourceImpl?: SseOptions<string>["EventSourceImpl"];
}

export function Dashboard({ WebSocketImpl, EventSourceImpl }: DashboardProps = {}) {
  const [logs, setLogs] = useState<LogStream | null>(null);
  const [running, setRunning] = useState(false);

  return (
    <div className="flex flex-col gap-4">
      {/* The old page has no visible title; this one is for screen readers and the shell test. */}
      <h1 className="sr-only">Configuration</h1>
      <UpdateNotice />
      <PromoCards />
      <BotsPanel onViewLogs={setLogs} onRunningChange={setRunning} WebSocketImpl={WebSocketImpl} />
      <GlobalConfigSection />
      <MappingsSection locked={running} />
      <NotificationsSection />
      <SyncSettingsSection />
      <ProxySection />
      <LogModal which={logs} onClose={() => setLogs(null)} EventSourceImpl={EventSourceImpl} />
    </div>
  );
}
