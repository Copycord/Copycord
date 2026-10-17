// Configuration page: the update notice, the Bots card with its log modal,
// then the remaining cards, each in its own file under dashboard/.

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

export { DashboardSkeleton } from "./dashboard/DashboardSkeleton";

export interface DashboardProps {
  /** For tests. */
  WebSocketImpl?: UseWsOptions["WebSocketImpl"];
  EventSourceImpl?: SseOptions<string>["EventSourceImpl"];
}

export function Dashboard({ WebSocketImpl, EventSourceImpl }: DashboardProps = {}) {
  const [logs, setLogs] = useState<LogStream | null>(null);
  const [running, setRunning] = useState(false);
  const [startBlocked, setStartBlocked] = useState<string | null>(null);

  return (
    <div className="flex flex-col">
      {/* No visible title; this one is for screen readers and the shell test. */}
      <h1 className="sr-only">Configuration</h1>
      <UpdateNotice />
      <PromoCards />
      <BotsPanel
        onViewLogs={setLogs}
        onRunningChange={setRunning}
        startBlocked={startBlocked}
        WebSocketImpl={WebSocketImpl}
      />
      <GlobalConfigSection onStartBlockedChange={setStartBlocked} botsRunning={running} />
      <MappingsSection locked={running} />
      <NotificationsSection />
      <SyncSettingsSection />
      <ProxySection />
      <LogModal which={logs} onClose={() => setLogs(null)} EventSourceImpl={EventSourceImpl} />
    </div>
  );
}
