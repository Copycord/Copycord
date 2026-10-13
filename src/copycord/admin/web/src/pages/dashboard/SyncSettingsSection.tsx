// Slot for the old #sync-settings-card card: startup sync delays.
// Its lane replaces the LegacySection link with the ported card.

import { LegacySection } from "./LegacySection";

export function SyncSettingsSection() {
  return <LegacySection title="Sync Settings" anchor="sync-settings-card" />;
}
