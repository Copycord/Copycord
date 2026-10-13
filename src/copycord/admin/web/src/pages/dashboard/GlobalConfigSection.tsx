// Slot for the old #global-config-card card: global configuration form, Save/Cancel, the bot invite link and the backup tokens modal.
// Its lane replaces the LegacySection link with the ported card.

import { LegacySection } from "./LegacySection";

export function GlobalConfigSection() {
  return <LegacySection title="Global Configuration" anchor="global-config-card" />;
}
