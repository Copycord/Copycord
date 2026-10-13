// Slot for the old #guild-config-card card: mapping cards with search, the mapping settings modal and the filters modal.
// Its lane replaces the LegacySection link with the ported card.

import { LegacySection } from "./LegacySection";

export function MappingsSection() {
  return <LegacySection title="Guild Mappings" anchor="guild-config-card" />;
}
