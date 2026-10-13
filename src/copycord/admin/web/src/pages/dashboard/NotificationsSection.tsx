// Slot for the old #notifications-card card: notification webhook and alert toggles.
// Its lane replaces the LegacySection link with the ported card.

import { LegacySection } from "./LegacySection";

export function NotificationsSection() {
  return <LegacySection title="Notifications" anchor="notifications-card" />;
}
