// Slot for the old #srv-proxy-card card: proxy list, the proxy settings modal and the favorites help modal.
// Its lane replaces the LegacySection link with the ported card.

import { LegacySection } from "./LegacySection";

export function ProxySection() {
  return <LegacySection title="Proxies" anchor="srv-proxy-card" />;
}
