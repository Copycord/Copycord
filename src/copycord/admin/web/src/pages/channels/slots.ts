// Extension points of the channel tree. Later lanes fill these from
// Channels.tsx without touching ChannelTree.tsx:
//   lane 23: row actions (customize, blacklist, orphan delete) via channelActions/categoryActions
//   lane 24: backfill live progress via channelStatus (pages/channels/backfill), orphan scan (pages/channels/verify)

import type { ReactNode } from "react";
import type { TreeChannel, TreeSection } from "./model";

export interface ChannelTreeSlots {
  /** Rendered in the top right corner of each channel card (the old "⋯" menu button). */
  channelActions?: (channel: TreeChannel, section: TreeSection) => ReactNode;
  /** Rendered inside each category chip after its name, except for Uncategorized. */
  categoryActions?: (section: TreeSection) => ReactNode;
  /** Rendered at the bottom of each channel card (status pill, progress bar). */
  channelStatus?: (channel: TreeChannel) => ReactNode;
}
