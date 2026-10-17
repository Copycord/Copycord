// Extension points of the channel tree, filled from Channels.tsx: the row actions
// (customize, blacklist, orphan delete) go in through channelActions and categoryActions.

import type { ReactNode } from "react";
import type { TreeChannel, TreeSection } from "./model";

export interface ChannelTreeSlots {
  /** Rendered in the top right corner of each channel card (the "⋯" menu button). */
  channelActions?: (channel: TreeChannel, section: TreeSection) => ReactNode;
  /** Rendered inside each category chip after its name, except for Uncategorized. */
  categoryActions?: (section: TreeSection) => ReactNode;
}
