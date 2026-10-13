// SLOT for the next lane (inventory item 21): the "Token Message Cloning" section with the
// USE_USER_TOKENS switch, strategy, sticky options, delays and the per mapping token manager.
// It renders nothing yet. The form passes it everything it needs: the saved mapping id (null
// until the mapping exists), the settings state with its setter, and the settings search query.

import type { MappingSettingValue, MappingSettings } from "../../../../types/dashboard-mappings";

export interface UserTokensBlockProps {
  /** Null while creating or cloning: tokens can only be managed once the mapping exists. */
  mappingId: string | null;
  settings: MappingSettings;
  setSetting: (key: string, value: MappingSettingValue) => void;
  /** Current #mappingSettingsSearch text. */
  search: string;
}

export function UserTokensBlock(_props: UserTokensBlockProps) {
  return null;
}
