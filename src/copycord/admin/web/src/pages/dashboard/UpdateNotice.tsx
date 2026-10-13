// The "New Update Available" notice from index.html, shown when /version
// reports a newer release; clicking opens the release page.

import { useApi } from "../../hooks/useApi";
import type { VersionInfo } from "../../types/dashboard";
import { VERSION_URL } from "./model";

export function UpdateNotice() {
  const { data } = useApi<VersionInfo>(VERSION_URL);
  if (!data?.update_available) return null;
  return (
    <a
      id="update-notice"
      href={data.url}
      target="_blank"
      rel="noopener"
      title={`Copycord ${data.latest}`}
      className="block text-center text-[0.8rem] font-bold leading-tight text-[#e53935] no-underline hover:underline"
    >
      New Update Available
    </a>
  );
}
