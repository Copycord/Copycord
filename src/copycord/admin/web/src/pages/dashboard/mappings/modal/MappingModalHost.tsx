// Renders the mapping modal for the request openMappingModal recorded.

import { useSyncExternalStore } from "react";
import { closeMappingModal, getMappingModal, subscribeMappingModal } from "../openers";
import { MappingModal } from "./MappingModal";

export function MappingModalHost({ onSaved }: { onSaved: () => void | Promise<void> }) {
  const req = useSyncExternalStore(subscribeMappingModal, getMappingModal, getMappingModal);
  if (!req) return null;
  return <MappingModal key={req.seq} mapping={req.mapping} mode={req.mode} onClose={closeMappingModal} onSaved={onSaved} />;
}
