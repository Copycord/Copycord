// What New Forward, Create your first rule, Edit and Duplicate ask for. The
// Forwarding page opens RuleModal with it unless a test passes its own opener.

import type { ForwardingRule } from "../../types/forwarding";

export type RuleEditorRequest =
  | { mode: "create" }
  | { mode: "edit"; rule: ForwardingRule }
  | { mode: "duplicate"; rule: ForwardingRule };

export type OpenRuleEditor = (req: RuleEditorRequest) => void;
