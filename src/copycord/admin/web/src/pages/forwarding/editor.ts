// Hook point for the rule create/edit modal (plan lane 10). Until that lands
// the New Forward, Create your first rule, Edit and Duplicate buttons call
// the no-op opener below and open nothing.

import type { ForwardingRule } from "../../types/forwarding";

export type RuleEditorRequest =
  | { mode: "create" }
  | { mode: "edit"; rule: ForwardingRule }
  | { mode: "duplicate"; rule: ForwardingRule };

export type OpenRuleEditor = (req: RuleEditorRequest) => void;

export const openRuleEditorStub: OpenRuleEditor = () => undefined;
