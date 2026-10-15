// Shared by every start form: POST /api/tasks/runs, then open the run with a toast. A 422 comes
// back as its {field: message} map so the form can show each message on its field.

import { useCallback, useState } from "react";
import { useNavigate } from "react-router";
import { useToast } from "../../../components/Toast";
import { ApiError, api, errorText } from "../../../lib/api";
import type { TaskRunResponse, TaskType } from "../../../types/tasks";

/** The props every start form gets from the Tasks page. */
export interface TaskFormProps {
  type: TaskType;
  open: boolean;
  onClose: () => void;
}

export function useStartRun(type: TaskType) {
  const navigate = useNavigate();
  const toast = useToast();
  const [busy, setBusy] = useState(false);

  const start = useCallback(
    async (inputs: Record<string, unknown>): Promise<Record<string, string> | null> => {
      setBusy(true);
      try {
        const res = await api.post<TaskRunResponse>("/api/tasks/runs", { type: type.key, inputs });
        const run = res.run;
        toast.show(`Started ${type.title} #${run.number}`, { kind: "success" });
        navigate(`/tasks/runs/${encodeURIComponent(run.id)}`);
        return null;
      } catch (e) {
        if (e instanceof ApiError && e.status === 422) {
          const errors = (e.body as { errors?: Record<string, string> } | null)?.errors;
          if (errors && typeof errors === "object") return errors;
        }
        toast.show(errorText(e, `Could not start ${type.title}`), { kind: "error" });
        return {};
      } finally {
        setBusy(false);
      }
    },
    [type, navigate, toast],
  );

  return { start, busy };
}
