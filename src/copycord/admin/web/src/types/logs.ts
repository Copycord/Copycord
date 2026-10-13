// Shapes of the /api/event-logs replies (app.py api_get_event_logs and friends).

/** One row of the event_logs table as the server returns it. */
export interface EventLog {
  log_id: string;
  event_type: string;
  details: string | null;
  /** Discord snowflakes are stored as INTEGER, so JSON may hand back a number. */
  guild_id: string | number | null;
  guild_name: string | null;
  channel_id: string | number | null;
  channel_name: string | null;
  category_id: string | number | null;
  category_name: string | null;
  /** JSON text, or already parsed by a future server. */
  extra_json: string | Record<string, unknown> | null;
  /** Unix seconds. */
  created_at: number | null;
}

/** GET /api/event-logs */
export interface EventLogsResponse {
  ok: boolean;
  logs: EventLog[];
  total: number;
  types: string[];
}

/** GET /api/event-log-types */
export interface EventLogTypesResponse {
  ok: boolean;
  types: string[];
}

/** DELETE /api/event-logs/{id} */
export interface EventLogDeleteResponse {
  ok: boolean;
}

/** POST /api/event-logs/delete-bulk and DELETE /api/event-logs */
export interface EventLogBulkDeleteResponse {
  ok: boolean;
  deleted: number;
}

/** Body of POST /api/event-logs/delete-bulk. */
export interface EventLogBulkDeleteRequest {
  ids: string[];
}
