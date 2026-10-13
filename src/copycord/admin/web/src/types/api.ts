// Hand written shapes of the server replies, filled in page by page.

/** Messages pushed on /ws/ui and /ws/out and on the /bus/stream SSE feed. */
export interface BusMessage {
  type?: string;
  kind?: string;
  role?: string;
  source?: string;
  data?: unknown;
  [key: string]: unknown;
}

export type WsPath = "/ws/ui" | "/ws/out";

export type LogStream = "server" | "client";

/** Reply of POST /logout. */
export interface LogoutResponse {
  ok?: boolean;
}

export interface SidebarItem {
  key: "config" | "channels" | "guilds" | "forwarding" | "scraper" | "system" | "logs";
  label: string;
  to: string;
}
