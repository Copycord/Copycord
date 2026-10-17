// Hand written shapes of the server replies.

/** Messages pushed on /ws/ui and /ws/out. */
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

export interface SidebarItem {
  key: "config" | "channels" | "guilds" | "forwarding" | "scraper" | "system" | "logs";
  label: string;
  to: string;
}
