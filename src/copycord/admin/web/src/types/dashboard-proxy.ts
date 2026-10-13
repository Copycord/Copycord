// Shapes behind the Proxy card on the Configuration page.

/** GET /api/server/proxies. */
export interface ProxyConfig {
  ok: boolean;
  proxies?: string[];
  enabled?: boolean;
  rotation_interval?: number;
  favorites?: string[];
  favorite_recheck?: boolean;
  favorite_recheck_interval?: number;
  error?: string;
}

/** Reply of the PUT endpoints that only answer ok or an error. */
export interface ProxyOk {
  ok: boolean;
  error?: string;
  favorites?: string[];
}

export type ProxySettingKey = "PROXY_SUSPEND_DURATION" | "PROXY_TEST_BATCH_SIZE" | "PROXY_SLOW_THRESHOLD";

export type ProxySettings = Record<ProxySettingKey, number>;

/** GET /api/server/proxies/settings. */
export interface ProxySettingsResponse {
  ok: boolean;
  settings?: Partial<ProxySettings>;
  error?: string;
}

/** One tested proxy inside a progress or complete message. */
export interface ProxyTestResult {
  proxy: string;
  ok: boolean;
  ms?: number;
  error?: string;
}

/** The payload of a bus message with kind "proxy_test". */
export interface ProxyTestEvent {
  type: "started" | "progress" | "complete" | "stopped";
  total?: number;
  current?: number;
  results?: ProxyTestResult[];
}

/** GET /api/server/proxies/test/status. */
export interface ProxyTestStatus {
  ok: boolean;
  running?: boolean;
}

export type ProxyChipTest = "loading" | "pass" | "slow" | "fail";

/** What a chip shows while or after a test: its state and badge text. */
export interface ProxyChipResult {
  state: ProxyChipTest;
  label: string;
}

export type IntervalUnit = "60" | "3600";
