import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
  // Skeletons size themselves from counts a page stored (components/ui/lastCount.ts): every test starts fresh.
  localStorage.clear();
});
