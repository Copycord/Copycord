import { expect, type Locator, type Page } from "@playwright/test";

export const NO_MOTION_CSS = `
*, *::before, *::after {
  animation-duration: 0s !important; animation-delay: 0s !important;
  transition-duration: 0s !important; transition-delay: 0s !important;
  caret-color: transparent !important; scroll-behavior: auto !important;
}`;

/** Wait for fonts, the app loader and the network to settle, then freeze motion. */
export async function settle(page: Page): Promise<void> {
  await page.waitForLoadState("networkidle", { timeout: 15_000 }).catch(() => undefined);
  await page.addStyleTag({ content: NO_MOTION_CSS });
  await page.evaluate(async () => {
    await document.fonts.ready;
  });
  const loader = page.locator("#app-loader");
  if (await loader.count()) {
    await loader.waitFor({ state: "hidden", timeout: 10_000 }).catch(() => undefined);
  }
  await page.waitForTimeout(300);
}

export function modal(page: Page, id: string): Locator {
  return page.locator(`#${id}`);
}

export async function expectOpen(m: Locator): Promise<void> {
  await expect(m).toHaveClass(/(^|\s)show(\s|$)/);
}

export async function expectClosed(m: Locator): Promise<void> {
  await expect(m).not.toHaveClass(/(^|\s)show(\s|$)/);
}

/** The X button inside a modal. */
export function closeX(m: Locator): Locator {
  return m
    .locator(
      '.verify-close, .modal-close, [data-close], [data-dismiss], button[aria-label="Close"], button[aria-label="close"]',
    )
    .first();
}
