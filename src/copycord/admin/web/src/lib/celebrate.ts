// Confetti for a claimed code, as v3 fired it for a new mapping: for three seconds, every 250 ms, a burst from the
// left and one from the right in the brand purples and reds, thinning out as time runs down, drawn above every layer
// (modal backdrops included). It plays under prefers-reduced-motion too (owner's call, 2026-10-20). canvas-confetti
// loads on first use so it stays out of the main bundle.

const DURATION = 3000;
const INTERVAL = 250;
const DEFAULTS = {
  startVelocity: 30,
  spread: 360,
  ticks: 60,
  zIndex: 99999,
  colors: ["#8b5cf6", "#a78bfa", "#c4b5fd", "#ef4444", "#f87171", "#fca5a5"],
};

const between = (min: number, max: number) => Math.random() * (max - min) + min;

export async function celebrate(): Promise<void> {
  const mod = await import("canvas-confetti").catch(() => null);
  if (!mod) return;
  const confetti = mod.default;
  const end = Date.now() + DURATION;
  const timer = setInterval(() => {
    const left = end - Date.now();
    if (left <= 0) {
      clearInterval(timer);
      return;
    }
    const particleCount = 50 * (left / DURATION);
    void confetti({ ...DEFAULTS, particleCount, origin: { x: between(0.1, 0.3), y: Math.random() - 0.2 } });
    void confetti({ ...DEFAULTS, particleCount, origin: { x: between(0.7, 0.9), y: Math.random() - 0.2 } });
  }, INTERVAL);
}
