// Compare two screenshot folders: side-by-side and diff PNGs plus a summary.
// Usage: node --experimental-strip-types scripts/compare.ts <old-dir> <new-dir> [out-dir]
import fs from "node:fs";
import path from "node:path";
import pixelmatch from "pixelmatch";
import { PNG } from "pngjs";

const THRESHOLD = 0.1;
const FLAG_PERCENT = 3;
const GAP = 16;

const [oldDir, newDir, outArg] = process.argv.slice(2);
if (!oldDir || !newDir) {
  console.error("usage: compare.ts <old-dir> <new-dir> [out-dir]");
  process.exit(2);
}
const outDir = path.resolve(outArg ?? "compare-out");
fs.mkdirSync(path.join(outDir, "side-by-side"), { recursive: true });
fs.mkdirSync(path.join(outDir, "diff"), { recursive: true });

function read(file: string): PNG {
  return PNG.sync.read(fs.readFileSync(file));
}

/** Copy src onto a white canvas of w x h at (dx, 0). */
function blit(dst: PNG, src: PNG, dx: number): void {
  for (let y = 0; y < src.height; y++) {
    const from = y * src.width * 4;
    src.data.copy(dst.data, (y * dst.width + dx) * 4, from, from + src.width * 4);
  }
}

function canvas(w: number, h: number): PNG {
  const png = new PNG({ width: w, height: h });
  png.data.fill(255);
  return png;
}

function pad(src: PNG, w: number, h: number): PNG {
  if (src.width === w && src.height === h) return src;
  const out = canvas(w, h);
  blit(out, src, 0);
  return out;
}

interface Row {
  name: string;
  status: "ok" | "flag" | "missing-old" | "missing-new";
  percent: number | null;
  size: string;
}

const pngs = (d: string) => new Set(fs.readdirSync(d).filter((f) => f.endsWith(".png")));
const olds = pngs(oldDir);
const news = pngs(newDir);
const rows: Row[] = [];

for (const name of [...new Set([...olds, ...news])].sort()) {
  if (!olds.has(name) || !news.has(name)) {
    rows.push({ name, status: olds.has(name) ? "missing-new" : "missing-old", percent: null, size: "" });
    continue;
  }
  const a = read(path.join(oldDir, name));
  const b = read(path.join(newDir, name));
  const w = Math.max(a.width, b.width);
  const h = Math.max(a.height, b.height);
  const pa = pad(a, w, h);
  const pb = pad(b, w, h);
  const diff = new PNG({ width: w, height: h });
  const changed = pixelmatch(pa.data, pb.data, diff.data, w, h, { threshold: THRESHOLD });
  const percent = (changed / (w * h)) * 100;
  fs.writeFileSync(path.join(outDir, "diff", name), PNG.sync.write(diff));

  const sbs = canvas(w * 2 + GAP, h);
  blit(sbs, a, 0);
  blit(sbs, b, w + GAP);
  fs.writeFileSync(path.join(outDir, "side-by-side", name), PNG.sync.write(sbs));

  const size = a.width === b.width && a.height === b.height ? `${w}x${h}` : `${a.width}x${a.height} vs ${b.width}x${b.height}`;
  rows.push({ name, status: percent > FLAG_PERCENT ? "flag" : "ok", percent, size });
}

const lines = [
  `# Screenshot comparison`,
  ``,
  `old: ${path.resolve(oldDir)}  `,
  `new: ${path.resolve(newDir)}  `,
  `pixelmatch threshold ${THRESHOLD}, flagged when more than ${FLAG_PERCENT}% of pixels changed.`,
  ``,
  `| page | changed % | size | status |`,
  `| --- | ---: | --- | --- |`,
  ...rows.map((r) => `| ${r.name} | ${r.percent === null ? "" : r.percent.toFixed(2)} | ${r.size} | ${r.status} |`),
];
const flagged = rows.filter((r) => r.status !== "ok");
lines.push("", `${rows.length} pages, ${flagged.length} flagged or missing.`);
fs.writeFileSync(path.join(outDir, "summary.md"), lines.join("\n") + "\n");
fs.writeFileSync(path.join(outDir, "summary.json"), JSON.stringify(rows, null, 2) + "\n");
console.log(lines.join("\n"));
process.exit(flagged.length ? 1 : 0);
