// Diff two elemdump folders (old UI and SPA): one markdown file per screen and viewport plus a summary.
// Usage: node --experimental-strip-types scripts/elemdiff.ts <old-dump-dir> <new-dump-dir> [out-dir]
// Elements are paired by probe key; boxes and px values are rounded to 1 px, colors written as hex.
import fs from "node:fs";
import path from "node:path";

interface Measured {
  probe: string;
  key: string;
  selector: string;
  text: string;
  visible: boolean;
  box: { x: number; y: number; w: number; h: number };
  styles: Record<string, string>;
}

interface Dump {
  ui: string;
  screen: string;
  viewport: string;
  url: string;
  elements: Measured[];
  missing: string[];
  errors: string[];
}

const [oldDir, newDir, outArg] = process.argv.slice(2);
if (!oldDir || !newDir) {
  console.error("usage: elemdiff.ts <old-dump-dir> <new-dump-dir> [out-dir]");
  process.exit(2);
}
const outDir = path.resolve(outArg ?? "elemdiff");
fs.mkdirSync(outDir, { recursive: true });

const dumps = (d: string) =>
  new Set(fs.readdirSync(d).filter((f) => f.endsWith(".json") && !f.startsWith("index-")));
const olds = dumps(oldDir);
const news = dumps(newDir);
const read = (d: string, f: string) => JSON.parse(fs.readFileSync(path.join(d, f), "utf8")) as Dump;

const hex2 = (n: number) => Math.max(0, Math.min(255, Math.round(n))).toString(16).padStart(2, "0");

/** rgb()/rgba() to #rrggbb or #rrggbbaa, px rounded to whole pixels, quotes dropped from font names. */
function norm(v: string): string {
  return v
    .replace(/rgba?\(([^)]*)\)/g, (_m, inner: string) => {
      const [r = 0, g = 0, b = 0, a = 1] = inner.split(/[\s,/]+/).filter(Boolean).map((p) => parseFloat(p));
      return `#${hex2(r)}${hex2(g)}${hex2(b)}${a < 1 ? hex2(a * 255) : ""}`;
    })
    .replace(/(-?\d*\.?\d+)px/g, (_m, n: string) => `${Math.round(parseFloat(n))}px`)
    .replace(/"/g, "")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

const cell = (s: string) => (s === "" ? "(empty)" : s.replace(/\|/g, "\\|"));

function props(m: Measured): Record<string, string> {
  const out: Record<string, string> = {
    visible: String(m.visible),
    text: m.text,
    x: `${Math.round(m.box.x)}`,
    y: `${Math.round(m.box.y)}`,
    w: `${Math.round(m.box.w)}`,
    h: `${Math.round(m.box.h)}`,
  };
  for (const [k, v] of Object.entries(m.styles)) out[k] = norm(v);
  return out;
}

interface Row {
  screen: string;
  viewport: string;
  old: number;
  new: number;
  matched: number;
  differ: number;
  identical: number;
  onlyOld: number;
  onlyNew: number;
  note: string;
}

const rows: Row[] = [];
const propCounts = new Map<string, number>();

for (const file of [...new Set([...olds, ...news])].sort()) {
  const a = olds.has(file) ? read(oldDir, file) : null;
  const b = news.has(file) ? read(newDir, file) : null;
  const ref = (a ?? b)!;
  const name = file.replace(/\.json$/, "");
  if (!a || !b) {
    rows.push({ screen: ref.screen, viewport: ref.viewport, old: a?.elements.length ?? 0, new: b?.elements.length ?? 0,
      matched: 0, differ: 0, identical: 0, onlyOld: 0, onlyNew: 0, note: a ? "no new dump" : "no old dump" });
    continue;
  }
  const byKey = (d: Dump) => new Map(d.elements.map((e) => [e.key, e]));
  const ma = byKey(a);
  const mb = byKey(b);
  const diffs: string[] = [];
  const identical: string[] = [];
  const onlyOld: Measured[] = [];
  const onlyNew: Measured[] = [];
  for (const [key, ea] of ma) {
    const eb = mb.get(key);
    if (!eb) {
      onlyOld.push(ea);
      continue;
    }
    const pa = props(ea);
    const pb = props(eb);
    const changed = Object.keys({ ...pa, ...pb }).filter((k) => (pa[k] ?? "") !== (pb[k] ?? ""));
    if (!changed.length) {
      identical.push(key);
      continue;
    }
    for (const k of changed) propCounts.set(k, (propCounts.get(k) ?? 0) + 1);
    const list = changed.filter((k) => k !== "text").map((k) => `${k}: ${cell(pa[k] ?? "")} -> ${cell(pb[k] ?? "")}`);
    diffs.push(`| ${cell(key)} | ${cell(ea.text)} | ${cell(eb.text)} | ${list.join("; ") || "text only"} |`);
  }
  for (const [key, eb] of mb) if (!ma.has(key)) onlyNew.push(eb);

  const one = (m: Measured) =>
    `| ${cell(m.key)} | ${cell(m.text)} | ${m.visible} | ${Math.round(m.box.x)}, ${Math.round(m.box.y)}, ${Math.round(m.box.w)} x ${Math.round(m.box.h)} | \`${cell(m.selector)}\` |`;
  const lines = [
    `# ${ref.screen} ${ref.viewport}`,
    "",
    `old: ${a.url}  `,
    `new: ${b.url}  `,
    `${a.elements.length} elements old, ${b.elements.length} new; ${diffs.length + identical.length} matched, ${diffs.length} differ, ${identical.length} identical, ${onlyOld.length} only in old, ${onlyNew.length} only in new.`,
    "",
    "## Differences (old -> new)",
    "",
    "| element | old text | new text | differences |",
    "| --- | --- | --- | --- |",
    ...diffs,
    "",
    "## Missing in new (present in old only)",
    "",
    "| element | text | visible | x, y, w x h | selector |",
    "| --- | --- | --- | --- | --- |",
    ...onlyOld.map(one),
    "",
    "## Missing in old (present in new only)",
    "",
    "| element | text | visible | x, y, w x h | selector |",
    "| --- | --- | --- | --- | --- |",
    ...onlyNew.map(one),
    "",
    "## Identical",
    "",
    identical.length ? identical.map(cell).join(", ") : "(none)",
    "",
    "## Probes that matched nothing",
    "",
    `old: ${a.missing.join(", ") || "(none)"}  `,
    `new: ${b.missing.join(", ") || "(none)"}`,
  ];
  if (a.errors.length || b.errors.length) {
    lines.push("", "## Probe errors", "", ...a.errors.map((e) => `- old ${e}`), ...b.errors.map((e) => `- new ${e}`));
  }
  fs.writeFileSync(path.join(outDir, `${name}.md`), lines.join("\n") + "\n");
  rows.push({ screen: ref.screen, viewport: ref.viewport, old: a.elements.length, new: b.elements.length,
    matched: diffs.length + identical.length, differ: diffs.length, identical: identical.length,
    onlyOld: onlyOld.length, onlyNew: onlyNew.length, note: "" });
}

const summary = [
  "# Element diff summary",
  "",
  `old: ${path.resolve(oldDir)}  `,
  `new: ${path.resolve(newDir)}`,
  "",
  "| screen | viewport | old | new | matched | differ | identical | only old | only new | note |",
  "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
  ...rows.map((r) => `| ${r.screen} | ${r.viewport} | ${r.old} | ${r.new} | ${r.matched} | ${r.differ} | ${r.identical} | ${r.onlyOld} | ${r.onlyNew} | ${r.note} |`),
  "",
  `${rows.length} screens; ${rows.reduce((n, r) => n + r.old, 0)} elements old, ${rows.reduce((n, r) => n + r.new, 0)} new; ${rows.reduce((n, r) => n + r.differ, 0)} differ, ${rows.reduce((n, r) => n + r.onlyOld, 0)} only in old, ${rows.reduce((n, r) => n + r.onlyNew, 0)} only in new.`,
  "",
  "## Properties that differ most often (elements, all screens)",
  "",
  "| property | elements |",
  "| --- | ---: |",
  ...[...propCounts].sort((x, y) => y[1] - x[1]).map(([k, n]) => `| ${k} | ${n} |`),
];
fs.writeFileSync(path.join(outDir, "summary.md"), summary.join("\n") + "\n");
fs.writeFileSync(path.join(outDir, "summary.json"), JSON.stringify(rows, null, 1) + "\n");
console.log(summary.slice(0, 40).join("\n"));
