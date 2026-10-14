# Copycord e2e

Playwright harness for the dashboard (the React SPA in `src/copycord/admin/web`, served at the root).
`global-setup.ts` builds the SPA, installs the repo into `../.venv`, starts `copycord run` against a throwaway
home on port 18080 (`E2E_ADMIN_PORT`), seeds a mapping, a forwarding rule and some event logs, and logs in
through the SPA login page. No Discord tokens are needed; `/api/client-guilds` is stubbed in the forwarding
tests.

```sh
cd e2e && npm ci && npx playwright install chromium
npx tsc --noEmit -p .                      # typecheck
npx playwright test specs/parity.spec.ts   # parity smoke
npx playwright test                        # every spec
```

Set `E2E_SKIP_BUILD=1` once `src/copycord/admin/web/dist` is built. On Titan use `run-on-titan.sh`.

## Screenshots

`baseline/old/` holds the screenshots of the old Jinja UI, kept as reference images only. To compare the SPA
with them:

```sh
npx playwright test specs/baseline.spec.ts   # into baseline/new
node --experimental-strip-types scripts/compare.ts baseline/old baseline/new compare-out
```

`compare.ts` writes `side-by-side/` and `diff/` PNGs plus `summary.md` and `summary.json`, and exits 1 when a
screenshot is missing on one side or more than 3 percent of its pixels changed. Comparing `baseline/old` with
itself reports 0 flagged. `baseline/new/` and `compare-out/` are gitignored.

## Old UI next to the SPA, element by element

`scripts/elemdump.ts` logs in to one running dashboard and visits the screens `baseline.spec.ts` covers, at
1440x900 and 390x844, with the same stubs. For the probes in `lib/elements.ts` (shell, cards, headings, tables,
controls, page parts, each dialog) it writes every matched element's text, box and computed styles to
`<out>/<screen>-<viewport>.json`; with `E2E_SHOTS_DIR` set it also saves the screenshot under the baseline name.
`--ui old` runs it against the old Jinja UI (f4211c1, logs page at `/event-logs`), `--ui new` against the SPA.
`scripts/elemdiff.ts <old-dump> <new-dump> [out]` writes one markdown table per screen and viewport (every
property that differs, px rounded, colors as hex, elements found on one side only) and `summary.md`.

```sh
E2E_BASE_URL=http://127.0.0.1:18181 E2E_SHOTS_DIR=uicmp/shots-old \
  node --experimental-strip-types scripts/elemdump.ts --ui old --out uicmp/dump-old
node --experimental-strip-types scripts/elemdiff.ts uicmp/dump-old uicmp/dump-new elemdiff
```

On Titan, `scripts/uicmp-titan.sh` builds both sides and runs it in steps that each fit one ssh call:
`setup`, `capture old [WxH]`, `capture new [WxH]`, `diff` (tarball at `/root/uicmp-out.tgz`).
