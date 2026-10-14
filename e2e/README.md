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
