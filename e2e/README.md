# Copycord e2e

Playwright harness for the dashboard. `global-setup.ts` builds the frontend, installs the repo into `../.venv`,
starts `copycord run` against a throwaway home on port 18080 (`E2E_ADMIN_PORT`), seeds a mapping, a forwarding
rule and some event logs, and logs in. No Discord tokens are needed; `/api/client-guilds` is stubbed in the
forwarding tests.

```sh
cd e2e && npm ci && npx playwright install chromium
npx tsc --noEmit -p .                      # typecheck
npx playwright test specs/baseline.spec.ts # screenshots into baseline/old
npx playwright test specs/parity.spec.ts   # parity smoke
```

Set `E2E_SKIP_BUILD=1` once `src/copycord/admin/static/assets` is built. On Titan use `run-on-titan.sh`.

## Against the new UI (/app)

`E2E_BASE_PATH=/app` points every page URL at the SPA; the specs stay the same.

```sh
E2E_BASE_PATH=/app npx playwright test specs/parity.spec.ts
E2E_BASE_PATH=/app E2E_SHOTS_DIR=baseline/new npx playwright test specs/baseline.spec.ts
node --experimental-strip-types scripts/compare.ts baseline/old baseline/new compare-out
```

`compare.ts` writes `side-by-side/` and `diff/` PNGs plus `summary.md` and `summary.json`, and exits 1 when a
screenshot is missing on one side or more than 3 percent of its pixels changed. Comparing `baseline/old` with
itself reports 0 flagged. `baseline/new/` and `compare-out/` are gitignored.
