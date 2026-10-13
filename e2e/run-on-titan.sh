#!/usr/bin/env bash
# Run the e2e suite on Titan WSL from a fresh clone of a branch.
# Usage: B=<branch> [D=/tmp/cc-e2e-src] [SPECS="specs/baseline.spec.ts specs/parity.spec.ts"] bash e2e/run-on-titan.sh
# Env passed through: E2E_BASE_PATH (e.g. /app), E2E_SHOTS_DIR (e.g. baseline/new), E2E_ADMIN_PORT.
set -u
B=${B:?branch}
D=${D:-/root/cc-e2e-src}
SPECS=${SPECS:-}
REPO_URL=${REPO_URL:-https://github.com/Copycord/Copycord.git}

export NVM_DIR=/root/.nvm
if [ ! -s "$NVM_DIR/nvm.sh" ]; then
  curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | PROFILE=/dev/null bash || exit 80
fi
# shellcheck disable=SC1091
. "$NVM_DIR/nvm.sh"
nvm install 22 >/dev/null 2>&1 || exit 81
nvm use 22 >/dev/null || exit 81

rm -rf "$D"
git clone -q --depth 1 -b "$B" "$REPO_URL" "$D" || exit 90
cd "$D" || exit 91
echo "HEAD $(git rev-parse HEAD)"

echo "STEP venv"
python3 -m venv .venv || exit 92
.venv/bin/pip install -q --upgrade pip >/dev/null 2>&1
COPYCORD_SKIP_FRONTEND=1 .venv/bin/pip install -q -e . >/dev/null 2>&1 || exit 93

echo "STEP npm"
cd e2e || exit 94
if [ -f package-lock.json ]; then
  npm ci --no-audit --no-fund >/dev/null 2>&1 || exit 95
else
  npm install --no-audit --no-fund >/dev/null 2>&1 || exit 95
fi
# Browsers only; --with-deps needs apt and can hang under WSL. Run it once by hand on a new box.
echo "STEP browsers"
npx playwright install chromium >/dev/null 2>&1 || exit 96
npx tsc --noEmit || echo "TYPECHECK_FAILED"

# shellcheck disable=SC2086
npx playwright test $SPECS 2>&1 | tail -60
echo "PLAYWRIGHT_EXIT ${PIPESTATUS[0]}"
