#!/usr/bin/env bash
# UI parity run on Titan WSL: the old Jinja UI (f4211c1) running next to the SPA (this branch), element dumps
# and screenshots from both, element diff and screenshot compare, all packed into /root/uicmp-out.tgz.
# WSL stops when the ssh session ends, so every step fits in one call and starts its own server:
#   bash uicmp-titan.sh setup                          clone and build both sides (rerun until it prints SETUP_OK)
#   bash uicmp-titan.sh capture old|new [WxH] [regex]  run copycord for that side, dump and screenshot its screens
#   bash uicmp-titan.sh diff                           elemdiff, compare.ts, tarball
#   bash uicmp-titan.sh all                            everything in one go (only where the time limit allows)
set -u
OLD_SHA=f4211c15edab2f5e56a85e0b50b0afa5cf66e101
BRANCH=${BRANCH:-v4/ui-elemcmp}
REPO_URL=${REPO_URL:-https://github.com/Copycord/Copycord.git}
OLD=/root/uicmp-old
NEW=/root/uicmp-new
REUSE=${REUSE:-/root/cutover-fe/src}
WORK=/root/uicmp-work
OUT_TGZ=/root/uicmp-out.tgz
FE_REV_NEW=${FE_REV_NEW:-/root/fe-rev/new}
PW=e2e-test-password-1

export NVM_DIR=/root/.nvm
# shellcheck disable=SC1091
. "$NVM_DIR/nvm.sh" >/dev/null 2>&1 || exit 80
nvm use 22 >/dev/null || exit 81
mkdir -p "$WORK"

log() { echo "[uicmp $(date +%H:%M:%S)] $*"; }

setup_old() {
  if [ ! -d "$OLD/.git" ]; then
    log "fetch old $OLD_SHA"
    git init -q "$OLD" && git -C "$OLD" remote add origin "$REPO_URL" &&
      git -C "$OLD" fetch -q --depth 1 origin "$OLD_SHA" && git -C "$OLD" checkout -q FETCH_HEAD || return 90
  fi
  [ "$(git -C "$OLD" rev-parse HEAD)" = "$OLD_SHA" ] || { log "old HEAD is not $OLD_SHA"; return 91; }
  local fe="$OLD/src/copycord/admin/frontend" static="$OLD/src/copycord/admin/static"
  if [ ! -f "$static/assets/main.js" ]; then
    log "build old frontend"
    (cd "$fe" && npm ci --no-audit --no-fund >"$WORK/old-npm.log" 2>&1 && npm run build >>"$WORK/old-npm.log" 2>&1) || return 92
    # Editable installs serve admin/static from the checkout, as the f4211c1 global setup did.
    mkdir -p "$static/assets" && cp -a "$fe/dist/assets/." "$static/assets/" || return 93
  fi
  if [ ! -x "$OLD/.venv/bin/copycord" ]; then
    log "venv old"
    python3 -m venv "$OLD/.venv" || return 94
    "$OLD/.venv/bin/pip" install -q --upgrade pip >/dev/null 2>&1
    COPYCORD_SKIP_FRONTEND=1 "$OLD/.venv/bin/pip" install -q -e "$OLD" >"$WORK/old-pip.log" 2>&1 || return 95
  fi
  log "old ready at $(git -C "$OLD" rev-parse --short HEAD)"
}

setup_new() {
  if [ -d "$NEW/.git" ]; then
    git -C "$NEW" fetch -q --depth 1 origin "$BRANCH" && git -C "$NEW" checkout -q -B uicmp FETCH_HEAD || return 100
  else
    git clone -q --depth 1 -b "$BRANCH" "$REPO_URL" "$NEW" || return 100
  fi
  log "new HEAD $(git -C "$NEW" rev-parse HEAD)"
  local app="$NEW"
  # The branch changes only e2e/, so a built checkout whose src tree and pyproject match can serve the SPA.
  if [ -x "$REUSE/.venv/bin/copycord" ] && [ -f "$REUSE/src/copycord/admin/web/dist/index.html" ] &&
    [ "$(git -C "$REUSE" rev-parse HEAD:src)" = "$(git -C "$NEW" rev-parse HEAD:src)" ] &&
    [ "$(git -C "$REUSE" rev-parse HEAD:pyproject.toml)" = "$(git -C "$NEW" rev-parse HEAD:pyproject.toml)" ]; then
    app="$REUSE"
    log "reuse $REUSE (HEAD $(git -C "$REUSE" rev-parse --short HEAD), same src tree)"
  else
    local web="$NEW/src/copycord/admin/web"
    if [ ! -f "$web/dist/index.html" ]; then
      log "build SPA"
      (cd "$web" && npm ci --no-audit --no-fund >"$WORK/new-npm.log" 2>&1 && npm run build >>"$WORK/new-npm.log" 2>&1) || return 101
    fi
    if [ ! -x "$NEW/.venv/bin/copycord" ]; then
      log "venv new"
      python3 -m venv "$NEW/.venv" || return 102
      "$NEW/.venv/bin/pip" install -q --upgrade pip >/dev/null 2>&1
      COPYCORD_SKIP_FRONTEND=1 "$NEW/.venv/bin/pip" install -q -e "$NEW" >"$WORK/new-pip.log" 2>&1 || return 103
    fi
  fi
  echo "NEW_APP=$app" >"$WORK/env"
  if [ ! -d "$NEW/e2e/node_modules/@playwright/test" ]; then
    if cmp -s "$NEW/e2e/package-lock.json" "$REUSE/e2e/package-lock.json" && [ -d "$REUSE/e2e/node_modules/@playwright/test" ]; then
      log "copy e2e node_modules from $REUSE"
      cp -a "$REUSE/e2e/node_modules" "$NEW/e2e/" || return 104
    else
      log "npm ci e2e"
      (cd "$NEW/e2e" && npm ci --no-audit --no-fund >"$WORK/e2e-npm.log" 2>&1) || return 104
    fi
  fi
  (cd "$NEW/e2e" && npx playwright install chromium >/dev/null 2>&1) || return 105
  if (cd "$NEW/e2e" && npx tsc --noEmit -p . >"$WORK/typecheck.txt" 2>&1); then log "typecheck ok"; else log "TYPECHECK_FAILED"; head -40 "$WORK/typecheck.txt"; fi
}

healthy() {
  local i
  for i in $(seq 1 180); do
    if curl -fsS "http://127.0.0.1:$1/health" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  return 1
}

capture() {
  local side=${1:?old or new} vp=${2:-} only=${3:-} app port cport
  # shellcheck disable=SC1091
  . "$WORK/env" 2>/dev/null || { log "run setup first"; return 110; }
  if [ "$side" = old ]; then app=$OLD port=18181 cport=18291; else app=$NEW_APP port=18182 cport=18295; fi
  local home="/root/uicmp-home-$side"
  rm -rf "$home" && mkdir -p "$home"
  cat >"$home/.env" <<EOF
PASSWORD=$PW
DASHBOARD_HOST=127.0.0.1
ADMIN_PORT=$port
CONTROL_PORT_SERVER=$cport
CONTROL_PORT_CLIENT=$((cport + 1))
SERVER_WS_PORT=$((cport + 2))
CLIENT_WS_PORT=$((cport + 3))
COPYCORD_AUTOSTART=false
EOF
  chmod 600 "$home/.env"
  log "start $side ($app) on $port"
  (cd "$home" && env -u SERVER_TOKEN -u CLIENT_TOKEN -u DISCORD_TOKEN COPYCORD_HOME="$home" COPYCORD_ALLOW_ROOT=1 \
    PASSWORD="$PW" DASHBOARD_HOST=127.0.0.1 ADMIN_PORT="$port" CONTROL_PORT_SERVER="$cport" \
    CONTROL_PORT_CLIENT=$((cport + 1)) SERVER_WS_PORT=$((cport + 2)) CLIENT_WS_PORT=$((cport + 3)) \
    setsid "$app/.venv/bin/copycord" run >"$WORK/server-$side.log" 2>&1 &
    echo $! >"$WORK/server-$side.pid")
  local rc=0
  if healthy "$port"; then
    log "dump $side ${vp:-both viewports} ${only:+only $only}"
    (cd "$NEW/e2e" && E2E_BASE_URL="http://127.0.0.1:$port" E2E_ADMIN_PORT="$port" E2E_COPYCORD_HOME="$home" \
      E2E_PASSWORD="$PW" E2E_SHOTS_DIR="$WORK/shots-$side" \
      timeout 500 node --experimental-strip-types scripts/elemdump.ts --ui "$side" --out "$WORK/dump-$side" \
      ${vp:+--viewport "$vp"} ${only:+--only "$only"} --seed-python "$app/.venv/bin/python") ||
      rc=$?
  else
    log "server $side not healthy"; tail -30 "$WORK/server-$side.log"; rc=111
  fi
  local pid; pid=$(cat "$WORK/server-$side.pid")
  kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null
  sleep 2
  kill -9 -- "-$pid" 2>/dev/null
  log "capture $side rc=$rc shots=$(ls "$WORK/shots-$side" 2>/dev/null | wc -l) dumps=$(ls "$WORK/dump-$side" 2>/dev/null | grep -vc '^index-')"
  return $rc
}

diff_step() {
  rm -rf "$WORK/elemdiff" "$WORK/compare-old-vs-fe-rev" "$WORK/compare-old-vs-new"
  (cd "$NEW/e2e" && node --experimental-strip-types scripts/elemdiff.ts "$WORK/dump-old" "$WORK/dump-new" "$WORK/elemdiff" >"$WORK/elemdiff.txt" 2>&1)
  log "ELEMDIFF_EXIT $?"
  # compare.ts exits 1 whenever a screenshot is flagged or missing, which is the expected outcome here.
  (cd "$NEW/e2e" && node --experimental-strip-types scripts/compare.ts "$WORK/shots-old" "$FE_REV_NEW" "$WORK/compare-old-vs-fe-rev" >"$WORK/compare-old-vs-fe-rev.txt" 2>&1)
  log "COMPARE_FE_REV_EXIT $?"
  (cd "$NEW/e2e" && node --experimental-strip-types scripts/compare.ts "$WORK/shots-old" "$WORK/shots-new" "$WORK/compare-old-vs-new" >"$WORK/compare-old-vs-new.txt" 2>&1)
  log "COMPARE_NEW_EXIT $?"
  log "old shots $(ls "$WORK/shots-old" | wc -l), new shots $(ls "$WORK/shots-new" | wc -l), fe-rev new $(ls "$FE_REV_NEW" | wc -l)"
  log "elements old $(cat "$WORK"/dump-old/*-*.json 2>/dev/null | grep -c '"probe"'), new $(cat "$WORK"/dump-new/*-*.json 2>/dev/null | grep -c '"probe"')"
  local f
  for f in old new; do cp -f "$WORK/server-$f.log" "$WORK/server-$f.txt" 2>/dev/null; done
  tar -czf "$OUT_TGZ" -C "$WORK" elemdiff dump-old dump-new shots-old shots-new compare-old-vs-fe-rev compare-old-vs-new \
    $(cd "$WORK" && ls ./*.txt 2>/dev/null) || return 120
  log "tarball $(du -h "$OUT_TGZ" | cut -f1) $OUT_TGZ"
  head -30 "$WORK/elemdiff/summary.md"
}

case "${1:-}" in
  setup) setup_old && setup_new && log SETUP_OK ;;
  capture) shift; capture "$@" ;;
  diff) diff_step ;;
  all) setup_old && setup_new && capture old && capture new && diff_step ;;
  *) echo "usage: uicmp-titan.sh setup | capture old|new [WxH] [regex] | diff | all" >&2; exit 2 ;;
esac
rc=$?
log "EXIT $rc"
exit $rc
