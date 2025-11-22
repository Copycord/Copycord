#!/usr/bin/env bash
set -euo pipefail

# Directory where this script lives (repo root)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
CODE_DIR="$ROOT/code"
VENV_ROOT="$ROOT/venvs"

ADMIN_VENV="$VENV_ROOT/admin"
SERVER_VENV="$VENV_ROOT/server"
CLIENT_VENV="$VENV_ROOT/client"

if [[ ! -d "$CODE_DIR" ]]; then
  echo "Error: code/ directory not found at $CODE_DIR"
  echo "Make sure you ran: python install_standalone.py"
  exit 1
fi

if [[ ! -d "$ADMIN_VENV" || ! -d "$SERVER_VENV" || ! -d "$CLIENT_VENV" ]]; then
  echo "Error: one or more virtualenvs are missing in $VENV_ROOT"
  echo "Run: python install_standalone.py"
  exit 1
fi

# Default admin port, overridable via code/.env (ADMIN_PORT=...)
ADMIN_PORT="8080"
ENV_FILE="$CODE_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
  ENV_PORT="$(grep -E '^ADMIN_PORT=' "$ENV_FILE" | head -n1 | cut -d= -f2- | tr -d '\r' || true)"
  if [[ -n "${ENV_PORT:-}" ]]; then
    ADMIN_PORT="$ENV_PORT"
  fi
fi

cd "$CODE_DIR"

echo "Starting Copycord admin UI on port $ADMIN_PORT…"
"$ADMIN_VENV/bin/python" -m uvicorn admin.app:app --host 0.0.0.0 --port "$ADMIN_PORT" &
ADMIN_PID=$!

echo "Starting Copycord server agent control service…"
ROLE=server CONTROL_PORT=9101 "$SERVER_VENV/bin/python" -m control.control &
SERVER_PID=$!

echo "Starting Copycord client agent control service…"
ROLE=client CONTROL_PORT=9102 "$CLIENT_VENV/bin/python" -m control.control &
CLIENT_PID=$!

echo
echo "Copycord is running."
echo "  Admin UI: http://localhost:$ADMIN_PORT"
echo
echo "PIDs:"
echo "  admin : $ADMIN_PID"
echo "  server: $SERVER_PID"
echo "  client: $CLIENT_PID"
echo
echo "Press Ctrl+C here to stop all components."

cleanup() {
  echo
  echo "Stopping Copycord…"
  kill "$ADMIN_PID" "$SERVER_PID" "$CLIENT_PID" 2>/dev/null || true
  wait || true
}

trap cleanup INT TERM

wait
