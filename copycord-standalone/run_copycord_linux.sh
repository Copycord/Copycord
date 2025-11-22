#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Copycord Linux Launcher
# ============================================================
# Starts Admin UI, Server Control, and Client Control
# Gracefully handles missing installations
# ============================================================

# Directory where this script lives (repo root)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
CODE_DIR="$ROOT/code"
VENV_ROOT="$ROOT/venvs"

ADMIN_VENV="$VENV_ROOT/admin"
SERVER_VENV="$VENV_ROOT/server"
CLIENT_VENV="$VENV_ROOT/client"

# Colors for friendly output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==============================================="
echo -e "      🚀 Copycord Linux Launcher"
echo -e "===============================================${NC}"

# ============================================================
# Sanity Checks
# ============================================================

if [[ ! -d "$CODE_DIR" ]]; then
  echo -e "${RED}❌ Error:${NC} code/ directory not found at ${YELLOW}$CODE_DIR${NC}"
  echo -e "Run: ${GREEN}python install_standalone.py${NC}"
  exit 1
fi

if [[ ! -d "$ADMIN_VENV" ]]; then
  echo -e "${RED}❌ Error:${NC} Admin virtual environment missing at ${YELLOW}$ADMIN_VENV${NC}"
  echo -e "Run: ${GREEN}python install_standalone.py${NC}"
  exit 1
fi

if [[ ! -d "$SERVER_VENV" ]]; then
  echo -e "${RED}❌ Error:${NC} Server virtual environment missing at ${YELLOW}$SERVER_VENV${NC}"
  echo -e "Run: ${GREEN}python install_standalone.py${NC}"
  exit 1
fi

if [[ ! -d "$CLIENT_VENV" ]]; then
  echo -e "${RED}❌ Error:${NC} Client virtual environment missing at ${YELLOW}$CLIENT_VENV${NC}"
  echo -e "Run: ${GREEN}python install_standalone.py${NC}"
  exit 1
fi

# ============================================================
# Load Admin Port from .env
# ============================================================

ADMIN_PORT="8080"
ENV_FILE="$CODE_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
  ENV_PORT="$(grep -E '^ADMIN_PORT=' "$ENV_FILE" | head -n1 | cut -d= -f2- | tr -d '\r' || true)"
  if [[ -n "${ENV_PORT:-}" ]]; then
    ADMIN_PORT="$ENV_PORT"
  fi
fi

# ============================================================
# Start Services
# ============================================================

cd "$CODE_DIR"

echo -e "${GREEN}✅ Starting Copycord...${NC}"
echo -e "  Root: ${YELLOW}$ROOT${NC}"
echo -e "  Admin UI: ${BLUE}http://localhost:$ADMIN_PORT${NC}"
echo

# Start Admin UI
echo -e "▶️  Launching Admin UI..."
"$ADMIN_VENV/bin/python" -m uvicorn admin.app:app --host 0.0.0.0 --port "$ADMIN_PORT" &
ADMIN_PID=$!

# Start Server control
echo -e "▶️  Launching Server control..."
ROLE=server CONTROL_PORT=9101 "$SERVER_VENV/bin/python" -m control.control &
SERVER_PID=$!

# Start Client control
echo -e "▶️  Launching Client control..."
ROLE=client CONTROL_PORT=9102 "$CLIENT_VENV/bin/python" -m control.control &
CLIENT_PID=$!

echo
echo -e "${GREEN}✅ Copycord is running!${NC}"
echo -e "  Admin UI:    ${BLUE}http://localhost:$ADMIN_PORT${NC}"
echo -e "  Server Ctrl: ws://localhost:9101"
echo -e "  Client Ctrl: ws://localhost:9102"
echo
echo "PIDs:"
echo "  Admin : $ADMIN_PID"
echo "  Server: $SERVER_PID"
echo "  Client: $CLIENT_PID"
echo
echo -e "Press ${YELLOW}Ctrl+C${NC} to stop all components."

cleanup() {
  echo
  echo -e "${YELLOW}🛑 Stopping Copycord…${NC}"
  kill "$ADMIN_PID" "$SERVER_PID" "$CLIENT_PID" 2>/dev/null || true
  wait || true
  echo -e "${GREEN}✅ All processes stopped.${NC}"
}

trap cleanup INT TERM

wait
