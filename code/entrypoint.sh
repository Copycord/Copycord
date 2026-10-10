#!/bin/sh
set -euo pipefail
export PYTHONPATH=${PYTHONPATH:-/app}

case "${ROLE:-all}" in
  all)
    # Admin, server and client in one process (copycord/supervisor.py).
    exec /opt/venvs/copycord/bin/python -u -m copycord run
    ;;

  admin)
    exec /opt/venvs/admin/bin/uvicorn admin.app:app \
      --host 0.0.0.0 \
      --port "${ADMIN_PORT:-8080}" \
      --log-level "${LOG_LEVEL:-info}" \
      --timeout-keep-alive 2 \
      --timeout-graceful-shutdown 1
    ;;

  server)
    # Default to 9101 unless user provided CONTROL_PORT
    : "${CONTROL_PORT:=9101}"
    export CONTROL_PORT
    exec /opt/venvs/server/bin/python -u /app/control/control.py
    ;;

  client)
    # Default to 9102 unless user provided CONTROL_PORT
    : "${CONTROL_PORT:=9102}"
    export CONTROL_PORT
    exec /opt/venvs/client/bin/python -u /app/control/control.py
    ;;

  *)
    echo "Unknown ROLE=${ROLE}"
    exit 1
    ;;
esac
