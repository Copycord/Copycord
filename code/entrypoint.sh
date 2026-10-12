#!/bin/sh
set -euo pipefail
export PYTHONPATH=${PYTHONPATH:-/app}

# Admin, server and client in one process (copycord/supervisor.py).
exec /opt/venvs/copycord/bin/python -u -m copycord run
