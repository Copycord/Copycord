#!/bin/sh
set -euo pipefail

# Admin, server and client (src/copycord/supervisor.py), via the
# console script pyproject.toml installs into the venv.
exec /opt/venvs/copycord/bin/copycord run
