#!/bin/sh
set -eu

# The container runs as the non-root copycord user. A ./data folder left root-owned
# by an older image needs a one-time fix on the host: sudo chown -R 1000:1000 ./data
DATA="${DATA_DIR:-/data}"
if [ ! -w "$DATA" ]; then
    echo "copycord: $DATA is not writable by uid $(id -u). Run once on the host: sudo chown -R 1000:1000 <your data folder>" >&2
    exit 1
fi

# Admin, server and client (src/copycord/supervisor.py), via the
# console script pyproject.toml installs into the venv.
exec /opt/venvs/copycord/bin/copycord run
