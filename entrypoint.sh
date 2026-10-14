#!/bin/sh
set -eu

# The container runs as the non-root copycord user. A ./data folder left root-owned
# by an older image needs a one-time fix on the host: sudo chown -R 1000:1000 ./data
DATA="${DATA_DIR:-/data}"
# v3 ran as root, so data.db and backups can stay root-owned inside a writable folder.
for p in "$DATA" "$DATA/backups" "${DB_PATH:-$DATA/data.db}"; do
    if [ "$p" = "$DATA" ] || [ -e "$p" ]; then
        if [ ! -w "$p" ]; then
            echo "copycord: $p is not writable by uid $(id -u). Run once on the host: sudo chown -R 1000:1000 <your data folder>" >&2
            exit 1
        fi
    fi
done

# Admin, server and client (src/copycord/supervisor.py), via the
# console script pyproject.toml installs into the venv.
exec /opt/venvs/copycord/bin/copycord run
