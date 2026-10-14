#!/bin/sh
set -eu

# The image starts as root only to hand files the v3 image left root-owned (it ran
# as root) to the copycord user (uid 1000), then drops to that user. Where chown
# does not take (Windows Docker Desktop and some network bind mounts) it stays root,
# as v3 did, instead of refusing to start.
DATA="${DATA_DIR:-/data}"
DB="${DB_PATH:-$DATA/data.db}"
# Same rule as backups_dir() in src/copycord/common/db.py.
BACKUPS="${BACKUP_DIR:-$DATA/backups}"
APP_UID=1000
APP_GID=1000
# Admin, server and client (src/copycord/supervisor.py), via the
# console script pyproject.toml installs into the venv.
COPYCORD="${VENV_BASE:-/opt/venvs}/copycord/bin/copycord"

# Prints the first path copycord writes that "$@ test -w" says it cannot.
first_unwritable() {
    for p in "$DATA" "$BACKUPS" "$DB"; do
        if [ "$p" = "$DATA" ] || [ -e "$p" ]; then
            if ! "$@" test -w "$p"; then
                echo "$p"
                return
            fi
        fi
    done
}

if [ "$(id -u)" != 0 ]; then
    # Started with --user or compose user:, so nothing here can fix ownership.
    bad="$(first_unwritable env)"
    if [ -n "$bad" ]; then
        echo "copycord: $bad is not writable by uid $(id -u). Run once on the host: sudo chown -R $APP_UID:$APP_GID <your data folder>" >&2
        exit 1
    fi
    exec "$COPYCORD" run
fi

# Only files not already owned by uid 1000, so a big folder costs one walk.
for p in "$DATA" "$BACKUPS" "$DB"; do
    if [ -e "$p" ]; then
        find "$p" \( ! -uid "$APP_UID" -o ! -gid "$APP_GID" \) -exec chown -h "$APP_UID:$APP_GID" {} + 2>/dev/null || true
    fi
done

AS_APP="setpriv --reuid=$APP_UID --regid=$APP_GID --clear-groups"
# shellcheck disable=SC2086
# Windows Docker Desktop bind mounts keep their own owner and mode, so chown does not
# take there and uid 1000 cannot write. Nothing the user can do about it, so no
# message: run as root, as the v3 image did.
if [ -n "$(first_unwritable $AS_APP)" ]; then
    exec "$COPYCORD" run
fi

# HOME follows the user, as it did when the image switched with USER.
HOME="$(getent passwd "$APP_UID" | cut -d: -f6)"
export HOME="${HOME:-/}"
# shellcheck disable=SC2086
exec $AS_APP "$COPYCORD" run
