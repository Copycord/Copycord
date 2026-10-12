#!/bin/sh
# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
#
# Install Copycord on Linux or macOS from a GitHub release:
#
#   curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
#
# Environment:
#   COPYCORD_CHANNEL=stable|beta  stable (default): newest release;
#                                 beta: newest release, pre-releases included.
#                                 Saved as the `copycord update` channel.
#   COPYCORD_VERSION=x.y.z        install exactly release vx.y.z instead.
#   COPYCORD_ALLOW_ROOT=1         allow running as root.
#
# Installs uv (https://docs.astral.sh/uv/) if it is missing, then installs the
# release wheel with `uv tool install`. It does not touch Docker.

set -eu

REPO="Copycord/Copycord"
API="https://api.github.com/repos/$REPO/releases"
UV_INSTALLER="https://astral.sh/uv/install.sh"

say() {
    printf '%s\n' "$*"
}

die() {
    printf 'copycord install: ERROR: %s\n' "$*" >&2
    exit 1
}

fetch() {
    # fetch URL: print the body, fail on HTTP errors.
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL -H "Accept: application/vnd.github+json" "$1"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- --header="Accept: application/vnd.github+json" "$1"
    else
        die "neither curl nor wget is installed"
    fi
}

json_field() {
    # json_field KEY: first string value of KEY in the JSON on stdin.
    tr ',' '\n' | sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -n 1
}

# --- checks -------------------------------------------------------------------

if [ "$(id -u)" = "0" ] && [ "${COPYCORD_ALLOW_ROOT:-}" != "1" ]; then
    die "do not run this as root; Copycord installs for your user. Set COPYCORD_ALLOW_ROOT=1 to do it anyway."
fi

CHANNEL="${COPYCORD_CHANNEL:-stable}"
case "$CHANNEL" in
    stable | beta) ;;
    *) die "COPYCORD_CHANNEL must be stable or beta, not '$CHANNEL'" ;;
esac
VERSION="${COPYCORD_VERSION:-}"
VERSION="${VERSION#v}"

# --- plan ---------------------------------------------------------------------

say "Copycord installer"
if [ -n "$VERSION" ]; then
    say "  release:  v$VERSION (COPYCORD_VERSION)"
else
    say "  channel:  $CHANNEL"
fi
if command -v uv >/dev/null 2>&1; then
    say "  uv:       found at $(command -v uv)"
else
    say "  uv:       not found, will install it with $UV_INSTALLER"
fi
say "  then:     uv tool install --force <release wheel URL>"
say ""

# --- uv -----------------------------------------------------------------------

if ! command -v uv >/dev/null 2>&1; then
    say "Installing uv"
    fetch "$UV_INSTALLER" | sh || die "the uv installer failed"
    PATH="${XDG_BIN_HOME:-$HOME/.local/bin}:$HOME/.cargo/bin:$PATH"
    export PATH
    command -v uv >/dev/null 2>&1 || die "uv was installed but is not on PATH; open a new shell and run this again"
fi

# --- release ------------------------------------------------------------------

if [ -n "$VERSION" ]; then
    TAG="v$VERSION"
elif [ "$CHANNEL" = "beta" ]; then
    # Newest first; drafts are not listed for anonymous requests.
    LIST_JSON="$(fetch "$API?per_page=1")" || die "could not list releases from $API"
    TAG="$(printf '%s' "$LIST_JSON" | json_field tag_name)"
else
    # GitHub's latest release: newest that is not a draft or a pre-release.
    LATEST_JSON="$(fetch "$API/latest")" || die "could not get the latest release from $API/latest"
    TAG="$(printf '%s' "$LATEST_JSON" | json_field tag_name)"
fi
[ -n "$TAG" ] || die "no $CHANNEL release found on https://github.com/$REPO/releases"

RELEASE_JSON="$(fetch "$API/tags/$TAG")" || die "no release $TAG on https://github.com/$REPO/releases"
WHEEL_URL="$(printf '%s' "$RELEASE_JSON" | tr ',' '\n' \
    | sed -n 's/.*"browser_download_url"[[:space:]]*:[[:space:]]*"\([^"]*\/copycord-[^"]*\.whl\)".*/\1/p' \
    | head -n 1)"
[ -n "$WHEEL_URL" ] || die "release $TAG has no wheel asset (copycord-*.whl); see https://github.com/$REPO/releases/tag/$TAG"

say "Installing Copycord $TAG"
say "  $WHEEL_URL"
uv tool install --force "$WHEEL_URL" || die "uv tool install failed"

# --- PATH ---------------------------------------------------------------------

BIN_DIR="$(uv tool dir --bin)" || die "could not get the uv tool bin directory"
case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *)
        say "Adding $BIN_DIR to PATH (uv tool update-shell)"
        uv tool update-shell || die "uv tool update-shell failed; add $BIN_DIR to PATH yourself"
        ;;
esac

# --- update channel -----------------------------------------------------------

if [ -z "$VERSION" ] && [ -n "${COPYCORD_CHANNEL:-}" ]; then
    "$BIN_DIR/copycord" update --channel "$CHANNEL" --no-install \
        || die "could not save the update channel $CHANNEL"
fi

say ""
say "Copycord $TAG is installed."
say "Next steps (open a new shell first if copycord is not found):"
say "  copycord doctor                  check the setup"
say "  copycord run                     start Copycord"
say "  copycord service install --user  run it as a service at login"
