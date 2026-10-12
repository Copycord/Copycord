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
# Install Copycord on Linux or macOS:
#
#   curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh -s -- --help
#
# Two install methods:
#   release (default)  the wheel asset of a GitHub release
#   git                a clone of any repo and ref (branch, tag or commit),
#                      frontend built with npm, no release needed
#
# Installs uv (https://docs.astral.sh/uv/) if it is missing, then installs
# Copycord with `uv tool install`. The git method also needs git and Node 20+;
# Node is installed per user when it is missing. It does not touch Docker.
# Run with --help for the flags and their environment variables.

set -eu

DEFAULT_REPO="Copycord/Copycord"
UV_INSTALLER="https://astral.sh/uv/install.sh"
NODE_DIST="https://nodejs.org/dist"
NODE_MIN_MAJOR=20

# --- options ------------------------------------------------------------------

METHOD="${COPYCORD_INSTALL_METHOD:-release}"
REPO="${COPYCORD_REPO:-$DEFAULT_REPO}"
VERSION="${COPYCORD_VERSION:-}"
CHANNEL="${COPYCORD_CHANNEL:-stable}"
CHANNEL_SET="${COPYCORD_CHANNEL:+1}"
[ "${COPYCORD_BETA:-0}" = "1" ] && CHANNEL="beta" && CHANNEL_SET=1
DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
GIT_DIR_PATH="${COPYCORD_GIT_DIR:-$DATA_HOME/copycord/src}"
NODE_VERSION="${COPYCORD_NODE_VERSION:-22.12.0}"
NODE_HOME="$DATA_HOME/copycord/node"
ONBOARD=1
[ "${COPYCORD_NO_ONBOARD:-0}" = "1" ] && ONBOARD=0
VERIFY="${COPYCORD_VERIFY:-0}"
DRY_RUN="${COPYCORD_DRY_RUN:-0}"
VERBOSE="${COPYCORD_VERBOSE:-0}"
SHOW_HELP=0

usage() {
    cat <<EOF
Copycord installer

Usage:
  curl -fsSL https://raw.githubusercontent.com/$DEFAULT_REPO/main/install.sh | sh -s -- [options]

Options:
  --install-method, --method release|git
                         install a GitHub release (default) or a git checkout
  --git, --github        shortcut for --install-method git
  --release              shortcut for --install-method release
  --repo owner/name      GitHub repository (default: $DEFAULT_REPO)
  --version <value>      release: a release version, e.g. 4.0.0
                         git: a branch, tag or commit sha (default: main)
  --beta                 release: newest release, pre-releases included;
                         saved as the \`copycord update\` channel
  --git-dir, --dir <path>
                         git: checkout directory
                         (default: \$XDG_DATA_HOME/copycord/src)
  --no-onboard           skip the post-install \`copycord doctor\` run
  --verify               after installing, run \`copycord version\` and
                         \`copycord doctor\` and fail if either fails
  --dry-run              print the plan and the actions, change nothing
  --verbose              stream the output of every step
  --help, -h             show this help

Environment (flags win):
  COPYCORD_INSTALL_METHOD=release|git   COPYCORD_REPO=owner/name
  COPYCORD_VERSION=<version|ref>        COPYCORD_CHANNEL=stable|beta
  COPYCORD_BETA=1                       COPYCORD_GIT_DIR=<path>
  COPYCORD_NO_ONBOARD=1                 COPYCORD_VERIFY=1
  COPYCORD_DRY_RUN=1                    COPYCORD_VERBOSE=1
  COPYCORD_NODE_VERSION=<x.y.z>         COPYCORD_ALLOW_ROOT=1
  NO_COLOR=1                            plain output

Examples:
  # newest stable release
  curl -fsSL https://raw.githubusercontent.com/$DEFAULT_REPO/main/install.sh | sh
  # test a branch, no release needed
  curl -fsSL https://raw.githubusercontent.com/$DEFAULT_REPO/v4-dev/install.sh | sh -s -- --git --version v4-dev
  # a branch of a fork
  curl -fsSL https://raw.githubusercontent.com/$DEFAULT_REPO/main/install.sh | sh -s -- --git --repo you/Copycord --version my-fix
EOF
}

need_value() {
    # need_value FLAG VALUE: VALUE must be a non-empty, non-flag argument.
    case "${2:-}" in
        "" | -*) printf 'copycord install: %s needs a value (see --help)\n' "$1" >&2; exit 2 ;;
    esac
}

while [ $# -gt 0 ]; do
    case "$1" in
        --install-method | --method) need_value "$1" "${2:-}"; METHOD="$2"; shift ;;
        --install-method=* | --method=*) METHOD="${1#*=}" ;;
        --git | --github) METHOD="git" ;;
        --release) METHOD="release" ;;
        --repo) need_value "$1" "${2:-}"; REPO="$2"; shift ;;
        --repo=*) REPO="${1#*=}" ;;
        --version) need_value "$1" "${2:-}"; VERSION="$2"; shift ;;
        --version=*) VERSION="${1#*=}" ;;
        --beta) CHANNEL="beta"; CHANNEL_SET=1 ;;
        --git-dir | --dir) need_value "$1" "${2:-}"; GIT_DIR_PATH="$2"; shift ;;
        --git-dir=* | --dir=*) GIT_DIR_PATH="${1#*=}" ;;
        --no-onboard) ONBOARD=0 ;;
        --onboard) ONBOARD=1 ;;
        --verify) VERIFY=1 ;;
        --dry-run) DRY_RUN=1 ;;
        --verbose) VERBOSE=1 ;;
        --help | -h) SHOW_HELP=1 ;;
        *) printf 'copycord install: unknown option %s (see --help)\n' "$1" >&2; exit 2 ;;
    esac
    shift
done

if [ "$SHOW_HELP" = "1" ]; then
    usage
    exit 0
fi

# --- output -------------------------------------------------------------------

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ] && [ "${TERM:-}" != "dumb" ]; then
    ESC="$(printf '\033')"
    BOLD="$ESC[1m"; DIM="$ESC[2m"; RED="$ESC[31m"; GREEN="$ESC[32m"
    YELLOW="$ESC[33m"; ACCENT="$ESC[38;5;99m"; RESET="$ESC[0m"
    TTY=1
else
    BOLD=""; DIM=""; RED=""; GREEN=""; YELLOW=""; ACCENT=""; RESET=""
    TTY=0
fi
case "${LC_ALL:-${LC_CTYPE:-${LANG:-}}}" in
    *UTF-8* | *utf-8* | *UTF8* | *utf8*)
        OK_MARK="✓"; FAIL_MARK="✗"; WARN_MARK="!"; DOT="•"
        SPIN_FRAMES="⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏"
        BOX_TL="╭"; BOX_TR="╮"; BOX_BL="╰"; BOX_BR="╯"; BOX_H="─"; BOX_V="│"
        ;;
    *)
        OK_MARK="ok"; FAIL_MARK="x"; WARN_MARK="!"; DOT="-"
        SPIN_FRAMES="| / - \\"
        BOX_TL="+"; BOX_TR="+"; BOX_BL="+"; BOX_BR="+"; BOX_H="-"; BOX_V="|"
        ;;
esac

say() { printf '%s\n' "$*"; }
info() { printf '  %s%s%s %s\n' "$DIM" "$DOT" "$RESET" "$*"; }
warn() { printf '  %s%s%s %s\n' "$YELLOW" "$WARN_MARK" "$RESET" "$*" >&2; }

die() {
    printf '\n  %s%s error:%s %s\n\n' "$RED$BOLD" "$FAIL_MARK" "$RESET" "$*" >&2
    exit 1
}

banner() {
    say ""
    say "  $ACCENT$BOLD  ___                             _ $RESET"
    say "  $ACCENT$BOLD / __|___ _ __ _  _ __ ___ _ _ __| |$RESET"
    say "  $ACCENT$BOLD| (__/ _ \\ '_ \\ || / _/ _ \\ '_/ _\` |$RESET"
    say "  $ACCENT$BOLD \\___\\___/ .__/\\_, \\__\\___/_| \\__,_|$RESET"
    say "  $ACCENT$BOLD         |_|   |__/$RESET   ${DIM}installer$RESET"
    say ""
}

plan_line() { printf '  %s%-10s%s %s\n' "$DIM" "$1" "$RESET" "$2"; }

STEP=0
STEP_TOTAL=0
step() {
    STEP=$((STEP + 1))
    printf '\n%s[%s/%s]%s %s%s%s\n' "$ACCENT$BOLD" "$STEP" "$STEP_TOTAL" "$RESET" "$BOLD" "$*" "$RESET"
}
done_line() { printf '  %s%s%s %s\n' "$GREEN" "$OK_MARK" "$RESET" "$*"; }
dry() { printf '  %sdry run:%s %s\n' "$YELLOW" "$RESET" "$*"; }

LOG="$(mktemp "${TMPDIR:-/tmp}/copycord-install.XXXXXX")"
TMP_DIR=""
cleanup() {
    rm -f "$LOG"
    [ -n "$TMP_DIR" ] && rm -rf "$TMP_DIR"
    return 0
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

run() {
    # run LABEL CMD...: run CMD with a spinner (TTY) and its output in $LOG;
    # --verbose streams it. On failure, show the tail of the output and stop.
    label="$1"
    shift
    if [ "$VERBOSE" = "1" ]; then
        info "$label"
        "$@" || die "$label failed (output above)"
        done_line "$label"
        return 0
    fi
    : >"$LOG"
    if [ "$TTY" = "1" ]; then
        "$@" >"$LOG" 2>&1 </dev/null &
        pid=$!
        while kill -0 "$pid" 2>/dev/null; do
            for frame in $SPIN_FRAMES; do
                kill -0 "$pid" 2>/dev/null || break
                printf '\r  %s%s%s %s' "$ACCENT" "$frame" "$RESET" "$label"
                sleep 0.1 2>/dev/null || sleep 1
            done
        done
        if wait "$pid"; then rc=0; else rc=$?; fi
        printf '\r%s' "$ESC[2K"
    else
        if "$@" >"$LOG" 2>&1 </dev/null; then rc=0; else rc=$?; fi
    fi
    if [ "$rc" -ne 0 ]; then
        printf '  %s%s%s %s\n' "$RED" "$FAIL_MARK" "$RESET" "$label" >&2
        printf '%s    last lines of output:%s\n' "$DIM" "$RESET" >&2
        tail -n 25 "$LOG" | sed 's/^/    /' >&2
        die "$label failed (exit $rc)"
    fi
    done_line "$label"
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

download() {
    # download URL FILE
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL -o "$2" "$1"
    else
        wget -qO "$2" "$1"
    fi
}

json_field() {
    # json_field KEY: first string value of KEY in the JSON on stdin.
    tr ',' '\n' | sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -n 1
}

# --- checks -------------------------------------------------------------------

case "$METHOD" in
    release | git) ;;
    *) printf 'copycord install: --install-method must be release or git, not %s\n' "$METHOD" >&2; exit 2 ;;
esac
case "$CHANNEL" in
    stable | beta) ;;
    *) die "COPYCORD_CHANNEL must be stable or beta, not '$CHANNEL'" ;;
esac
case "$REPO" in
    */*) ;;
    *) die "--repo must look like owner/name, not '$REPO'" ;;
esac
if [ "$(id -u)" = "0" ] && [ "${COPYCORD_ALLOW_ROOT:-}" != "1" ]; then
    if [ "$DRY_RUN" = "1" ]; then
        warn "running as root; a real install refuses unless COPYCORD_ALLOW_ROOT=1"
    else
        die "do not run this as root; Copycord installs for your user. Set COPYCORD_ALLOW_ROOT=1 to do it anyway."
    fi
fi

API="https://api.github.com/repos/$REPO/releases"
REPO_URL="https://github.com/$REPO"
if [ "$METHOD" = "release" ]; then
    VERSION="${VERSION#v}"
else
    REF="${VERSION:-main}"
fi

node_major() {
    command -v node >/dev/null 2>&1 || return 0
    v="$(node -v 2>/dev/null || true)"
    v="${v#v}"
    printf '%s' "${v%%.*}"
}

need_node=0
if [ "$METHOD" = "git" ]; then
    major="$(node_major)"
    if [ -z "$major" ] || [ "$major" -lt "$NODE_MIN_MAJOR" ] 2>/dev/null \
        || ! command -v npm >/dev/null 2>&1; then
        need_node=1
    fi
fi
need_uv=0
command -v uv >/dev/null 2>&1 || need_uv=1

# --- plan ---------------------------------------------------------------------

banner
say "  ${BOLD}Plan$RESET"
if [ "$METHOD" = "release" ]; then
    plan_line "method" "release (GitHub release wheel)"
    plan_line "repo" "$REPO"
    if [ -n "$VERSION" ]; then
        plan_line "version" "v$VERSION"
    else
        plan_line "channel" "$CHANNEL (newest release)"
    fi
else
    plan_line "method" "git (build from source, no release needed)"
    plan_line "repo" "$REPO_URL"
    plan_line "ref" "$REF"
    plan_line "checkout" "$GIT_DIR_PATH"
    if [ "$need_node" = "1" ]; then
        plan_line "node" "Node $NODE_VERSION into $NODE_HOME (per user)"
    else
        plan_line "node" "$(node -v) at $(command -v node)"
    fi
fi
if [ "$need_uv" = "1" ]; then
    plan_line "uv" "not found, will install it from $UV_INSTALLER"
else
    plan_line "uv" "$(command -v uv)"
fi
[ "$VERIFY" = "1" ] && plan_line "verify" "copycord version and copycord doctor"
[ "$ONBOARD" = "0" ] && plan_line "onboard" "skipped (--no-onboard)"
[ "$DRY_RUN" = "1" ] && plan_line "dry run" "nothing will be changed"

# uv, install, finishing up; release: find the release; git: fetch, build.
if [ "$METHOD" = "git" ]; then STEP_TOTAL=5; else STEP_TOTAL=4; fi
[ "$need_node" = "1" ] && STEP_TOTAL=$((STEP_TOTAL + 1))
if [ "$VERIFY" = "1" ] || [ "$ONBOARD" = "1" ]; then STEP_TOTAL=$((STEP_TOTAL + 1)); fi

# --- uv -----------------------------------------------------------------------

install_uv() {
    fetch "$UV_INSTALLER" | sh
}

step "Preparing uv"
if [ "$need_uv" = "0" ]; then
    done_line "uv found: $(uv --version 2>/dev/null || command -v uv)"
elif [ "$DRY_RUN" = "1" ]; then
    dry "curl -fsSL $UV_INSTALLER | sh"
else
    run "Installing uv" install_uv
    PATH="${XDG_BIN_HOME:-$HOME/.local/bin}:$HOME/.cargo/bin:$PATH"
    export PATH
    command -v uv >/dev/null 2>&1 || die "uv was installed but is not on PATH; open a new shell and run this again"
fi

# --- release method -------------------------------------------------------------

install_release() {
    step "Finding the release"
    if [ "$DRY_RUN" = "1" ]; then
        if [ -n "$VERSION" ]; then
            dry "look up release v$VERSION on $REPO_URL/releases"
        else
            dry "look up the newest $CHANNEL release on $REPO_URL/releases"
        fi
        LABEL="${VERSION:+v$VERSION}"
        LABEL="${LABEL:-$CHANNEL release}"
        step "Installing Copycord"
        dry "uv tool install --force <copycord wheel of $LABEL>"
        return 0
    fi
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
    [ -n "$TAG" ] || die "no $CHANNEL release found on $REPO_URL/releases"

    RELEASE_JSON="$(fetch "$API/tags/$TAG")" || die "no release $TAG on $REPO_URL/releases"
    WHEEL_URL="$(printf '%s' "$RELEASE_JSON" | tr ',' '\n' \
        | sed -n 's/.*"browser_download_url"[[:space:]]*:[[:space:]]*"\([^"]*\/copycord-[^"]*\.whl\)".*/\1/p' \
        | head -n 1)"
    [ -n "$WHEEL_URL" ] || die "release $TAG has no wheel asset (copycord-*.whl); see $REPO_URL/releases/tag/$TAG"
    done_line "Release $TAG"
    info "$WHEEL_URL"

    step "Installing Copycord"
    run "uv tool install copycord $TAG" uv tool install --force "$WHEEL_URL"
    LABEL="$TAG"
}

# --- git method -----------------------------------------------------------------

node_platform() {
    case "$(uname -s)" in
        Linux) os="linux" ;;
        Darwin) os="darwin" ;;
        *) die "no Node build for $(uname -s); install Node $NODE_MIN_MAJOR+ yourself and run this again" ;;
    esac
    case "$(uname -m)" in
        x86_64 | amd64) arch="x64" ;;
        aarch64 | arm64) arch="arm64" ;;
        armv7l) arch="armv7l" ;;
        *) die "no Node build for $(uname -m); install Node $NODE_MIN_MAJOR+ yourself and run this again" ;;
    esac
    printf 'node-v%s-%s-%s' "$NODE_VERSION" "$os" "$arch"
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d ' ' -f 1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | cut -d ' ' -f 1
    else
        die "need sha256sum or shasum to check the Node download"
    fi
}

install_node() {
    # The official tarball, checked against SHASUMS256.txt, unpacked into
    # $NODE_HOME/<name>; no sudo, nothing outside the user's data directory.
    name="$1"
    download "$NODE_DIST/v$NODE_VERSION/SHASUMS256.txt" "$TMP_DIR/SHASUMS256.txt" || return 1
    expected="$(grep "  $name.tar.gz\$" "$TMP_DIR/SHASUMS256.txt" | cut -d ' ' -f 1 | head -n 1)"
    [ -n "$expected" ] || { echo "no checksum for $name.tar.gz"; return 1; }
    download "$NODE_DIST/v$NODE_VERSION/$name.tar.gz" "$TMP_DIR/node.tar.gz" || return 1
    actual="$(sha256_of "$TMP_DIR/node.tar.gz")"
    [ "$actual" = "$expected" ] || { echo "sha256 mismatch: expected $expected, got $actual"; return 1; }
    mkdir -p "$NODE_HOME" || return 1
    rm -rf "${NODE_HOME:?}/$name"
    tar -xzf "$TMP_DIR/node.tar.gz" -C "$NODE_HOME"
}

checkout_ref() {
    # Clone or update $GIT_DIR_PATH and check out $REF detached: a branch
    # (origin/REF), a tag, or a commit sha fetched from origin. Every command
    # checks its status: errexit does not apply inside `run`.
    if [ -d "$GIT_DIR_PATH/.git" ]; then
        git -C "$GIT_DIR_PATH" remote set-url origin "$REPO_URL.git" || return 1
    else
        git clone --filter=blob:none --no-checkout "$REPO_URL.git" "$GIT_DIR_PATH" || return 1
    fi
    git -C "$GIT_DIR_PATH" fetch --force --prune --tags origin || return 1
    if git -C "$GIT_DIR_PATH" rev-parse -q --verify "refs/remotes/origin/$REF^{commit}" >/dev/null; then
        target="refs/remotes/origin/$REF"
    elif git -C "$GIT_DIR_PATH" rev-parse -q --verify "refs/tags/$REF^{commit}" >/dev/null; then
        target="refs/tags/$REF"
    else
        git -C "$GIT_DIR_PATH" fetch --force origin "$REF" || {
            echo "$REF is not a branch, tag or commit of $REPO_URL"
            return 1
        }
        target="FETCH_HEAD"
    fi
    git -C "$GIT_DIR_PATH" checkout --force --detach "$target" || return 1
    git -C "$GIT_DIR_PATH" clean -fdx -e code/admin/frontend/node_modules
}

build_frontend() {
    # A fresh dist for this ref; hatch_build.py then ships it in the wheel.
    rm -rf "$GIT_DIR_PATH/code/admin/frontend/dist" || return 1
    (cd "$GIT_DIR_PATH/code/admin/frontend" && npm ci --no-audit --no-fund && npm run build)
}

install_git() {
    command -v git >/dev/null 2>&1 \
        || die "git is not installed; install it (e.g. sudo apt install git, or xcode-select --install) and run this again"
    if [ -e "$GIT_DIR_PATH" ] && [ ! -d "$GIT_DIR_PATH/.git" ] \
        && [ -n "$(ls -A "$GIT_DIR_PATH" 2>/dev/null)" ]; then
        die "$GIT_DIR_PATH exists and is not a git checkout; move it or pass --git-dir"
    fi

    if [ "$need_node" = "1" ]; then
        step "Installing Node $NODE_VERSION"
        node_name="$(node_platform)" || exit 1
        if [ "$DRY_RUN" = "1" ]; then
            dry "download $NODE_DIST/v$NODE_VERSION/$node_name.tar.gz, check its sha256, unpack into $NODE_HOME"
        else
            TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/copycord-node.XXXXXX")"
            run "Node $NODE_VERSION into $NODE_HOME" install_node "$node_name"
            PATH="$NODE_HOME/$node_name/bin:$PATH"
            export PATH
            done_line "node $(node -v), npm $(npm -v)"
        fi
    fi

    step "Fetching $REPO@$REF"
    if [ "$DRY_RUN" = "1" ]; then
        dry "git clone $REPO_URL.git $GIT_DIR_PATH (or fetch when it exists)"
        dry "git checkout --detach $REF"
    else
        run "Checking out $REF" checkout_ref
        SHA="$(git -C "$GIT_DIR_PATH" rev-parse --short HEAD)"
        done_line "$REF at $SHA"
    fi

    step "Building the admin frontend"
    if [ "$DRY_RUN" = "1" ]; then
        dry "cd $GIT_DIR_PATH/code/admin/frontend && npm ci && npm run build"
    else
        run "npm ci && npm run build" build_frontend
    fi

    step "Installing Copycord"
    if [ "$DRY_RUN" = "1" ]; then
        dry "uv tool install --force --reinstall-package copycord $GIT_DIR_PATH"
        LABEL="$REF"
    else
        # --reinstall-package: uv would otherwise reuse a cached build of the
        # same directory from an older ref.
        run "uv tool install copycord from $GIT_DIR_PATH" \
            uv tool install --force --reinstall-package copycord "$GIT_DIR_PATH"
        LABEL="$REF ($SHA)"
    fi
}

if [ "$METHOD" = "release" ]; then
    install_release
else
    install_git
fi

# --- PATH and update channel ------------------------------------------------------

step "Finishing up"
if [ "$DRY_RUN" = "1" ]; then
    dry "uv tool update-shell, when uv's tool bin directory is not on PATH"
    BIN_DIR="<uv tool bin directory>"
else
    BIN_DIR="$(uv tool dir --bin)" || die "could not get the uv tool bin directory"
    case ":$PATH:" in
        *":$BIN_DIR:"*) done_line "$BIN_DIR is on PATH" ;;
        *)
            run "Adding $BIN_DIR to PATH (uv tool update-shell)" uv tool update-shell
            PATH="$BIN_DIR:$PATH"
            export PATH
            ;;
    esac
fi

# The channel file (update.json) is written by `copycord update --no-install`,
# so its format stays with the CLI.
CHANNEL_ARGS=""
if [ "$METHOD" = "release" ]; then
    if [ -z "$VERSION" ] && [ "$CHANNEL_SET" = "1" ]; then
        CHANNEL_ARGS="--channel $CHANNEL"
    fi
elif [ "$REPO" = "$DEFAULT_REPO" ]; then
    if [ "$DRY_RUN" = "1" ] || git -C "$GIT_DIR_PATH" rev-parse -q --verify "refs/remotes/origin/$REF" >/dev/null; then
        CHANNEL_ARGS="--channel dev --branch $REF"
    else
        info "$REF is not a branch: the \`copycord update\` channel is left as it is"
    fi
else
    warn "\`copycord update\` follows $DEFAULT_REPO, not $REPO; run this installer again to update from the fork"
fi
if [ -n "$CHANNEL_ARGS" ]; then
    if [ "$DRY_RUN" = "1" ]; then
        dry "copycord update $CHANNEL_ARGS --no-install"
    else
        # shellcheck disable=SC2086 # CHANNEL_ARGS is a word list on purpose
        run "Saving the update channel ($CHANNEL_ARGS)" "$BIN_DIR/copycord" update $CHANNEL_ARGS --no-install
    fi
fi

# --- verify / onboard ---------------------------------------------------------------

if [ "$VERIFY" = "1" ]; then
    step "Verifying"
    if [ "$DRY_RUN" = "1" ]; then
        dry "copycord version && copycord doctor"
    else
        run "copycord version" "$BIN_DIR/copycord" version
        sed 's/^/    /' "$LOG"
        run "copycord doctor" "$BIN_DIR/copycord" doctor
        sed 's/^/    /' "$LOG"
    fi
elif [ "$ONBOARD" = "1" ]; then
    step "Checking the setup"
    if [ "$DRY_RUN" = "1" ]; then
        dry "copycord doctor"
    elif "$BIN_DIR/copycord" doctor >"$LOG" 2>&1; then
        sed 's/^/    /' "$LOG"
        done_line "copycord doctor passed"
    else
        sed 's/^/    /' "$LOG"
        warn "copycord doctor reported problems; fix the FAIL lines above, then run it again"
    fi
fi

# --- done -------------------------------------------------------------------------

box_line() { printf '  %s%s%s %-52s %s%s%s\n' "$ACCENT" "$BOX_V" "$RESET" "$1" "$ACCENT" "$BOX_V" "$RESET"; }
box_rule() {
    i=0
    rule=""
    while [ $i -lt 54 ]; do rule="$rule$BOX_H"; i=$((i + 1)); done
    printf '  %s%s%s%s%s\n' "$ACCENT" "$1" "$rule" "$2" "$RESET"
}

URL="http://localhost:${ADMIN_PORT:-8080}"
say ""
if [ "$DRY_RUN" = "1" ]; then
    say "  ${YELLOW}${BOLD}Dry run complete:$RESET nothing was changed."
    say ""
    exit 0
fi
box_rule "$BOX_TL" "$BOX_TR"
box_line "Copycord $LABEL is installed"
box_line ""
box_line "Next:"
box_line "  copycord           start Copycord"
box_line "  copycord update    update it later"
box_line "  $URL"
box_line "                     open the web UI once it runs"
box_rule "$BOX_BL" "$BOX_BR"
say ""
say "  ${DIM}Open a new shell first if copycord is not found.$RESET"
say ""
