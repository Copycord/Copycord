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
# Installs uv if it is missing, then Copycord with `uv tool install`, either
# from a release wheel (default) or from a git ref built locally, which needs
# git and Node 22+. Run with --help for the options.

set -eu

DEFAULT_REPO="Copycord/Copycord"
# The Python version uv is asked for.
PYTHON_VERSION="3.14"
UV_INSTALLER="https://astral.sh/uv/install.sh"
NODE_DIST="https://nodejs.org/dist"
NODE_MIN_MAJOR=22

# --- options ------------------------------------------------------------------

METHOD="${COPYCORD_INSTALL_METHOD:-release}"
METHOD_SET="${COPYCORD_INSTALL_METHOD:+1}"
REPO="${COPYCORD_REPO:-$DEFAULT_REPO}"
VERSION="${COPYCORD_VERSION:-}"
CHANNEL="${COPYCORD_CHANNEL:-stable}"
CHANNEL_SET="${COPYCORD_CHANNEL:+1}"
[ "${COPYCORD_BETA:-0}" = "1" ] && CHANNEL="beta" && CHANNEL_SET=1
COPYCORD_HOME="${COPYCORD_HOME:-$HOME/.copycord}"
GIT_DIR_PATH="${COPYCORD_GIT_DIR:-}"
# Keep in step with NODE_VERSION in src/copycord/node_runtime.py.
NODE_VERSION="${COPYCORD_NODE_VERSION:-22.12.0}"
VERIFY="${COPYCORD_VERIFY:-0}"
DRY_RUN="${COPYCORD_DRY_RUN:-0}"
VERBOSE="${COPYCORD_VERBOSE:-0}"
FORCE="${COPYCORD_FORCE:-0}"
SHOW_HELP=0

usage() {
    cat <<EOF
Copycord installer

Usage:
  curl -fsSL https://raw.githubusercontent.com/$DEFAULT_REPO/main/install.sh | sh -s -- [options]

Options:
  --repo owner/name      GitHub repository (default: $DEFAULT_REPO)
  --version <value>      a release version (4.0.0.1, v4.0.0.1) installs
                         that release; anything else (a branch, tag or
                         commit sha) installs from git at that ref
  --beta                 release: newest release, pre-releases included;
                         saved as the copycord update channel
  --home <path>          Copycord home: data, logs, backups, src/ and node/
                         (default: ~/.copycord)
  --git-dir, --dir <path>
                         git: a folder the installer owns; its files are
                         replaced on each install, so never point it at
                         your own clone (default: <home>/src)
  --verify               after installing, run copycord --version and
                         copycord doctor and fail if either fails
  --dry-run              print the plan and the actions, change nothing
  --verbose              stream the output of every step
  --force                reinstall even when this version is already
                         installed (without it the installer stops there)
  --help, -h             show this help

Advanced:
  --install-method release|git (alias --method), --git and --release force the
  method instead of inferring it from --version; COPYCORD_INSTALL_METHOD does too.

Environment (flags win):
  COPYCORD_REPO=owner/name
  COPYCORD_VERSION=<version|ref>        COPYCORD_CHANNEL=stable|beta
  COPYCORD_BETA=1                       COPYCORD_GIT_DIR=<path>
  COPYCORD_VERIFY=1                     COPYCORD_DRY_RUN=1
  COPYCORD_VERBOSE=1                    COPYCORD_NODE_VERSION=<x.y.z>
  COPYCORD_ALLOW_ROOT=1                 COPYCORD_HOME=<path>
  COPYCORD_FORCE=1
  NO_COLOR=1                            plain output

Examples:
  # newest stable release
  curl -fsSL https://raw.githubusercontent.com/$DEFAULT_REPO/main/install.sh | sh
  # test a branch, no release needed
  curl -fsSL https://raw.githubusercontent.com/$DEFAULT_REPO/v4-dev/install.sh | sh -s -- --version v4-dev
  # a branch of a fork
  curl -fsSL https://raw.githubusercontent.com/$DEFAULT_REPO/main/install.sh | sh -s -- --repo you/Copycord --version my-fix
EOF
}

need_value() {
    # need_value FLAG VALUE
    case "${2:-}" in
        "" | -*) printf 'copycord install: %s needs a value (see --help)\n' "$1" >&2; exit 2 ;;
    esac
}

while [ $# -gt 0 ]; do
    case "$1" in
        --install-method | --method) need_value "$1" "${2:-}"; METHOD="$2"; METHOD_SET=1; shift ;;
        --install-method=* | --method=*) METHOD="${1#*=}"; METHOD_SET=1 ;;
        --git | --github) METHOD="git"; METHOD_SET=1 ;;
        --release) METHOD="release"; METHOD_SET=1 ;;
        --repo) need_value "$1" "${2:-}"; REPO="$2"; shift ;;
        --repo=*) REPO="${1#*=}" ;;
        --version) need_value "$1" "${2:-}"; VERSION="$2"; shift ;;
        --version=*) VERSION="${1#*=}" ;;
        --beta) CHANNEL="beta"; CHANNEL_SET=1 ;;
        --home) need_value "$1" "${2:-}"; COPYCORD_HOME="$2"; shift ;;
        --home=*) COPYCORD_HOME="${1#*=}" ;;
        --git-dir | --dir) need_value "$1" "${2:-}"; GIT_DIR_PATH="$2"; shift ;;
        --git-dir=* | --dir=*) GIT_DIR_PATH="${1#*=}" ;;
        --verify) VERIFY=1 ;;
        --dry-run) DRY_RUN=1 ;;
        --verbose) VERBOSE=1 ;;
        --force) FORCE=1 ;;
        --help | -h) SHOW_HELP=1 ;;
        *) printf 'copycord install: unknown option %s (see --help)\n' "$1" >&2; exit 2 ;;
    esac
    shift
done

if [ "$SHOW_HELP" = "1" ]; then
    usage
    exit 0
fi

case "$COPYCORD_HOME" in
    "~") COPYCORD_HOME="$HOME" ;;
    "~/"*) COPYCORD_HOME="$HOME/${COPYCORD_HOME#"~/"}" ;;
esac
case "$COPYCORD_HOME" in
    /*) ;;
    *) printf 'copycord install: --home must be an absolute path (got %s)\n' "$COPYCORD_HOME" >&2; exit 2 ;;
esac
COPYCORD_HOME="${COPYCORD_HOME%/}"
[ -n "$GIT_DIR_PATH" ] || GIT_DIR_PATH="$COPYCORD_HOME/src"
NODE_HOME="$COPYCORD_HOME/node"
HOME_POINTER="$HOME/.copycord-home"
export COPYCORD_HOME

# --- output -------------------------------------------------------------------

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ] && [ "${TERM:-}" != "dumb" ]; then
    ESC="$(printf '\033')"
    BOLD="$ESC[1m"; DIM="$ESC[2m"; RED="$ESC[31m"; GREEN="$ESC[32m"
    YELLOW="$ESC[33m"; CYAN="$ESC[36m"; ACCENT="$ESC[38;5;99m"; RESET="$ESC[0m"
    TTY=1
else
    BOLD=""; DIM=""; RED=""; GREEN=""; YELLOW=""; CYAN=""; ACCENT=""; RESET=""
    TTY=0
fi
# Windows Terminal shows OSC 9;4 progress on the tab while a step runs.
PROGRESS=0
if [ "$TTY" = "1" ] && [ -n "${WT_SESSION:-}" ]; then PROGRESS=1; fi
PROGRESS_ON=0
progress_off() {
    if [ "$PROGRESS_ON" = "1" ]; then
        printf '%s]9;4;0;0%s\\' "$ESC" "$ESC"
        PROGRESS_ON=0
    fi
}
# Clickable links (OSC 8) only in terminals known to show them; tmux and screen do not pass them on.
HYPERLINKS=0
if [ "$TTY" = "1" ] && [ -z "${TMUX:-}" ] && [ -z "${STY:-}" ]; then
    case "${TERM_PROGRAM:-}" in vscode | WezTerm | ghostty) HYPERLINKS=1 ;; esac
    if [ -n "${WT_SESSION:-}" ] || [ -n "${KITTY_WINDOW_ID:-}" ] || [ -n "${WEZTERM_PANE:-}" ]; then HYPERLINKS=1; fi
    case "${VTE_VERSION:-}" in *[!0-9]* | "") ;; *) [ "$VTE_VERSION" -ge 5000 ] && HYPERLINKS=1 ;; esac
    if [ "${TERM_PROGRAM:-}" = "iTerm.app" ]; then
        case "${TERM_PROGRAM_VERSION:-}" in 3.[1-9]* | [4-9].* | [1-9][0-9]*) HYPERLINKS=1 ;; esac
    fi
fi
case "${LC_ALL:-${LC_CTYPE:-${LANG:-}}}" in
    *UTF-8* | *utf-8* | *UTF8* | *utf8*)
        OK_MARK="✓"; FAIL_MARK="✗"; WARN_MARK="!"; DOT="•"
        SPIN_FRAMES="⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏"
        BOX_TL="╭"; BOX_TR="╮"; BOX_BL="╰"; BOX_BR="╯"; BOX_H="─"; BOX_V="│"
        ;;
    *)
        OK_MARK="+"; FAIL_MARK="x"; WARN_MARK="!"; DOT="-"
        SPIN_FRAMES="| / - \\"
        BOX_TL="+"; BOX_TR="+"; BOX_BL="+"; BOX_BR="+"; BOX_H="-"; BOX_V="|"
        ;;
esac

say() { printf '%s\n' "$*"; }
# A command inside a message, in the CLI's command color.
cmd() { printf '%s%s%s' "$CYAN" "$1" "$RESET"; }
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
    progress_off
    # A failed install starts the service it stopped again.
    if [ "${SERVICE_STOPPED:-0}" = "1" ]; then
        "$SERVICE_EXE" service start </dev/null >/dev/null 2>&1 || true
    fi
    rm -f "$LOG"
    [ -n "$TMP_DIR" ] && rm -rf "$TMP_DIR"
    return 0
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

run() {
    # run LABEL CMD...: output goes to $LOG; on failure print its tail and exit.
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
        if [ "$PROGRESS" = "1" ]; then
            printf '%s]9;4;3;0%s\\' "$ESC" "$ESC"
            PROGRESS_ON=1
        fi
        started=$(date +%s)
        spent=""
        while kill -0 "$pid" 2>/dev/null; do
            # The time spent shows from 3 seconds on.
            secs=$(($(date +%s) - started))
            if [ "$secs" -ge 60 ]; then
                spent="  $DIM$((secs / 60))m $(printf '%02d' $((secs % 60)))s$RESET"
            elif [ "$secs" -ge 3 ]; then
                spent="  $DIM${secs}s$RESET"
            fi
            for frame in $SPIN_FRAMES; do
                kill -0 "$pid" 2>/dev/null || break
                printf '\r  %s%s%s %s%s' "$ACCENT" "$frame" "$RESET" "$label" "$spent"
                sleep 0.1 2>/dev/null || sleep 1
            done
        done
        if wait "$pid"; then rc=0; else rc=$?; fi
        printf '\r%s' "$ESC[2K"
        progress_off
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
    # fetch URL; GITHUB_TOKEN, when set, goes to the GitHub API only (a higher rate limit).
    auth=""
    case "$1" in https://api.github.com/*) [ -n "${GITHUB_TOKEN:-}" ] && auth="Authorization: Bearer $GITHUB_TOKEN" ;; esac
    if command -v curl >/dev/null 2>&1; then
        if [ -n "$auth" ]; then
            curl -fsSL -H "Accept: application/vnd.github+json" -H "$auth" "$1"
        else
            curl -fsSL -H "Accept: application/vnd.github+json" "$1"
        fi
    elif command -v wget >/dev/null 2>&1; then
        if [ -n "$auth" ]; then
            wget -qO- --header="Accept: application/vnd.github+json" --header="$auth" "$1"
        else
            wget -qO- --header="Accept: application/vnd.github+json" "$1"
        fi
    else
        die "neither curl nor wget is installed"
    fi
}

newest_version() {
    # The highest version among the tag names on stdin: v4.0.0.2 > v4.0.0.1 > v4.0.0,
    # and older suffix tags still sort (v4.0.0 > v4.0.0rc1 > v4.0.0b2).
    awk '{
        v = $0; sub(/^v/, "", v)
        if (!match(v, /^[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?/)) next
        rest = substr(v, RLENGTH + 1)
        if (split(substr(v, 1, RLENGTH), p, ".") < 4) p[4] = 0
        rank = 3; n = 0
        if (rest != "") {
            rank = -1
            if (rest ~ /^[-.]?(a|alpha)/) rank = 0
            else if (rest ~ /^[-.]?(b|beta)/) rank = 1
            else if (rest ~ /^[-.]?(c|rc|pre|preview)/) rank = 2
            num = rest; gsub(/[^0-9]/, " ", num); split(num, q, " "); n = q[1] + 0
        }
        print p[1], p[2], p[3], p[4], rank, n, $0
    }' | sort -k1,1n -k2,2n -k3,3n -k4,4n -k5,5n -k6,6n | tail -n 1 | awk '{ print $7 }'
}

release_tag() {
    # The tag this run installs: --version, else the newest stable release
    # (/latest, which skips GitHub pre-releases), or for beta the highest version
    # of the non-draft releases, pre-releases included.
    if [ -n "$VERSION" ]; then
        printf 'v%s\n' "$VERSION"
        return 0
    fi
    if [ "$CHANNEL" = "beta" ]; then
        json="$(fetch "$API?per_page=30")" || return 1
        tag="$(printf '%s' "$json" | tr ',' '\n' | awk '
            /"tag_name"[[:space:]]*:/ { t = $0; sub(/.*"tag_name"[[:space:]]*:[[:space:]]*"/, "", t); sub(/".*/, "", t) }
            /"draft"[[:space:]]*:[[:space:]]*false/ { if (t != "") print t; t = "" }' | newest_version)"
    else
        json="$(fetch "$API/latest")" || return 1
        tag="$(printf '%s' "$json" | json_field tag_name)"
    fi
    [ -n "$tag" ] || return 1
    printf '%s\n' "$tag"
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
    # json_field KEY: first string value of KEY in the JSON on stdin
    tr ',' '\n' | sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -n 1
}

# --- checks -------------------------------------------------------------------

# Without an explicit method, a --version that looks like a release version
# means release, anything else a git ref.
INFERRED=""
if [ -z "$METHOD_SET" ] && [ -n "$VERSION" ]; then
    if printf '%s' "$VERSION" | grep -Eq '^v?[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?([-.]?(a|b|c|rc|alpha|beta|pre|preview|dev)[.-]?[0-9A-Za-z.]*)?$'; then
        METHOD="release"
    else
        METHOD="git"
    fi
    INFERRED=", inferred from --version"
fi

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
# A root login (a VPS with only root) installs for root. sudo from a normal account
# is refused: it would leave root-owned files and the install under the wrong user.
if [ "$(id -u)" = "0" ] && [ "${COPYCORD_ALLOW_ROOT:-}" != "1" ]; then
    if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
        if [ "$DRY_RUN" = "1" ]; then
            warn "running with sudo; a real install refuses unless COPYCORD_ALLOW_ROOT=1"
        else
            die "do not run this with sudo; run it as $SUDO_USER without sudo. Set COPYCORD_ALLOW_ROOT=1 to install for root anyway."
        fi
    else
        warn "running as root; Copycord installs for root"
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

# An upgrade or reinstall ends with `copycord doctor --fix`.
UPGRADE=0
if command -v copycord >/dev/null 2>&1 || [ -x "$HOME/.local/bin/copycord" ] \
    || [ -e "$COPYCORD_HOME/.env" ] || [ -e "$COPYCORD_HOME/data/data.db" ] \
    || [ -e "$COPYCORD_HOME/data.db" ]; then
    UPGRADE=1
fi

# --- already installed? ---------------------------------------------------------

installed_copycord() {
    # The copycord command on PATH or in a bin dir the installer uses.
    if command -v copycord >/dev/null 2>&1; then
        command -v copycord
        return 0
    fi
    for dir in "${UV_TOOL_BIN_DIR:-}" "${XDG_BIN_HOME:-}" "$HOME/.local/bin"; do
        if [ -n "$dir" ] && [ -x "$dir/copycord" ]; then
            printf '%s\n' "$dir/copycord"
            return 0
        fi
    done
    return 1
}

remote_head() {
    # remote_head REF: commit of a branch or tag of $REPO_URL (a tag peeled).
    command -v git >/dev/null 2>&1 || return 1
    heads="$(GIT_TERMINAL_PROMPT=0 git ls-remote "$REPO_URL.git" "refs/heads/$1" "refs/tags/$1" "refs/tags/$1^{}" 2>/dev/null)" || return 1
    printf '%s\n' "$heads" | awk -v r="$1" '
        $2 == "refs/tags/" r "^{}" { peeled = $1 }
        $2 == "refs/heads/" r { head = $1 }
        $2 == "refs/tags/" r { tag = $1 }
        END { s = head; if (s == "") s = peeled; if (s == "") s = tag; if (s == "") exit 1; print s }'
}

already_installed() {
    # Prints the installed label when it is what this run would install.
    # Any doubt (no network, unknown output) means install.
    exe="$(installed_copycord)" || return 1
    out="$(NO_COLOR=1 "$exe" --version </dev/null 2>/dev/null)" || return 1
    first="$(printf '%s\n' "$out" | sed -n 1p)"
    # "dev channel · v4-dev" on line 2; older versions print "channel dev (branch v4-dev)".
    chan="$(printf '%s\n' "$out" | sed -n 's/^channel //p' | head -n 1)"
    [ -n "$chan" ] || chan="$(printf '%s\n' "$out" | sed -n 2p)"
    have_version="${first%% *}"
    have_commit="$(printf '%s' "$first" | sed -n 's/^[^ ]* (\([0-9a-f]\{7,40\}\))$/\1/p')"
    [ -n "$have_version" ] && [ -n "$chan" ] || return 1
    if [ "$METHOD" = "git" ]; then
        [ -n "$have_commit" ] || return 1
        if printf '%s' "$REF" | grep -Eq '^[0-9a-f]{7,40}$'; then
            want="$REF"
        else
            # The installer saves this channel for a branch of the default repo.
            if [ "$REPO" = "$DEFAULT_REPO" ]; then
                case "$chan" in
                    "dev (branch $REF)" | "dev channel · $REF" | "dev channel - $REF") ;;
                    *) return 1 ;;
                esac
            fi
            want="$(remote_head "$REF")" || return 1
        fi
        case "$want" in "$have_commit"*) ;; *) return 1 ;; esac
        printf '%s (%s)\n' "$REF" "$(printf '%s' "$have_commit" | cut -c1-7)"
    else
        case "$chan" in stable | beta | "stable channel" | "beta channel") ;; *) return 1 ;; esac
        if [ -n "$VERSION" ]; then
            want="$VERSION"
        else
            [ "$CHANNEL_SET" != "1" ] || [ "$chan" = "$CHANNEL" ] || return 1
            want="$(release_tag 2>/dev/null)" || return 1
            want="${want#v}"
        fi
        [ -n "$want" ] && [ "${have_version#v}" = "$want" ] || return 1
        printf 'v%s\n' "$want"
    fi
}

if [ "$FORCE" != "1" ] && [ "$DRY_RUN" != "1" ]; then
    if HAVE="$(already_installed)"; then
        banner
        done_line "Copycord $HAVE is already installed"
        info "Run copycord update to update it, or rerun with --force to reinstall."
        say ""
        exit 0
    fi
fi

# --- plan ---------------------------------------------------------------------

# Only a dry run prints the plan.
banner
if [ "$DRY_RUN" = "1" ]; then
    say "  ${BOLD}Plan$RESET"
    if [ "$METHOD" = "release" ]; then
        plan_line "home" "$COPYCORD_HOME"
        plan_line "method" "release (GitHub release wheel$INFERRED)"
        plan_line "repo" "$REPO"
        if [ -n "$VERSION" ]; then
            plan_line "version" "v$VERSION"
        else
            plan_line "channel" "$CHANNEL (newest release)"
        fi
    else
        plan_line "home" "$COPYCORD_HOME"
        plan_line "method" "git (build from source, no release needed$INFERRED)"
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
    [ "$UPGRADE" = "1" ] && plan_line "repair" "copycord doctor --fix (an upgrade or reinstall)"
    [ "$VERIFY" = "1" ] && plan_line "verify" "copycord --version and copycord doctor"
    plan_line "dry run" "nothing will be changed"
fi

if [ "$METHOD" = "git" ]; then STEP_TOTAL=5; else STEP_TOTAL=4; fi
[ "$need_node" = "1" ] && STEP_TOTAL=$((STEP_TOTAL + 1))
if [ "$METHOD" = "git" ] && [ "$DRY_RUN" != "1" ] && ! command -v git >/dev/null 2>&1; then
    STEP_TOTAL=$((STEP_TOTAL + 1))
fi
[ "$UPGRADE" = "1" ] && STEP_TOTAL=$((STEP_TOTAL + 1))
[ "$VERIFY" = "1" ] && STEP_TOTAL=$((STEP_TOTAL + 1))

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
        dry "uv tool install --force --python $PYTHON_VERSION <copycord wheel of $LABEL>"
        return 0
    fi
    TAG="$(release_tag)" \
        || die "could not find the newest $CHANNEL release on $REPO_URL/releases (see the error above; if GitHub says 403, wait an hour or set GITHUB_TOKEN)"

    RELEASE_JSON="$(fetch "$API/tags/$TAG")" \
        || die "no release $TAG on $REPO_URL/releases. If $VERSION is a branch, tag or commit, use --version with that name, e.g. --version v4-dev"
    # Each asset lists its "digest" (sha256:...) before its browser_download_url.
    WHEEL="$(printf '%s' "$RELEASE_JSON" | tr ',' '\n' | awk '
        /"digest"[[:space:]]*:/ { d = $0; sub(/.*"sha256:/, "", d); sub(/".*/, "", d); if ($0 !~ /sha256:/) d = "" }
        /"browser_download_url"[[:space:]]*:.*\/copycord-[^"\/]*\.whl"/ {
            u = $0; sub(/.*"browser_download_url"[[:space:]]*:[[:space:]]*"/, "", u); sub(/".*/, "", u)
            print u " " d; exit
        }
        /"name"[[:space:]]*:/ { d = "" }')"
    WHEEL_URL="${WHEEL%% *}"
    WHEEL_SHA="${WHEEL#* }"
    [ -n "$WHEEL_URL" ] || die "release $TAG has no wheel asset (copycord-*.whl); see $REPO_URL/releases/tag/$TAG"
    done_line "Release $TAG"
    info "$WHEEL_URL"

    step "Installing Copycord"
    TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/copycord-wheel.XXXXXX")"
    WHEEL_FILE="$TMP_DIR/${WHEEL_URL##*/}"
    run "Downloading the wheel" download "$WHEEL_URL" "$WHEEL_FILE"
    if [ -n "$WHEEL_SHA" ]; then
        actual="$(sha256_of "$WHEEL_FILE")"
        [ "$actual" = "$WHEEL_SHA" ] \
            || die "the downloaded wheel does not match the checksum of release $TAG (expected $WHEEL_SHA, got $actual); nothing was installed"
        done_line "Checksum matches the release"
    else
        warn "release $TAG lists no checksum for the wheel; it was not checked"
    fi
    stop_service
    run "uv tool install copycord $TAG" uv tool install --force --python "$PYTHON_VERSION" "$WHEEL_FILE"
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
        die "need sha256sum or shasum to check a download"
    fi
}

install_node() {
    # The official tarball, checked against SHASUMS256.txt, unpacked into $NODE_HOME.
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
    # Check out $REF detached in $GIT_DIR_PATH. errexit does not apply inside
    # `run`, so every command checks its status.
    if [ -d "$GIT_DIR_PATH/.git" ]; then
        git -C "$GIT_DIR_PATH" remote set-url origin "$REPO_URL.git" || return 1
    else
        git clone --filter=blob:none --no-checkout "$REPO_URL.git" "$GIT_DIR_PATH" || return 1
    fi
    # Marks the checkout as the installer's own (see owned_checkout).
    : >"$GIT_DIR_PATH/.git/$GIT_MARKER" || return 1
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
    git -C "$GIT_DIR_PATH" clean -fdx -e src/copycord/admin/web/node_modules
}

GIT_MARKER="copycord-installer"

owned_checkout() {
    # A checkout the installer may reset and clean: one it created, or a clean
    # Copycord checkout whose origin is $REPO_URL (from an older installer).
    [ -f "$GIT_DIR_PATH/.git/$GIT_MARKER" ] && return 0
    origin="$(git -C "$GIT_DIR_PATH" remote get-url origin 2>/dev/null)" || return 1
    case "$origin" in "$REPO_URL" | "$REPO_URL.git" | "$REPO_URL/" | "$REPO_URL.git/") ;; *) return 1 ;; esac
    [ -f "$GIT_DIR_PATH/src/copycord/__init__.py" ] || return 1
    [ -z "$(git -C "$GIT_DIR_PATH" status --porcelain 2>/dev/null)" ]
}

build_frontend() {
    rm -rf "$GIT_DIR_PATH/src/copycord/admin/web/dist" || return 1
    (cd "$GIT_DIR_PATH/src/copycord/admin/web" && npm ci --no-audit --no-fund && npm run build)
}

git_hint() {
    # The package manager command for git; the installer never downloads git itself.
    if [ "$(uname -s)" = "Darwin" ]; then echo "xcode-select --install (or brew install git)"
    elif command -v apt-get >/dev/null 2>&1; then echo "sudo apt install git"
    elif command -v dnf >/dev/null 2>&1; then echo "sudo dnf install git"
    elif command -v yum >/dev/null 2>&1; then echo "sudo yum install git"
    elif command -v pacman >/dev/null 2>&1; then echo "sudo pacman -S git"
    elif command -v zypper >/dev/null 2>&1; then echo "sudo zypper install git"
    elif command -v apk >/dev/null 2>&1; then echo "sudo apk add git"
    else echo "your package manager"
    fi
}

git_can_prompt() {
    # A person at a terminal: stdin is one, or (curl ... | sh) stdout is and
    # /dev/tty opens, which is where sudo asks. Never in CI.
    case "${CI:-}" in "" | 0 | false | no) ;; *) return 1 ;; esac
    [ -t 0 ] && return 0
    [ -t 1 ] && (: </dev/tty) 2>/dev/null
}

git_pkg_run() {
    # git_pkg_run CMD...: one package manager command, as root.
    if [ -n "$GIT_SUDO" ]; then $GIT_SUDO "$@"; else "$@"; fi
}

git_pkg_install() {
    # Installs git with brew (macOS) or the Linux package manager, as root
    # through sudo when needed; sudo asks for the password on the terminal.
    if [ "$(uname -s)" = "Darwin" ]; then
        command -v brew >/dev/null 2>&1 \
            || die "git is not installed and Homebrew is not either; install git with $(git_hint) and run this again"
        run "git with brew" brew install git
        return 0
    fi
    if command -v apk >/dev/null 2>&1; then pkg=apk
    elif command -v apt-get >/dev/null 2>&1; then pkg=apt-get
    elif command -v pacman >/dev/null 2>&1; then pkg=pacman
    elif command -v dnf >/dev/null 2>&1; then pkg=dnf
    elif command -v yum >/dev/null 2>&1; then pkg=yum
    else die "git is not installed and no known package manager was found; install git with $(git_hint) and run this again"
    fi
    GIT_SUDO=""
    if [ "$(id -u)" != "0" ]; then
        command -v sudo >/dev/null 2>&1 \
            || die "git is not installed, and installing it with $pkg needs root, but sudo is not installed; install sudo or run this again as root"
        say "  Copycord needs administrator rights to install git with $pkg; sudo may ask for your password."
        sudo -v || die "sudo did not get administrator rights; install git with $(git_hint) and run this again"
        # Credentials are cached now; -n fails rather than prompting behind the spinner.
        GIT_SUDO="sudo -n"
    fi
    case "$pkg" in
        apk) run "git with apk" git_pkg_run apk add --no-cache git ;;
        apt-get)
            run "apt-get update" git_pkg_run env DEBIAN_FRONTEND=noninteractive apt-get update -qq
            run "git with apt-get" git_pkg_run env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git
            ;;
        pacman) run "git with pacman" git_pkg_run pacman -Sy --noconfirm git ;;
        dnf) run "git with dnf" git_pkg_run dnf install -y -q git ;;
        yum) run "git with yum" git_pkg_run yum install -y -q git ;;
    esac
}

install_git() {
    if ! command -v git >/dev/null 2>&1; then
        if [ "$DRY_RUN" = "1" ]; then
            dry "install git with $(git_hint)"
        elif git_can_prompt; then
            step "Installing git"
            git_pkg_install
            command -v git >/dev/null 2>&1 \
                || die "git was installed but is not on PATH; open a new terminal and run this again"
            done_line "installed $(git --version)"
        else
            die "git is not installed and the git method needs it; install it with $(git_hint) and run this again"
        fi
    fi
    if [ -e "$GIT_DIR_PATH" ] && [ ! -d "$GIT_DIR_PATH/.git" ] \
        && [ -n "$(ls -A "$GIT_DIR_PATH" 2>/dev/null)" ]; then
        die "$GIT_DIR_PATH exists and is not a git checkout; move it or pass --git-dir"
    fi
    # The install replaces every file in the checkout, so a folder the
    # installer did not make (your own clone, say) is never touched.
    if [ -e "$GIT_DIR_PATH/.git" ] && ! owned_checkout; then
        die "$GIT_DIR_PATH is a git folder the installer did not create, or it has changes. The install would overwrite its files, so nothing was changed. Pass --git-dir with an empty or new folder."
    fi

    if [ "$need_node" = "1" ]; then
        step "Installing Node $NODE_VERSION"
        node_name="$(node_platform)" || exit 1
        if [ -x "$NODE_HOME/$node_name/bin/npm" ]; then
            PATH="$NODE_HOME/$node_name/bin:$PATH"
            export PATH
            done_line "reusing $NODE_HOME/$node_name"
        elif [ "$DRY_RUN" = "1" ]; then
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
        dry "cd $GIT_DIR_PATH/src/copycord/admin/web && npm ci && npm run build"
    else
        run "npm ci && npm run build" build_frontend
    fi

    step "Installing Copycord"
    if [ "$DRY_RUN" = "1" ]; then
        dry "uv tool install --force --python $PYTHON_VERSION --reinstall-package copycord $GIT_DIR_PATH"
        LABEL="$REF"
    else
        stop_service
        # Otherwise uv reuses a cached build of this directory from an older ref.
        run "uv tool install copycord from $GIT_DIR_PATH" \
            uv tool install --force --python "$PYTHON_VERSION" --reinstall-package copycord "$GIT_DIR_PATH"
        LABEL="$REF ($SHA)"
    fi
}

# A running service is stopped while its files are replaced and started again
# after (see restart_service). `service status --json` exits 0 only when it runs.
SERVICE_STOPPED=0
stop_service() {
    [ "$UPGRADE" = "1" ] && [ "$DRY_RUN" != "1" ] || return 0
    exe="$(installed_copycord)" || return 0
    "$exe" service status --json </dev/null >/dev/null 2>&1 || return 0
    if "$exe" service stop </dev/null >"$LOG" 2>&1; then
        SERVICE_STOPPED=1
        SERVICE_EXE="$exe"
        done_line "Stopped the Copycord service for the install"
    else
        warn "could not stop the Copycord service; installing anyway"
    fi
}

restart_service() {
    [ "$SERVICE_STOPPED" = "1" ] || return 0
    SERVICE_STOPPED=0
    if "$BIN_DIR/copycord" service start </dev/null >"$LOG" 2>&1; then
        done_line "Started the Copycord service again"
    else
        warn "could not start the Copycord service; run $(cmd "copycord service start")"
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

# Save a custom home in ~/.copycord-home so the CLI and service find it.
if [ "$COPYCORD_HOME" != "$HOME/.copycord" ]; then
    if [ "$DRY_RUN" = "1" ]; then
        dry "save $COPYCORD_HOME in $HOME_POINTER"
    else
        mkdir -p "$COPYCORD_HOME" && printf '%s\n' "$COPYCORD_HOME" >"$HOME_POINTER" \
            || die "could not save the home in $HOME_POINTER"
        done_line "Saved the Copycord home in $HOME_POINTER"
    fi
elif [ -f "$HOME_POINTER" ] && [ "$DRY_RUN" != "1" ]; then
    rm -f "$HOME_POINTER"
fi

# Write <home>/.env once, never overwriting it. The installed CLI writes it
# (copycord config init), so the default settings live in one place.
env_value() {
    # env_value KEY [--show-secrets]: KEY from <home>/.env, empty when unset.
    NO_COLOR=1 "$BIN_DIR/copycord" config get "$@" </dev/null 2>/dev/null | tail -n 1
}

# A new file gets a random dashboard password, shown once in the box below.
ENV_FILE="$COPYCORD_HOME/.env"
NEW_PASSWORD=""
if [ -e "$ENV_FILE" ]; then
    :
elif [ "$DRY_RUN" = "1" ]; then
    dry "write the settings file $ENV_FILE with a random password (copycord config init)"
elif WROTE="$("$BIN_DIR/copycord" config init 2>>"$LOG" </dev/null)" && [ -n "$WROTE" ]; then
    ENV_FILE="$WROTE"
    done_line "Wrote the settings file $ENV_FILE"
    NEW_PASSWORD="$(env_value PASSWORD --show-secrets)"
else
    warn "could not write $ENV_FILE; $(cmd "copycord config set") creates it later"
fi

# The CLI writes update.json, so its format lives in one place.
CHANNEL_ARGS=""
if [ "$METHOD" = "release" ]; then
    if [ -z "$VERSION" ] && [ "$CHANNEL_SET" = "1" ]; then
        CHANNEL_ARGS="--channel $CHANNEL"
    fi
elif [ "$REPO" = "$DEFAULT_REPO" ]; then
    if [ "$DRY_RUN" = "1" ] || git -C "$GIT_DIR_PATH" rev-parse -q --verify "refs/remotes/origin/$REF" >/dev/null; then
        CHANNEL_ARGS="--channel dev --ref $REF"
    else
        info "$REF is not a branch: the $(cmd "copycord update") channel is left as it is"
    fi
else
    warn "$(cmd "copycord update") follows $DEFAULT_REPO, not $REPO; run this installer again to update from the fork"
fi
if [ -n "$CHANNEL_ARGS" ]; then
    if [ "$DRY_RUN" = "1" ]; then
        dry "copycord update $CHANNEL_ARGS --no-install"
    else
        # shellcheck disable=SC2086 # CHANNEL_ARGS is a word list on purpose
        run "Saving the update channel ($CHANNEL_ARGS)" "$BIN_DIR/copycord" update $CHANNEL_ARGS --no-install
    fi
fi

# --- repair -----------------------------------------------------------------------

# Piped, the doctor never prompts. Its exit status does not fail the install.
if [ "$UPGRADE" = "1" ]; then
    step "Repairing (copycord doctor --fix)"
    if [ "$DRY_RUN" = "1" ]; then
        dry "copycord doctor --fix --brief"
    else
        # Plain output, restyled with the installer's marks.
        NO_COLOR=1 "$BIN_DIR/copycord" doctor --fix --brief </dev/null 2>&1 | while IFS= read -r line; do
            case "$line" in
                "✓ "* | "+ "*) done_line "${line#* }" ;;
                "! "*) printf '  %s%s%s %s\n' "$YELLOW" "$WARN_MARK" "$RESET" "${line#! }" ;;
                "✗ "* | "x "*) printf '  %s%s%s %s\n' "$RED" "$FAIL_MARK" "$RESET" "${line#* }" ;;
                "PASS  "*) done_line "${line#PASS  }" ;;
                "WARN  "*) printf '  %s%s%s %s\n' "$YELLOW" "$WARN_MARK" "$RESET" "${line#WARN  }" ;;
                "FAIL  "*) printf '  %s%s%s %s\n' "$RED" "$FAIL_MARK" "$RESET" "${line#FAIL  }" ;;
                *) say "    $line" ;;
            esac
        done || true
    fi
fi
restart_service

# --- verify -----------------------------------------------------------------------

if [ "$VERIFY" = "1" ]; then
    step "Verifying"
    if [ "$DRY_RUN" = "1" ]; then
        dry "copycord --version && copycord doctor"
    else
        run "copycord --version" "$BIN_DIR/copycord" --version
        sed 's/^/    /' "$LOG"
        run "copycord doctor" "$BIN_DIR/copycord" doctor
        sed 's/^/    /' "$LOG"
    fi
fi

# --- done -------------------------------------------------------------------------

# The box rows are collected first so the box can be as wide as its longest row.
BOX_NL='
'
BOX_ROWS=""
box_add() { BOX_ROWS="$BOX_ROWS$1$2$BOX_NL"; }
box_line() { box_add L "$1"; }
box_item() { box_add I "$1"; }
box_link() { box_add K "$1"; }
# box_fill N CHAR: sets FILL to CHAR repeated N times.
box_fill() {
    FILL=""
    fill_n=$1
    while [ "$fill_n" -gt 0 ]; do FILL="$FILL$2"; fill_n=$((fill_n - 1)); done
}
box_rule() {
    box_fill $((BOX_W + 2)) "$BOX_H"
    printf '  %s%s%s%s%s\n' "$ACCENT" "$1" "$FILL" "$2" "$RESET"
}
# box_flush: draws the top rule, every collected row at the width of the longest, the bottom rule.
box_flush() {
    BOX_W=52
    while IFS= read -r row; do
        [ -n "$row" ] || continue
        text="${row#?}"
        case "$row" in
            I*) len=$((${#text} + 2)) ;;
            K*) len=$((${#text} + 4)) ;;
            *) len=${#text} ;;
        esac
        [ "$len" -le "$BOX_W" ] || BOX_W=$len
    done <<EOF_ROWS
$BOX_ROWS
EOF_ROWS
    box_rule "$BOX_TL" "$BOX_TR"
    while IFS= read -r row; do
        [ -n "$row" ] || continue
        text="${row#?}"
        mark=""
        case "$row" in
            I*) len=$((${#text} + 2)); mark="$YELLOW$BOLD!$RESET " ;;
            K*) len=$((${#text} + 4)); text="    $text" ;;
            *) len=${#text} ;;
        esac
        box_fill $((BOX_W - len)) " "
        body="$text$FILL"
        if [ "$HYPERLINKS" = "1" ] && [ "${row%"${row#?}"}" = "K" ]; then
            url="${row#?}"
            body="${text%%"$url"*}$ESC]8;;$url$ESC\\$url$ESC]8;;$ESC\\${text#*"$url"}$FILL"
        fi
        printf '  %s%s%s %s%s %s%s%s\n' "$ACCENT" "$BOX_V" "$RESET" "$mark" "$body" "$ACCENT" "$BOX_V" "$RESET"
    done <<EOF_ROWS
$BOX_ROWS
EOF_ROWS
    box_rule "$BOX_BL" "$BOX_BR"
}

say ""
if [ "$DRY_RUN" = "1" ]; then
    say "  ${YELLOW}${BOLD}Dry run complete:$RESET nothing was changed."
    say ""
    exit 0
fi
# The address from the settings file: this computer unless DASHBOARD_HOST names one.
PORT="$(env_value ADMIN_PORT)"
case "$PORT" in "" | *[!0-9]*) PORT="${ADMIN_PORT:-8080}" ;; esac
HOST="$(env_value DASHBOARD_HOST)"
case "$HOST" in "" | 0.0.0.0 | :: | "[::]" | 127.0.0.1 | localhost) HOST="localhost" ;; esac
case "$HOST" in *:*) case "$HOST" in \[*) ;; *) HOST="[$HOST]" ;; esac ;; esac
URL="http://$HOST:$PORT"
box_line "Copycord $LABEL is installed"
box_line "Home: $COPYCORD_HOME"
box_line "Settings: $ENV_FILE"
box_line ""
box_line "Next:"
box_line "  copycord run       start Copycord"
box_line "  copycord restart   apply a changed setting"
box_line "  copycord update    update it later"
box_line ""
box_line "Needs your attention:"
box_item "Finish the setup in the dashboard once it runs:"
box_link "$URL"
if [ -n "$NEW_PASSWORD" ]; then
    box_item "Your dashboard password (shown once):"
    box_line "    $NEW_PASSWORD"
    box_line "    saved as PASSWORD in the settings file"
elif grep -qx 'PASSWORD=copycord' "$ENV_FILE" 2>/dev/null; then
    box_item "Change the default dashboard password:"
    box_line "    edit PASSWORD in the settings file above,"
    box_line "    then run: copycord restart"
fi
box_flush
say ""
say "  ${DIM}Open a new shell first if copycord is not found.$RESET"
say ""
