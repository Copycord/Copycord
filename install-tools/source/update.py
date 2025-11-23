from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from textwrap import dedent
from urllib.request import urlopen, Request


GITHUB_REPO = os.getenv("GITHUB_REPO", "Copycord/Copycord")
GITHUB_TAG = os.getenv("GITHUB_TAG")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH")


VERSION_FILE_NAME = ".version"


def find_system_python() -> list[str]:
    """
    Return a command list to invoke a real Python interpreter.

    - When running normally: use this interpreter (sys.executable)
    - When frozen in an .exe: try 'py -3', 'py', 'python', 'python3'
    """
    import sys
    import subprocess

    if not getattr(sys, "frozen", False):
        return [sys.executable]

    candidates = [
        ["py", "-3"],
        ["py"],
        ["python"],
        ["python3"],
    ]
    for cmd in candidates:
        try:
            subprocess.check_call(
                cmd + ["--version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return cmd
        except Exception:
            continue

    raise SystemExit(
        "[Copycord] ERROR: No suitable Python interpreter found on this system.\n"
        "Please install Python 3.10+ from https://www.python.org/downloads/ "
        "and then run this .exe again."
    )


def run(cmd: list[str], cwd: Path | None = None) -> str:
    print(f"[updater] $ {' '.join(cmd)}")
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        print(proc.stdout)
        raise SystemExit(f"Command failed with exit code {proc.returncode}")
    return proc.stdout


def run_pip_step(
    cmd: list[str],
    *,
    step: int,
    total: int,
    label: str,
    cwd: Path | None = None,
) -> None:
    """
    Run a pip-related command quietly and show a simple progress bar line instead
    of full pip logs.
    """
    bar_width = 24
    filled = int(bar_width * step / total)
    bar = "#" * filled + "-" * (bar_width - filled)

    print(f"[updater] [{bar}] {step}/{total} {label}...", end="", flush=True)

    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        text=True,
    )

    if proc.returncode != 0:
        print(" failed")
        cmd_str = " ".join(cmd)
        raise SystemExit(
            "[updater] ERROR: pip command failed while "
            f"{label}.\n"
            f"Command was:\n"
            f"    {cmd_str}\n"
            "Please run that command manually to see the full error output, "
            "then fix the issue and re-run the updater."
        )

    print(" done")


def detect_repo_root() -> Path:
    """
    Try to find the Copycord install root (the folder that contains `code/`).

    When frozen (PyInstaller .exe), __file__ points into a temp _MEI folder,
    so we must use sys.executable (the actual .exe path).

    When running as a normal script, we can safely use __file__.
    """
    import sys

    bases: list[Path] = []

    if getattr(sys, "frozen", False):
        bases.append(Path(sys.executable).resolve().parent)
    else:

        bases.append(Path(__file__).resolve().parent)

    bases.append(Path.cwd())

    checked: list[Path] = []
    for base in bases:
        if base in checked:
            continue
        checked.append(base)

        code_dir = base / "code"
        if code_dir.is_dir():
            return base

        parent_code_dir = base.parent / "code"
        if parent_code_dir.is_dir():
            return base.parent

    lines = ["Could not find `code/` directory; tried:"]
    for base in checked:
        lines.append(f"  {base / 'code'}")
        lines.append(f"  {base.parent / 'code'}")

    raise SystemExit("\n".join(lines))


def fetch_latest_tag(repo: str) -> str:
    """
    Query GitHub for the list of tags and return the first one,
    which is treated as the "latest" tag.
    """
    api_url = f"https://api.github.com/repos/{repo}/tags"
    req = Request(api_url, headers={"User-Agent": "Copycord-Standalone-Updater"})
    print(f"[updater] Fetching latest tag from {api_url}")
    with urlopen(req) as resp:
        data = json.load(resp)

    if not data:
        raise SystemExit(
            "[updater] No tags found on GitHub; cannot determine latest version."
        )

    tag = data[0].get("name")
    if not tag:
        raise SystemExit("[updater] Unexpected tag payload from GitHub.")

    print(f"[updater] Latest tag: {tag}")
    return tag


def read_local_ref(code_dir: Path) -> str | None:
    """
    Read the last-installed ref (tag or branch) from code/.version.
    """
    version_file = code_dir / VERSION_FILE_NAME
    if not version_file.is_file():
        return None
    try:
        return version_file.read_text(encoding="utf-8").strip() or None
    except Exception:
        return None


def download_code(prefix: Path, ref: str, *, is_branch: bool = False) -> Path:
    """
    Download the Copycord archive from GitHub into prefix/code, replacing any
    existing code/ directory, and update code/.version with the ref.

    Preserves an existing code/.env file by backing it up before replacing
    code/ and restoring it afterwards. If the new archive ships a .env, that
    file is renamed to .env.example so we don't lose it.

    If is_branch is True, we use refs/heads/<ref>.zip, otherwise refs/tags/<ref>.zip.
    """
    prefix = prefix.resolve()
    code_dir = prefix / "code"

    if is_branch:
        archive_url = f"https://github.com/{GITHUB_REPO}/archive/refs/heads/{ref}.zip"
        label = f"branch {ref}"
    else:
        archive_url = f"https://github.com/{GITHUB_REPO}/archive/refs/tags/{ref}.zip"
        label = f"tag {ref}"

    zip_path = prefix / f"copycord-{ref}.zip"
    tmp_dir = prefix / "_copycord_src"

    existing_env_content: str | None = None
    existing_env_path = code_dir / ".env"
    if existing_env_path.is_file():
        try:
            existing_env_content = existing_env_path.read_text(encoding="utf-8")
            print(f"[updater] Backed up existing .env from {existing_env_path}")
        except Exception as e:
            print(
                f"[updater] WARNING: Failed to read existing .env at "
                f"{existing_env_path}: {e}"
            )

    if code_dir.is_dir():
        print(f"[updater] Removing existing code/ at {code_dir}")
        shutil.rmtree(code_dir)

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    print(f"[updater] Downloading {label} from {archive_url}")
    with urlopen(archive_url) as resp:
        data = resp.read()
    zip_path.write_bytes(data)
    print(f"[updater] Saved archive to {zip_path}")

    tmp_dir.mkdir(parents=True, exist_ok=True)
    print(f"[updater] Extracting archive into {tmp_dir}")
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(tmp_dir)

    candidates = [p for p in tmp_dir.iterdir() if p.is_dir()]
    if not candidates:
        raise SystemExit(
            "[updater] Downloaded archive did not contain any directories; "
            "cannot locate repo root."
        )

    repo_src_root = candidates[0]
    src_code_dir = repo_src_root / "code"

    if not src_code_dir.is_dir():
        raise SystemExit(
            f"[updater] Downloaded archive does not contain a `code/` directory "
            f"(looked in {src_code_dir})."
        )

    print(f"[updater] Moving {src_code_dir} -> {code_dir}")
    shutil.move(str(src_code_dir), str(code_dir))

    if existing_env_content is not None:
        new_env_path = code_dir / ".env"
        if new_env_path.exists():
            example_path = code_dir / ".env.example"
            try:
                new_env_path.rename(example_path)
                print(
                    f"[updater] Renamed downloaded .env to {example_path} "
                    "to preserve user configuration."
                )
            except Exception as e:
                print(
                    f"[updater] WARNING: Failed to rename downloaded .env to "
                    f"{example_path}: {e}"
                )

        try:
            (code_dir / ".env").write_text(existing_env_content, encoding="utf-8")
            print("[updater] Restored existing .env into code/.")
        except Exception as e:
            print(f"[updater] WARNING: Failed to restore existing .env into code/: {e}")

    version_file = code_dir / VERSION_FILE_NAME
    version_file.write_text(ref.strip() + "\n", encoding="utf-8")
    print(f"[updater] Recorded {label} in {version_file}")

    shutil.rmtree(tmp_dir, ignore_errors=True)
    try:
        zip_path.unlink()
    except FileNotFoundError:
        pass

    print(f"[updater] Code downloaded to {code_dir}")
    return code_dir


def upgrade_venv(venv_dir: Path, requirements: Path) -> None:
    """Upgrade pip and requirements inside a venv with quiet logs + progress."""
    if not venv_dir.exists():
        print(f"[updater] venv missing (skipping): {venv_dir}")
        return

    bin_dir = venv_dir / ("Scripts" if os.name == "nt" else "bin")
    pip = bin_dir / "pip"
    if not pip.exists():
        print(f"[updater] pip missing in {venv_dir} (skipping)")
        return
    if not requirements.is_file():
        print(f"[updater] requirements not found: {requirements}")
        return

    print(f"\n[updater] Updating venv: {venv_dir}")

    total_steps = 2
    step = 1

    run_pip_step(
        [str(pip), "install", "--upgrade", "pip"],
        step=step,
        total=total_steps,
        label="Upgrading pip",
    )
    step += 1

    run_pip_step(
        [str(pip), "install", "-r", str(requirements)],
        step=step,
        total=total_steps,
        label="Installing requirements",
    )


def build_frontend(app_root: Path) -> None:
    """
    Build the admin frontend using npm and copy the built assets into admin/static/,
    mirroring what the Docker 'webbuild' stage does.
    """
    frontend_dir = app_root / "admin" / "frontend"
    package_json = frontend_dir / "package.json"

    if not package_json.is_file():
        print("[updater] No admin frontend package.json found; skipping npm build.")
        return

    npm = shutil.which("npm")
    if not npm:
        print(
            "[updater] WARNING: npm is not installed or not in PATH; skipping frontend build."
        )
        print(
            "           The admin UI may not reflect the latest changes until you build the "
            "frontend manually."
        )
        print(
            f"           To build manually later: cd {frontend_dir} && npm ci && npm run build"
        )
        return

    print(f"[updater] Rebuilding admin frontend via npm in {frontend_dir}")
    run([npm, "ci"], cwd=frontend_dir)
    run([npm, "run", "build"], cwd=frontend_dir)

    dist_dir = frontend_dir / "dist"
    if not dist_dir.is_dir():
        print(
            f"[updater] WARNING: npm build did not produce dist/ at {dist_dir}; "
            "leaving existing admin/static/ as-is."
        )
        return

    static_dir = app_root / "admin" / "static"
    static_dir.mkdir(parents=True, exist_ok=True)

    for item in dist_dir.iterdir():
        dest = static_dir / item.name
        if item.is_dir():
            shutil.copytree(item, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(item, dest)

    print(f"[updater] Copied built frontend to {static_dir}")


def write_start_scripts(repo_root: Path) -> None:
    """
    Always (re)write start scripts so installer and updater behave the same.
    - Windows:
        - copycord_windows.bat (spawns 3 PS windows)
        - scripts\preflight.ps1 (checks ALL ports + Python 3.11.x in venvs)
    - Linux/macOS:
        - copycord_linux.sh (LF, +x) with preflight
    """
    win_bat = repo_root / "copycord_windows.bat"
    ps_dir = repo_root / "scripts"
    ps_dir.mkdir(exist_ok=True)

    ps_header = "\r\n".join(
        [
            "$ErrorActionPreference = 'Stop'",
            "try { if ($PSVersionTable.PSVersion.Major -ge 7) { $PSStyle.OutputRendering = 'PlainText' } } catch {}",
            "[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)",
            "$here = Split-Path -Parent $MyInvocation.MyCommand.Path",
            "$root = Split-Path -Parent $here",
            "$code = Join-Path $root 'code'",
            "",
        ]
    )

    preflight_ps1 = ps_dir / "preflight.ps1"
    preflight_body = r"""
$envFile   = Join-Path $code '.env'
$venvAdmin = Join-Path $root 'venvs\admin\Scripts\python.exe'
$venvServer= Join-Path $root 'venvs\server\Scripts\python.exe'
$venvClient= Join-Path $root 'venvs\client\Scripts\python.exe'

function Assert-Py311 {
  param([string]$Interpreter, [string]$Name)
  if (-not (Test-Path -LiteralPath $Interpreter)) {
    throw "Missing $Name interpreter at $Interpreter"
  }
  $ver = & $Interpreter -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"
  if (-not $ver) { throw "Unable to get Python version for $Name ($Interpreter)" }
  $parts = $ver.Split('.') | ForEach-Object { [int]$_ }
  $major = $parts[0]
  $minor = $parts[1]
  if ($major -ne 3 -or $minor -ne 11) {
    throw "$Name requires Python 3.11.x (found $ver at $Interpreter)"
  }
}

function Get-EnvPorts {
  param([string]$Path)
  $ports = New-Object 'System.Collections.Generic.HashSet[int]'
  $defaults = @(8080,8765,8766,9101,9102)

  if (-not (Test-Path -LiteralPath $Path)) {
    foreach ($p in $defaults) { [void]$ports.Add([int]$p) }
    return @($ports)
  }

  $lines = Get-Content -LiteralPath $Path -Encoding UTF8
  foreach ($line in $lines) {
    # *_PORT=####
    if ($line -match '^[A-Z0-9_]+_PORT\s*=\s*([0-9]{1,5})\s*$') {
      $v = [int]$Matches[1]
      if ($v -ge 1 -and $v -le 65535) { [void]$ports.Add($v) }
    }
    # Only treat :#### as a port if it appears in a URL
    $matches = [System.Text.RegularExpressions.Regex]::Matches(
      $line, '(?i)\b(?:ws|wss|http|https)://[^:\s]+:(\d{2,5})\b'
    )
    foreach ($m in $matches) {
      $v = [int]$m.Groups[1].Value
      if ($v -ge 1 -and $v -le 65535) { [void]$ports.Add($v) }
    }
  }
  return @($ports)
}

function Test-PortBusy {
  param([int]$Port)
  try {
    $conn = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop
    if ($conn) { return $true }
  } catch {
    $net = netstat -ano | Select-String "LISTENING.*:$Port\b"
    if ($net) { return $true }
  }
  return $false
}

Write-Host '[preflight] Checking Python versions in venvs (need 3.11.x)…'
try {
  Assert-Py311 -Interpreter $venvAdmin -Name 'admin venv'
  Assert-Py311 -Interpreter $venvServer -Name 'server venv'
  Assert-Py311 -Interpreter $venvClient -Name 'client venv'
} catch {
  Write-Host ('[preflight] ERROR: ' + $_)
  exit 1
}
Write-Host '[preflight] Python looks good.'

$ports = @(Get-EnvPorts -Path $envFile)
if (-not $ports -or $ports.Count -eq 0) { $ports = @(8080,8765,8766,9101,9102) }

$busy = @()
foreach ($p in ($ports | Sort-Object -Unique)) {
  if (Test-PortBusy -Port $p) {
    $procId = $null; $pname = $null
    $line = netstat -ano | Select-String "LISTENING.*:$p\b" | Select-Object -First 1
    if ($line) {
      $parts = ($line -split '\s+') | Where-Object { $_ -ne '' }
      if ($parts.Count -ge 5) { $procId = $parts[-1] }
    }
    if ($procId) {
      try { $pname = (Get-Process -Id $procId -ErrorAction Stop).ProcessName } catch {}
      if ($pname) { $busy += ("Port {0} is in use by PID {1} ({2})" -f $p, $procId, $pname) }
      else { $busy += ("Port {0} is in use by PID {1}" -f $p, $procId) }
    } else {
      $busy += ("Port {0} is in use" -f $p)
    }
  }
}

if ($busy.Count -gt 0) {
  Write-Host '[preflight] One or more ports referenced in code\.env are busy:'
  $busy | ForEach-Object { Write-Host ("  • " + $_) }
  Write-Host 'Fix: close the process(es) using these ports or change values in code\.env, then relaunch.'
  exit 1
} else {
  Write-Host '[preflight] All referenced ports appear free.'
}
"""
    preflight_ps1.write_text(
        ps_header + preflight_body.replace("\n", "\r\n"), encoding="utf-8-sig"
    )

    # --- admin.ps1 (show URL) ---
    admin_ps1 = ps_dir / "admin.ps1"
    admin_ps1.write_text(
        ps_header
        + "\r\n".join(
            [
                "$venv = Join-Path $root 'venvs\\admin\\Scripts\\python.exe'",
                "$envPath = Join-Path $code '.env'",
                "$port = 8080",
                "$hostVal = 'localhost'",
                "if (Test-Path $envPath) {",
                "  $line = (Get-Content -LiteralPath $envPath -Encoding UTF8 | Where-Object { $_ -match '^ADMIN_PORT=' } | Select-Object -First 1)",
                "  if ($line) { $port = ($line -split '=',2)[1].Trim() }",
                "  $hline = (Get-Content -LiteralPath $envPath -Encoding UTF8 | Where-Object { $_ -match '^ADMIN_HOST=' } | Select-Object -First 1)",
                "  if ($hline) { $hostVal = ($hline -split '=',2)[1].Trim() }",
                "}",
                "$displayHost = if ($hostVal -eq '0.0.0.0') { 'localhost' } else { $hostVal }",
                "Set-Location -LiteralPath $code",
                "Write-Host ('[admin] Web UI Started: http://' + $displayHost + ':' + $port)",
                "try {",
                "  & $venv -m uvicorn admin.app:app --host 0.0.0.0 --port $port",
                '  if ($LASTEXITCODE) { throw "Exit code: $LASTEXITCODE" }',
                "} catch {",
                '  Write-Host ("[admin] crashed: $_")',
                "  Read-Host 'Press Enter to close'",
                "}",
                "",
            ]
        ),
        encoding="utf-8-sig",
    )

    # --- server.ps1 ---
    server_ps1 = ps_dir / "server.ps1"
    server_ps1.write_text(
        ps_header
        + "\r\n".join(
            [
                "$venv = Join-Path $root 'venvs\\server\\Scripts\\python.exe'",
                "Set-Location -LiteralPath $code",
                "$env:ROLE = 'server'",
                "$env:CONTROL_PORT = '9101'",
                "Write-Host '[server] starting…'",
                "& $venv -m control.control",
                "if ($LASTEXITCODE) { Write-Host ('[server] crashed with ' + $LASTEXITCODE); Read-Host 'Press Enter to close' }",
                "",
            ]
        ),
        encoding="utf-8-sig",
    )

    # --- client.ps1 ---
    client_ps1 = ps_dir / "client.ps1"
    client_ps1.write_text(
        ps_header
        + "\r\n".join(
            [
                "$venv = Join-Path $root 'venvs\\client\\Scripts\\python.exe'",
                "Set-Location -LiteralPath $code",
                "$env:ROLE = 'client'",
                "$env:CONTROL_PORT = '9102'",
                "Write-Host '[client] starting…'",
                "& $venv -m control.control",
                "if ($LASTEXITCODE) { Write-Host ('[client] crashed with ' + $LASTEXITCODE); Read-Host 'Press Enter to close' }",
                "",
            ]
        ),
        encoding="utf-8-sig",
    )

    # --- Windows .bat launcher ---
    win_bat.write_text(
        r"""
@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "CODE_DIR=%ROOT%\code"
set "VENV_ROOT=%ROOT%\venvs"

if not exist "%CODE_DIR%" (
  echo Error: code\ directory not found at "%CODE_DIR%"
  echo Make sure you ran: python install_standalone.py
  goto :EOF
)
if not exist "%VENV_ROOT%\admin\Scripts\python.exe" ( echo Error: admin venv missing & goto :EOF )
if not exist "%VENV_ROOT%\server\Scripts\python.exe" ( echo Error: server venv missing & goto :EOF )
if not exist "%VENV_ROOT%\client\Scripts\python.exe" ( echo Error: client venv missing & goto :EOF )

rem ---- Preflight: Python 3.11.x in venvs + ALL ports free ----
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%ROOT%\scripts\preflight.ps1"
if errorlevel 1 (
  echo.
  echo Preflight failed; fix the reported issues and try again.
  pause
  goto :EOF
)

set "PS=powershell.exe -NoLogo -NoProfile -NoExit -ExecutionPolicy Bypass -File"
start "Copycord Admin"  /D "%CODE_DIR%" %PS% "%ROOT%\scripts\admin.ps1"
start "Copycord Server" /D "%CODE_DIR%" %PS% "%ROOT%\scripts\server.ps1"
start "Copycord Client" /D "%CODE_DIR%" %PS% "%ROOT%\scripts\client.ps1"

echo.
echo Launched: Admin, Server, Client (each in its own PowerShell).
echo Close those windows to stop the services.
echo.
endlocal
""".lstrip(
            "\n"
        ),
        encoding="utf-8",
        newline="\r\n",
    )

    print(f"[installer] Wrote Windows start script: {win_bat}")
    print(f"[installer] Wrote PS launchers in: {ps_dir}")

    # --- Linux/macOS ---
    sh_path = repo_root / "copycord_linux.sh"
    sh_script = """#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
CODE_DIR="$ROOT/code"
VENV_ROOT="$ROOT/venvs"
ADMIN_VENV="$VENV_ROOT/admin"
SERVER_VENV="$VENV_ROOT/server"
CLIENT_VENV="$VENV_ROOT/client"
[[ -d "$CODE_DIR" ]] || { echo "Missing $CODE_DIR"; exit 1; }
[[ -d "$ADMIN_VENV" && -d "$SERVER_VENV" && -d "$CLIENT_VENV" ]] || { echo "Missing one or more venvs in $VENV_ROOT"; exit 1; }

ENV_FILE="$CODE_DIR/.env"

ensure_py311 () {
  local bin="$1" name="$2"
  [[ -x "$bin" ]] || { echo "[preflight] ERROR: Missing $name interpreter at $bin"; exit 1; }
  local ver
  ver="$("$bin" -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")')" || {
    echo "[preflight] ERROR: Unable to get Python version for $name ($bin)"; exit 1; }
  local major="${ver%%.*}"; local rest="${ver#*.}"; local minor="${rest%%.*}"
  if ! (( major == 3 && minor == 11 )); then
    echo "[preflight] ERROR: $name requires Python 3.11.x (found $ver at $bin)"; exit 1;
  fi
}

get_ports() {
  if [[ ! -f "$ENV_FILE" ]]; then
    echo 8080 8765 8766 9101 9102
    return
  fi
  awk -F'=' '/^[A-Z0-9_]+_PORT[[:space:]]*=/ {gsub(/[[:space:]]/,"",$2); if ($2 ~ /^[0-9]+$/) print $2}' "$ENV_FILE"
  grep -Eo ':[0-9]{2,5}' "$ENV_FILE" | sed 's/^://'
}

port_in_use() {
  local p="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -lnt | awk '{print $4}' | grep -qE "(:|\\.)$p$" && return 0 || return 1
  elif command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1 && return 0 || return 1
  else
    command -v netstat >/dev/null 2>&1 && netstat -lnt 2>/dev/null | awk '{print $4}' | grep -qE "(:|\\.)$p$" && return 0
    return 1
  fi
}

# --- Preflight: Python >= 3.10 in all venvs ---
ensure_py311 "$ADMIN_VENV/bin/python"  "admin venv"
ensure_py311 "$SERVER_VENV/bin/python" "server venv"
ensure_py311 "$CLIENT_VENV/bin/python" "client venv"
echo "[preflight] Python looks good."

# --- Preflight: ALL referenced ports free ---
mapfile -t PORTS < <(get_ports | awk '$1>=1 && $1<=65535 {print $1}' | sort -n | uniq)

BUSY=()
for p in "${PORTS[@]}"; do
  if port_in_use "$p"; then
    if command -v lsof >/dev/null 2>&1; then
      who=$(lsof -iTCP:"$p" -sTCP:LISTEN -nP 2>/dev/null | awk 'NR>1 {print $1"["$2"]"}' | sort -u | tr "\\n" " ")
      BUSY+=("Port $p is in use${who:+ by }${who}")
    else
      BUSY+=("Port $p is in use")
    fi
  fi
done

if [[ ${#BUSY[@]} -gt 0 ]]; then
  echo "[preflight] One or more ports referenced in code/.env are busy:"
  for m in "${BUSY[@]}"; do echo "  • $m"; done
  echo "Fix: close the process(es) using these ports or change values in code/.env, then relaunch."
  exit 1
fi

# --- Resolve admin host/port for message ---
ADMIN_PORT="8080"
if [[ -f "$ENV_FILE" ]]; then
  ENV_PORT="$(grep -E '^ADMIN_PORT=' "$ENV_FILE" | head -n1 | cut -d= -f2- | tr -d $'\\r' || true)"
  [[ -n "${ENV_PORT:-}" ]] && ADMIN_PORT="$ENV_PORT"
fi

ADMIN_HOST="localhost"
if [[ -f "$ENV_FILE" ]]; then
  ENV_HOST="$(grep -E '^ADMIN_HOST=' "$ENV_FILE" | head -n1 | cut -d= -f2- | tr -d $'\\r' || true)"
  [[ -n "${ENV_HOST:-}" ]] && ADMIN_HOST="$ENV_HOST"
fi
DISPLAY_HOST="$ADMIN_HOST"
[[ "$DISPLAY_HOST" == "0.0.0.0" ]] && DISPLAY_HOST="localhost"

echo "[admin] Web UI Started: http://$DISPLAY_HOST:$ADMIN_PORT"

cd "$CODE_DIR"
"$ADMIN_VENV/bin/python" -m uvicorn admin.app:app --host 0.0.0.0 --port "$ADMIN_PORT" & ADMIN_PID=$!
ROLE=server CONTROL_PORT=9101 "$SERVER_VENV/bin/python" -m control.control & SERVER_PID=$!
ROLE=client CONTROL_PORT=9102 "$CLIENT_VENV/bin/python" -m control.control & CLIENT_PID=$!
trap 'kill "$ADMIN_PID" "$SERVER_PID" "$CLIENT_PID" 2>/dev/null || true; wait || true' INT TERM
wait
"""
    sh_path.write_text(sh_script, encoding="utf-8")
    try:
        sh_path.chmod(sh_path.stat().st_mode | 0o111)
    except Exception:
        pass
    print(f"[installer] Wrote Linux/macOS start script: {sh_path}")


def main(argv: list[str] | None = None) -> int:
    repo_root = detect_repo_root()
    code_dir = repo_root / "code"
    print(f"[updater] Repo root: {repo_root}")
    print(f"[updater] Code dir:  {code_dir}")

    current_ref = read_local_ref(code_dir)
    print(f"[updater] Currently installed ref: {current_ref or 'none'}")

    if GITHUB_BRANCH:
        target_ref = GITHUB_BRANCH
        print(f"[updater] GITHUB_BRANCH is set; updating from branch: {target_ref}")
        print(
            "[updater] Note: for branches we always download the latest archive, "
            "since there is no simple way to detect 'no changes' via the .version file."
        )
        download_code(repo_root, target_ref, is_branch=True)

        app_root = repo_root / "code"

        venv_root = repo_root / "venvs"
        print("\n[updater] Updating virtualenv dependencies…")
        upgrade_venv(venv_root / "admin", app_root / "admin" / "requirements.txt")
        upgrade_venv(venv_root / "server", app_root / "server" / "requirements.txt")
        upgrade_venv(venv_root / "client", app_root / "client" / "requirements.txt")

        print("\n[updater] Rebuilding admin frontend…")
        build_frontend(app_root)

        write_start_scripts(repo_root)

        print("\n[updater] Done. Restart Copycord to run the updated build.")
        return 0

    target_tag = GITHUB_TAG or fetch_latest_tag(GITHUB_REPO)
    print(f"[updater] Target tag: {target_tag}")

    write_start_scripts(repo_root)

    if current_ref == target_tag:
        print("[updater] Already on the latest tag; nothing to do.")

        return 2

    print(
        f"[updater] Tag mismatch -> updating code from "
        f"{current_ref or 'none'} to {target_tag}"
    )
    download_code(repo_root, target_tag, is_branch=False)

    app_root = repo_root / "code"

    venv_root = repo_root / "venvs"
    print("\n[updater] Updating virtualenv dependencies…")
    upgrade_venv(venv_root / "admin", app_root / "admin" / "requirements.txt")
    upgrade_venv(venv_root / "server", app_root / "server" / "requirements.txt")
    upgrade_venv(venv_root / "client", app_root / "client" / "requirements.txt")

    print("\n[updater] Rebuilding admin frontend…")
    build_frontend(app_root)

    print("\n[updater] Done. Restart Copycord to run the updated version.")
    return 0


def _run_with_pause_updater() -> int:
    import traceback

    exit_code = 0
    sys_exit_message: str | None = None

    try:
        exit_code = main()
    except SystemExit as e:

        if isinstance(e.code, int):
            exit_code = e.code
        else:
            sys_exit_message = str(e.code)
            exit_code = 1
    except Exception:
        print("\n[updater] Unexpected error:")
        traceback.print_exc()
        exit_code = 1

    if exit_code == 0:
        print("\n[updater] Update complete. You are now running the latest version.")
    elif exit_code == 2:
        print("\n[updater] Copycord is already up to date. No changes were made.")
    else:
        if sys_exit_message:
            print(f"\n[updater] {sys_exit_message}")
        print("\n[updater] Finished with errors. Please review the messages above.")

    if os.name == "nt" and getattr(sys, "frozen", False):
        try:
            input("\nPress Enter to close this window...")
        except EOFError:
            pass

    return exit_code


if __name__ == "__main__":
    raise SystemExit(_run_with_pause_updater())
