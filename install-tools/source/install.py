from __future__ import annotations

import argparse
import json
import os
import sys
import subprocess
import shutil
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


def run(cmd: list[str], **kwargs) -> None:
    print(f"[installer] $ {' '.join(cmd)}")
    subprocess.check_call(cmd, **kwargs)


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

    print(f"[installer] [{bar}] {step}/{total} {label}...", end="", flush=True)

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
            "[installer] ERROR: pip command failed while "
            f"{label}.\n"
            f"Command was:\n"
            f"    {cmd_str}\n"
            "Please run that command manually to see the full error output, "
            "then fix the issue and re-run the installer."
        )

    print(" done")


def fetch_latest_tag(repo: str) -> str:
    """Query GitHub for the list of tags and return the first one."""
    api_url = f"https://api.github.com/repos/{repo}/tags"
    req = Request(api_url, headers={"User-Agent": "Copycord-Standalone-Installer"})
    print(f"[installer] Fetching latest tag from {api_url}")
    with urlopen(req) as resp:
        data = json.load(resp)

    if not data:
        raise SystemExit(
            "[installer] No tags found on GitHub; cannot determine latest version."
        )

    tag = data[0].get("name")
    if not tag:
        raise SystemExit("[installer] Unexpected tag payload from GitHub.")

    print(f"[installer] Latest tag: {tag}")
    return tag


def download_code(prefix: Path, ref: str, is_branch: bool = False) -> Path:
    """Download the tagged or branch Copycord archive into prefix/code.

    Preserves an existing code/.env file by backing it up before replacing
    code/ and restoring it afterwards. If the new archive also ships a .env,
    that file is renamed to .env.example so we don't lose it.
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
            print(f"[installer] Backed up existing .env from {existing_env_path}")
        except Exception as e:
            print(
                f"[installer] WARNING: Failed to read existing .env at "
                f"{existing_env_path}: {e}"
            )

    if code_dir.is_dir():
        print(f"[installer] Removing existing code/ at {code_dir}")
        shutil.rmtree(code_dir)

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    print(f"[installer] Downloading {label} from {archive_url}")
    with urlopen(archive_url) as resp:
        data = resp.read()
    zip_path.write_bytes(data)
    print(f"[installer] Saved archive to {zip_path}")

    tmp_dir.mkdir(parents=True, exist_ok=True)
    print(f"[installer] Extracting archive into {tmp_dir}")
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(tmp_dir)

    candidates = [p for p in tmp_dir.iterdir() if p.is_dir()]

    if not candidates:
        raise SystemExit(
            "[installer] Downloaded archive did not contain any directories."
        )

    repo_src_root = candidates[0]
    src_code_dir = repo_src_root / "code"

    if not src_code_dir.is_dir():
        raise SystemExit(
            f"[installer] Downloaded archive missing code/ (looked in {src_code_dir})."
        )

    print(f"[installer] Moving {src_code_dir} -> {code_dir}")
    shutil.move(str(src_code_dir), str(code_dir))

    if existing_env_content is not None:
        new_env_path = code_dir / ".env"
        if new_env_path.exists():

            example_path = code_dir / ".env.example"
            try:
                new_env_path.rename(example_path)
                print(
                    f"[installer] Renamed downloaded .env to {example_path} "
                    "to preserve user configuration."
                )
            except Exception as e:
                print(
                    f"[installer] WARNING: Failed to rename downloaded .env to "
                    f"{example_path}: {e}"
                )

        try:
            (code_dir / ".env").write_text(existing_env_content, encoding="utf-8")
            print("[installer] Restored existing .env into code/.")
        except Exception as e:
            print(
                f"[installer] WARNING: Failed to restore existing .env into code/: {e}"
            )

    version_file = code_dir / VERSION_FILE_NAME
    version_file.write_text(ref.strip() + "\n", encoding="utf-8")
    print(f"[installer] Recorded {label} in {version_file}")

    shutil.rmtree(tmp_dir, ignore_errors=True)
    try:
        zip_path.unlink()
    except FileNotFoundError:
        pass

    print(f"[installer] Code downloaded to {code_dir}")
    return code_dir


def detect_roots(prefix: Path) -> tuple[Path, Path]:
    """Decide whether to download by branch or tag and return repo/app roots."""
    repo_root = prefix.resolve()

    if GITHUB_BRANCH:
        print(f"[installer] Using branch: {GITHUB_BRANCH}")
        code_dir = download_code(repo_root, GITHUB_BRANCH, is_branch=True)
    else:
        tag = GITHUB_TAG or fetch_latest_tag(GITHUB_REPO)
        print(f"[installer] Using tag: {tag}")
        code_dir = download_code(repo_root, tag)

    return repo_root, code_dir


def build_frontend(app_root: Path) -> None:
    """Build the admin frontend using npm and copy built assets into admin/static/."""
    frontend_dir = app_root / "admin" / "frontend"
    package_json = frontend_dir / "package.json"

    if not package_json.is_file():
        print("[installer] No admin frontend package.json found; skipping npm build.")
        return

    npm = shutil.which("npm")
    if not npm:

        raise SystemExit(
            "[installer] ERROR: npm not found in PATH, but admin frontend requires it.\n"
            "Install Node.js (which includes npm) and re-run the installer."
        )

    print(f"[installer] Building admin frontend via npm in {frontend_dir}")
    run([npm, "ci"], cwd=str(frontend_dir))
    run([npm, "run", "build"], cwd=str(frontend_dir))

    dist_dir = frontend_dir / "dist"
    if not dist_dir.is_dir():
        print(f"[installer] WARNING: npm build did not produce dist/ at {dist_dir}.")
        return

    static_dir = app_root / "admin" / "static"
    static_dir.mkdir(parents=True, exist_ok=True)

    for item in dist_dir.iterdir():
        dest = static_dir / item.name
        if item.is_dir():
            shutil.copytree(item, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(item, dest)

    print(f"[installer] Copied built frontend to {static_dir}")


def create_venv(
    venv_dir: Path,
    requirements: Path,
    extra_packages: list[str] | None = None,
) -> None:
    """Create and install packages into a virtual environment, with quiet pip logs + progress lines."""
    if not requirements.is_file():
        raise SystemExit(f"Missing requirements file: {requirements}")

    print(f"\n[installer] Setting up venv: {venv_dir}")

    python_cmd = find_system_python()

    if venv_dir.exists():
        print(f"[installer] venv already exists: {venv_dir}")
    else:
        print(f"[installer] Creating venv at {venv_dir}")
        run(python_cmd + ["-m", "venv", str(venv_dir)])

    bin_dir = venv_dir / ("Scripts" if os.name == "nt" else "bin")

    candidates = ["python.exe", "python", "python3"]
    python_exe = next((bin_dir / n for n in candidates if (bin_dir / n).exists()), None)
    if not python_exe:
        raise SystemExit(f"Python executable not found in venv: {bin_dir}")

    try:
        run([str(python_exe), "-m", "ensurepip", "--upgrade"])
    except Exception as e:
        print(f"[installer] Warning: ensurepip failed ({e}), continuing…")

    total_steps = 2 + (1 if extra_packages else 0)
    step = 1

    run_pip_step(
        [str(python_exe), "-m", "pip", "install", "--upgrade", "pip"],
        step=step,
        total=total_steps,
        label="Upgrading pip",
    )
    step += 1

    run_pip_step(
        [str(python_exe), "-m", "pip", "install", "-r", str(requirements)],
        step=step,
        total=total_steps,
        label="Installing requirements",
    )
    step += 1

    if extra_packages:
        run_pip_step(
            [str(python_exe), "-m", "pip", "install", *extra_packages],
            step=step,
            total=total_steps,
            label="Installing extra packages",
        )


def ensure_env_file(app_root: Path, data_dir: Path, admin_port: int) -> Path:
    """Create a default .env next to the code/ folder if it doesn't exist."""
    env_path = app_root / ".env"
    if env_path.exists():
        print(f"[installer] .env already exists at {env_path}, leaving it alone.")
        return env_path

    content = f"""\
DATA_DIR={data_dir.as_posix()}
DB_PATH={(data_dir / 'data.db').as_posix()}


ADMIN_HOST=127.0.0.1
ADMIN_PORT={admin_port}


SERVER_WS_HOST=127.0.0.1
SERVER_WS_PORT=8765

CLIENT_WS_HOST=127.0.0.1
CLIENT_WS_PORT=8766

CONTROL_PORT_SERVER=9101
CONTROL_PORT_CLIENT=9102



ADMIN_WS_URL=ws://127.0.0.1:${{ADMIN_PORT}}/bus
WS_SERVER_URL=ws://127.0.0.1:${{SERVER_WS_PORT}}
WS_CLIENT_URL=ws://127.0.0.1:${{CLIENT_WS_PORT}}
WS_SERVER_CTRL_URL=ws://127.0.0.1:${{CONTROL_PORT_SERVER}}
WS_CLIENT_CTRL_URL=ws://127.0.0.1:${{CONTROL_PORT_CLIENT}}

PASSWORD=copycord 

BACKUP_DIR={(data_dir / 'backups').as_posix()}
BACKUP_RETAIN=14
BACKUP_AT=03:17
"""
    env_path.write_text(dedent(content).strip() + "\n", encoding="utf-8")
    print(f"[installer] Wrote default .env to {env_path}")
    return env_path


def write_start_scripts(repo_root: Path) -> None:
    """
    Always (re)write start scripts so installer and updater behave the same.
    - Windows:
        - copycord_windows.bat (spawns 3 PS windows)
        - scripts\preflight.ps1 (checks ALL ports + Python >= 3.10 in venvs)
        - scripts\admin.ps1 / server.ps1 / client.ps1
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

function Assert-Py310 {
  param([string]$Interpreter, [string]$Name)
  if (-not (Test-Path -LiteralPath $Interpreter)) {
    throw "Missing $Name interpreter at $Interpreter"
  }
  $ver = & $Interpreter -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"
  if (-not $ver) { throw "Unable to get Python version for $Name ($Interpreter)" }
  $parts = $ver.Split('.') | ForEach-Object { [int]$_ }
  if ($parts[0] -lt 3 -or ($parts[0] -eq 3 -and $parts[1] -lt 10)) {
    throw "$Name requires Python >= 3.10 (found $ver at $Interpreter)"
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

Write-Host '[preflight] Checking Python versions in venvs (need >= 3.10)…'
try {
  Assert-Py310 -Interpreter $venvAdmin -Name 'admin venv'
  Assert-Py310 -Interpreter $venvServer -Name 'server venv'
  Assert-Py310 -Interpreter $venvClient -Name 'client venv'
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

rem ---- Preflight: Python >= 3.10 in venvs + ALL ports free ----
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

ensure_py310 () {
  local bin="$1" name="$2"
  [[ -x "$bin" ]] || { echo "[preflight] ERROR: Missing $name interpreter at $bin"; exit 1; }
  local ver
  ver="$("$bin" -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")')" || {
    echo "[preflight] ERROR: Unable to get Python version for $name ($bin)"; exit 1; }
  local major="${ver%%.*}"; local rest="${ver#*.}"; local minor="${rest%%.*}"
  if (( major < 3 || (major == 3 && minor < 10) )); then
    echo "[preflight] ERROR: $name requires Python >= 3.10 (found $ver at $bin)"; exit 1;
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
ensure_py310 "$ADMIN_VENV/bin/python"  "admin venv"
ensure_py310 "$SERVER_VENV/bin/python" "server venv"
ensure_py310 "$CLIENT_VENV/bin/python" "client venv"
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


def _probe(cmd: list[str]) -> str | None:
    """Run a command and return its stdout (stripped), or None on failure."""
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return (out or "").strip()
    except Exception:
        return None


def check_prereqs() -> None:
    """
    Check that required tools are available and clearly print what's missing.

    Requirements:
      - Python 3.10+ (the interpreter running this script or the system Python we use)
      - pip OR ensurepip (for that Python)
      - npm (Node.js) for building the admin frontend
    """
    print("[installer] Checking prerequisites…")

    python_cmd = find_system_python()

    py_ver_str = _probe(
        python_cmd
        + [
            "-c",
            "import sys;print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')",
        ]
    )
    py_ok = False
    py_detail = "unknown"
    if py_ver_str:
        py_detail = py_ver_str
        try:
            major, minor, *_ = (int(x) for x in py_ver_str.split("."))
            py_ok = (major, minor) >= (3, 10)
        except Exception:
            py_ok = False

    pip_ver = _probe(python_cmd + ["-m", "pip", "--version"])
    ensurepip_ver = _probe(python_cmd + ["-m", "ensurepip", "--version"])
    pip_ok = bool(pip_ver or ensurepip_ver)

    npm_path = shutil.which("npm")
    npm_ver = _probe([npm_path, "--version"]) if npm_path else None
    npm_ok = bool(npm_ver)

    print("[installer] Detected:")
    print(f"  - Python: {py_detail} ({'OK' if py_ok else 'need >= 3.10'})")
    print(
        f"  - pip: {'found' if pip_ver else 'not found'}"
        f"{f' ({pip_ver})' if pip_ver else ''}"
    )
    print(
        f"  - ensurepip: {'found' if ensurepip_ver else 'not found'}"
        f"{f' ({ensurepip_ver})' if ensurepip_ver else ''}"
    )
    print(
        f"  - npm: {'found' if npm_ok else 'not found'}"
        f"{f' (v{npm_ver})' if npm_ver else ''}"
    )

    missing: list[str] = []
    if not py_ok:
        missing.append(f"Python 3.10+ (found {py_detail})")
    if not pip_ok:
        missing.append("pip or ensurepip for the detected Python")
    if not npm_ok:
        missing.append("npm (Node.js)")

    if missing:
        print("\n[installer] ERROR: Missing prerequisites:")
        for item in missing:
            print(f"  • {item}")
        print(
            "\nHow to fix:\n"
            "  - Install Python 3.10+ from https://www.python.org/downloads/ (ensure it’s on PATH).\n"
            "  - Ensure `pip` works for that Python (or install `ensurepip`).\n"
            "  - Install Node.js (which includes npm): https://nodejs.org/\n"
            "\nOnce installed, re-run:\n"
            "    python install_standalone.py\n"
        )
        raise SystemExit(1)

    print("[installer] Prerequisites OK.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install Copycord in standalone mode.")
    parser.add_argument(
        "--prefix", type=Path, help="Install prefix (default: current directory)"
    )
    parser.add_argument(
        "--admin-port", type=int, default=8080, help="Port for admin web UI"
    )
    args = parser.parse_args(argv)

    prefix = args.prefix or Path.cwd()

    check_prereqs()

    repo_root, app_root = detect_roots(prefix)

    print(f"[installer] Repo root: {repo_root}")
    print(f"[installer] App root:  {app_root}")

    build_frontend(app_root)

    data_dir = repo_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "backups").mkdir(exist_ok=True)
    print(f"[installer] Data dir:  {data_dir}")

    venv_root = repo_root / "venvs"
    venv_root.mkdir(exist_ok=True)

    create_venv(venv_root / "admin", app_root / "admin" / "requirements.txt")
    create_venv(
        venv_root / "server",
        app_root / "server" / "requirements.txt",
        extra_packages=["python-dotenv==1.1.1"],
    )
    create_venv(
        venv_root / "client",
        app_root / "client" / "requirements.txt",
        extra_packages=["python-dotenv==1.1.1"],
    )

    env_path = ensure_env_file(app_root, data_dir, args.admin_port)

    write_start_scripts(repo_root)

    print("\n[installer] Done.")
    print(
        f"  1) Change any environment settings like PASSWORD, PORTS, etc. in {env_path}."
    )
    print("  2) To run everything on Windows:")
    print("       double-click copycord_windows.bat")
    print("  3) To run everything on Linux/macOS:")
    print("       ./copycord_linux.sh")
    print("     (make sure it is executable: chmod +x copycord_linux.sh if needed)")

    return 0


def _pause(msg: str = "\n[installer] Press any key to close this window...") -> None:
    """Pause the process so the window doesn't disappear immediately."""
    try:
        if os.name == "nt":
            try:
                import msvcrt

                print(msg, end="", flush=True)
                msvcrt.getch()
                print()
                return
            except Exception:
                pass
        input(msg)
    except EOFError:

        pass


def _run_with_pause_installer() -> int:
    import traceback

    exit_code = 0
    try:
        exit_code = main()
    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else 1
    except Exception:
        print("\n[installer] Unexpected error:")
        traceback.print_exc()
        exit_code = 1

    is_frozen = bool(getattr(sys, "frozen", False))

    should_pause = is_frozen or (os.name == "nt" and (exit_code or exit_code is True))
    if should_pause:
        _pause()

    return int(exit_code or 0)


if __name__ == "__main__":
    raise SystemExit(_run_with_pause_installer())
