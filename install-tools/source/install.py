from __future__ import annotations

import argparse
import json
import os
import sys
import subprocess
import shutil
import zipfile
import time
from pathlib import Path
from textwrap import dedent
from urllib.request import urlopen, Request


GITHUB_REPO = os.getenv("GITHUB_REPO", "Copycord/Copycord")
GITHUB_TAG = os.getenv("GITHUB_TAG")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH")

VERSION_FILE_NAME = ".version"

QUIET = True

BAR_WIDTH = 40
_progress_percent = 0
_current_status = ""
_last_render_line = ""
_last_render_len = 0

MAX_STATUS = 50


def _render_progress() -> None:
    """Render the single-line progress bar in quiet mode."""
    global _last_render_line, _last_render_len
    if not QUIET:
        return

    percent = max(0, min(_progress_percent, 100))
    filled = int(BAR_WIDTH * percent / 100)
    bar = "#" * filled + "-" * (BAR_WIDTH - filled)

    status = _current_status or ""

    if len(status) > MAX_STATUS:
        status = "…" + status[-(MAX_STATUS - 1) :]

    core = f"[installer] [{bar}] {percent:3d}%  {status:<{MAX_STATUS}}"

    padding = " " * max(0, _last_render_len - len(core))

    line = "\r" + core + padding

    if line != _last_render_line:
        _last_render_line = line
        _last_render_len = len(core)
        print(line, end="", flush=True)


def _advance_one(status: str) -> None:
    """Advance the global progress bar by 1% (up to 100)."""
    global _progress_percent, _current_status
    if not QUIET:
        return
    if _progress_percent >= 100:
        return
    _progress_percent += 1
    _current_status = status
    _render_progress()


class StageProgress:
    """Limit how many 1% ticks a given stage can consume."""

    def __init__(self, max_ticks: int):
        self.max_ticks = max_ticks
        self.used = 0

    def tick(self, status: str) -> None:
        if self.used >= self.max_ticks:
            return
        self.used += 1
        _advance_one(status)


STAGE_PROGRESS: dict[str, StageProgress] = {
    "prereqs": StageProgress(5),
    "download": StageProgress(25),
    "frontend": StageProgress(15),
    "data": StageProgress(5),
    "venv_admin": StageProgress(15),
    "venv_server": StageProgress(10),
    "venv_client": StageProgress(10),
    "env_scripts": StageProgress(15),
}


def info(msg: str) -> None:
    """Info / progress logs (hidden when QUIET=True)."""
    if not QUIET:
        print(msg)


def error(msg: str) -> None:
    """Errors always show."""
    print(msg, file=sys.stderr)


def show_final_bar() -> None:
    """Legacy helper (no longer used in the main flow)."""
    bar_width = 30
    for pct in range(1, 101):
        filled = int(bar_width * pct / 100)
        bar = "#" * filled + "-" * (bar_width - filled)
        print(f"\r[installer] [{bar}] {pct:3d}%", end="", flush=True)
        time.sleep(0.01)
    print()


def find_system_python() -> list[str]:
    """
    Return a command list to invoke a real Python interpreter.

    - When running normally: use this interpreter (sys.executable)
    - When frozen in an .exe: try 'py -3', 'py', 'python', 'python3'
    """
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
        "Please install Python 3.11.x (64-bit) from https://www.python.org/downloads/ "
        "and then run this .exe again."
    )


def run(cmd: list[str], **kwargs) -> None:
    """
    Wrapper for subprocess.check_call.

    - When QUIET=False: echo the command and inherit stdout/stderr.
    - When QUIET=True: silence stdout/stderr unless the caller overrides.
    """
    if not QUIET:
        info(f"[installer] $ {' '.join(cmd)}")
        subprocess.check_call(cmd, **kwargs)
        return

    quiet_kwargs = dict(kwargs)
    quiet_kwargs.setdefault("stdout", subprocess.DEVNULL)
    quiet_kwargs.setdefault("stderr", subprocess.STDOUT)
    subprocess.check_call(cmd, **quiet_kwargs)


def run_pip_step(
    cmd: list[str],
    *,
    step: int,
    total: int,
    label: str,
    cwd: Path | None = None,
) -> None:
    """
    Run a pip-related command.

    - QUIET=False  → let pip show its own output + a simple step label.
    - QUIET=True   → hide pip output completely (global bar handles UX).
    """
    if not QUIET:
        print(f"[installer] ({step}/{total}) {label}…")
        proc = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
        )
        if proc.returncode != 0:
            print("  -> failed")
            cmd_str = " ".join(cmd)
            raise SystemExit(
                "[installer] ERROR: pip command failed while "
                f"{label}.\n"
                f"Command was:\n"
                f"    {cmd_str}\n"
            )
        print("  -> done")
        return

    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        text=True,
    )

    if proc.returncode != 0:
        cmd_str = " ".join(cmd)
        error(
            "[installer] ERROR: pip command failed while "
            f"{label}.\n"
            f"Command was:\n"
            f"    {cmd_str}\n"
            "Please run that command manually to see the full error output, "
            "then fix the issue and re-run the installer."
        )
        raise SystemExit(1)


def fetch_latest_tag(repo: str) -> str:
    """Query GitHub for the list of tags and return the first one."""
    api_url = f"https://api.github.com/repos/{repo}/tags"
    req = Request(api_url, headers={"User-Agent": "Copycord-Standalone-Installer"})
    info(f"[installer] Fetching latest tag from {api_url}")
    with urlopen(req) as resp:
        data = json.load(resp)

    if not data:
        raise SystemExit(
            "[installer] No tags found on GitHub; cannot determine latest version."
        )

    tag = data[0].get("name")
    if not tag:
        raise SystemExit("[installer] Unexpected tag payload from GitHub.")

    info(f"[installer] Latest tag: {tag}")
    return tag


def download_code(prefix: Path, ref: str, is_branch: bool = False) -> Path:
    """Download the tagged or branch Copycord archive into prefix/code.

    Preserves an existing code/.env file by backing it up before replacing
    code/ and restoring it afterwards. If the new archive also ships a .env,
    that file is renamed to .env.example so we don't lose it.
    """
    prefix = prefix.resolve()
    code_dir = prefix / "code"
    stage = STAGE_PROGRESS.get("download")

    if is_branch:
        archive_url = f"https://github.com/{GITHUB_REPO}/archive/refs/heads/{ref}.zip"
        label = f"branch {ref}"
    else:
        archive_url = f"https://github.com/{GITHUB_REPO}/archive/refs/tags/{ref}.zip"
        label = f"tag {ref}"

    zip_path = prefix / f"copycord-{ref}.zip"
    tmp_dir = prefix / "_copycord_src"

    if stage:
        stage.tick(f"Preparing download ({label})…")

    existing_env_content: str | None = None
    existing_env_path = code_dir / ".env"
    if existing_env_path.is_file():
        try:
            existing_env_content = existing_env_path.read_text(encoding="utf-8")
            info(f"[installer] Backed up existing .env from {existing_env_path}")
        except Exception as e:
            error(
                f"[installer] WARNING: Failed to read existing .env at "
                f"{existing_env_path}: {e}"
            )

    if code_dir.is_dir():
        info(f"[installer] Removing existing code/ at {code_dir}")
        shutil.rmtree(code_dir)

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    info(f"[installer] Downloading {label} from {archive_url}")

    with urlopen(archive_url) as resp, open(zip_path, "wb") as f:
        chunk_size = 64 * 1024
        chunk_count = 0
        while True:
            chunk = resp.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)
            chunk_count += 1
            if stage and chunk_count % 5 == 0:
                stage.tick("Downloading archive…")

    info(f"[installer] Saved archive to {zip_path}")

    tmp_dir.mkdir(parents=True, exist_ok=True)
    info(f"[installer] Extracting archive into {tmp_dir}")
    with zipfile.ZipFile(zip_path) as z:
        members = z.infolist()
        total_members = len(members) or 1
        step = max(1, total_members // 10)
        for idx, m in enumerate(members, start=1):
            z.extract(m, tmp_dir)
            if stage and idx % step == 0:
                stage.tick("Extracting files…")

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

    info(f"[installer] Moving {src_code_dir} -> {code_dir}")
    shutil.move(str(src_code_dir), str(code_dir))
    if stage:
        stage.tick("Copying code into place…")

    if existing_env_content is not None:
        new_env_path = code_dir / ".env"
        if new_env_path.exists():
            example_path = code_dir / ".env.example"
            try:
                new_env_path.rename(example_path)
                info(
                    f"[installer] Renamed downloaded .env to {example_path} "
                    "to preserve user configuration."
                )
            except Exception as e:
                error(
                    f"[installer] WARNING: Failed to rename downloaded .env to "
                    f"{example_path}: {e}"
                )

        try:
            (code_dir / ".env").write_text(existing_env_content, encoding="utf-8")
            info("[installer] Restored existing .env into code/.")
        except Exception as e:
            error(
                f"[installer] WARNING: Failed to restore existing .env into code/: {e}"
            )

    version_file = code_dir / VERSION_FILE_NAME
    version_file.write_text(ref.strip() + "\n", encoding="utf-8")
    info(f"[installer] Recorded {label} in {version_file}")

    shutil.rmtree(tmp_dir, ignore_errors=True)
    try:
        zip_path.unlink()
    except FileNotFoundError:
        pass

    if stage:
        stage.tick("Download and extraction complete.")

    info(f"[installer] Code downloaded to {code_dir}")
    return code_dir


def detect_roots(prefix: Path) -> tuple[Path, Path]:
    """Decide whether to download by branch or tag and return repo/app roots."""
    repo_root = prefix.resolve()

    if GITHUB_BRANCH:
        info(f"[installer] Using branch: {GITHUB_BRANCH}")
        code_dir = download_code(repo_root, GITHUB_BRANCH, is_branch=True)
    else:
        tag = GITHUB_TAG or fetch_latest_tag(GITHUB_REPO)
        info(f"[installer] Using tag: {tag}")
        code_dir = download_code(repo_root, tag)

    return repo_root, code_dir


def build_frontend(app_root: Path) -> None:
    """Build the admin frontend using npm and copy built assets into admin/static/."""
    frontend_dir = app_root / "admin" / "frontend"
    package_json = frontend_dir / "package.json"
    stage = STAGE_PROGRESS.get("frontend")

    if not package_json.is_file():
        info("[installer] No admin frontend package.json found; skipping npm build.")
        if stage:
            stage.tick("No admin frontend to build.")
        return

    npm = shutil.which("npm")
    if not npm:
        raise SystemExit(
            "[installer] ERROR: npm not found in PATH, but admin frontend requires it.\n"
            "Install Node.js (which includes npm) and re-run the installer."
        )

    info(f"[installer] Building admin frontend via npm in {frontend_dir}")
    if stage:
        stage.tick("Installing frontend dependencies…")
    run([npm, "ci"], cwd=str(frontend_dir))
    if stage:
        stage.tick("Building admin UI…")
    run([npm, "run", "build"], cwd=str(frontend_dir))

    dist_dir = frontend_dir / "dist"
    if not dist_dir.is_dir():
        error(f"[installer] WARNING: npm build did not produce dist/ at {dist_dir}.")
        return

    static_dir = app_root / "admin" / "static"
    static_dir.mkdir(parents=True, exist_ok=True)

    items = list(dist_dir.iterdir())
    total_items = len(items) or 1
    step = max(1, total_items // 10)
    for idx, item in enumerate(items, start=1):
        dest = static_dir / item.name
        if item.is_dir():
            shutil.copytree(item, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(item, dest)
        if stage and idx % step == 0:
            stage.tick("Copying frontend assets…")

    if stage:
        stage.tick("Frontend build finished.")
    info(f"[installer] Copied built frontend to {static_dir}")


def create_venv(
    venv_dir: Path,
    requirements: Path,
    extra_packages: list[str] | None = None,
    stage_name: str | None = None,
) -> None:
    """Create and install packages into a virtual environment."""
    if not requirements.is_file():
        raise SystemExit(f"Missing requirements file: {requirements}")

    stage = STAGE_PROGRESS.get(stage_name) if stage_name else None

    info(f"\n[installer] Setting up venv: {venv_dir}")

    python_cmd = find_system_python()

    if venv_dir.exists():
        info(f"[installer] venv already exists: {venv_dir}")
    else:
        info(f"[installer] Creating venv at {venv_dir}")
        if stage:
            stage.tick(f"Creating venv at {venv_dir}…")
        run(python_cmd + ["-m", "venv", str(venv_dir)])

    bin_dir = venv_dir / ("Scripts" if os.name == "nt" else "bin")

    candidates = ["python.exe", "python", "python3"]
    python_exe = next((bin_dir / n for n in candidates if (bin_dir / n).exists()), None)
    if not python_exe:
        raise SystemExit(f"Python executable not found in venv: {bin_dir}")

    try:
        run([str(python_exe), "-m", "ensurepip", "--upgrade"])
        if stage:
            stage.tick("Ensuring pip in venv…")
    except Exception as e:
        error(f"[installer] Warning: ensurepip failed ({e}), continuing…")

    total_steps = 2 + (1 if extra_packages else 0)
    step_no = 1

    if stage:
        stage.tick("Upgrading pip in venv…")
    run_pip_step(
        [str(python_exe), "-m", "pip", "install", "--upgrade", "pip"],
        step=step_no,
        total=total_steps,
        label="Upgrading pip",
    )
    step_no += 1

    if stage:
        stage.tick("Installing core requirements…")
    run_pip_step(
        [str(python_exe), "-m", "pip", "install", "-r", str(requirements)],
        step=step_no,
        total=total_steps,
        label="Installing requirements",
    )
    step_no += 1

    if extra_packages:
        if stage:
            stage.tick("Installing extra packages…")
        run_pip_step(
            [str(python_exe), "-m", "pip", "install", *extra_packages],
            step=step_no,
            total=total_steps,
            label="Installing extra packages",
        )

    if stage:
        stage.tick("Virtual environment ready.")


def ensure_env_file(app_root: Path, data_dir: Path, admin_port: int) -> Path:
    """Create a default .env inside the code/ folder if it doesn't exist."""
    env_path = app_root / ".env"
    stage = STAGE_PROGRESS.get("env_scripts")

    if env_path.exists():
        info(f"[installer] .env already exists at {env_path}, leaving it alone.")
        if stage:
            stage.tick(".env already present.")
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
    info(f"[installer] Wrote default .env to {env_path}")
    if stage:
        stage.tick("Wrote default .env configuration.")
    return env_path


def write_start_scripts(repo_root: Path) -> None:
    """
    Always (re)write start scripts so installer and updater behave the same.
    - Windows:
        - copycord_windows.bat (spawns 3 PS windows)
        - scripts\preflight.ps1 (checks ALL ports + Python 3.11.x in venvs)
    - Linux/macOS:
        - copycord_linux.sh (LF, +x) with preflight
    """
    stage = STAGE_PROGRESS.get("env_scripts")

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
    
    if ($line -match '^[A-Z0-9_]+_PORT\s*=\s*([0-9]{1,5})\s*$') {
      $v = [int]$Matches[1]
      if ($v -ge 1 -and $v -le 65535) { [void]$ports.Add($v) }
    }
    
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

    info(f"[installer] Wrote Windows start script: {win_bat}")
    info(f"[installer] Wrote PS launchers in: {ps_dir}")

    sh_path = repo_root / "copycord_linux.sh"
    sh_script = """
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

ensure_py311 "$ADMIN_VENV/bin/python"  "admin venv"
ensure_py311 "$SERVER_VENV/bin/python" "server venv"
ensure_py311 "$CLIENT_VENV/bin/python" "client venv"
echo "[preflight] Python looks good."

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

if (( ${
  echo "[preflight] One or more ports referenced in code/.env are busy:"
  for m in "${BUSY[@]}"; do
    echo "  • $m"
  done
  echo "Fix: close the process(es) using these ports or change values in code/.env, then relaunch."
  exit 1
else
  echo "[preflight] All referenced ports appear free."
fi

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
    info(f"[installer] Wrote Linux/macOS start script: {sh_path}")

    if stage:
        stage.tick("Start scripts written.")


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
      - Python 3.11.x (the interpreter running this script or the system Python we use)
      - pip OR ensurepip (for that Python)
      - npm (Node.js) for building the admin frontend
    """
    stage = STAGE_PROGRESS.get("prereqs")
    info("[installer] Checking prerequisites…")

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
            py_ok = major == 3 and minor == 11
        except Exception:
            py_ok = False
    if stage:
        stage.tick("Checking Python 3.11…")

    pip_ver = _probe(python_cmd + ["-m", "pip", "--version"])
    ensurepip_ver = _probe(python_cmd + ["-m", "ensurepip", "--version"])
    pip_ok = bool(pip_ver or ensurepip_ver)
    if stage:
        stage.tick("Checking pip/ensurepip…")

    npm_path = shutil.which("npm")
    npm_ver = _probe([npm_path, "--version"]) if npm_path else None
    npm_ok = bool(npm_ver)
    if stage:
        stage.tick("Checking npm…")

    if not QUIET:
        print("[installer] Detected:")
        print(f"  - Python: {py_detail} ({'OK' if py_ok else 'need 3.11.x'})")
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
        missing.append(f"Python 3.11.x (found {py_detail})")
    if not pip_ok:
        missing.append("pip or ensurepip for the detected Python")
    if not npm_ok:
        missing.append("npm (Node.js)")

    if missing:
        error("\n[installer] ERROR: Missing prerequisites:")
        for item in missing:
            error(f"  • {item}")
        error(
            "\nHow to fix:\n"
            "  - Install Python 3.11.x from https://www.python.org/downloads/ (ensure it’s on PATH).\n"
            "  - Ensure `pip` works for that Python (or install `ensurepip`).\n"
            "  - Install Node.js (which includes npm): https://nodejs.org/\n"
            "\nOnce installed, re-run:\n"
            "    python install_standalone.py\n"
        )
        raise SystemExit(1)

    if stage:
        stage.tick("Prerequisites OK.")


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

    info(f"[installer] Repo root: {repo_root}")
    info(f"[installer] App root:  {app_root}")

    build_frontend(app_root)

    stage_data = STAGE_PROGRESS.get("data")
    data_dir = repo_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    if stage_data:
        stage_data.tick("Creating data directory…")
    (data_dir / "backups").mkdir(exist_ok=True)
    if stage_data:
        stage_data.tick("Creating backup directory…")
    info(f"[installer] Data dir:  {data_dir}")

    venv_root = repo_root / "venvs"
    venv_root.mkdir(exist_ok=True)

    create_venv(
        venv_root / "admin",
        app_root / "admin" / "requirements.txt",
        stage_name="venv_admin",
    )
    create_venv(
        venv_root / "server",
        app_root / "server" / "requirements.txt",
        extra_packages=["python-dotenv==1.1.1"],
        stage_name="venv_server",
    )
    create_venv(
        venv_root / "client",
        app_root / "client" / "requirements.txt",
        extra_packages=["python-dotenv==1.1.1"],
        stage_name="venv_client",
    )

    env_path = ensure_env_file(app_root, data_dir, args.admin_port)
    write_start_scripts(repo_root)

    if not QUIET:
        print("[installer] Installation complete ✅")
        print(
            f"[installer] You can edit environment settings like PASSWORD, PORTS, etc. in: {env_path}"
        )
        print("[installer] To run everything on Windows:")
        print("  - use Copycord.exe or")
        print("  - use copycord_windows.bat")
        print("[installer] To run everything on Linux/macOS:")
        print("  - ./copycord_linux.sh")
        print("    (make sure it is executable: chmod +x copycord_linux.sh if needed)")
        print()

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

    global _progress_percent, _current_status, _last_render_line

    exit_code = 0

    _progress_percent = 0
    _current_status = ""
    _last_render_line = ""

    try:
        exit_code = main()
    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else 1
    except Exception:
        error("\n[installer] Unexpected error:")
        traceback.print_exc()
        exit_code = 1

    if QUIET:
        while _progress_percent < 100:
            _advance_one("Finishing installation…")

        print()

        if not exit_code:
            print("[installer] Installation complete ✅")
            print(
                "[installer] You can edit environment settings like PASSWORD, PORTS, etc. in: code/.env"
            )
            print("[installer] To run everything on Windows:")
            print("  - use Copycord.exe or")
            print("  - use copycord_windows.bat")
            print("[installer] To run everything on Linux/macOS:")
            print("  - ./copycord_linux.sh")
            print(
                "    (make sure it is executable: chmod +x copycord_linux.sh if needed)"
            )
            print()
        else:
            print("[installer] Installation failed. See the error messages above.")
            print()

    is_frozen = bool(getattr(sys, "frozen", False))

    should_pause = is_frozen or (os.name == "nt" and (exit_code or exit_code is True))
    if should_pause:
        _pause()

    return int(exit_code or 0)


if __name__ == "__main__":
    raise SystemExit(_run_with_pause_installer())
