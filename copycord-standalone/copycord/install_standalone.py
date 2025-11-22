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

# GitHub repo configuration
GITHUB_REPO = os.getenv("GITHUB_REPO", "Copycord/Copycord")
GITHUB_TAG = os.getenv("GITHUB_TAG")
# Default branch is copycord-standalone unless overridden
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "copycord-standalone")

# Version file for updater comparison
VERSION_FILE_NAME = ".version"


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
        raise SystemExit("[installer] No tags found on GitHub; cannot determine latest version.")

    tag = data[0].get("name")
    if not tag:
        raise SystemExit("[installer] Unexpected tag payload from GitHub.")

    print(f"[installer] Latest tag: {tag}")
    return tag


def download_code(prefix: Path, ref: str, is_branch: bool = False) -> Path:
    """Download the tagged or branch Copycord archive into prefix/code."""
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
        raise SystemExit("[installer] Downloaded archive did not contain any directories.")
    repo_src_root = candidates[0]
    src_code_dir = repo_src_root / "code"

    if not src_code_dir.is_dir():
        raise SystemExit(f"[installer] Downloaded archive missing code/ (looked in {src_code_dir}).")

    print(f"[installer] Moving {src_code_dir} -> {code_dir}")
    shutil.move(str(src_code_dir), str(code_dir))

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
        # In normal flow this should already have been caught by check_prereqs()
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

    if venv_dir.exists():
        print(f"[installer] venv already exists: {venv_dir}")
    else:
        print(f"[installer] Creating venv at {venv_dir}")
        run([sys.executable, "-m", "venv", str(venv_dir)])

    bin_dir = venv_dir / ("Scripts" if os.name == "nt" else "bin")

    candidates = ["python.exe", "python", "python3"]
    python_exe = next((bin_dir / n for n in candidates if (bin_dir / n).exists()), None)
    if not python_exe:
        raise SystemExit(f"Python executable not found in venv: {bin_dir}")

    # Make sure pip exists in the venv (some installs may not include it by default)
    try:
        run([str(python_exe), "-m", "ensurepip", "--upgrade"])
    except Exception as e:
        print(f"[installer] Warning: ensurepip failed ({e}), continuing…")

    # Progress bar over the pip-related steps
    total_steps = 2 + (1 if extra_packages else 0)
    step = 1

    # 1) Upgrade pip
    run_pip_step(
        [str(python_exe), "-m", "pip", "install", "--upgrade", "pip"],
        step=step,
        total=total_steps,
        label="Upgrading pip",
    )
    step += 1

    # 2) Install main requirements
    run_pip_step(
        [str(python_exe), "-m", "pip", "install", "-r", str(requirements)],
        step=step,
        total=total_steps,
        label="Installing requirements",
    )
    step += 1

    # 3) Optional extra packages
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
    # Copycord standalone configuration

    DATA_DIR={data_dir.as_posix()}
    DB_PATH={(data_dir / 'data.db').as_posix()}

    ADMIN_HOST=127.0.0.1
    ADMIN_PORT={admin_port}
    ADMIN_WS_URL=ws://127.0.0.1:{admin_port}/bus
    PASSWORD=copycord # Comment out to disable login page (not recommended)

    SERVER_WS_HOST=127.0.0.1
    SERVER_WS_PORT=8765
    WS_SERVER_URL=ws://127.0.0.1:8765

    CLIENT_WS_HOST=127.0.0.1
    CLIENT_WS_PORT=8766
    WS_CLIENT_URL=ws://127.0.0.1:8766

    WS_SERVER_CTRL_URL=ws://127.0.0.1:9101
    WS_CLIENT_CTRL_URL=ws://127.0.0.1:9102

    CONTROL_PORT_SERVER=9101
    CONTROL_PORT_CLIENT=9102

    BACKUP_DIR={(data_dir / 'backups').as_posix()}
    BACKUP_RETAIN=14
    BACKUP_AT=03:17
    """

    env_path.write_text(dedent(content).strip() + "\n", encoding="utf-8")
    print(f"[installer] Wrote default .env to {env_path}")
    return env_path


def write_start_scripts(repo_root: Path) -> None:
    """Create Windows and Linux start scripts in the repo root if missing."""
    win_path = repo_root / "copycord_windows.bat"
    sh_path = repo_root / "copycord_linux.sh"

    if not win_path.exists():
        win_script = dedent(r"""\
        @echo off
        setlocal enabledelayedexpansion

        :: Force Windows console to use UTF-8 (code page 65001)
        chcp 65001 >nul
        set PYTHONUTF8=1
        set PYTHONIOENCODING=utf-8

        REM Directory where this script lives (repo root)
        set "ROOT=%~dp0"
        REM Remove trailing backslash if present
        if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

        set "CODE_DIR=%ROOT%\code"
        set "VENV_ROOT=%ROOT%\venvs"

        set "ADMIN_PY=%VENV_ROOT%\admin\Scripts\python.exe"
        set "SERVER_PY=%VENV_ROOT%\server\Scripts\python.exe"
        set "CLIENT_PY=%VENV_ROOT%\client\Scripts\python.exe"

        if not exist "%CODE_DIR%" (
            echo Error: code\ directory not found at "%CODE_DIR%"
            echo Make sure you ran: python install_standalone.py
            goto :EOF
        )

        if not exist "%ADMIN_PY%" (
            echo Error: admin venv python not found at "%ADMIN_PY%"
            echo Make sure you ran: python install_standalone.py
            goto :EOF
        )

        if not exist "%SERVER_PY%" (
            echo Error: server venv python not found at "%SERVER_PY%"
            echo Make sure you ran: python install_standalone.py
            goto :EOF
        )

        if not exist "%CLIENT_PY%" (
            echo Error: client venv python not found at "%CLIENT_PY%"
            echo Make sure you ran: python install_standalone.py
            goto :EOF
        )

        REM Default admin port; can be overridden by ADMIN_PORT in code\.env
        set "ADMIN_PORT=8080"
        if exist "%CODE_DIR%\.env" (
            for /f "usebackq tokens=1,* delims==" %%A in ("%CODE_DIR%\.env") do (
                if /I "%%~A"=="ADMIN_PORT" (
                    set "ADMIN_PORT=%%~B"
                )
            )
        )

        echo.
        echo Starting Copycord...
        echo   Root: %ROOT%
        echo   Admin UI port: %ADMIN_PORT%
        echo.

        REM Start Admin UI in its own terminal window
        echo Starting Copycord Admin UI window...
        start "Copycord Admin" /D "%CODE_DIR%" "%ADMIN_PY%" -m uvicorn admin.app:app --host 0.0.0.0 --port %ADMIN_PORT%

        REM Start Server control in its own terminal window
        echo Starting Copycord Server control window...
        set "ROLE=server"
        set "CONTROL_PORT=9101"
        start "Copycord Server" /D "%CODE_DIR%" "%SERVER_PY%" -m control.control

        REM Start Client control in its own terminal window
        echo Starting Copycord Client control window...
        set "ROLE=client"
        set "CONTROL_PORT=9102"
        start "Copycord Client" /D "%CODE_DIR%" "%CLIENT_PY%" -m control.control

        echo.
        echo All Copycord components started in separate terminals.
        echo   - Copycord Admin   (web UI)
        echo   - Copycord Server  (server control service)
        echo   - Copycord Client  (client control service)
        echo Close those windows to stop the services.
        echo.

        endlocal
        """).lstrip("\n")
        win_path.write_text(win_script, encoding="utf-8", newline="\r\n")
        print(f"[installer] Wrote Windows start script: {win_path}")
    else:
        print(f"[installer] Windows start script already exists at {win_path}, leaving it alone.")

    if not sh_path.exists():
        sh_script = dedent("""\
        #!/usr/bin/env bash
        set -euo pipefail

        # Directory where this script lives (repo root)
        ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
        CODE_DIR="$ROOT/code"
        VENV_ROOT="$ROOT/venvs"

        ADMIN_VENV="$VENV_ROOT/admin"
        SERVER_VENV="$VENV_ROOT/server"
        CLIENT_VENV="$VENV_ROOT/client"

        if [[ ! -d "$CODE_DIR" ]]; then
          echo "Error: code/ directory not found at $CODE_DIR"
          echo "Make sure you ran: python install_standalone.py"
          exit 1
        fi

        if [[ ! -d "$ADMIN_VENV" || ! -d "$SERVER_VENV" || ! -d "$CLIENT_VENV" ]]; then
          echo "Error: one or more virtualenvs are missing in $VENV_ROOT"
          echo "Run: python install_standalone.py"
          exit 1
        fi

        # Default admin port, overridable via code/.env (ADMIN_PORT=...)
        ADMIN_PORT="8080"
        ENV_FILE="$CODE_DIR/.env"
        if [[ -f "$ENV_FILE" ]]; then
          ENV_PORT="$(grep -E '^ADMIN_PORT=' "$ENV_FILE" | head -n1 | cut -d= -f2- | tr -d '\r' || true)"
          if [[ -n "${ENV_PORT:-}" ]]; then
            ADMIN_PORT="$ENV_PORT"
          fi
        fi

        cd "$CODE_DIR"

        echo "Starting Copycord admin UI on port $ADMIN_PORT…"
        "$ADMIN_VENV/bin/python" -m uvicorn admin.app:app --host 0.0.0.0 --port "$ADMIN_PORT" &
        ADMIN_PID=$!

        echo "Starting Copycord server agent control service…"
        ROLE=server CONTROL_PORT=9101 "$SERVER_VENV/bin/python" -m control.control &
        SERVER_PID=$!

        echo "Starting Copycord client agent control service…"
        ROLE=client CONTROL_PORT=9102 "$CLIENT_VENV/bin/python" -m control.control &
        CLIENT_PID=$!

        echo
        echo "Copycord is running."
        echo "  Admin UI: http://localhost:$ADMIN_PORT"
        echo
        echo "PIDs:"
        echo "  admin : $ADMIN_PID"
        echo "  server: $SERVER_PID"
        echo "  client: $CLIENT_PID"
        echo
        echo "Press Ctrl+C here to stop all components."

        cleanup() {
          echo
          echo "Stopping Copycord…"
          kill "$ADMIN_PID" "$SERVER_PID" "$CLIENT_PID" 2>/dev/null || true
          wait || true
        }

        trap cleanup INT TERM

        wait
        """).lstrip("\n")
        sh_path.write_text(sh_script, encoding="utf-8")
        try:
            sh_path.chmod(sh_path.stat().st_mode | 0o111)
        except Exception:
            pass
        print(f"[installer] Wrote Linux/macOS start script: {sh_path}")
    else:
        print(f"[installer] Linux/macOS start script already exists at {sh_path}, leaving it alone.")


def check_prereqs() -> None:
    """Check that pip/ensurepip and npm are available; abort with a clear error if not."""
    print("[installer] Checking prerequisites…")

    # Check pip (or at least ensurepip) for this Python
    ok = False
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "--version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        ok = True
    except Exception:
        # Try ensurepip as a fallback (some envs rely on that to bootstrap pip)
        try:
            subprocess.check_call(
                [sys.executable, "-m", "ensurepip", "--version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            ok = True
        except Exception:
            ok = False

    if not ok:
        raise SystemExit(
            "[installer] ERROR: Neither `python -m pip` nor `python -m ensurepip` "
            "appear to be available for this Python interpreter.\n"
            "Install pip (or use a Python build that includes pip/ensurepip) and re-run:\n"
            "    python install_standalone.py"
        )

    # Check npm presence (required to build the admin frontend)
    npm = shutil.which("npm")
    if npm is None:
        raise SystemExit(
            "[installer] ERROR: npm is not installed or not found in your PATH.\n"
            "Copycord's admin UI requires building the frontend with npm.\n"
            "Install Node.js (which includes npm) and then re-run this installer."
        )

    print("[installer] Prerequisites OK (pip/ensurepip & npm found).")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install Copycord in standalone mode.")
    parser.add_argument("--prefix", type=Path, help="Install prefix (default: current directory)")
    parser.add_argument("--admin-port", type=int, default=8080, help="Port for admin web UI")
    args = parser.parse_args(argv)

    prefix = args.prefix or Path.cwd()

    # Check pip & npm before doing any heavy work
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

    # Create venvs and install requirements
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

    # Create start scripts in repo root
    write_start_scripts(repo_root)

    print("\n[installer] Done.")
    print(f"  1) Change any environment settings as needed in {env_path}.")
    print("  2) To run everything on Windows:")
    print("       double-click start_copycord.bat")
    print("  3) To run everything on Linux/macOS:")
    print("       ./start_copycord.sh")
    print("     (make sure it is executable: chmod +x start_copycord.sh if needed)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
