#!/usr/bin/env python3
"""
Copycord standalone installer.

- Always downloads the Copycord code from GitHub (tagged archive) into code/
- Builds the admin frontend with npm (if npm is available)
- Creates three virtual environments (admin, server, client)
- Installs requirements from code/admin|server|client/requirements.txt
- Creates data/ directory
- Generates a .env file with sane defaults for standalone runs.

Usage:

    python install_standalone.py [--prefix /path/to/install-root]

If --prefix is omitted, the current directory is used as the install root.

GitHub config:

- GITHUB_REPO: "owner/repo" (default: "Copycord/Copycord")
- GITHUB_TAG:  explicit tag to install; if not set, the installer will
               query GitHub for the latest tag and use that.
"""

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

# GitHub repo + tag to download from
GITHUB_REPO = os.getenv("GITHUB_REPO", "Copycord/Copycord")
# If set, installer will use this tag instead of querying GitHub for "latest"
GITHUB_TAG = os.getenv("GITHUB_TAG")

# Where we record the installed tag so the updater can compare later
VERSION_FILE_NAME = ".copycord_tag"


def run(cmd: list[str], **kwargs) -> None:
    print(f"[installer] $ {' '.join(cmd)}")
    subprocess.check_call(cmd, **kwargs)


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


def download_code(prefix: Path, tag: str) -> Path:
    """Download the tagged Copycord archive into prefix/code."""
    prefix = prefix.resolve()
    code_dir = prefix / "code"

    archive_url = f"https://github.com/{GITHUB_REPO}/archive/refs/tags/{tag}.zip"
    zip_path = prefix / f"copycord-{tag}.zip"
    tmp_dir = prefix / "_copycord_src"

    if code_dir.is_dir():
        print(f"[installer] Removing existing code/ at {code_dir}")
        shutil.rmtree(code_dir)

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    print(f"[installer] Downloading {archive_url}")
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
    version_file.write_text(tag.strip() + "\n", encoding="utf-8")
    print(f"[installer] Recorded tag {tag} in {version_file}")

    shutil.rmtree(tmp_dir, ignore_errors=True)
    try:
        zip_path.unlink()
    except FileNotFoundError:
        pass

    print(f"[installer] Code downloaded to {code_dir}")
    return code_dir


def detect_roots(prefix: Path) -> tuple[Path, Path]:
    """Always downloads fresh code from GitHub into repo_root/code."""
    repo_root = prefix.resolve()
    desired_tag = GITHUB_TAG or fetch_latest_tag(GITHUB_REPO)
    code_dir = download_code(repo_root, desired_tag)
    app_root = code_dir
    return repo_root, app_root


def build_frontend(app_root: Path) -> None:
    """Build the admin frontend using npm and copy built assets into admin/static/."""
    frontend_dir = app_root / "admin" / "frontend"
    package_json = frontend_dir / "package.json"

    if not package_json.is_file():
        print("[installer] No admin frontend package.json found; skipping npm build.")
        return

    npm = shutil.which("npm")
    if not npm:
        print("[installer] WARNING: npm not found; skipping frontend build.")
        return

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


def create_venv(venv_dir: Path, requirements: Path, extra_packages: list[str] | None = None) -> None:
    """Create and install packages into a virtual environment."""
    if not requirements.is_file():
        raise SystemExit(f"Missing requirements file: {requirements}")

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

    try:
        run([str(python_exe), "-m", "ensurepip", "--upgrade"])
    except Exception as e:
        print(f"[installer] Warning: ensurepip failed ({e}), continuing…")

    run([str(python_exe), "-m", "pip", "install", "--upgrade", "pip"])
    run([str(python_exe), "-m", "pip", "install", "-r", str(requirements)])

    if extra_packages:
        run([str(python_exe), "-m", "pip", "install", *extra_packages])


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
    PASSWORD=changeme

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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install Copycord in standalone mode.")
    parser.add_argument("--prefix", type=Path, help="Install prefix (default: current directory)")
    parser.add_argument("--admin-port", type=int, default=8080, help="Port for admin web UI")
    args = parser.parse_args(argv)

    prefix = args.prefix or Path.cwd()
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

    print("\n[installer] Done.")
    print(f"  1) Edit {env_path} and fill in SERVER_TOKEN, CLIENT_TOKEN, COMMAND_USERS, etc.")
    print("  2) To run the admin UI:")
    if os.name == "nt":
        print(f"       venvs\\admin\\Scripts\\python.exe -m uvicorn admin.app:app --host 0.0.0.0 --port {args.admin_port}")
    else:
        print(f"       ./venvs/admin/bin/python -m uvicorn admin.app:app --host 0.0.0.0 --port {args.admin_port}")
    print("  3) To run the Discord server control service:")
    if os.name == "nt":
        print("       set ROLE=server && venvs\\server\\Scripts\\python.exe -m control.control")
    else:
        print("       ROLE=server ./venvs/server/bin/python -m control.control")
    print("  4) To run the Discord client control service:")
    if os.name == "nt":
        print("       set ROLE=client && venvs\\client\\Scripts\\python.exe -m control.control")
    else:
        print("       ROLE=client ./venvs/client/bin/python -m control.control")
    print("\nYou can run these three processes in separate terminals. The Admin UI will connect automatically.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
