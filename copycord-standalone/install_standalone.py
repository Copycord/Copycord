#!/usr/bin/env python3
"""
Copycord standalone installer.

- Downloads the Copycord code from the GitHub main branch (if needed)
- Builds the admin frontend with npm (if npm is available)
- Creates three virtual environments (admin, server, client)
- Installs requirements from code/admin|server|client/requirements.txt
- Creates data/ and logs/ directories
- Generates a .env file with sane defaults for standalone runs.

Usage:

    python install_standalone.py [--prefix /path/to/install-root]

If --prefix is omitted, the current directory is used as the install root.
"""

from __future__ import annotations

import argparse
import os
import sys
import subprocess
import shutil
import zipfile
from pathlib import Path
from textwrap import dedent
from urllib.request import urlopen

# GitHub repo and branch to download from
GITHUB_REPO = os.getenv("GITHUB_REPO", "Copycord/Copycord")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "copycord-standalone")


def run(cmd: list[str], **kwargs) -> None:
    print(f"[installer] $ {' '.join(cmd)}")
    subprocess.check_call(cmd, **kwargs)


def download_main_code(prefix: Path) -> Path:
    """
    Download the code/ tree from the main branch of the GitHub repo
    into the given prefix directory.

    Returns the path to the resulting `code/` directory.
    """
    prefix = prefix.resolve()
    code_dir = prefix / "code"

    if code_dir.is_dir():
        print(f"[installer] Found existing code/ at {code_dir}, skipping download.")
        return code_dir

    archive_url = (
        f"https://github.com/{GITHUB_REPO}/archive/refs/heads/{GITHUB_BRANCH}.zip"
    )
    zip_path = prefix / "copycord-main.zip"
    tmp_dir = prefix / "_copycord_src"

    print(f"[installer] No code/ directory found.")
    print(f"[installer] Downloading {archive_url}")
    with urlopen(archive_url) as resp:
        data = resp.read()
    zip_path.write_bytes(data)
    print(f"[installer] Saved archive to {zip_path}")

    # Extract into a temp directory
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    print(f"[installer] Extracting archive into {tmp_dir}")
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(tmp_dir)

    # Locate the repo root inside temp (usually something like Copycord-main/)
    candidates = [p for p in tmp_dir.iterdir() if p.is_dir()]
    if not candidates:
        raise SystemExit(
            "[installer] Downloaded archive did not contain any directories; "
            "cannot locate repo root."
        )
    repo_src_root = candidates[0]
    src_code_dir = repo_src_root / "code"

    if not src_code_dir.is_dir():
        raise SystemExit(
            f"[installer] Downloaded archive does not contain a `code/` directory "
            f"(looked in {src_code_dir})."
        )

    # Move code/ to the prefix root so our layout is:
    #   prefix/
    #     code/
    #     venvs/
    #     data/
    if code_dir.exists():
        print(f"[installer] Warning: {code_dir} already exists, removing it.")
        shutil.rmtree(code_dir)

    print(f"[installer] Moving {src_code_dir} -> {code_dir}")
    shutil.move(str(src_code_dir), str(code_dir))

    # Clean up temp + zip
    shutil.rmtree(tmp_dir, ignore_errors=True)
    try:
        zip_path.unlink()
    except FileNotFoundError:
        pass

    print(f"[installer] Code downloaded to {code_dir}")
    return code_dir


def detect_roots(prefix: Path) -> tuple[Path, Path]:
    """
    Returns (repo_root, app_root).

    repo_root: top-level folder that will hold venvs/, data/, etc.
    app_root: folder that contains the Copycord code (admin/, server/, client/, common/, control/).
    """
    repo_root = prefix.resolve()
    code_dir = repo_root / "code"
    if not code_dir.is_dir():
        # Download the code from GitHub main branch if it's not present.
        code_dir = download_main_code(repo_root)

    app_root = code_dir
    return repo_root, app_root


def build_frontend(app_root: Path) -> None:
    """
    Build the admin frontend using npm (like the Docker webbuild stage) and
    copy the built assets into admin/static/.
    """
    frontend_dir = app_root / "admin" / "frontend"
    package_json = frontend_dir / "package.json"

    if not package_json.is_file():
        print("[installer] No admin frontend package.json found; skipping npm build.")
        return

    npm = shutil.which("npm")
    if not npm:
        print(
            "[installer] WARNING: npm is not installed or not in PATH; skipping frontend build."
        )
        print(
            "           The admin UI may not work correctly until you build the frontend manually."
        )
        print(
            f"           To build manually later: cd {frontend_dir} && npm ci && npm run build"
        )
        return

    print(f"[installer] Building admin frontend via npm in {frontend_dir}")
    run([npm, "ci"], cwd=str(frontend_dir))
    run([npm, "run", "build"], cwd=str(frontend_dir))

    dist_dir = frontend_dir / "dist"
    if not dist_dir.is_dir():
        print(
            f"[installer] WARNING: npm build did not produce dist/ at {dist_dir}; "
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

    print(f"[installer] Copied built frontend to {static_dir}")


def create_venv(
    venv_dir: Path,
    requirements: Path,
    extra_packages: list[str] | None = None,
) -> None:
    if not requirements.is_file():
        raise SystemExit(f"Missing requirements file: {requirements}")

    if venv_dir.exists():
        print(f"[installer] venv already exists: {venv_dir}")
    else:
        print(f"[installer] Creating venv at {venv_dir}")
        run([sys.executable, "-m", "venv", str(venv_dir)])

    bin_dir = venv_dir / ("Scripts" if os.name == "nt" else "bin")

    # Use the venv's Python, and run `-m pip` for portability (handles pip.exe, etc.)
    candidates = (
        ["python.exe", "python", "python3"]
        if os.name == "nt"
        else ["python", "python3"]
    )
    python_exe: Path | None = None
    for name in candidates:
        p = bin_dir / name
        if p.exists():
            python_exe = p
            break

    if not python_exe:
        raise SystemExit(f"Python executable not found in venv: {bin_dir}")

    # Make sure pip exists in the venv (some installs may not include it by default)
    try:
        run([str(python_exe), "-m", "ensurepip", "--upgrade"])
    except Exception as e:
        print(f"[installer] Warning: ensurepip failed ({e}), continuing anyway…")

    # Upgrade pip & install requirements
    run([str(python_exe), "-m", "pip", "install", "--upgrade", "pip"])
    run([str(python_exe), "-m", "pip", "install", "-r", str(requirements)])

    if extra_packages:
        run([str(python_exe), "-m", "pip", "install", *extra_packages])


def ensure_env_file(app_root: Path, data_dir: Path, admin_port: int) -> Path:
    """
    Create a default .env next to the code/ folder if it doesn't exist.
    """
    env_path = app_root / ".env"
    if env_path.exists():
        print(f"[installer] .env already exists at {env_path}, leaving it alone.")
        return env_path

    # Use loopback + default ports in standalone mode.
    content = f"""\
    # Copycord standalone configuration

    # Where Copycord stores its SQLite DB, backups, exports, logs, etc.
    DATA_DIR={data_dir.as_posix()}
    DB_PATH={(data_dir / 'data.db').as_posix()}

    # Admin web UI
    ADMIN_HOST=127.0.0.1
    ADMIN_PORT={admin_port}
    ADMIN_WS_URL=ws://127.0.0.1:{admin_port}/bus
    PASSWORD=changeme # Comment out to disable admin UI auth (not recommended)

    # Websocket endpoints for the Discord server + client agents
    SERVER_WS_HOST=127.0.0.1
    SERVER_WS_PORT=8765
    WS_SERVER_URL=ws://127.0.0.1:8765

    CLIENT_WS_HOST=127.0.0.1
    CLIENT_WS_PORT=8766
    WS_CLIENT_URL=ws://127.0.0.1:8766

    # Control ports for the server + client (used by the Admin UI)
    WS_SERVER_CTRL_URL=ws://127.0.0.1:9101
    WS_CLIENT_CTRL_URL=ws://127.0.0.1:9102
    
    # Control ports for the server + client (used by controller)
    CONTROL_PORT_SERVER=9101
    CONTROL_PORT_CLIENT=9102

    # Backups
    BACKUP_DIR={(data_dir / 'backups').as_posix()}
    BACKUP_RETAIN=14
    BACKUP_AT=03:17
    """

    env_path.write_text(dedent(content).strip() + "\n", encoding="utf-8")
    print(f"[installer] Wrote default .env to {env_path}")
    return env_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install Copycord in standalone mode.")
    parser.add_argument(
        "--prefix",
        type=Path,
        help="Install prefix (defaults to current working directory)",
    )
    parser.add_argument(
        "--admin-port",
        type=int,
        default=8080,
        help="Port for the admin web UI (default: 8080)",
    )
    args = parser.parse_args(argv)

    prefix = args.prefix or Path.cwd()
    repo_root, app_root = detect_roots(prefix)

    print(f"[installer] Repo root: {repo_root}")
    print(f"[installer] App root:  {app_root}")

    # Build the frontend before spinning up venvs, so static assets are in place.
    build_frontend(app_root)

    data_dir = repo_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "backups").mkdir(exist_ok=True)
    print(f"[installer] Data dir:  {data_dir}")

    venv_root = repo_root / "venvs"
    venv_root.mkdir(exist_ok=True)

    # Extra packages to avoid ZoneInfo issues etc.
    # - tzdata fixes ZoneInfo('UTC') on Windows
    # - python-dotenv is needed by server/client for standalone .env loading
    create_venv(
        venv_root / "admin",
        app_root / "admin" / "requirements.txt",
        extra_packages=["tzdata"],
    )
    create_venv(
        venv_root / "server",
        app_root / "server" / "requirements.txt",
        extra_packages=["python-dotenv==1.1.1", "tzdata"],
    )
    create_venv(
        venv_root / "client",
        app_root / "client" / "requirements.txt",
        extra_packages=["python-dotenv==1.1.1", "tzdata"],
    )

    # Seed .env
    env_path = ensure_env_file(app_root, data_dir, args.admin_port)

    print("\n[installer] Done.")
    print(
        f"  1) Open {env_path} in a text editor and fill in SERVER_TOKEN, CLIENT_TOKEN, COMMAND_USERS, etc."
    )
    print("  2) To run the admin UI:")
    if os.name == "nt":
        print(
            f"       venvs\\admin\\Scripts\\python.exe -m uvicorn admin.app:app --host 0.0.0.0 --port {args.admin_port}"
        )
    else:
        print(
            f"       ./venvs/admin/bin/python -m uvicorn admin.app:app --host 0.0.0.0 --port {args.admin_port}"
        )
    print("  3) To run the Discord server agent control service:")
    if os.name == "nt":
        print(
            "       set ROLE=server && set CONTROL_PORT=9101 && venvs\\server\\Scripts\\python.exe -m control.control"
        )
    else:
        print(
            "       ROLE=server CONTROL_PORT=9101 ./venvs/server/bin/python -m control.control"
        )
    print("  4) To run the Discord client agent control service:")
    if os.name == "nt":
        print(
            "       set ROLE=client && set CONTROL_PORT=9102 && venvs\\client\\Scripts\\python.exe -m control.control"
        )
    else:
        print(
            "       ROLE=client CONTROL_PORT=9102 ./venvs/client/bin/python -m control.control"
        )
    print(
        "\nYou can run these three processes in separate terminals. The Admin UI should then connect to both agents."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
