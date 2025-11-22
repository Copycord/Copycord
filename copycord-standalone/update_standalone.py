#!/usr/bin/env python3
"""
Copycord standalone updater.

Usage:
    python update_standalone.py

Behavior:
  - If this directory is a git clone (has `.git`) and git is available:
        * Fetch latest changes from origin
        * Check out and pull the `main` branch
  - Re-run `pip install -r` inside the existing venvs (admin/server/client)
    so dependency changes are picked up.
  - Rebuild the admin frontend with npm (if npm is available) and copy the
    built assets into code/admin/static/.

If this is a zip-based install without `.git`, the script will tell the user
to download a fresh standalone package from GitHub and re-run the installer.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

# Still configurable if you later want to use it for non-git updates.
GITHUB_REPO = os.getenv("GITHUB_REPO", "Copycord/Copycord")


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


def detect_repo_root() -> Path:
    here = Path(__file__).resolve().parent
    if (here / "code").is_dir():
        return here
    if (here.parent / "code").is_dir():
        return here.parent
    raise SystemExit("Could not find `code/` directory; run this from the repo root.")


def upgrade_venv(venv_dir: Path, requirements: Path) -> None:
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
    run([str(pip), "install", "--upgrade", "pip"])
    run([str(pip), "install", "-r", str(requirements)])


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


def main(argv: list[str] | None = None) -> int:
    repo_root = detect_repo_root()
    app_root = repo_root / "code"
    print(f"[updater] Repo root: {repo_root}")
    print(f"[updater] App root:  {app_root}")

    git_dir = repo_root / ".git"
    git = shutil.which("git")

    if git and git_dir.is_dir():
        print("[updater] Detected git repository; pulling `main` from origin…")
        run([git, "fetch", "origin"], cwd=repo_root)
        # Ensure we're on main, then pull
        run([git, "checkout", "main"], cwd=repo_root)
        run([git, "pull", "origin", "main"], cwd=repo_root)
    else:
        print(
            "[updater] This does not look like a git clone (no .git directory found)."
        )
        print(
            "[updater] If you installed from a zip/standalone package, "
            "please download the newest standalone zip from GitHub releases "
            "or the main branch build, extract it over this folder (or to a new folder) "
            "and re-run install_standalone.py."
        )

    # Whether or not we updated via git, refresh deps inside the venvs.
    venv_root = repo_root / "venvs"
    print("\n[updater] Updating virtualenv dependencies…")
    upgrade_venv(venv_root / "admin", app_root / "admin" / "requirements.txt")
    upgrade_venv(venv_root / "server", app_root / "server" / "requirements.txt")
    upgrade_venv(venv_root / "client", app_root / "client" / "requirements.txt")

    # Rebuild frontend to pick up any UI changes.
    print("\n[updater] Rebuilding admin frontend…")
    build_frontend(app_root)

    print("\n[updater] Done. Restart your Copycord processes to run the updated version.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
