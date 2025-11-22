from __future__ import annotations

import json
import os
import shutil
import subprocess
import zipfile
from pathlib import Path
from urllib.request import urlopen, Request

# GitHub repo + optional explicit tag/branch
GITHUB_REPO = os.getenv("GITHUB_REPO", "Copycord/Copycord")
GITHUB_TAG = os.getenv("GITHUB_TAG")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "copycord-standalone")

# Match the installer: version file that stores the last installed ref (tag or branch)
VERSION_FILE_NAME = ".version"


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
    here = Path(__file__).resolve().parent
    if (here / "code").is_dir():
        return here
    if (here.parent / "code").is_dir():
        return here.parent
    raise SystemExit("Could not find `code/` directory; run this from the repo root.")


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
        return (version_file.read_text(encoding="utf-8").strip() or None)
    except Exception:
        return None


def download_code(prefix: Path, ref: str, *, is_branch: bool = False) -> Path:
    """
    Download the Copycord archive from GitHub into prefix/code, replacing any
    existing code/ directory, and update code/.version with the ref.

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

    # 1) Upgrade pip
    run_pip_step(
        [str(pip), "install", "--upgrade", "pip"],
        step=step,
        total=total_steps,
        label="Upgrading pip",
    )
    step += 1

    # 2) Install requirements
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


def main(argv: list[str] | None = None) -> int:
    repo_root = detect_repo_root()
    code_dir = repo_root / "code"
    print(f"[updater] Repo root: {repo_root}")
    print(f"[updater] Code dir:  {code_dir}")

    current_ref = read_local_ref(code_dir)
    print(f"[updater] Currently installed ref: {current_ref or 'none'}")

    # If GITHUB_BRANCH is set, we track that branch and always re-download it.
    if GITHUB_BRANCH:
        target_ref = GITHUB_BRANCH
        print(f"[updater] GITHUB_BRANCH is set; updating from branch: {target_ref}")
        print(
            "[updater] Note: for branches we always download the latest archive, "
            "since there is no simple way to detect 'no changes' via the .version file."
        )
        download_code(repo_root, target_ref, is_branch=True)

        # Refresh app_root in case code/ was re-created
        app_root = repo_root / "code"

        # After branch update, refresh deps inside the venvs.
        venv_root = repo_root / "venvs"
        print("\n[updater] Updating virtualenv dependencies…")
        upgrade_venv(venv_root / "admin", app_root / "admin" / "requirements.txt")
        upgrade_venv(venv_root / "server", app_root / "server" / "requirements.txt")
        upgrade_venv(venv_root / "client", app_root / "client" / "requirements.txt")

        # Rebuild frontend to pick up any UI changes.
        print("\n[updater] Rebuilding admin frontend…")
        build_frontend(app_root)

        print("\n[updater] Done. Restart Copycord to run the updated build.")
        return 0

    # Otherwise, we are working with tags
    target_tag = GITHUB_TAG or fetch_latest_tag(GITHUB_REPO)
    print(f"[updater] Target tag: {target_tag}")

    # If already on the latest tag, stop immediately (no venv/npm work).
    if current_ref == target_tag:
        print("[updater] Already on the latest tag; nothing to do.")
        return 0

    print(
        f"[updater] Tag mismatch -> updating code from "
        f"{current_ref or 'none'} to {target_tag}"
    )
    download_code(repo_root, target_tag, is_branch=False)

    # Refresh app_root in case code/ was re-created
    app_root = repo_root / "code"

    # After code update, refresh deps inside the venvs.
    venv_root = repo_root / "venvs"
    print("\n[updater] Updating virtualenv dependencies…")
    upgrade_venv(venv_root / "admin", app_root / "admin" / "requirements.txt")
    upgrade_venv(venv_root / "server", app_root / "server" / "requirements.txt")
    upgrade_venv(venv_root / "client", app_root / "client" / "requirements.txt")

    # Rebuild frontend to pick up any UI changes.
    print("\n[updater] Rebuilding admin frontend…")
    build_frontend(app_root)

    print("\n[updater] Done. Restart Copycord to run the updated version.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
