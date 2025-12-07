from __future__ import annotations

import json
import os
import sys
import re
import subprocess
import time
from pathlib import Path
from urllib.request import Request, urlopen


DEFAULT_CONFIG_URL = (
    "https://github.com/Copycord/Copycord/blob/"
    "main/install-tools/source/config.json"
)

CONFIG_ENV_VAR = "COPYCORD_LAUNCHER_CONFIG_URL"

LAUNCHER_VERSION = "1.2.0"  
LATEST_REMOTE_VERSION: str | None = None
USER_AGENT = f"Copycord-Launcher/{LAUNCHER_VERSION}"

REMOTE_PAUSE_HANDLED = False
AUTO_UPDATE_LAUNCHER = True  

FRESH_ENV_FLAG = "COPYCORD_LAUNCHER_FRESH"


def _parse_ver(v: str) -> tuple:
    nums = [int(x) for x in re.findall(r"\d+", v)]
    nums = (nums + [0, 0, 0])[:3]
    return tuple(nums)


def _cmp_ver(a: str, b: str) -> int:
    A, B = _parse_ver(a), _parse_ver(b)
    return (A > B) - (A < B)


def _platform_download_url(cfg: dict) -> str | None:
    if os.name == "nt":
        return cfg.get("windows_launcher_url")
    return cfg.get("linux_launcher_url")


def _github_blob_to_raw(url: str) -> str:
    """
    Convert a normal GitHub "blob" URL into a raw.githubusercontent.com URL.

    If the URL is not a GitHub blob URL, it's returned unchanged.
    """
    if "github.com" not in url or "/blob/" not in url:
        return url

    try:
        before, after = url.split("github.com/", 1)
        parts = after.split("/")
        if len(parts) >= 5 and parts[2] == "blob":
            owner = parts[0]
            repo = parts[1]
            branch = parts[3]
            path = "/".join(parts[4:])
            return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
    except Exception:
        pass

    return url


def _fetch_text(url: str) -> str:
    """Download text content from a URL with a Copycord-specific User-Agent."""
    raw_url = _github_blob_to_raw(url)
    req = Request(raw_url, headers={"User-Agent": USER_AGENT})
    with urlopen(req) as resp:
        data = resp.read()
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("utf-8", errors="replace")


def _fetch_json(url: str) -> dict:
    """Download JSON from URL and parse it."""
    text = _fetch_text(url)
    return json.loads(text)


def _self_path() -> Path:
    """
    Path to the current launcher:
      - Frozen exe: sys.executable
      - Normal script: this file
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve()
    return Path(__file__).resolve()


def _download_file(url: str, dest: Path) -> None:
    """
    Download a binary file (exe or .py) to dest.
    Uses the same User-Agent + GitHub blob→raw logic as _fetch_text.
    """
    print("[Launcher] Downloading new launcher…")
    raw_url = _github_blob_to_raw(url)
    req = Request(raw_url, headers={"User-Agent": USER_AGENT})
    with urlopen(req) as resp, open(dest, "wb") as f:
        while True:
            chunk = resp.read(8192)
            if not chunk:
                break
            f.write(chunk)
    print("[Launcher] Download complete.")


def _clear_console() -> None:
    """Clear the current console/terminal."""
    if os.name == "nt":
        os.system("cls")
    else:
        os.system("clear")


def _restart_new_launcher(new_path: Path) -> None:
    """
    Start the newly-downloaded launcher and exit this process.
    We tag the new process with an env flag so it knows to clear the console
    after a short delay, giving a "fresh" look.
    """
    if os.name == "nt" and getattr(sys, "frozen", False):
        argv = [str(new_path)] + sys.argv[1:]
    else:
        argv = [sys.executable, str(new_path)] + sys.argv[1:]

    print("[Launcher] Loading new launcher…")

    env = os.environ.copy()
    env[FRESH_ENV_FLAG] = "1"

    subprocess.Popen(argv, cwd=str(new_path.parent), env=env)
    raise SystemExit(0)


def _finalize_self_update() -> None:
    """
    For Windows frozen launchers that were started from a versioned name
    like 'Copycord Launcher_v1.1.0.exe', clean up:

      - Delete the old base exe (e.g. 'Copycord Launcher.exe') if it exists.
      - Rename or copy this file to the base name.

    This runs in the *new* launcher process, after it's already running.
    """
    if not (os.name == "nt" and getattr(sys, "frozen", False)):
        return

    here = Path(sys.executable).resolve()
    m = re.match(r"^(?P<stem>.+?)_v\d+(?:\.\d+)*(?P<suffix>\.exe)$", here.name, re.IGNORECASE)
    if not m:
        return  

    base_name = m.group("stem") + m.group("suffix")
    base_path = here.with_name(base_name)

    if base_path == here:
        return

    try:
        if base_path.exists():
            try:
                base_path.unlink()
            except PermissionError:
                return

        
        try:
            here.rename(base_path)
            return
        except OSError:
            
            import shutil

            try:
                shutil.copy2(here, base_path)
            except OSError:
                
                return
    except Exception:
        
        return


def _maybe_clear_console_on_fresh_start() -> None:
    """
    If we were started as part of an auto-update, wait 3 seconds so the user
    can see "Loading new launcher…", then clear the console to fake a fresh start.
    """
    if os.environ.pop(FRESH_ENV_FLAG, None):
        time.sleep(3)
        _clear_console()


def auto_update_launcher_if_needed(cfg: dict) -> None:
    """
    If the remote config says there's a newer launcher, download the correct
    artifact for this platform and restart into it.

    On Windows + frozen exe:
      - Download to <base>_v<latest>.exe
      - Start that new exe directly.
      - The new exe will then clean up/rename itself on startup.

    On non-frozen (Python script):
      - Replace this .py file in-place, then re-run it.
    """
    global LATEST_REMOTE_VERSION

    latest = cfg.get("launcher_version")
    LATEST_REMOTE_VERSION = latest

    if not latest:
        return

    if _cmp_ver(LAUNCHER_VERSION, latest) >= 0:
        
        return

    print(
        f"[Launcher] New launcher version detected: v{latest} (current: v{LAUNCHER_VERSION})"
    )

    if not AUTO_UPDATE_LAUNCHER:
        print("[Launcher] Auto-update is disabled. Please download the new launcher manually.")
        return

    url = _platform_download_url(cfg)
    if not url:
        print("[Launcher] Update available but no launcher URL is configured for this platform.")
        return

    print("[Launcher] Updating launcher…")
    here = _self_path()

    if os.name == "nt" and getattr(sys, "frozen", False):
        stem = here.stem
        base_stem = re.sub(r"(?:_updated|_v\d+(?:\.\d+)*)$", "", stem)
        new_name = f"{base_stem}_v{latest}{here.suffix}"
        new_path = here.with_name(new_name)
    else:
        
        new_path = here

    tmp_path = new_path.with_suffix(new_path.suffix + ".tmp")

    try:
        _download_file(url, tmp_path)

        
        try:
            new_path.unlink(missing_ok=True)  
        except TypeError:
            try:
                new_path.unlink()
            except FileNotFoundError:
                pass

        tmp_path.rename(new_path)
    except Exception:
        print("[Launcher] Launcher update failed. Continuing with current version.")
        return

    print("[Launcher] Launcher update installed.")
    _restart_new_launcher(new_path)


def load_config(config_url: str | None = None) -> dict:
    """
    Load the config JSON that contains the URLs for install/update scripts.

    Precedence:
      1. --config-url argument
      2. COPYCORD_LAUNCHER_CONFIG_URL environment variable
      3. DEFAULT_CONFIG_URL constant
    """
    if not config_url:
        config_url = os.getenv(CONFIG_ENV_VAR, DEFAULT_CONFIG_URL)

    cfg = _fetch_json(config_url)

    install_url = cfg.get("install_url")
    update_url = cfg.get("update_url")

    if not install_url or not update_url:
        raise SystemExit(
            "[Launcher] ERROR: config.json must define 'install_url' and 'update_url'."
        )

    cfg["install_url"] = _github_blob_to_raw(install_url)
    cfg["update_url"] = _github_blob_to_raw(update_url)
    return cfg


def prompt_choice() -> str:
    """Simple interactive menu to choose Install, Update, or Run Copycord."""
    print()
    print("======================================")
    print(f"  Copycord Standalone Launcher v{LAUNCHER_VERSION}")
    if LATEST_REMOTE_VERSION and _cmp_ver(LAUNCHER_VERSION, LATEST_REMOTE_VERSION) < 0:
        print(f"  Update available: v{LATEST_REMOTE_VERSION}")
    print("======================================")
    print("1) Install Copycord")
    print("2) Update Copycord")
    print("3) Run Copycord (Windows)")
    print("4) Run Copycord (Linux)")
    print("Q) Quit")
    print()

    while True:
        choice = input("Select an option [1/2/3/4/Q]: ").strip().lower()
        if choice in ("1", "2", "3", "4", "q", "quit", "exit"):
            return choice
        print("Invalid choice. Please enter 1, 2, 3, 4, or Q.")


def run_remote(kind: str, url: str) -> int:
    """
    Download a remote Python script and execute it in-process.
    """
    global REMOTE_PAUSE_HANDLED

    source = _fetch_text(url)

    module_name = f"copycord_{kind}_remote"
    namespace: dict[str, object] = {
        "__name__": module_name,
        "__file__": f"<{module_name}>",
        "__package__": None,
    }

    code_obj = compile(source, f"<{module_name}>", "exec")
    exec(code_obj, namespace)

    if kind == "install":
        candidates = ["_run_with_pause_installer", "main"]
    else:
        candidates = ["_run_with_pause_updater", "main"]

    entry = None
    for name in candidates:
        obj = namespace.get(name)
        if callable(obj):
            entry = obj
            if name.startswith("_run_with_pause"):
                REMOTE_PAUSE_HANDLED = True
            break

    if entry is None:
        print(
            "[Launcher] ERROR: Remote script does not define a recognised entrypoint.\n"
            "Expected one of: " + ", ".join(candidates)
        )
        return 1

    old_argv = sys.argv
    try:
        sys.argv = [old_argv[0]]
        result = entry()
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        return int(code)
    finally:
        sys.argv = old_argv

    return int(result or 0)


def run_copycord_windows() -> int:
    """
    Locate and run copycord_windows.bat to start Copycord in separate windows.
    """
    if os.name != "nt":
        print("[Launcher] 'Run Copycord (Windows)' is only supported on Windows.")
        return 1

    if getattr(sys, "frozen", False):
        base = Path(sys.executable).resolve().parent
    else:
        base = Path(__file__).resolve().parent

    candidates = [
        base / "copycord_windows.bat",
        base.parent / "copycord_windows.bat",
    ]

    bat_path: Path | None = None
    for c in candidates:
        if c.is_file():
            bat_path = c
            break

    if bat_path is None:
        print("[Launcher] ERROR: Could not find 'copycord_windows.bat'.")
        print(
            "          Make sure the launcher is in the same folder as copycord_windows.bat."
        )
        return 1

    try:
        subprocess.run(
            ["cmd", "/c", str(bat_path)],
            cwd=str(bat_path.parent),
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"[Launcher] Copycord launch script exited with code {e.returncode}.")
        return int(e.returncode)
    except Exception:
        print("[Launcher] Failed to start Copycord.")
        return 1

    return 0


def run_copycord_linux() -> int:
    """
    Locate and run copycord_linux.sh to start Copycord on Linux/macOS.
    """
    if os.name == "nt":
        print("[Launcher] 'Run Copycord (Linux)' is only supported on Linux/macOS.")
        return 1

    if getattr(sys, "frozen", False):
        base = Path(sys.executable).resolve().parent
    else:
        base = Path(__file__).resolve().parent

    candidates = [
        base / "copycord_linux.sh",
        base.parent / "copycord_linux.sh",
    ]

    script_path: Path | None = None
    for c in candidates:
        if c.is_file():
            script_path = c
            break

    if script_path is None:
        print("[Launcher] ERROR: Could not find 'copycord_linux.sh'.")
        print(
            "          Make sure the launcher is in the same folder as copycord_linux.sh."
        )
        return 1

    try:
        mode = script_path.stat().st_mode
        if not (mode & 0o111):
            script_path.chmod(mode | 0o111)
    except Exception:
        pass

    try:
        subprocess.run(
            ["bash", str(script_path)],
            cwd=str(script_path.parent),
            check=True,
        )
    except FileNotFoundError:
        print("[Launcher] ERROR: 'bash' not found. Try running the script manually.")
        return 1
    except subprocess.CalledProcessError as e:
        print(f"[Launcher] Copycord launch script exited with code {e.returncode}.")
        return int(e.returncode)
    except Exception:
        print("[Launcher] Failed to start Copycord.")
        return 1

    return 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    
    _finalize_self_update()
    
    _maybe_clear_console_on_fresh_start()

    parser = argparse.ArgumentParser(
        description="Copycord all-in-one Install/Update/Run launcher."
    )
    parser.add_argument(
        "--config-url",
        help=(
            "Optional override for config.json URL. "
            f"Defaults to {CONFIG_ENV_VAR!r} env var or built-in URL."
        ),
    )
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args.config_url)
        auto_update_launcher_if_needed(cfg)
        
    except Exception as e:
        print(f"[Launcher] Failed to load config: {e}")
        return 1

    while True:
        choice = prompt_choice()

        if choice in ("q", "quit", "exit"):
            print("[Launcher] Exiting without changes.")
            return 0

        if choice == "1":
            return run_remote("install", cfg["install_url"])
        if choice == "2":
            return run_remote("update", cfg["update_url"])
        if choice == "3":
            return run_copycord_windows()
        if choice == "4":
            return run_copycord_linux()


def _pause_if_needed(exit_code: int) -> int:
    """
    On Windows + PyInstaller builds, pause before closing the console
    *only* if the remote script did NOT already handle its own pause.
    """
    if os.name == "nt" and getattr(sys, "frozen", False) and not REMOTE_PAUSE_HANDLED:
        try:
            input("\nPress Enter to close this window...")
        except EOFError:
            pass
    return exit_code


if __name__ == "__main__":
    code = main()
    raise SystemExit(_pause_if_needed(code))
