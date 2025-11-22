from __future__ import annotations

import json
import os
import sys
import subprocess
from pathlib import Path
from urllib.request import Request, urlopen

# Copycord all-in-one launcher
#
#   1. Downloads a small JSON config which points to install/update scripts.
#   2. Lets you choose "Install", "Update", or "Run Copycord".
#   3. For install/update:
#        - Downloads the corresponding Python source from GitHub.
#        - Executes the remote script inside this process, calling its entrypoint.
#   4. For "Run Copycord":
#        - Locates and runs copycord_windows.bat in the same folder as the EXE
#          (or its parent).
#
# The remote install/update scripts remain in the GitHub repo so they can
# be updated independently of this launcher EXE.

DEFAULT_CONFIG_URL = (
    "https://github.com/Copycord/Copycord/blob/"
    "build-in-installer/install-tools/source/config.json"
)

CONFIG_ENV_VAR = "COPYCORD_LAUNCHER_CONFIG_URL"

USER_AGENT = "Copycord-Launcher/1.0"


REMOTE_PAUSE_HANDLED = False


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
    print(f"[launcher] Downloading: {raw_url}")
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

    print(f"[launcher] Using config URL: {config_url}")
    cfg = _fetch_json(config_url)

    install_url = cfg.get("install_url")
    update_url = cfg.get("update_url")

    if not install_url or not update_url:
        raise SystemExit(
            "[launcher] ERROR: config.json must define 'install_url' and 'update_url'."
        )

    cfg["install_url"] = _github_blob_to_raw(install_url)
    cfg["update_url"] = _github_blob_to_raw(update_url)

    print(f"[launcher]   install_url -> {cfg['install_url']}")
    print(f"[launcher]   update_url -> {cfg['update_url']}")
    return cfg


def prompt_choice() -> str:
    """Simple interactive menu to choose Install, Update, or Run Copycord."""
    print()
    print("======================================")
    print("      Copycord Standalone Launcher    ")
    print("======================================")
    print("1) Install Copycord")
    print("2) Update Copycord")
    print("3) Run Copycord")
    print("Q) Quit")
    print()

    while True:
        choice = input("Select an option [1/2/3/Q]: ").strip().lower()
        if choice in ("1", "2", "3", "q", "quit", "exit"):
            return choice
        print("Invalid choice. Please enter 1, 2, 3, or Q.")


def run_remote(kind: str, url: str) -> int:
    """
    Download a remote Python script and execute it in-process.

    We specifically look for the following entrypoints:

      - Install script: _run_with_pause_installer() or main()
      - Update script : _run_with_pause_updater() or main()

    We temporarily reset sys.argv so the remote script's argparse doesn't see
    any launcher-specific CLI flags.
    """
    global REMOTE_PAUSE_HANDLED

    print(f"[launcher] Fetching remote {kind} script…")
    source = _fetch_text(url)
    print(f"[launcher] Downloaded {len(source)} bytes from {url}")

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
            print(f"[launcher] Running remote entrypoint: {name}()")
            if name.startswith("_run_with_pause"):
                REMOTE_PAUSE_HANDLED = True
            break

    if entry is None:
        print(
            "[launcher] ERROR: Remote script does not define a recognised entrypoint.\n"
            "Expected one of: " + ", ".join(candidates)
        )
        return 1

    old_argv = sys.argv
    try:
        sys.argv = [old_argv[0]]
        result = entry()
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        print(f"[launcher] Remote script requested exit with code {code}.")
        return int(code)
    finally:
        sys.argv = old_argv

    return int(result or 0)


def run_copycord() -> int:
    """
    Locate and run copycord_windows.bat to start Copycord in separate windows.

    We look for the .bat file next to the EXE (when frozen) or next to this
    script (when run as .py), and also in the parent directory as a fallback.
    """
    if os.name != "nt":
        print("[launcher] 'Run Copycord' is currently only supported on Windows.")
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
        print("[launcher] ERROR: Could not find 'copycord_windows.bat'.")
        print(
            "          Make sure the launcher is in the same folder as copycord_windows.bat."
        )
        return 1

    print(f"[launcher] Running {bat_path}…")
    try:

        subprocess.run(
            ["cmd", "/c", str(bat_path)],
            cwd=str(bat_path.parent),
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"[launcher] Copycord launch script exited with code {e.returncode}.")
        return int(e.returncode)
    except Exception as e:
        print(f"[launcher] Failed to start Copycord: {e}")
        return 1

    print(
        "[launcher] Copycord launch script finished."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    import argparse

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
    except Exception as e:
        print(f"[launcher] Failed to load config: {e}")
        return 1

    while True:
        choice = prompt_choice()

        if choice in ("q", "quit", "exit"):
            print("[launcher] Exiting without changes.")
            return 0

        if choice == "1":
            return run_remote("install", cfg["install_url"])
        if choice == "2":
            return run_remote("update", cfg["update_url"])
        if choice == "3":
            return run_copycord()


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
