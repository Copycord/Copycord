# Copycord Standalone (Launcher, Installer & Updater)

This folder contains the **standalone launcher/installer/updater** for Copycord.

You can install, update, and run Copycord without cloning the Git repo manually.

- On **Windows**, use **`Copycord.exe`** (all-in-one launcher).
- On **Linux/macOS**, use **`launcher.py`** (all-in-one launcher).

Once installed, you’ll use either the launcher or the provided start scripts to run Copycord.

---

## Folder Layout

After installation, this folder will typically look like:

- `Copycord.exe` – Windows launcher (install / update / run)
- `launcher.py` – Linux/macOS launcher (install / update / run)
- `copycord_windows.bat` – Start Copycord on Windows (created by the installer)
- `copycord_linux.sh` – Start Copycord on Linux/macOS (created by the installer)
- `code/` – Copycord application code (created by the installer)
- `venvs/` – Python virtual environments for admin/server/client
- `data/` – Your Copycord data (database, backups, etc.)

> **Important:**  
> Your actual data (servers, mappings, etc.) lives under the `data/` folder.  
> **Back this up** before deleting or moving the install.

---

## Requirements

### Windows

- **Python 3.11** installed (from python.org or Microsoft Store)
- **Node.js + npm** installed (for building the admin web UI)
- Internet access to download Copycord from GitHub


### Linux

- **Python 3.11+** with `venv` and `pip`
- **Node.js + npm**
- Internet access to download Copycord from GitHub

Example packages (Debian/Ubuntu-like):

```bash
sudo apt install python3 python3-venv python3-pip nodejs npm
```

---

## First-Time Install

### Windows (using `Copycord.exe`)

1. Create a folder where you want Copycord to live (for example: `C:\Copycord`).
2. Download `Copycord.exe` from the latest release and place it **inside that folder**.
3. **Double-click `Copycord.exe`.**
   - A console window will open with a menu.
   - Choose `1) Install Copycord`.
   - The launcher will:
     - Download the latest Copycord build from GitHub  
     - Build the admin frontend  
     - Create `code/`, `venvs/`, and `data/`  
     - Generate `copycord_windows.bat` (Windows start script)

4. To **start Copycord** after install:
   - Either run `Copycord.exe` again and choose `3) Run Copycord (Windows)`, **or**
   - Double-click `copycord_windows.bat`.


### Linux (using `launcher.py`)
`curl -L "https://raw.githubusercontent.com/Copycord/Copycord/refs/heads/main/install-tools/source/launcher.py" -o launcher.py`

1. Place these files in a folder where you want Copycord to live, for example:

   ```bash
   mkdir -p ~/copycord
   cd ~/copycord
   # Put launcher.py here
   ```

2. Run the launcher:

   ```bash
   cd ~/copycord
   python3 launcher.py
   ```

3. When the menu appears, choose: `1) Install Copycord`.

   The launcher will:

   - Download the latest Copycord release from GitHub
   - Build the admin frontend
   - Create `code/`, `venvs/`, and `data/`
   - Generate `copycord_linux.sh` (Linux/macOS start script)

4. To **start Copycord** after install:
   - Either run `python3 launcher.py` again and choose `4) Run Copycord (Linux)`, **or**
   - Run the start script directly:

     ```bash
     cd ~/copycord
     ./copycord_linux.sh
     ```

     (If needed, make it executable once with `chmod +x copycord_linux.sh`.)

---

## Starting Copycord

After the installer has run successfully (on either OS), the start scripts and launcher options will be available.

### On Windows

From your Copycord folder (the one that contains `Copycord.exe` and `copycord_windows.bat`):

- **Option A (recommended for new users):**  
  Double-click `Copycord.exe` and choose `3) Run Copycord (Windows)`.

- **Option B (direct script):**  
  Double-click `copycord_windows.bat`.

This will:

- Start the **Admin UI** (web interface)
- Start the **server agent**
- Start the **client agent**

Then open in your browser:

```text
http://localhost:8080
```

(or whatever port you configured in `.env`) to access the Copycord admin panel.

### On Linux / macOS

From your Copycord folder (for example `~/copycord`):

- **Option A (launcher):**

  ```bash
  cd ~/copycord
  python3 launcher.py
  ```

  Then choose `4) Run Copycord (Linux)`.

- **Option B (direct script):**

  ```bash
  cd ~/copycord
  ./copycord_linux.sh
  ```

This will start all components (admin UI, server agent, client agent).  
Then open in your browser:

```text
http://localhost:8080
```

(or the port you configured).

---

## Updating Copycord

When a new Copycord version is released, use the same launcher to update from this folder.

> **Always close any running Copycord windows/shells before updating.**  
> (Close the admin/server/client terminals.)

### Windows (using `Copycord.exe`)

1. Go to your Copycord folder (the one that contains `Copycord.exe` and `code/`).
2. **Double-click `Copycord.exe`.**
3. From the menu, choose `2) Update Copycord`.

The launcher will:

- Detect your current installed version (`code/.version`)
- Check GitHub for the latest tag
- If needed:
  - Download the new code
  - Update Python dependencies in `venvs/`
  - Rebuild the admin frontend

4. When it finishes, start Copycord again using:
   - `Copycord.exe` → `3) Run Copycord (Windows)`, or  
   - `copycord_windows.bat`.

---

### Linux / macOS (using `launcher.py`)

1. Stop Copycord (Ctrl+C in the terminal where it is running).
2. From the Copycord folder, run:

   ```bash
   cd ~/copycord
   python3 launcher.py
   ```

3. From the menu, choose: `2) Update Copycord`.

The launcher will:

- Check your current version in `code/.version`
- Compare with the latest GitHub tag
- Download new code if needed
- Update `venvs/` dependencies
- Rebuild the frontend

4. When it finishes, start Copycord again with:
   - `python3 launcher.py` → `4) Run Copycord (Linux)`, or  
   - `./copycord_linux.sh`.

---

## Environment variables

- The env file is fully controllable and can be found inside the `/code` folder in your Copycord directory (`code/.env`).  
- Here you can modify variables such as:
  - `PASSWORD`
  - `ADMIN_PORT`
  - WebSocket ports
  - Backup settings, etc.

---

## Troubleshooting

- **No Python found / Python error**  
  - Install Python 3.10+.  
  - On Windows, ensure it’s added to PATH or use the `py` launcher.

- **npm not found**  
  - Install Node.js (which includes npm).

- **“Could not find `code/` directory” / install not found**  
  - Make sure you are running `Copycord.exe` / `launcher.py` from the same folder where `code/` exists (after installation).

- **Port already in use (8080)**  
  - Edit `.env` (inside `code/`) and change `ADMIN_PORT`, then restart Copycord.

---
