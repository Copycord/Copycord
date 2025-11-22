# Copycord Standalone (Installer & Updater)

This folder contains the **standalone installer and updater** for Copycord.

You can install and update Copycord without cloning the Git repo manually.

- On **Windows**, use the `.exe` files.
- On **Linux/macOS**, use the `install.py` and `update.py` scripts.

Once installed, you’ll use the provided scripts to start Copycord.

---

## Folder Layout

After installation, this folder will typically look like:

- `Install.exe` – Windows installer
- `Update.exe` – Windows updater
- `install.py` – Linux/macOS installer
- `update.py` – Linux/macOS updater
- `copycord_windows.bat` – Start Copycord on Windows
- `copycord_linux.sh` – Start Copycord on Linux/macOS
- `code/` – Copycord application code (created by the installer)
- `venvs/` – Python virtual environments for admin/server/client
- `data/` – Your Copycord data (database, backups, etc.)

> **Important:**  
> Your actual data (servers, mappings, etc.) lives under the `data/` folder.  
> **Back this up** before deleting or moving the install.

---

## Requirements

### Windows

- **Python 3.10+** installed (from python.org or Microsoft Store)
- **Node.js + npm** installed (for building the admin web UI)
- Internet access to download Copycord from GitHub


### Linux

- **Python 3.10+** with `venv` and `pip`
- **Node.js + npm**
- Internet access to download Copycord from GitHub

Example packages (Debian/Ubuntu-like):

```bash
sudo apt install python3 python3-venv python3-pip nodejs npm
```

---

## First-Time Install

### Windows (using `Copycord.exe`)

1. Create a folder where you want Copycord to live.
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
   - Either run `Copycordr.exe` again and choose `3) Run Copycord`, **or**
   - Double-click `copycord_windows.bat`.


### Linux (using `install.py`)

1. Place these files in a folder where you want Copycord to live, e.g.:

   ```bash
   mkdir -p ~/copycord
   cd ~/copycord
   # (Put install.py, update.py, etc. here)
   ```

2. Run the installer:

   ```bash
   cd /path/to/copycord
   python3 install.py
   ```

   This will:

   - Download the latest Copycord release from GitHub
   - Build the admin frontend
   - Create `code/`, `venvs/`, and `data/`
   - Generate `copycord_linux.sh` (start script)

3. To **start Copycord**, see [Starting Copycord](#starting-copycord) below.

---

## Starting Copycord

After the installer has run successfully (on either OS), the start scripts will be available.

### On Windows

1. Open the Copycord folder (the one that contains `copycord_windows.bat`).
2. **Double-click `copycord_windows.bat`.**

This will:

- Start the **Admin UI** (web interface)
- Start the **server agent**
- Start the **client agent**

You should then be able to open:

```text
http://localhost:8080
```

(or whatever port you configured in `.env`) in your browser to access the Copycord admin panel.

### On Linux / macOS

1. Make sure the script is executable (one-time):

   ```bash
   cd /path/to/copycord
   chmod +x copycord_linux.sh
   ```

2. Start Copycord:

   ```bash
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

When a new Copycord version is released, use the updater from this same folder.

> **Always close any running Copycord windows/shells before updating.**  
> (Close the admin/server/client terminals.)

### Windows (using `Update.exe`)

1. Go to your Copycord folder (the one that contains `Update.exe` and `code/`).
2. **Double-click `Update.exe`.**

The updater will:

- Detect your current installed version (`code/.version`)
- Check GitHub for the latest tag
- If needed:
  - Download the new code
  - Update Python dependencies in `venvs/`
  - Rebuild the admin frontend

3. Press **Enter** to close the updater window.
4. Start Copycord again using `copycord_windows.bat`.

---

### Linux / macOS (using `update.py`)

1. Stop Copycord (Ctrl+C in the terminal where `copycord_linux.sh` is running).
2. From the Copycord folder, run:

   ```bash
   cd /path/to/copycord
   python3 update.py
   ```

3. The updater will:

   - Check your current version in `code/.version`
   - Compare with the latest GitHub tag
   - Download new code if needed
   - Update `venvs/` dependencies
   - Rebuild the frontend

4. When it finishes:

   - If it says the update completed, restart Copycord with:

     ```bash
     ./copycord_linux.sh
     ```

---

## Environment variables
- The env file is fully controllable and can be found inside the /code folder in your Copycord directory. Here you can modify variables like password, ports, etc.

---

## Troubleshooting

- **No Python found / Python error**  
  - Install Python 3.10+.
  - On Windows, ensure it’s added to PATH or use the “py” launcher.
- **npm not found**  
  - Install Node.js (which includes npm).
- **“Could not find `code/` directory” in updater**  
  - Make sure you are running `Update.exe` / `update.py` from the same folder where `code/` exists.
- **Port already in use (8080)**  
  - Edit `.env` (inside `code/`) and change `ADMIN_PORT`.

---

