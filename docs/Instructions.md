<p align="left">
  <img src="../logo/logo.png" alt="Copycord Logo" width="100"/>
</p>

# How to Run Copycord with Docker

This guide explains how to install Docker and Docker Compose and run Copycord.


---

## Step 1: Install Docker and Docker Compose

### Windows
1. Download and install **Docker Desktop for Windows** from the official site:
   https://www.docker.com/products/docker-desktop

2. Follow the installation instructions. After installation, Docker and Docker Compose will be available via Command Prompt or PowerShell.

3. You may need to restart your computer after installation.

---

### Linux (Ubuntu/Debian)
1. Open a terminal.

2. Run the following command to install Docker and Docker Compose in one step:

```bash
curl -fsSL https://get.docker.com | sh
```

5. Verify installation:

```bash
docker --version
docker compose version
```

---

## Step 2: Configure the Environment

- Update your `.env` file and `docker-compose.yml` with your tokens and guild IDs.
- Refer to the `README.md` in the repository for full configuration details.

---

## Step 4: Start Copycord
In your terminal, navigate to the Copycord directory you created:

```
docker compose up -d
```

This will start both the client and server containers.
---

## Step 5: Monitor Logs (optional)

```
docker compose logs -f
```

This command shows live logs from the running containers. If you are on Windows, simply open the Docker Desktop app to view logs.

---

## Stopping Copycord

To stop the containers:

```
docker compose down
```

Or on Windows, use the Docker Desktop app to stop the containers.
