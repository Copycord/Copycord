# Contributing — Quick Build & Test Guide

This guide provides a **minimal workflow** for contributors. It explains:
- How to build the **frontend** after making changes.
- How to test code changes in the **server** or **client** by using a **local Docker build**.

---

## Prerequisites
- **Docker** and **Docker Compose**
- **Node.js** + **npm** (for frontend build only)
- A cloned repository:
  ```bash
  git clone https://github.com/Copycord/Copycord.git
  cd Copycord
  ```

---

## 1) Build the Frontend (Admin UI)

> Do this **whenever** you change code in `code/admin/frontend`.

```bash
cd code/admin/frontend
npm ci          # Run once after cloning repo, or again if dependencies have changed
npm run build   # Compiles the frontend into /dist for production
```

Notes:
- Always run `npm run build` before opening a PR to ensure the build passes.

---

## 2) Test Server/Client Changes with Local Docker Build

When you change code in `client`, `server`, or any other non-frontend directory, rebuild the local Docker images and restart the stack with Compose.

From the **repo root** (where local `docker-compose.yml` is located):

### Build and run everything
```bash
docker compose build        # builds Copycord image locally
docker compose up -d        # Starts all Copycord services locally
```

### View logs
```bash
docker compose logs -f         # follow logs for all services
docker compose logs -f server  # follow logs for server only
```

### Stop and clean up
```bash
docker compose down            # stops all containers
```
---

## Quick Checklists

**Frontend changes**
- [ ] Modified files under `code/admin/frontend`
- [ ] Ran `npm run build` with no errors
- [ ] (Optional) Added screenshots for UI changes

**Server/Client changes**
- [ ] Rebuilt images: `docker compose build`
- [ ] Restarted services: `docker compose up -d`
- [ ] Verified logs and functionality

---

## TL;DR

```bash
# Frontend (Admin UI)
cd code/admin/frontend
npm ci          # Only required on first setup or when dependencies change
npm run build

# Test server/client changes
docker compose up -d
```
