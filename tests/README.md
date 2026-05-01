# Copycord Tests

## Prerequisites

- Python 3.11+
- Node.js 20+ (for frontend build check only)

## Install dependencies

```bash
pip install pytest pytest-asyncio -r tests/requirements-test.txt
```

## Running tests

**All tests:**

```bash
PYTHONPATH=code pytest tests/ -v
```

**DB unit tests only:**

```bash
PYTHONPATH=code pytest tests/test_db.py -v
```

**API endpoint tests only:**

```bash
PYTHONPATH=code pytest tests/test_api.py -v
```

## Linting

Install [ruff](https://docs.astral.sh/ruff/):

```bash
pip install ruff
```

Run the linter:

```bash
ruff check code/
```

Syntax check all Python files:

```bash
find code/ -name '*.py' -print0 | xargs -0 python -m py_compile
```

## What the tests cover

### `test_db.py` (64 tests)

Tests the `DBManager` class directly against a temporary SQLite database. No network or Discord connections needed.

- Schema creation and idempotent re-init
- App config get/set
- Version and settings tracking
- Guild, category, channel, role, emoji, sticker, and thread mapping CRUD
- Channel ID resolution
- Blocked keywords
- Event log CRUD, filtering, search, pagination, and bulk operations
- Message mappings
- Cascade delete (deleting a guild mapping cleans child tables)

### `test_api.py` (15 tests)

Tests the FastAPI admin endpoints using `httpx.AsyncClient`. WebSocket control commands are mocked so no running Server/Client containers are needed.

- `GET /health`
- Event logs API (create, list, filter, delete, bulk delete, clear)
- Guild mappings API (list, delete, toggle status)
- `GET /version`

## CI / GitHub workflows

### `ci.yml` — Runs on every PR and push to `main`

- **Lint & syntax check** — ruff + py_compile on all Python files
- **DB unit tests** — pytest `test_db.py`
- **API endpoint tests** — pytest `test_api.py`
- **Frontend build** — `npm ci && npm run build` in `code/admin/frontend/`

### `docker-build-test.yml` — Manual trigger

Builds the Docker image without pushing to verify the Dockerfile is valid. Trigger it from the GitHub Actions tab via "Run workflow".

You can optionally choose the platform (`linux/amd64`, `linux/arm64`, or both).

### `dependency-audit.yml` — Weekly (Monday 09:00 UTC) + manual trigger

- **Python** — runs `pip-audit` against each component's `requirements.txt` (admin, server, client)
- **npm** — runs `npm audit` on the frontend

Alerts you if any dependency has a known vulnerability.
