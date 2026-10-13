

FROM node:20-slim AS webbuild
WORKDIR /frontend

COPY src/copycord/admin/frontend/package*.json ./
RUN --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-${TARGETPLATFORM} npm ci

COPY src/copycord/admin/frontend/ .
RUN --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-${TARGETPLATFORM} npm run build


FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VENV_BASE=/opt/venvs \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=60 \
    DATA_DIR=/data \
    LOG_LEVEL=info \
    ADMIN_PORT=8080 \
    DASHBOARD_HOST=0.0.0.0


RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETPLATFORM} \
    apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# One venv for admin, server and client: the client runs on the in-repo
# selfclient, so nothing installs a second `discord` package and the
# requirements no longer conflict.
RUN python -m venv /opt/venvs/copycord

# Installed the same way as a native install: pyproject.toml is the one source
# for dependencies and packages. The built frontend goes where hatch_build.py
# picks it up and ships it as admin/static.
WORKDIR /src
COPY pyproject.toml hatch_build.py README.md LICENSE ./
COPY src/ ./src/
COPY --from=webbuild /frontend/dist/ ./src/copycord/admin/frontend/dist/

RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked,id=pip-copycord-${TARGETPLATFORM} \
    /opt/venvs/copycord/bin/pip install --upgrade pip \
    && /opt/venvs/copycord/bin/pip install /src \
    && rm -rf /src

WORKDIR /app
COPY entrypoint.sh /entrypoint.sh

RUN sed -i 's/\r$//' /entrypoint.sh \
    && sed -i '1s/^\xEF\xBB\xBF//' /entrypoint.sh \
    && chmod +x /entrypoint.sh

ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
