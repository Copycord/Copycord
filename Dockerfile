# Base images are pinned to exact versions so builds are reproducible.
# To bump: change the tags below to the new exact version, and update .nvmrc
# (Node) or .python-version (Python, major.minor) to match; the CI toolchain
# test fails when they disagree. The Node version also lives in
# src/copycord/node_runtime.py.

FROM node:22.12.0-slim AS webbuild
WORKDIR /web

COPY src/copycord/admin/web/package*.json ./
RUN --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-${TARGETPLATFORM} npm ci

COPY src/copycord/admin/web/ .
RUN --mount=type=cache,target=/root/.npm,sharing=locked,id=npm-${TARGETPLATFORM} npm run build


# Builder: installs the locked dependencies and the project into one venv.
FROM python:3.14.0-slim AS pybuild

COPY --from=ghcr.io/astral-sh/uv:0.9.0 /uv /usr/local/bin/uv

ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/copycord \
    UV_PYTHON=/usr/local/bin/python \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /src
COPY pyproject.toml uv.lock hatch_build.py README.md LICENSE ./
COPY src/ ./src/
COPY --from=webbuild /web/dist/ ./src/copycord/admin/web/dist/

# One venv for admin, server and client, installed from uv.lock (the same
# versions CI tested). The built SPA goes where hatch_build.py picks it up
# and ships it as admin/web_static.
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-copycord-${TARGETPLATFORM} \
    uv sync --frozen --no-dev --no-editable


# Runtime: no compiler, no uv, no sources.
FROM python:3.14.0-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VENV_BASE=/opt/venvs \
    DATA_DIR=/data \
    LOG_LEVEL=info \
    ADMIN_PORT=8080 \
    DASHBOARD_HOST=0.0.0.0

# Fixed uid/gid so a bind-mounted ./data can be chowned from the host.
RUN groupadd --gid 1000 copycord \
    && useradd --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin copycord \
    && mkdir -p /data /app \
    && chown copycord:copycord /data /app

COPY --from=pybuild /opt/venvs/copycord /opt/venvs/copycord

WORKDIR /app
COPY --chown=copycord:copycord entrypoint.sh /entrypoint.sh

RUN sed -i 's/\r$//' /entrypoint.sh \
    && sed -i '1s/^\xEF\xBB\xBF//' /entrypoint.sh \
    && chmod +x /entrypoint.sh

VOLUME ["/data"]
USER copycord

ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
