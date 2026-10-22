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

# hatch-vcs runs git to compute the version from the checkout.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY pyproject.toml uv.lock hatch_build.py README.md LICENSE ./

# The locked dependencies first, so a new commit does not reinstall them.
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-copycord-${TARGETPLATFORM} \
    uv sync --frozen --no-dev --no-editable --no-install-project

COPY src/ ./src/
COPY --from=webbuild /web/dist/ ./src/copycord/admin/web/dist/
# The git metadata, for a local build. The glob copies nothing when the context
# has no .git (a zip download). This stage is not in the final image.
COPY .gi[t] ./.git/

# The version: the COPYCORD_VERSION build arg when set (CI passes the release
# tag without the v, 4.0.0.3, or a dev version, 4.0.0.4.dev5+g1a2b3c4), else
# computed from .git, else 0+unknown. A clone without tags is 0.1.dev0+g<sha>.
ARG COPYCORD_VERSION=

# One venv for admin, server and client, installed from uv.lock (the same
# versions CI tested). The built SPA goes where hatch_build.py picks it up
# and ships it as admin/web_static.
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked,id=uv-copycord-${TARGETPLATFORM} \
    if [ -n "$COPYCORD_VERSION" ]; then export SETUPTOOLS_SCM_PRETEND_VERSION="$COPYCORD_VERSION"; fi \
    && uv sync --frozen --no-dev --no-editable --reinstall-package copycord


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
# No USER: entrypoint.sh starts as root to chown a ./data folder the root v3
# image left behind, then drops to copycord with setpriv.

ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
