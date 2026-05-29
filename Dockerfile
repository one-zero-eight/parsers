###########################################################
# Builder stage. Build dependencies.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

# Install build dependencies for compiling native extensions (maybe needed only for apple silicon)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app
COPY ./pyproject.toml ./uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev


###########################################################
# Production stage. Copy only runtime deps that were installed in the Builder stage.
FROM python:3.12-slim-bookworm AS production

ENV PYTHONUNBUFFERED=1

# Create user with the name uv
RUN groupadd -g 1500 uv && \
    useradd -m -u 1500 -g uv uv && \
    mkdir -p /app/output && \
    chown -R uv:uv /app

USER uv
WORKDIR /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

COPY --from=builder --chown=uv:uv /app/.venv /app/.venv
COPY --chown=uv:uv . /app

EXPOSE 8000
CMD ["python", "-m", "src", "--period", "3600"]
