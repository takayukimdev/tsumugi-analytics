# syntax=docker/dockerfile:1
# Shared Cloud Run Job image for both `ingest` and `dbt` (asia-northeast1).

FROM python:3.12-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.13.9 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON_PREFERENCE=only-system

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project


FROM python:3.12-slim-bookworm

RUN groupadd --gid 10001 appuser \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin appuser

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY tracking_plan ./tracking_plan
COPY generator ./generator
COPY ingest ./ingest
COPY runner ./runner
COPY dbt ./dbt
# Cloud Run Jobs authenticate via ADC (service account metadata server), never key files.
COPY dbt/profiles.yml.example ./dbt/profiles.yml

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED="1" \
    DBT_PROFILES_DIR="/app/dbt"

USER appuser

ENTRYPOINT ["python", "-m", "runner"]
