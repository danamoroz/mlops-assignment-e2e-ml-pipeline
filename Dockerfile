FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    docker.io \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /mlops-assignment

# uv-managed Python must not live under /root — pipeline containers run as AIRFLOW_UID.
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python

COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --locked \
 && chmod -R a+rX /opt/uv-python /mlops-assignment/.venv

ENV PATH="/mlops-assignment/.venv/bin:$PATH"

COPY pipeline pipeline/
COPY scripts scripts/

RUN chmod +x scripts/*.sh

# Build: docker build -t mlops-pipeline:latest .
