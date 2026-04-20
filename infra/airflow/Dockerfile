FROM apache/airflow:3.0.6-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends zip && rm -rf /var/lib/apt/lists/*

USER airflow
COPY pyproject.toml uv.lock /opt/airflow/
RUN pip install --no-cache-dir uv && \
    cd /opt/airflow && \
    uv sync --frozen --no-dev --no-install-project && \
    chmod -R 755 /opt/airflow/.venv/bin