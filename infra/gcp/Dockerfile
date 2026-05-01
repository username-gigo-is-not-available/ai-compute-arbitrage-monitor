FROM us-central1-docker.pkg.dev/cloud-dataproc/dataproc-containers/dataproc-container:2.2

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync \
    --frozen \
    --no-dev \
    --no-install-project \
    --python /usr/bin/python3

COPY src/refine/ /app/refine/
COPY src/config/ /app/config/
COPY src/common/ /app/common/

ENV PYTHONPATH="/app:${PYTHONPATH}"

USER $DATAPROC_USER
