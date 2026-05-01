FROM us-central1-docker.pkg.dev/cloud-dataproc/spark/dataproc_2.2

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync \
    --frozen \
    --no-dev \
    --no-install-project \
    --python /usr/bin/python3

COPY config/settings.yaml config/settings.yaml
COPY src/refine/  src/refine/
COPY src/config/  src/config/
COPY src/common/  src/common/

ENV PYTHONPATH="/src:${PYTHONPATH}"

USER $DATAPROC_USER

