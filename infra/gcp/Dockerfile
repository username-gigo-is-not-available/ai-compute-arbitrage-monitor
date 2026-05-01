FROM us-central1-docker.pkg.dev/cloud-dataproc/spark/dataproc_2.2

USER root

WORKDIR /app

RUN pip install --no-cache-dir pyyaml pydantic python-dotenv

COPY config/settings.yaml config/settings.yaml
COPY src/refine/  src/refine/
COPY src/config/  src/config/
COPY src/common/  src/common/

ENV PYTHONPATH="/app/src:${PYTHONPATH}"
ENV SETTINGS_PATH="/app/config/settings.yaml"


USER $DATAPROC_USER

