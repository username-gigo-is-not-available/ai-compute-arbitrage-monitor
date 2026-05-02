FROM us-central1-docker.pkg.dev/cloud-dataproc/spark/dataproc_2.2

USER root

WORKDIR /app

RUN conda install -y python=3.11 && \
    conda clean -afy \

RUN pip install --no-cache-dir pyyaml pydantic python-dotenv

COPY config/settings.yaml config/settings.yaml
COPY src/refine/  src/refine/
COPY src/config/  src/config/
COPY src/common/  src/common/

ENV PYTHONPATH="/app/src:${PYTHONPATH}"
ENV SETTINGS_PATH="/app/config/settings.yaml"
ENV PYSPARK_PYTHON=/opt/conda/bin/python


USER $DATAPROC_USER

