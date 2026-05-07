FROM apache/airflow:3.0.6-python3.12

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        zip \
        openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
COPY pyproject.toml uv.lock /opt/airflow/
RUN pip install --no-cache-dir uv && \
    cd /opt/airflow && \
    uv export --frozen --no-dev --no-install-project -o /tmp/requirements.txt && \
    pip install --no-cache-dir -r /tmp/requirements.txt

RUN curl -fSL https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/4.0.3/gcs-connector-4.0.3-shaded.jar \
    -o /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/gcs-connector-hadoop3-shaded.jar