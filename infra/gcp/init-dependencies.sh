#!/bin/bash
set -ex

curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"

BUCKET=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/project-bucket)

mkdir -p /opt/gpu_arbitrage
cd /opt/gpu_arbitrage
gsutil cp gs://$BUCKET/env/pyproject.toml .
gsutil cp gs://$BUCKET/env/uv.lock .

/root/.local/bin/uv sync --python /usr/bin/python3 --no-dev