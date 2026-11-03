"""lakehouse.setup reads only the table declarations, never the ingestors.

It runs on Dataproc, whose image has pyspark, pyiceberg and the catalog auth libraries but not the ingestors' HTTP
and parsing libraries. A fresh interpreter shows what importing it really loads.
"""

import os
import subprocess
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"


def test_setup_does_not_import_the_ingestors_or_their_libraries():
    script = (
        "import sys, lakehouse.setup; "
        # not tenacity: pyiceberg's REST catalog depends on it, so it comes with pyiceberg
        "print(','.join(m for m in ('ingest.base', 'ingest.registry', 'bs4', 'vastai', 'vastai_sdk') "
        "if m in sys.modules))"
    )
    env = {**os.environ, "PYTHONPATH": str(SRC)}

    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, check=True)

    assert result.stdout.strip() == ""
