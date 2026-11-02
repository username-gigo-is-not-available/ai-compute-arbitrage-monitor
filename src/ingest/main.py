import sys

from ingest.registry import run_ingest

# python -m ingest.main <dataset> [scheduled_at], e.g. python -m ingest.main compute_offers 2026-11-02T00:00:00+00:00
if __name__ == "__main__":
    run_ingest(*sys.argv[1:])
