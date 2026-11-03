import sys

from lakehouse.setup import run_setup

# python -m lakehouse.main <dataset>, e.g. python -m lakehouse.main compute_offers. Dataproc runs this file with the
# dataset as its one argument.
if __name__ == "__main__":
    run_setup(*sys.argv[1:])
