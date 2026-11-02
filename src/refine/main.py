import sys

from refine.registry import run_refine

# python -m refine.main <dataset>, e.g. python -m refine.main compute_offers. Dataproc runs this file with the
# dataset as its one argument.
if __name__ == "__main__":
    run_refine(*sys.argv[1:])
