import os
import subprocess
import sys
from pathlib import Path

import yaml

def load_settings() -> dict:
    path = Path(os.getenv("SETTINGS_PATH", "config/settings.yaml"))
    with open(path) as f:
        return yaml.safe_load(f)


def gcs_cp(src: str, dst: str) -> None:
    print(f"Syncing {src} → {dst}")
    try:
        subprocess.run(["gcloud", "storage", "cp", "-r", src, dst], check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: gcloud failed for {src} — {e}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    try:
        cfg = load_settings()
    except FileNotFoundError as e:
        print(f"ERROR: could not load settings — {e}", file=sys.stderr)
        sys.exit(1)

    bucket: str = cfg["gcp"]["gcs"]["bucket_name"]
    packages: list[str] = cfg["gcp"]["composer"]["runtime_packages"]

    if not packages:
        print("ERROR: gcp.composer.runtime_packages is empty or missing in settings.yaml", file=sys.stderr)
        sys.exit(1)

    missing = [m for m in packages if not Path(f"src/{m}").is_dir()]
    if missing:
        print(f"ERROR: runtime_packages references missing directories under src/: {missing}", file=sys.stderr)
        sys.exit(1)

    dst_dags = f"gs://{bucket}/dags/"

    gcs_cp("infra/airflow/dags/*", dst_dags)

    for module in packages:
        gcs_cp(f"src/{module}/", f"{dst_dags}{module}/")

    gcs_cp("config/settings.yaml", f"{dst_dags}config/settings.yaml")

    print(f"Composer sync complete — DAGs + {len(packages)} module(s) + settings.")


if __name__ == "__main__":
    main()

