import os
import subprocess
import sys
from pathlib import Path

import yaml


def load_settings() -> dict:
    path = Path(os.getenv("SETTINGS_PATH", "config/settings.yaml"))
    with open(path) as f:
        return yaml.safe_load(f)


def zip_packages(packages: list[str]) -> None:
    print(f"Packaging {packages} → modules.zip")
    try:
        subprocess.run(["zip", "-r", "../modules.zip", *packages], cwd="src", check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: zip failed — {e}", file=sys.stderr)
        sys.exit(1)


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
    packages: list[str] = cfg["gcp"]["dataproc"]["runtime_packages"]

    if not packages:
        print("ERROR: gcp.dataproc.runtime_packages is empty in settings.yaml", file=sys.stderr)
        sys.exit(1)

    missing = [p for p in packages if not Path(f"src/{p}").is_dir()]
    if missing:
        print(f"ERROR: runtime_packages references missing directories under src/: {missing}", file=sys.stderr)
        sys.exit(1)

    dst_jobs = f"gs://{bucket}/jobs/"

    zip_packages(packages)

    gcs_cp("src/refine/", f"{dst_jobs}refine/")
    gcs_cp("modules.zip", f"{dst_jobs}modules.zip")
    gcs_cp("config/settings.yaml", f"{dst_jobs}config/settings.yaml")

    print("Dataproc sync complete — entrypoints + modules.zip + settings.")


if __name__ == "__main__":
    main()