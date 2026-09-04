"""Cost-free tests for the GCS deployment scripts.

The only thing that touches GCP in scripts/package_dataproc_modules.py and
scripts/sync_composer_modules.py is ``gcs_cp()``, which shells out to
``gcloud storage cp``. These tests stub ``gcs_cp`` / ``zip_packages`` so the
full control flow runs to completion WITHOUT provisioning Composer/Dataproc or
hitting GCS. They verify that each script:

  * resolves the bucket from the environment (via load_dotenv() / os.environ),
  * fails loudly (KeyError) when GCS_BUCKET_NAME is unset,
  * refuses to run when a referenced package dir is missing under src/,
  * builds the correct gs:// destinations,
  * issues the expected ``gcloud storage cp`` command.

Run directly (no deps added, no GCP touched)::

    uv run python tests/test_deploy_scripts.py

Run under pytest (optional, once the dev-extra is installed)::

    uv run --extra dev pytest tests/test_deploy_scripts.py
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import traceback
from contextlib import contextmanager
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
SETTINGS_PATH = str(REPO_ROOT / "config" / "settings.yaml")
EXPECTED_BUCKET = "ai-compute-arbitrage-monitor-bucket"
EXPECTED_PROJECT = "graphic-mission-505412-j7"

_ENV_KEYS = ("SETTINGS_PATH", "GCS_BUCKET_NAME", "GCP_PROJECT_ID")
_ENV_SAVED = {k: os.environ.get(k) for k in _ENV_KEYS}


def _restore_env() -> None:
    for k in _ENV_KEYS:
        if _ENV_SAVED[k] is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = _ENV_SAVED[k]


def _set_env(bucket: str | None = EXPECTED_BUCKET) -> None:
    os.environ["SETTINGS_PATH"] = SETTINGS_PATH
    os.environ["GCP_PROJECT_ID"] = EXPECTED_PROJECT
    if bucket is None:
        os.environ.pop("GCS_BUCKET_NAME", None)
    else:
        os.environ["GCS_BUCKET_NAME"] = bucket


@contextmanager
def _chdir(path: Path):
    cwd = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(cwd)


@contextmanager
def _patched(module, **attrs):
    originals = {k: getattr(module, k) for k in attrs}
    for k, v in attrs.items():
        setattr(module, k, v)
    try:
        yield
    finally:
        for k, v in originals.items():
            setattr(module, k, v)


_SCRIPT_COUNTER = [0]


def _load_script(name: str):
    """Import a standalone script (scripts/ is not a package). Executes load_dotenv().

    ``name`` is the real script filename (no extension); each invocation gets a
    unique module name so tests never collide in ``sys.modules``.
    """
    _SCRIPT_COUNTER[0] += 1
    module_name = f"_deploy_script_{_SCRIPT_COUNTER[0]:03d}"
    spec = importlib.util.spec_from_file_location(module_name, SCRIPTS_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_dataproc_builds_correct_gcs_destinations() -> None:
    """Valid packages + bucket set -> exactly the three jobs/ blobs, correct gs:// path."""
    script = _load_script("package_dataproc_modules")
    calls: list[tuple[str, str]] = []
    try:
        _set_env()
        with _chdir(REPO_ROOT), _patched(
            script,
            gcs_cp=lambda src, dst: calls.append((src, dst)),
            zip_packages=lambda _packages: None,
        ):
            script.main()
    finally:
        _restore_env()
    expected = [
        ("src/refine/", f"gs://{EXPECTED_BUCKET}/jobs/refine/"),
        ("modules.zip", f"gs://{EXPECTED_BUCKET}/jobs/modules.zip"),
        ("config/settings.yaml", f"gs://{EXPECTED_BUCKET}/jobs/config/settings.yaml"),
    ]
    assert calls == expected, calls


def test_composer_builds_correct_gcs_destinations() -> None:
    """Valid packages + bucket set -> dags/ root + one dir per package + settings.yaml."""
    script = _load_script("sync_composer_modules")
    calls: list[tuple[str, str]] = []
    try:
        _set_env()
        with _chdir(REPO_ROOT), _patched(
            script, gcs_cp=lambda src, dst: calls.append((src, dst))
        ):
            script.main()
    finally:
        _restore_env()
    expected = [
        ("infra/airflow/dags/*", f"gs://{EXPECTED_BUCKET}/dags/"),
        ("src/common/*", f"gs://{EXPECTED_BUCKET}/dags/common/"),
        ("src/config/*", f"gs://{EXPECTED_BUCKET}/dags/config/"),
        ("src/ingest/*", f"gs://{EXPECTED_BUCKET}/dags/ingest/"),
        ("src/serializers/*", f"gs://{EXPECTED_BUCKET}/dags/serializers/"),
        ("config/settings.yaml", f"gs://{EXPECTED_BUCKET}/dags/config/settings.yaml"),
    ]
    assert calls == expected, calls


def test_missing_bucket_env_raises_keyerror() -> None:
    """GCS_BUCKET_NAME unset -> KeyError naming the var (no silent degrade)."""
    os.environ.pop("GCS_BUCKET_NAME", None)
    script = _load_script("package_dataproc_modules")
    try:
        _set_env(bucket=None)
        with _chdir(REPO_ROOT), _patched(
            script, gcs_cp=lambda src, dst: None, zip_packages=lambda _p: None
        ):
            script.main()
        raise AssertionError("expected KeyError('GCS_BUCKET_NAME')")
    except KeyError as e:
        assert e.args == ("GCS_BUCKET_NAME",), e.args
    finally:
        _restore_env()


def test_missing_package_dir_exits_nonzero() -> None:
    """Referenced package dir missing under src/ -> exits(1), never calls gcloud."""
    script = _load_script("package_dataproc_modules")
    try:
        _set_env()
        with tempfile.TemporaryDirectory() as tmp, _chdir(Path(tmp)):
            try:
                script.main()
            except SystemExit as e:
                assert e.code == 1, e.code
            else:
                raise AssertionError("expected SystemExit(1) for missing package dir")
    finally:
        _restore_env()


def test_gcs_cp_issues_expected_gcloud_command() -> None:
    """gcs_cp() shells out to `gcloud storage cp -r <src> <dst>`."""
    script = _load_script("package_dataproc_modules")
    captured: dict[str, object] = {}
    real_run = subprocess.run

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return None

    try:
        with _patched(subprocess, run=fake_run):
            script.gcs_cp("src/refine/", f"gs://{EXPECTED_BUCKET}/jobs/refine/")
    finally:
        _restore_env()
        subprocess.run = real_run
    assert captured.get("cmd") == [
        "gcloud",
        "storage",
        "cp",
        "-r",
        "src/refine/",
        f"gs://{EXPECTED_BUCKET}/jobs/refine/",
    ]


# ---------------------------------------------------------------------------
# Standalone runner (also collectable by pytest: the test_* functions above)
# ---------------------------------------------------------------------------

_TESTS = [
    test_dataproc_builds_correct_gcs_destinations,
    test_composer_builds_correct_gcs_destinations,
    test_missing_bucket_env_raises_keyerror,
    test_missing_package_dir_exits_nonzero,
    test_gcs_cp_issues_expected_gcloud_command,
]


def _run_standalone() -> int:
    # The scripts print a "→" (U+2192) which the Windows cp1252 console cannot
    # encode; force UTF-8 output so the command-logic assertions are what run.
    # (Linux/GitHub Actions already default to UTF-8.)
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass
    failures = 0
    for t in _TESTS:
        try:
            t()
        except Exception:  # noqa: BLE001 - report all failures in the standalone run
            failures += 1
            print(f"FAIL  {t.__name__}")
            traceback.print_exc()
        else:
            print(f"PASS  {t.__name__}")
    total = len(_TESTS)
    print(f"\n{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(_run_standalone())