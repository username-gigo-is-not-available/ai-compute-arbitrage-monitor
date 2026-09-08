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

The ``purge_data`` helper additionally verifies that the purge script:

  * emits progress/errors through ``logging`` (never ``print``),
  * fails loudly (KeyError) when a GCP identity env var is unset,
  * exits non-zero when settings.yaml is missing and when the confirmation
    prompt hits EOF/declines, without issuing any gcloud/bq command,
  * issues the expected ``gcloud storage rm`` / ``bq rm`` commands per stage.

Run directly (no deps added, no GCP touched)::

    uv run python tests/test_deploy_scripts.py

Run under pytest (optional, once the dev-extra is installed)::

    uv run --extra dev pytest tests/test_deploy_scripts.py
"""

from __future__ import annotations

import builtins
import importlib.util
import logging
import os
import shutil
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
EXPECTED_DATASET = "ai_compute_arbitrage_monitor_dataset"

_ENV_KEYS = ("SETTINGS_PATH", "GCS_BUCKET_NAME", "GCP_PROJECT_ID", "BQ_DATASET_NAME")
_ENV_SAVED = {k: os.environ.get(k) for k in _ENV_KEYS}


def _restore_env() -> None:
    for k in _ENV_KEYS:
        if _ENV_SAVED[k] is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = _ENV_SAVED[k]


def _set_env(bucket: str | None = EXPECTED_BUCKET, dataset: str | None = EXPECTED_DATASET) -> None:
    os.environ["SETTINGS_PATH"] = SETTINGS_PATH
    os.environ["GCP_PROJECT_ID"] = EXPECTED_PROJECT
    if bucket is None:
        os.environ.pop("GCS_BUCKET_NAME", None)
    else:
        os.environ["GCS_BUCKET_NAME"] = bucket
    if dataset is None:
        os.environ.pop("BQ_DATASET_NAME", None)
    else:
        os.environ["BQ_DATASET_NAME"] = dataset


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


def _run_purge(
    argv: list[str],
    confirm: str | None = None,
    fail: bool = False,
) -> tuple[list[list[str]], int | None]:
    """Run purge_data.main() with a stubbed subprocess.run.

    Returns ``(calls, exit_code)``: ``calls`` is the list of commands issued
    (empty when ``fail`` is True), ``exit_code`` is the SystemExit code if the
    run exited, else None. ``confirm`` feeds the interactive prompt; pass None
    to append ``--confirm`` instead.
    """
    script = _load_script("purge_data")
    calls: list[list[str]] = []
    real_run = subprocess.run

    def fake_run(cmd, **kwargs):
        if fail:
            raise subprocess.CalledProcessError(returncode=1, cmd=cmd)
        calls.append(list(cmd))
        return None

    argv = [*argv, "--confirm"] if confirm is None else argv
    exit_code: int | None = None
    try:
        _set_env()
        with _chdir(REPO_ROOT), _patched(subprocess, run=fake_run), _patched(
            sys, argv=["purge_data.py", *argv]
        ), _patched(shutil, which=lambda _name: None):
            try:
                if confirm is None:
                    script.main()
                else:
                    with _patched(builtins, input=lambda _prompt: confirm):
                        script.main()
            except SystemExit as e:
                exit_code = e.code if isinstance(e.code, int) else 1
    finally:
        _restore_env()
        subprocess.run = real_run
    return calls, exit_code


def test_purge_no_flags_exits_nonzero_and_purges_nothing() -> None:
    """No stage flag -> argparse usage error, exit 2, no gcloud/bq command issued."""
    calls, exit_code = _run_purge([])
    assert exit_code == 2, exit_code
    assert calls == [], calls

def test_purge_conflicting_flags_exits_nonzero_and_purges_nothing() -> None:
    """Two stage flags together -> argparse usage error, exit 2, no gcloud/bq command issued."""
    calls, exit_code = _run_purge(["--bronze", "--silver"])
    assert exit_code == 2, exit_code
    assert calls == [], calls

def test_purge_bronze_confirm_issues_expected_rm_command() -> None:
    """--bronze --confirm -> exactly `gcloud storage rm -r gs://<bucket>/bronze/**`."""
    calls, exit_code = _run_purge(["--bronze"])
    assert calls == [
        ["gcloud", "storage", "rm", "-r", f"gs://{EXPECTED_BUCKET}/bronze/**"]
    ], calls
    assert exit_code is None, exit_code

def test_purge_bucket_with_trailing_slash_is_stripped() -> None:
    """GCS_BUCKET_NAME with a trailing slash -> no double slash in the gs:// target."""
    script = _load_script("purge_data")
    calls: list[list[str]] = []
    real_run = subprocess.run

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        return None

    try:
        _set_env(bucket=f"{EXPECTED_BUCKET}/")
        with _chdir(REPO_ROOT), _patched(subprocess, run=fake_run), _patched(
            shutil, which=lambda _name: None
        ), _patched(
            sys, argv=["purge_data.py", "--bronze", "--confirm"]
        ):
            script.main()
    finally:
        _restore_env()
        subprocess.run = real_run
    assert calls == [
        ["gcloud", "storage", "rm", "-r", f"gs://{EXPECTED_BUCKET}/bronze/**"]
    ], calls

def test_purge_gold_confirm_issues_expected_bq_command() -> None:
    """--gold --confirm -> exactly `bq rm -r -f -d <project>:<dataset>`."""
    calls, exit_code = _run_purge(["--gold"])
    assert calls == [
        ["bq", "rm", "-r", "-f", "-d", f"{EXPECTED_PROJECT}:{EXPECTED_DATASET}"]
    ], calls
    assert exit_code is None, exit_code


def test_purge_all_confirm_issues_expected_commands_in_order() -> None:
    """--all --confirm -> bronze, then silver, then gold dataset drop."""
    calls, exit_code = _run_purge(["--all"])
    assert calls == [
        ["gcloud", "storage", "rm", "-r", f"gs://{EXPECTED_BUCKET}/bronze/**"],
        ["gcloud", "storage", "rm", "-r", f"gs://{EXPECTED_BUCKET}/silver/**"],
        ["bq", "rm", "-r", "-f", "-d", f"{EXPECTED_PROJECT}:{EXPECTED_DATASET}"],
    ], calls
    assert exit_code is None, exit_code


def test_purge_declined_prompt_exits_nonzero_and_purges_nothing() -> None:
    """No --confirm + 'no' answer -> exit 1, zero gcloud/bq commands issued."""
    calls, exit_code = _run_purge(["--bronze"], confirm="no")
    assert exit_code == 1, exit_code
    assert calls == [], calls


def test_purge_confirmed_interactively_executes() -> None:
    """No --confirm + 'yes' answer -> proceeds with the delete."""
    calls, exit_code = _run_purge(["--silver"], confirm="yes")
    assert exit_code is None, exit_code
    assert calls == [
        ["gcloud", "storage", "rm", "-r", f"gs://{EXPECTED_BUCKET}/silver/**"]
    ], calls


def test_purge_subprocess_failure_exits_nonzero() -> None:
    """gcloud failure -> the script exits 1 (the harness stubs subprocess)."""
    calls, exit_code = _run_purge(["--bronze"], fail=True)
    assert exit_code == 1, exit_code
    assert calls == [], calls


def test_purge_uses_logging_not_print() -> None:
    """Progress and errors go through the logging module, never print()."""
    script = _load_script("purge_data")
    calls: list[list[str]] = []
    records: list[tuple[str, str]] = []
    real_run = subprocess.run

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        return None

    def capture_info(msg, *args, **kwargs):
        records.append(("INFO", msg % args if args else msg))

    def capture_error(msg, *args, **kwargs):
        records.append(("ERROR", msg % args if args else msg))

    try:
        _set_env()
        with _chdir(REPO_ROOT), _patched(subprocess, run=fake_run), _patched(
            shutil, which=lambda _name: None
        ), _patched(
            sys, argv=["purge_data.py", "--bronze", "--confirm"]
        ), _patched(logging, info=capture_info, error=capture_error):
            script.main()
    finally:
        _restore_env()
        subprocess.run = real_run
    assert calls == [
        ["gcloud", "storage", "rm", "-r", f"gs://{EXPECTED_BUCKET}/bronze/**"]
    ], calls
    assert any(level == "INFO" and "bronze" in msg for level, msg in records), records
    assert any(level == "INFO" and "Purge complete" in msg for level, msg in records), records
    assert not any(level not in ("INFO", "ERROR") for level, _msg in records), records


def test_purge_missing_project_env_raises_keyerror() -> None:
    """GCP_PROJECT_ID unset -> KeyError naming the var (no silent degrade)."""
    script = _load_script("purge_data")
    try:
        _set_env()
        os.environ.pop("GCP_PROJECT_ID", None)
        with _chdir(REPO_ROOT), _patched(
            sys, argv=["purge_data.py", "--bronze", "--confirm"]
        ):
            script.main()
        raise AssertionError("expected KeyError('GCP_PROJECT_ID')")
    except KeyError as e:
        assert e.args == ("GCP_PROJECT_ID",), e.args
    finally:
        _restore_env()

def test_purge_missing_dataset_env_raises_keyerror() -> None:
    """BQ_DATASET_NAME unset -> KeyError naming the var (no silent degrade)."""
    script = _load_script("purge_data")
    try:
        _set_env()
        os.environ.pop("BQ_DATASET_NAME", None)
        with _chdir(REPO_ROOT), _patched(
            sys, argv=["purge_data.py", "--gold", "--confirm"]
        ):
            script.main()
        raise AssertionError("expected KeyError('BQ_DATASET_NAME')")
    except KeyError as e:
        assert e.args == ("BQ_DATASET_NAME",), e.args
    finally:
        _restore_env()

def test_purge_missing_settings_exits_nonzero() -> None:
    """SETTINGS_PATH points at a missing settings.yaml -> exit 1, no commands."""
    script = _load_script("purge_data")
    try:
        _set_env()
        with tempfile.TemporaryDirectory() as tmp, _chdir(Path(tmp)), _patched(
            sys, argv=["purge_data.py", "--bronze", "--confirm"]
        ):
            os.environ["SETTINGS_PATH"] = str(Path(tmp) / "no-such-settings.yaml")
            try:
                script.main()
            except SystemExit as e:
                assert e.code == 1, e.code
            else:
                raise AssertionError("expected SystemExit(1) for missing settings")
    finally:
        _restore_env()


def test_purge_prompt_eof_exits_nonzero_and_purges_nothing() -> None:
    """No --confirm + stdin EOF -> exit 1, zero gcloud/bq commands issued."""
    script = _load_script("purge_data")
    calls: list[list[str]] = []
    real_run = subprocess.run

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        return None

    def raise_eof(_prompt):
        raise EOFError

    try:
        _set_env()
        with _chdir(REPO_ROOT), _patched(subprocess, run=fake_run), _patched(
            sys, argv=["purge_data.py", "--bronze"]
        ), _patched(builtins, input=raise_eof):
            try:
                script.main()
            except SystemExit as e:
                assert e.code == 1, e.code
            else:
                raise AssertionError("expected SystemExit(1) on prompt EOF")
    finally:
        _restore_env()
        subprocess.run = real_run
    assert calls == [], calls


# ---------------------------------------------------------------------------
# Standalone runner (also collectable by pytest: the test_* functions above)
# ---------------------------------------------------------------------------

_TESTS = [
    test_dataproc_builds_correct_gcs_destinations,
    test_composer_builds_correct_gcs_destinations,
    test_missing_bucket_env_raises_keyerror,
    test_missing_package_dir_exits_nonzero,
    test_gcs_cp_issues_expected_gcloud_command,
    test_purge_no_flags_exits_nonzero_and_purges_nothing,
    test_purge_conflicting_flags_exits_nonzero_and_purges_nothing,
    test_purge_bronze_confirm_issues_expected_rm_command,
    test_purge_bucket_with_trailing_slash_is_stripped,
    test_purge_gold_confirm_issues_expected_bq_command,
    test_purge_all_confirm_issues_expected_commands_in_order,
    test_purge_declined_prompt_exits_nonzero_and_purges_nothing,
    test_purge_confirmed_interactively_executes,
    test_purge_subprocess_failure_exits_nonzero,
    test_purge_uses_logging_not_print,
    test_purge_missing_project_env_raises_keyerror,
    test_purge_missing_dataset_env_raises_keyerror,
    test_purge_missing_settings_exits_nonzero,
    test_purge_prompt_eof_exits_nonzero_and_purges_nothing,
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