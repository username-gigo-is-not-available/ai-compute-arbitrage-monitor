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
"""

import importlib.util
import itertools
import logging
import subprocess
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
EXPECTED_BUCKET = "ai-compute-arbitrage-monitor-bucket"
EXPECTED_PROJECT = "graphic-mission-505412-j7"
EXPECTED_DATASET = "ai_compute_arbitrage_monitor_dataset"

_script_ids = itertools.count(1)


def _load_script(name: str) -> ModuleType:
    """Import a standalone script (scripts/ is not a package). Executes load_dotenv().

    Each load gets a unique module name, so tests never share a script's module state.
    """
    spec = importlib.util.spec_from_file_location(f"_deploy_script_{next(_script_ids):03d}", SCRIPTS_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """The repo root as working directory and every variable the scripts read; all undone after the test."""
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setenv("SETTINGS_PATH", str(REPO_ROOT / "config" / "settings.yaml"))
    monkeypatch.setenv("GCS_BUCKET_NAME", EXPECTED_BUCKET)
    monkeypatch.setenv("GCP_PROJECT_ID", EXPECTED_PROJECT)
    monkeypatch.setenv("BQ_DATASET_NAME", EXPECTED_DATASET)
    return monkeypatch


def test_dataproc_builds_correct_gcs_destinations(env) -> None:
    """Valid packages + bucket set -> exactly the three jobs/ blobs, correct gs:// path."""
    script = _load_script("package_dataproc_modules")
    calls: list[tuple[str, str]] = []
    env.setattr(script, "gcs_cp", lambda src, dst: calls.append((src, dst)))
    env.setattr(script, "zip_packages", lambda _packages: None)
    script.main()
    assert calls == [
        ("src/lakehouse/", f"gs://{EXPECTED_BUCKET}/jobs/lakehouse/"),
        ("src/refine/", f"gs://{EXPECTED_BUCKET}/jobs/refine/"),
        ("modules.zip", f"gs://{EXPECTED_BUCKET}/jobs/modules.zip"),
        ("config/settings.yaml", f"gs://{EXPECTED_BUCKET}/jobs/config/settings.yaml"),
    ]


def test_composer_builds_correct_gcs_destinations(env) -> None:
    """Valid packages + bucket set -> dags/ root + one dir per package + settings.yaml."""
    script = _load_script("sync_composer_modules")
    calls: list[tuple[str, str]] = []
    env.setattr(script, "gcs_cp", lambda src, dst: calls.append((src, dst)))
    script.main()
    assert calls == [
        ("infra/airflow/dags/*", f"gs://{EXPECTED_BUCKET}/dags/"),
        ("src/common/*", f"gs://{EXPECTED_BUCKET}/dags/common/"),
        ("src/config/*", f"gs://{EXPECTED_BUCKET}/dags/config/"),
        ("src/ingest/*", f"gs://{EXPECTED_BUCKET}/dags/ingest/"),
        ("config/settings.yaml", f"gs://{EXPECTED_BUCKET}/dags/config/settings.yaml"),
    ]


def test_missing_bucket_env_raises_keyerror(env) -> None:
    """GCS_BUCKET_NAME unset -> KeyError naming the var (no silent degrade)."""
    script = _load_script("package_dataproc_modules")
    env.delenv("GCS_BUCKET_NAME")
    env.setattr(script, "gcs_cp", lambda src, dst: None)
    env.setattr(script, "zip_packages", lambda _packages: None)
    with pytest.raises(KeyError) as raised:
        script.main()
    assert raised.value.args == ("GCS_BUCKET_NAME",)


def test_missing_package_dir_exits_nonzero(env, tmp_path) -> None:
    """Referenced package dir missing under src/ -> exits(1), never calls gcloud."""
    script = _load_script("package_dataproc_modules")
    env.chdir(tmp_path)
    with pytest.raises(SystemExit) as raised:
        script.main()
    assert raised.value.code == 1


def test_gcs_cp_issues_expected_gcloud_command(monkeypatch) -> None:
    """gcs_cp() shells out to `gcloud storage cp -r <src> <dst>`."""
    script = _load_script("package_dataproc_modules")
    commands: list[list[str]] = []
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kwargs: commands.append(cmd))
    script.gcs_cp("src/refine/", f"gs://{EXPECTED_BUCKET}/jobs/refine/")
    assert commands == [["gcloud", "storage", "cp", "-r", "src/refine/", f"gs://{EXPECTED_BUCKET}/jobs/refine/"]]


class _FakeCatalog:
    """Stands in for the Iceberg REST catalog: one table per namespace; drops are recorded into ``calls``
    as ``["drop_table", "<ns>.<table>"]`` so their order relative to gcloud commands is asserted too."""

    def __init__(self, calls: list[list[str]]):
        self.calls = calls

    def list_tables(self, namespace: str) -> list[tuple[str, str]]:
        return [(namespace, "t")]

    def drop_table(self, identifier: tuple[str, str]) -> None:
        self.calls.append(["drop_table", ".".join(identifier)])


def _expected_stage(stage: str) -> list[list[str]]:
    """Per namespace: drop its tables from the catalog, then delete its GCS prefix (orphans included)."""
    expected: list[list[str]] = []
    for kind in ("sources", "seeds"):
        ns = f"{stage}_{kind}"
        expected.append(["drop_table", f"{ns}.t"])
        expected.append(["gcloud", "storage", "rm", "-r", f"gs://{EXPECTED_BUCKET}/{ns}/**"])
    return expected


GOLD_DROP = ["bq", "rm", "-r", "-f", "-d", f"{EXPECTED_PROJECT}:{EXPECTED_DATASET}"]


@pytest.fixture
def purge(env) -> tuple[ModuleType, list[list[str]]]:
    """purge_data with every gcloud/bq command and catalog drop recorded instead of run, in the order issued."""
    script = _load_script("purge_data")
    calls: list[list[str]] = []
    env.setattr(subprocess, "run", lambda cmd, **kwargs: calls.append(list(cmd)))
    env.setattr("shutil.which", lambda _name: None)
    env.setattr(script, "open_catalog", lambda: _FakeCatalog(calls))
    # basicConfig(force=True) would remove pytest's log capture handler; logging setup is not under test.
    env.setattr(script, "_setup_logging", lambda *_args: None)
    return script, calls


def _run_purge(env, script, *args: str, answer: str | None = None) -> int | None:
    """Run purge_data.main() with ``args``; ``answer`` is typed at the prompt. Returns the exit code, or None."""
    env.setattr("sys.argv", ["purge_data.py", *args])
    if answer is not None:
        env.setattr("builtins.input", lambda _prompt: answer)
    try:
        script.main()
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1
    return None


@pytest.mark.parametrize("args", [[], ["--bronze", "--silver"]], ids=["no stage", "two stages"])
def test_purge_without_exactly_one_stage_is_a_usage_error(env, purge, args) -> None:
    """No stage flag, or two -> argparse usage error, exit 2, nothing purged."""
    script, calls = purge
    assert _run_purge(env, script, *args, "--confirm") == 2
    assert calls == []


def test_purge_bronze_confirm_drops_tables_then_deletes_namespace_prefixes(env, purge) -> None:
    """--bronze --confirm -> per Bronze namespace: drop Iceberg tables, then `gcloud storage rm` its prefix."""
    script, calls = purge
    assert _run_purge(env, script, "--bronze", "--confirm") is None
    assert calls == _expected_stage("bronze")


def test_purge_bucket_with_trailing_slash_is_stripped(env, purge) -> None:
    """GCS_BUCKET_NAME with a trailing slash -> no double slash in the gs:// target."""
    script, calls = purge
    env.setenv("GCS_BUCKET_NAME", f"{EXPECTED_BUCKET}/")
    _run_purge(env, script, "--bronze", "--confirm")
    assert calls == _expected_stage("bronze")


def test_purge_gold_confirm_issues_expected_bq_command(env, purge) -> None:
    """--gold --confirm -> exactly `bq rm -r -f -d <project>:<dataset>`."""
    script, calls = purge
    assert _run_purge(env, script, "--gold", "--confirm") is None
    assert calls == [GOLD_DROP]


def test_purge_all_confirm_issues_expected_commands_in_order(env, purge) -> None:
    """--all --confirm -> bronze, then silver, then gold dataset drop."""
    script, calls = purge
    assert _run_purge(env, script, "--all", "--confirm") is None
    assert calls == [*_expected_stage("bronze"), *_expected_stage("silver"), GOLD_DROP]


def test_purge_declined_prompt_exits_nonzero_and_purges_nothing(env, purge) -> None:
    """No --confirm + 'no' answer -> exit 1, nothing purged."""
    script, calls = purge
    assert _run_purge(env, script, "--bronze", answer="no") == 1
    assert calls == []


def test_purge_confirmed_interactively_executes(env, purge) -> None:
    """No --confirm + 'yes' answer -> proceeds with the delete."""
    script, calls = purge
    assert _run_purge(env, script, "--silver", answer="yes") is None
    assert calls == _expected_stage("silver")


def test_purge_prompt_eof_exits_nonzero_and_purges_nothing(env, purge) -> None:
    """No --confirm + stdin EOF -> exit 1, nothing purged."""
    script, calls = purge

    def raise_eof(_prompt):
        raise EOFError

    env.setattr("builtins.input", raise_eof)
    assert _run_purge(env, script, "--bronze") == 1
    assert calls == []


def test_purge_subprocess_failure_exits_nonzero(env, purge) -> None:
    """gcloud failure -> the script exits 1."""
    script, _calls = purge

    def fail(cmd, **kwargs):
        raise subprocess.CalledProcessError(returncode=1, cmd=cmd)

    env.setattr(subprocess, "run", fail)
    assert _run_purge(env, script, "--bronze", "--confirm") == 1


def test_purge_uses_logging_not_print(env, purge, caplog, capsys) -> None:
    """Progress goes through the logging module, never print()."""
    script, calls = purge
    caplog.set_level(logging.INFO)
    _run_purge(env, script, "--bronze", "--confirm")
    assert calls == _expected_stage("bronze")
    messages = [r.getMessage() for r in caplog.records]
    assert any("bronze" in m for m in messages), messages
    assert "Purge complete." in messages
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(("variable", "stage"), [("GCP_PROJECT_ID", "--bronze"), ("BQ_DATASET_NAME", "--gold")])
def test_purge_missing_identity_env_raises_keyerror(env, purge, variable, stage) -> None:
    """GCP_PROJECT_ID or BQ_DATASET_NAME unset -> KeyError naming the var (no silent degrade)."""
    script, _calls = purge
    env.delenv(variable)
    env.setattr("sys.argv", ["purge_data.py", stage, "--confirm"])
    with pytest.raises(KeyError) as raised:
        script.main()
    assert raised.value.args == (variable,)


def test_purge_missing_settings_exits_nonzero(env, purge, tmp_path) -> None:
    """SETTINGS_PATH points at a missing settings.yaml -> exit 1, nothing purged."""
    script, calls = purge
    env.setenv("SETTINGS_PATH", str(tmp_path / "no-such-settings.yaml"))
    assert _run_purge(env, script, "--bronze", "--confirm") == 1
    assert calls == []
