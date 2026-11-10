"""ConfigLoader reads settings.yaml into the typed configs."""

import logging

import pytest

from common.enums import ExecutionType
from config.loader import ConfigLoader


@pytest.mark.parametrize(
    ("setting", "expected"),
    [("execution_type: gcp", ExecutionType.GCP), ("logging: {}", ExecutionType.LOCAL)],
    ids=["set", "missing"],
)
def test_execution_type_comes_from_settings(tmp_path, setting, expected):
    path = tmp_path / "settings.yaml"
    path.write_text(setting, encoding="utf-8")

    assert ConfigLoader(path).get_execution_type() == expected


def test_an_unknown_execution_type_falls_back_to_local(tmp_path, monkeypatch, caplog):
    # basicConfig(force=True) would remove pytest's log capture handler; logging setup is not under test.
    monkeypatch.setattr(ConfigLoader, "_setup_logging", lambda self: None)
    path = tmp_path / "settings.yaml"
    path.write_text("execution_type: kubernetes", encoding="utf-8")
    loader = ConfigLoader(path)

    with caplog.at_level(logging.WARNING, logger="config.loader"):
        assert loader.get_execution_type() == ExecutionType.LOCAL

    assert "Unknown execution type 'kubernetes'" in caplog.text
