"""ConfigLoader reads settings.yaml into the typed configs."""

import pytest
import yaml

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


def test_an_unknown_execution_type_fails(tmp_path):
    path = tmp_path / "settings.yaml"
    path.write_text("execution_type: gpc", encoding="utf-8")

    with pytest.raises(ValueError, match="Unknown execution_type 'gpc'.*local, gcp"):
        ConfigLoader(path).get_execution_type()


def test_a_missing_settings_file_fails_at_once(tmp_path):
    with pytest.raises(FileNotFoundError, match="no-such-settings.yaml"):
        ConfigLoader(tmp_path / "no-such-settings.yaml")


@pytest.mark.parametrize("content", ["", "just a string"], ids=["empty", "not-a-mapping"])
def test_an_empty_or_non_mapping_settings_file_fails_at_once(tmp_path, content):
    path = tmp_path / "settings.yaml"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(TypeError, match="empty or not a YAML mapping"):
        ConfigLoader(path)


def test_broken_yaml_fails_at_once(tmp_path):
    path = tmp_path / "settings.yaml"
    path.write_text("http: [unclosed", encoding="utf-8")

    with pytest.raises(yaml.YAMLError):
        ConfigLoader(path)
