"""ConfigLoader reads settings.yaml into the typed configs."""

import pytest
import yaml
from pydantic import ValidationError

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


def test_a_misspelled_key_in_a_settings_block_fails(tmp_path):
    # Without extra="forbid" the typo would be dropped and tariff_blocks_limit would silently stay 4 (#125)
    path = tmp_path / "settings.yaml"
    evn = {
        "enabled": True,
        "tariff_tiers_url": "https://evn.test",
        "tariff_system_url": "https://evn.test",
        "tariff_blocks_limt": 5,
    }
    path.write_text(yaml.safe_dump({"apis": {"evn": evn}}), encoding="utf-8")

    with pytest.raises(ValidationError, match="tariff_blocks_limt"):
        ConfigLoader(path).get_evn()
