from pathlib import Path

import pytest
import yaml

from src.utils import load_config

TEST_CONFIG_PATH = Path(__file__).parent / "test_config.yaml"


class TestLoadConfig:
    def test_loads_valid_yaml_file(self):
        config = load_config(str(TEST_CONFIG_PATH))

        assert config == yaml.safe_load(TEST_CONFIG_PATH.read_text())

    def test_raises_file_not_found_when_missing(self, tmp_path):
        missing_path = tmp_path / "does_not_exist.yaml"

        with pytest.raises(FileNotFoundError):
            load_config(str(missing_path))

    def test_raises_value_error_when_file_is_empty(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("")

        with pytest.raises(ValueError):
            load_config(str(config_path))

    def test_raises_value_error_when_yaml_parses_to_null(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("null")

        with pytest.raises(ValueError):
            load_config(str(config_path))

    def test_default_path_is_config_yaml_in_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config.yaml").write_text(TEST_CONFIG_PATH.read_text())

        config = load_config()

        assert config == yaml.safe_load(TEST_CONFIG_PATH.read_text())
