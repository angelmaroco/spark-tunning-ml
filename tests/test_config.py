from __future__ import annotations

import json
from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from spark_tunning_ml.config import Config

OPEN_FUNCTION = "builtins.open"
CONFIG_FILE = "config.json"


@pytest.fixture
def config_data():
    return {"property1": "value1", "property2": 42, "property3": True}


def test_config_loads_properties(config_data):
    # Mock the built-in open function and return a file with the specified content
    with patch(OPEN_FUNCTION, mock_open(read_data=json.dumps(config_data))) as mock_file:
        # Replace with the actual path to your config file
        config = Config(CONFIG_FILE)

        # Verify that the file was opened with the correct path
        mock_file.assert_called_once_with(CONFIG_FILE, "r")

        # Verify that the config object contains the correct properties
        assert config.get_all_properties() == config_data


def test_config_get_prop(config_data):
    with patch(OPEN_FUNCTION, mock_open(read_data=json.dumps(config_data))):
        config = Config(CONFIG_FILE)

        # Test getting a specific property
        assert config.get("property1") == "value1"
        assert config.get("property2") == 42

        # Test getting a non-existent property
        with pytest.raises(KeyError):
            config.get("non_existent_property")


def test_config_file_not_found():
    # Test when the file is not found
    with pytest.raises(FileNotFoundError):
        with patch(OPEN_FUNCTION, side_effect=FileNotFoundError):
            Config("non_existent_config.json")


def test_config_invalid_json():
    # Test when the file contains invalid JSON
    with pytest.raises(ValueError):
        with patch(OPEN_FUNCTION, mock_open(read_data="invalid_json")):
            Config(CONFIG_FILE)
