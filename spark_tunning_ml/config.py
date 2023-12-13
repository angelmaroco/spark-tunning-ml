from __future__ import annotations

import json
import os


class Config:
    def __init__(self, file_path):
        self.file_path = file_path
        self.config_data = self._load_config()

    def _load_config(self):
        try:
            with open(self.file_path, "r") as file:
                config_data = json.load(file)
            return config_data
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Config file not found at path: {self.file_path}",
            )
        except json.JSONDecodeError:
            raise ValueError(
                f"Invalid JSON format in config file: {self.file_path}",
            )

    def get(self, key):
        """
        Get a configuration property by key.

        Args:
            key (str): The key of the configuration property.

        Returns:
            Any: The value of the configuration property.

        Raises:
            KeyError: If the specified key is not found in the config file.
        """
        return self.config_data[key]

    def get_all_properties(self):
        """
        Get all configuration properties.

        Returns:
            dict: All configuration properties.
        """
        return self.config_data


config = Config(
    os.path.join(os.path.dirname(os.path.dirname(__file__))) + "/config/config.json",
)
