"""
Shared utilities for the PLN exchange rate pipeline.
"""

from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    """
    Load pipeline configuration from a YAML file.

    Raises FileNotFoundError with a clear message if the config is missing.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found at '{config_path}'. "
            "Expected a config.yaml in the project root."
        )

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    if not config:
        raise ValueError(f"Config file at '{config_path}' is empty or invalid.")

    return config