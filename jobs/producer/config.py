from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str | Path) -> dict[str, Any]:
    with Path(config_path).open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)