import json
from pathlib import Path

import yaml


def load_yaml(file_path: Path) -> dict:
    with file_path.open() as file:
        return yaml.load(file, yaml.FullLoader)


def load_json(file_path: Path) -> dict:
    with file_path.open() as file:
        return json.load(file)
