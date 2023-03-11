from pathlib import Path

from odd_dbt.utils import load_json


class Manifest:
    def __init__(self, file: Path) -> None:
        self._manifest = load_json(file)

    @property
    def nodes(self):
        return self._manifest["nodes"]
