from pathlib import Path

from odd_dbt.utils import load_json
from funcy import walk_values
from dbt.contracts.graph.nodes import ParsedNode


class Manifest:
    def __init__(self, file: Path) -> None:
        self._manifest = load_json(file)

    @property
    def nodes(self) -> list[ParsedNode]:
        return walk_values(ParsedNode._deserialize, self._manifest["nodes"])
