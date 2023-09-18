from pathlib import Path

from dbt.contracts.graph.nodes import ParsedNode
from funcy import walk_values

from odd_dbt.utils import load_json


class Manifest:
    def __init__(self, file: Path) -> None:
        self._manifest = load_json(file)

    @property
    def nodes(self) -> dict[str, ParsedNode]:
        return walk_values(ParsedNode._deserialize, self._manifest["nodes"])
