from pathlib import Path

from dbt.contracts.graph.nodes import ParsedNode, GenericTestNode
from funcy import walk_values, select_values

from odd_dbt.domain.source import Source
from odd_dbt.utils import load_json


class Manifest:
    def __init__(self, file: Path) -> None:
        self._manifest = load_json(file)

    @property
    def nodes(self) -> dict[str, ParsedNode]:
        return walk_values(ParsedNode._deserialize, self._manifest["nodes"])

    @property
    def sources(self) -> dict[str, Source]:
        return walk_values(Source._deserialize, self._manifest["sources"])

    @property
    def generic_tests(self) -> dict[str, GenericTestNode]:
        return select_values(lambda x: isinstance(x, GenericTestNode), self.nodes)
