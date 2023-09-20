from pathlib import Path

from dbt.contracts.graph.nodes import ParsedNode, GenericTestNode, ModelNode, SeedNode
from funcy import walk_values, select_values

from odd_dbt.domain.source import Source
from odd_dbt.utils import load_json
from functools import cached_property


class Manifest:
    def __init__(self, file: Path) -> None:
        self._manifest = load_json(file)

    @cached_property
    def nodes(self) -> dict[str, ParsedNode]:
        return walk_values(ParsedNode._deserialize, self._manifest["nodes"])

    @cached_property
    def sources(self) -> dict[str, Source]:
        return walk_values(Source._deserialize, self._manifest["sources"])

    @cached_property
    def generic_tests(self) -> dict[str, GenericTestNode]:
        return select_values(lambda x: isinstance(x, GenericTestNode), self.nodes)

    @cached_property
    def models(self) -> dict[str, ModelNode]:
        return select_values(lambda x: isinstance(x, ModelNode), self.nodes)

    @cached_property
    def seeds(self) -> dict[str, SeedNode]:
        return select_values(lambda x: isinstance(x, SeedNode), self.nodes)
