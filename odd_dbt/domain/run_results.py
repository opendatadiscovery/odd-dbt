from functools import cached_property
from pathlib import Path

from funcy import lmap

from odd_dbt.utils import load_json
from .result import Result


class RunResults:
    def __init__(self, file: Path) -> None:
        self._results = load_json(file)

    @cached_property
    def results(self) -> list[Result]:
        return lmap(Result, self._results.get("results", []))

    @property
    def invocation_id(self) -> str:
        return self._results["metadata"]["invocation_id"]

    @property
    def profiles_dir(self) -> Path:
        return Path(self._results["args"]["profiles_dir"])
