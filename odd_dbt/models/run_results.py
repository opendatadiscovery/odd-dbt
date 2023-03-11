from functools import cached_property
from pathlib import Path

from funcy import lmap

from odd_dbt.utils import load_json

from .result import Result


class RunResults:
    def __init__(self, file: Path) -> None:
        self._run_results = load_json(file)

    @cached_property
    def results(self):
        return lmap(Result, self._run_results.get("results", []))

    @property
    def invocation_id(self):
        return self._run_results["metadata"]["invocation_id"]

    @property
    def profiles_dir(self) -> Path:
        return Path(self._run_results["args"]["profiles_dir"])
