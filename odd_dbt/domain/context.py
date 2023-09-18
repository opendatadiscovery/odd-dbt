from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.nodes import ParsedNode

from odd_dbt.domain import Manifest, Credentials, RunResults, Result
from odd_dbt.domain.cli_args import CliArgs
from odd_dbt.errors import DbtInternalError
from odd_dbt.utils import load_json


class DbtContext:
    def __init__(self, cli_args: CliArgs):
        try:
            self._config = RuntimeConfig.from_args(cli_args)
            self.target_path = cli_args.project_dir / self._config.target_path
        except Exception as e:
            raise DbtInternalError(f"Failed getting dbt context: {e}") from e

    @property
    def adapter_type(self) -> str:
        return self._config.get_metadata().adapter_type

    @property
    def catalog(self):
        if (catalog := self.target_path / "catalog.json").is_file():
            return load_json(catalog)

        return None

    @property
    def credentials(self) -> Credentials:
        return Credentials(**self._config.credentials.to_dict())

    @property
    def manifest(self) -> Manifest:
        return Manifest(self.target_path / "manifest.json")

    @property
    def run_results(self) -> RunResults:
        return RunResults(self.target_path / "run_results.json")

    @property
    def results(self) -> list[Result]:
        return self.run_results.results

    @property
    def invocation_id(self) -> str:
        return self.run_results.invocation_id

    @property
    def nodes(self) -> dict[str, ParsedNode]:
        return self.manifest.nodes
