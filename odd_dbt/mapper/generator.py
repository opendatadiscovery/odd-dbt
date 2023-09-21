import abc
from functools import singledispatchmethod
from typing import Type, Protocol, Any

from dbt.contracts.graph.nodes import ModelNode
from oddrn_generator import generators as odd

from odd_dbt.domain.credentials import Credentials
from odd_dbt.domain.source import Source


class Generator(Protocol):
    generator_cls: Type[odd.Generator]
    credentials: Credentials

    @singledispatchmethod
    def get_oddrn_for(self, node: Any) -> str:
        ...

    @get_oddrn_for.register
    def _(self, node: ModelNode) -> str:
        return self._get_oddrn_for_model(node)

    @get_oddrn_for.register
    def _(self, node: Source) -> str:
        return self._get_oddrn_for_source(node)

    @abc.abstractmethod
    def _get_oddrn_for_model(self, model: ModelNode) -> str:
        ...

    @abc.abstractmethod
    def _get_oddrn_for_source(self, source: Source) -> str:
        ...


class PostgresGenerator(Generator):
    generator_cls = odd.PostgresqlGenerator

    def __init__(self, credentials: Credentials) -> None:
        self.credentials = credentials

    def _get_oddrn_for_model(self, model: ModelNode) -> str:
        host = self.credentials["host"]
        database = self.credentials["database"] or self.credentials["dbname"]
        generator = self.generator_cls(host_settings=host, databases=database)
        path = "views" if model.config.materialized == "view" else "tables"
        generator.set_oddrn_paths(**{"schemas": model.schema})
        return generator.get_oddrn_by_path(path, model.name)

    def _get_oddrn_for_source(self, source: Source) -> str:
        host = self.credentials["host"]
        database = source.database
        generator = self.generator_cls(host_settings=host, databases=database)
        generator.set_oddrn_paths(**{"schemas": source.schema, "tables": source.name})
        return generator.get_oddrn_by_path("tables")


class SnowflakeGenerator(Generator):
    generator_cls = odd.SnowflakeGenerator

    def __init__(self, credentials: Credentials) -> None:
        self.credentials = credentials

    def _get_oddrn_for_model(self, model: ModelNode) -> str:
        host = f"{self.credentials['account'].upper()}.snowflakecomputing.com"
        database = self.credentials["database"] or self.credentials["dbname"]
        database = database.upper()

        generator = self.generator_cls(host_settings=host, databases=database)

        name = model.name.upper()
        path = "views" if model.config.materialized == "view" else "tables"
        generator.set_oddrn_paths(**{"schemas": model.schema.upper()})
        return generator.get_oddrn_by_path(path, name)

    def _get_oddrn_for_source(self, source: Source) -> str:
        host = f"{self.credentials['account'].upper()}.snowflakecomputing.com"
        database = source.database.upper()

        generator = self.generator_cls(host_settings=host, databases=database)

        generator.set_oddrn_paths(
            **{"schemas": source.schema.upper(), "tables": source.name.upper()}
        )
        return generator.get_oddrn_by_path("tables")


ODDRN_GENERATORS: dict[str, Type[Generator]] = {
    "postgres": PostgresGenerator,
    "snowflake": SnowflakeGenerator,
}


def create_generator(adapter_type: str, credentials: Credentials) -> Generator:
    """
    :param adapter_type: Dbt adapter type
    :param credentials: Credentials for adapter
    :return: GeneratorWrapper
    """
    try:
        generator = ODDRN_GENERATORS[adapter_type]
        return generator(credentials)
    except KeyError:
        raise KeyError(
            f"Unsupported adapter type {adapter_type}. Available: {ODDRN_GENERATORS.keys()}"
        )
