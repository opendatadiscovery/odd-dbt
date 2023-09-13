from typing import Optional, Type, Protocol

from dbt.contracts.graph.nodes import ModelNode
from oddrn_generator import generators as odd

from odd_dbt.domain.credentials import Credentials


class Generator(Protocol):
    generator_cls: Type[odd.Generator]
    credentials: Credentials

    def get_oddrn_for(self, model: ModelNode, path: Optional[str] = None) -> str:
        ...


class PostgresGenerator(Generator):
    generator_cls = odd.PostgresqlGenerator

    def __init__(self, credentials: Credentials) -> None:
        self.credentials = credentials

    def get_oddrn_for(self, model: ModelNode, path: Optional[str] = None) -> str:
        host = self.credentials["host"]
        database = self.credentials["database"] or self.credentials["dbname"]
        generator = self.generator_cls(host_settings=host, databases=database)
        path = "views" if model.config.materialized == "view" else "tables"
        generator.set_oddrn_paths(**{"schemas": model.schema})
        return generator.get_oddrn_by_path(path, model.name)


class SnowflakeGenerator(Generator):
    generator_cls = odd.SnowflakeGenerator

    def __init__(self, credentials: Credentials) -> None:
        self.credentials = credentials

    def get_oddrn_for(self, model: ModelNode, path: Optional[str] = None) -> str:
        host = f"{self.credentials.get('account').upper()}.snowflakecomputing.com"
        database = self.credentials.get("database") or self.credentials.get("dbname")
        database = database.upper()

        generator = self.generator_cls(host_settings=host, databases=database)

        name = model.name.upper()
        path = "views" if model.config.materialized == "view" else "tables"
        generator.set_oddrn_paths(**{"schemas": model.schema.upper()})
        return generator.get_oddrn_by_path(path, name)


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
