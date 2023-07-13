from enum import Enum
from typing import Optional, Type

from dbt.contracts.graph.nodes import ModelNode
from oddrn_generator import (
    Generator,
    MssqlGenerator,
    PostgresqlGenerator,
    RedshiftGenerator,
    SnowflakeGenerator,
)

from odd_dbt.models.profile import DataSourceType, Profile


class GeneratorAdapter:
    def __init__(self, generator_cls: Type[Generator], profile: Profile) -> None:
        self.generator_cls = generator_cls
        self.profile = profile

    def get_oddrn_for(self, model: ModelNode, path: Optional[str] = None) -> str:
        host = self.profile.get("host")
        database = self.profile.get("database") or self.profile.get("dbname")
        generator = self.generator_cls(host_settings=host, databases=database)

        name = model.name
        path = "views" if model.config.materialized == "view" else "tables"
        generator.set_oddrn_paths(**{"schemas": model.schema})
        return generator.get_oddrn_by_path(path, name)


class SnowflakeGeneratorAdapter(GeneratorAdapter):
    def get_oddrn_for(self, model: ModelNode, path: Optional[str] = None) -> str:
        host = f"{self.profile.get('account').upper()}.snowflakecomputing.com"
        database = (
            self.profile.get("database").upper() or self.profile.get("dbname").upper()
        )

        generator = self.generator_cls(host_settings=host, databases=database)
        name = model.name.upper()
        path = "views" if model.config.materialized == "view" else "tables"
        generator.set_oddrn_paths(**{"schemas": model.schema.upper()})
        return generator.get_oddrn_by_path(path, name)


def get_generator(profile: Profile) -> GeneratorAdapter:
    DATA_SOURCE_GENERATORS: dict[
        Enum, tuple[Type[GeneratorAdapter], Type[Generator], Profile]
    ] = {
        DataSourceType.POSTGRES: (GeneratorAdapter, PostgresqlGenerator, profile),
        DataSourceType.SNOWFLAKE: (
            SnowflakeGeneratorAdapter,
            SnowflakeGenerator,
            profile,
        ),
        DataSourceType.REDSHIFT: (GeneratorAdapter, RedshiftGenerator, profile),
        DataSourceType.MSSQL: (GeneratorAdapter, MssqlGenerator, profile),
    }

    if generator_config := DATA_SOURCE_GENERATORS.get(profile.type):
        return generator_config[0](generator_config[1], generator_config[2])
    else:
        raise ValueError(
            f"Unknown profile type {profile.type}. Available: {DATA_SOURCE_GENERATORS.keys()}"
        )
