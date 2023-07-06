from enum import Enum
from typing import Type

from oddrn_generator import (
    Generator,
    MssqlGenerator,
    PostgresqlGenerator,
    RedshiftGenerator,
    SnowflakeGenerator,
)

from odd_dbt.models import Profile


class DataSource(Enum):
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    POSTGRES = "postgres"
    MSSQL = "mssql"


class GeneratorAdaptee:
    def __init__(
        self,
        generator_cls: Type[Generator],
    ) -> None:
        self._generator_cls = generator_cls

    def create(self, database: str, profile: Profile) -> Generator:
        host = profile.host
        return self._create(database, host)

    def _create(self, database: str, host: str) -> Generator:
        return self._generator_cls(host_settings=host, databases=database)


class SnowflakeAdaptee(GeneratorAdaptee):
    def create(self, database: str, profile: Profile) -> Generator:
        host = f"{profile.account}.snowflakecomputing.com"
        return super()._create(database, host)


DATA_SOURCE_GENERATORS: dict[DataSource, GeneratorAdaptee] = {
    DataSource.POSTGRES: GeneratorAdaptee(PostgresqlGenerator),
    DataSource.SNOWFLAKE: SnowflakeAdaptee(SnowflakeGenerator),
    DataSource.REDSHIFT: GeneratorAdaptee(RedshiftGenerator),
    DataSource.MSSQL: GeneratorAdaptee(MssqlGenerator),
}


def get_datasource_generator(
    data_source: DataSource, database: str, profile: Profile
) -> Generator:
    if generator := DATA_SOURCE_GENERATORS.get(data_source):
        return generator.create(database=database, profile=profile)
    else:
        raise ValueError(
            f"Unknown {data_source=}. Available: {DATA_SOURCE_GENERATORS.keys()}"
        )
