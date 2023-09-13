from odd_models import DataEntityList
from odd_models.api_client.v2.odd_api_client import Client

from odd_dbt import config
from odd_dbt.logger import logger


def create_datasource(name: str, dbt_host: str, client: Client) -> None:
    generator = config.create_dbt_generator(host=dbt_host)
    oddrn = generator.get_data_source_oddrn()
    client.create_data_source(
        data_source_name=name,
        data_source_oddrn=oddrn,
    )
    return oddrn


def ingest_entities(data_entities: DataEntityList, client: Client) -> None:
    client.ingest_data_entity_list(data_entities=data_entities)
    logger.success(
        f"Injecting test results finished. Ingested {len(data_entities.items)} entities"
    )
