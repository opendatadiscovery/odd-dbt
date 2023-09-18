import funcy
from odd_models import DataEntityList
from odd_models.api_client.v2.odd_api_client import Client

from odd_dbt.libs.odd import create_dbt_generator
from odd_dbt.logger import logger


def create_datasource(name: str, dbt_host: str, client: Client) -> str:
    generator = create_dbt_generator(host=dbt_host)
    oddrn = generator.get_data_source_oddrn()
    client.create_data_source(
        data_source_name=name,
        data_source_oddrn=oddrn,
    )
    return oddrn


def ingest_entities(data_entities: DataEntityList, client: Client) -> None:
    client.ingest_data_entity_list(data_entities=data_entities)
    show_message(data_entities)


def show_message(data_entities: DataEntityList) -> None:
    grouped = funcy.group_by(lambda x: x.type, data_entities.items)
    ingested = ", ".join(
        f"{len(entities)} {type_.value}" for type_, entities in grouped.items()
    )
    logger.success(f"Injecting test results finished. Ingested {ingested}")
