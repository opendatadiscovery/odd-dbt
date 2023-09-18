from odd_models import DataEntity
from oddrn_generator import DbtGenerator


def create_dbt_generator_from_oddrn(oddrn: str) -> DbtGenerator:
    return DbtGenerator(host_settings=extract_host_from_oddrn(oddrn))


def create_dbt_generator(host: str) -> DbtGenerator:
    return DbtGenerator(host_settings=host)


def extract_host_from_oddrn(oddrn: str) -> str:
    return oddrn.split("//dbt/host/")[-1]


def is_data_transformer(data_entity: DataEntity) -> bool:
    return data_entity.data_transformer is not None
