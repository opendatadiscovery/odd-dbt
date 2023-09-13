import oddrn_generator as odd
from odd_models.api_client.v2.odd_api_client import Client
from pydantic import BaseSettings


class Config(BaseSettings):
    odd_platform_host: str
    odd_platform_token: str
    dbt_data_source_oddrn: str


def create_odd_client(host: str = None, token: str = None) -> Client:
    return Client(host=host, token=token)


def create_dbt_generator_from_oddrn(oddrn: str) -> odd.DbtGenerator:
    return odd.DbtGenerator(host_settings=extract_host_from_oddrn(oddrn))


def create_dbt_generator(host: str) -> odd.DbtGenerator:
    return odd.DbtGenerator(host_settings=host)


def extract_host_from_oddrn(oddrn: str) -> str:
    return oddrn.split("//dbt/host/")[-1]
