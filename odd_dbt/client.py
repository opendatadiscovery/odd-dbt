import os

from odd_models.api_client.open_data_discovery_ingestion_api import ODDApiClient
from odd_models.models import DataEntityList, DataSource, DataSourceList
from requests import HTTPError

from odd_dbt.errors import CreateDataSourceError, IngestionEntitiesError


class Client:
    def __init__(self, host: str, token: str) -> None:
        host = host or os.getenv("ODD_PLATFORM_HOST", None)
        token = token or os.getenv("ODD_PLATFORM_TOKEN", None)

        if host is None:
            raise ValueError(
                "Couldn't create HttpClient. Host was not set. Possible solutions:\n"
                "Set env variable :export ODD_PLATFORM_HOST=http://localhost:8080"
            )

        if token is None:
            raise ValueError(
                "Couldn't create HttpClient. Token was not set. Possible solutions:\n"
                "1. Create token using odd-cli\n"
                "2. Create collector token using platform UI\n"
                "Set env variable :export ODD_PLATFORM_TOKEN=<TOKEN_FROM_STEPS_ABOVE>"
            )

        self._client = ODDApiClient(host)
        self._token = token
        self._headers = {"Authorization": f"Bearer {token}"}

    def ingest_data_source(self, data_source_oddrn: str, name: str) -> None:
        response = self._client.create_data_source(
            DataSourceList(items=[DataSource(oddrn=data_source_oddrn, name=name)]),
            headers=self._headers,
        )

        try:
            response.raise_for_status()
        except HTTPError as e:
            message = e.response.json().get("message")
            raise CreateDataSourceError(name, data_source_oddrn, message) from e

    def ingest_data_entities(self, data_entities: DataEntityList) -> None:
        response = self._client.post_data_entity_list(
            data_entities, headers=self._headers
        )

        try:
            response.raise_for_status()
        except HTTPError as e:
            message = e.response.json().get("message")
            raise IngestionEntitiesError(data_entities.data_source_oddrn, message)
