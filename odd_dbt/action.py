import traceback

from odd_dbt.logger import logger
from odd_dbt.client import Client
from odd_dbt.mapper import DbtTestMapper
from odd_dbt.parser import DbtArtifactParser
from oddrn_generator.generators import DbtGenerator


class ODDAction:
    def __init__(
            self,
            parser: DbtArtifactParser,
            platform_host: str = None,
            platform_token: str = None,
    ):
        self._odd_client = Client(platform_host, platform_token)
        self._host = "localhost"
        self._parser = parser
        self._generator = DbtGenerator(host_settings=self._host)
        self._data_source_name = "dbt_adapter"

    def run(self):
        try:
            logger.info("Start collecting metadata")
            client = self._odd_client
            generator = self._generator
            tests, context = self._parser.parse_metadata()

            client.ingest_data_source(
                data_source_oddrn=generator.get_data_source_oddrn(),
                name=self._data_source_name,
            )

            data_entity_list = DbtTestMapper(tests, context, generator).map()

            logger.info("Injecting data to ODD platform")
            client.ingest_data_entities(data_entity_list)

            logger.info(
                f"Metadata successfully loaded to Platform. Ingested {len(data_entity_list.items)} entities"
            )
        except Exception as e:
            logger.error(f"Error during collecting metadata. {e}")
            logger.error(traceback.format_exc())
