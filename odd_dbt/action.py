import traceback

from odd_models.models import DataEntityList
from odd_dbt.logger import logger
from odd_dbt.mapper import DbtTestMapper
from odd_dbt.parser import DbtArtifactParser
from oddrn_generator.generators import DbtGenerator


class ODDAction:
    def __init__(
            self,
            parser: DbtArtifactParser,
    ):
        self._host = "localhost"
        self._parser = parser
        self._generator = DbtGenerator(host_settings=self._host)

    def run(self) -> DataEntityList:
        try:
            logger.info("Start collecting metadata")
            generator = self._generator
            tests, context = self._parser.parse_metadata()

            data_entity_list = DbtTestMapper(tests, context, generator).map()

            return data_entity_list
        except Exception as e:
            logger.error(f"Error during collecting metadata. {e}")
            logger.error(traceback.format_exc())
