import traceback
from datetime import datetime
from typing import Optional

from odd_models.models import (DataEntity, DataEntityList, DataEntityType,
                               DataQualityTest, DataQualityTestExpectation,
                               DataQualityTestRun, LinkedUrl, QualityRunStatus)
from oddrn_generator import DbtGenerator, SnowflakeGenerator, Generator
from odd_dbt.logger import logger
from odd_dbt.parser import DbtRunContext


class DbtTestMapper:
    def __init__(self, tests: list[dict], context: DbtRunContext, generator: DbtGenerator) -> None:
        self._tests = tests
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        data_entities = []
        for test_result in self._tests:
            data_entities.extend(self._map_result(test_result))

        return DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=data_entities,
        )

    def _map_result(self, test_result: dict) -> Optional[tuple[DataEntity, DataEntity]]:
        try:
            test_id = test_result['unique_id']
            invocation_id = self._context.run_results["metadata"]["invocation_id"]
            start_time_str = test_result["timing"][0]["completed_at"]
            status = QualityRunStatus.SUCCESS if test_result["status"] == "pass" else QualityRunStatus.FAILED
            job = self._map_config(test_id)
            oddrn = self._generator.get_oddrn_by_path("runs", f"{test_id}.{invocation_id}")

            run = DataEntity(
                oddrn=oddrn,
                name=test_id,
                type=DataEntityType.JOB_RUN,
                data_quality_test_run=DataQualityTestRun(
                    data_quality_test_oddrn=job.oddrn,
                    start_time=datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(),
                    end_time=datetime.now().astimezone(),
                    status=status,
                ),
            )
            return job, run
        except Exception as e:
            logger.error(f"Error during mapping: {e}")
            logger.error(traceback.format_exc())
        return

    def _map_config(self, test_id: str) -> DataEntity:
        nodes = self._context.manifest["nodes"]

        config = nodes[test_id]
        test_name = config["name"]
        original_type = config["alias"]
        database = config["database"]

        dataset_list = [self._get_dataset_oddrn(config, nodes)]
        self._generator.set_oddrn_paths(**{"databases": database})
        oddrn = self._generator.get_oddrn_by_path("tests", test_id)

        dqt = DataQualityTest(
            suite_name=test_name,
            dataset_list=dataset_list,
            expectation=DataQualityTestExpectation(type=original_type),
        )

        return DataEntity(
            oddrn=oddrn,
            name=f"{test_name}",
            type=DataEntityType.JOB,
            data_quality_test=dqt,
        )

    def _get_dataset_oddrn(self, test_config: dict, nodes: dict):
        ref_name = test_config["refs"][0][0]
        model_id = "model." + test_config["package_name"] + "." + ref_name
        model_config = nodes[model_id]
        adapter_type = self._context.manifest["metadata"]["adapter_type"]
        database = model_config["database"]
        schema = model_config["schema"]
        path = "tables" if model_config["config"]["materialized"] == "table" else "views"

        generator = self._get_dataset_generator(adapter_type, database)
        generator.set_oddrn_paths(**{"schemas": schema})
        dataset_oddrn = generator.get_oddrn_by_path(path, model_config["name"].upper())

        return dataset_oddrn

    def _get_dataset_generator(self, adapter_type: str, database: str) -> Optional[Generator]:

        host_settings = self._context.profile["account"]
        host_settings += ".snowflakecomputing.com" if adapter_type == "snowflake" else ""
        fabric = {
            "snowflake": SnowflakeGenerator,
        }
        try:
            generator = fabric[adapter_type](host_settings=host_settings, databases=database)
            return generator
        except KeyError:
            logger.warning(f"Generator not available for dataset: {adapter_type}")
            return None
