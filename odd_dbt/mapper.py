import traceback
import re
from datetime import datetime
from enum import Enum
from typing import Optional

from odd_models.models import (
    DataEntity, DataEntityList, DataEntityType,
    DataQualityTest, DataQualityTestExpectation,
    DataQualityTestRun, QualityRunStatus)
from oddrn_generator import (
    Generator, DbtGenerator, SnowflakeGenerator,
    RedshiftGenerator, PostgresqlGenerator
)
from odd_dbt.logger import logger
from odd_dbt.parser import DbtRunContext


class AdapterType(Enum):
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    POSTGRES = "postgres"


class GeneratorFabric:
    def __init__(self, adapter_type: AdapterType, databases: str, context_profile: dict):
        self.generator_classes = {
            "snowflake": SnowflakeGenerator,
            "redshift": RedshiftGenerator,
            "postgres": PostgresqlGenerator,
        }
        self.adapter_type = adapter_type
        self.context_profile = context_profile
        self.databases = databases

    def get_generator(self) -> Optional[Generator]:
        generator_class = self.generator_classes.get(self.adapter_type.value)
        if not generator_class:
            logger.warning(f"Generator not available for dataset: {self.adapter_type}")
            return
        return generator_class(host_settings=self.host_settings, databases=self.databases)

    @property
    def host_settings(self) -> str:
        if self.adapter_type == AdapterType.SNOWFLAKE:
            return self.context_profile["account"] + ".snowflakecomputing.com"
        else:
            return self.context_profile["host"]


class StatusReason:
    def __init__(self, test_def: dict):
        self.test_type = test_def["test_metadata"]["name"]
        self.metadata = test_def["test_metadata"]["kwargs"]
        self.model = re.search(r"'(.*)'", self.metadata["model"]).group(1)
        self.column = self.metadata["column_name"]

    def get_reason(self) -> str:
        method = getattr(self, self.test_type, self.default)
        return method()

    def unique(self) -> str:
        return f"The {self.column} column in the {self.model} model should be unique"

    def not_null(self) -> str:
        return f"the {self.column} column in the {self.model} model should not contain null values"

    def accepted_values(self) -> str:
        acc_values = self.metadata["values"]
        return f"The {self.column} column in the {self.model} should be one of {acc_values}"

    def relationships(self) -> str:
        ref_model = re.search(r"'(.*)'", self.metadata["to"]).group(1)
        ref_field = self.metadata["field"]
        return f"Each value in the {self.column} in the {self.model} should exists as an {ref_field} in the {ref_model}"

    def default(self) -> str:
        return f"Status reason for test {self.test_type} not implemented yet"


class DbtTestMapper:
    def __init__(self, tests_results: list[dict], context: DbtRunContext, generator: DbtGenerator) -> None:
        self._tests_results = tests_results
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        data_entities = []
        for test_result in self._tests_results:
            data_entities.extend(self._map_result(test_result))

        return DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=data_entities,
        )

    def _map_result(self, test_result: dict) -> Optional[tuple[DataEntity, DataEntity]]:
        try:
            test_id = test_result['unique_id']
            invocation_id = self._context.run_results["metadata"]["invocation_id"]
            start_time_str = test_result["timing"][0]["started_at"]
            end_time_str = test_result["timing"][1]["completed_at"]

            job = self._map_config(test_id)
            oddrn = self._generator.get_oddrn_by_path("runs", f"{test_id}.{invocation_id}")
            test_def = self._context.manifest["nodes"][test_id]
            test_status = test_result["status"]
            status, status_reason = self._get_status(test_status, test_def)

            run = DataEntity(
                oddrn=oddrn,
                name=test_id,
                type=DataEntityType.JOB_RUN,
                data_quality_test_run=DataQualityTestRun(
                    data_quality_test_oddrn=job.oddrn,
                    start_time=datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(),
                    end_time=datetime.strptime(end_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(),
                    status=status,
                    status_reason=status_reason
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
        adapter_type = AdapterType(self._context.manifest["metadata"]["adapter_type"])
        model_name = re.search(r"'(.*)'", test_config["test_metadata"]["kwargs"]["model"]).group(1)
        model_id = "model." + test_config["package_name"] + "." + model_name
        model_config = nodes[model_id]
        database = model_config["database"]
        schema = model_config["schema"]
        name = model_config["name"].upper() if adapter_type == AdapterType.SNOWFLAKE else model_config["name"]
        path = "tables" if model_config["config"]["materialized"] == "table" else "views"

        generator = GeneratorFabric(adapter_type, database, self._context.profile).get_generator()
        generator.set_oddrn_paths(**{"schemas": schema})
        dataset_oddrn = generator.get_oddrn_by_path(path, name)

        return dataset_oddrn

    @staticmethod
    def _get_status(test_status: str, test_def: dict) -> tuple[QualityRunStatus, Optional[str]]:
        status = QualityRunStatus.SUCCESS
        status_reason = None

        if test_status == "fail":
            status = QualityRunStatus.FAILED
            status_reason = StatusReason(test_def).get_reason()

        return status, status_reason
