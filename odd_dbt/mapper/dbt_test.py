import re
from datetime import datetime
from typing import Optional

from funcy.colls import get_in
from funcy.seqs import lmapcat
from odd_models.models import (
    DataEntity,
    DataEntityList,
    DataEntityType,
    DataQualityTest,
    DataQualityTestExpectation,
    DataQualityTestRun,
    QualityRunStatus,
)
from oddrn_generator import DbtGenerator

from odd_dbt.context import DbtContext
from odd_dbt.mapper.data_source import DataSource, get_datasource_generator
from odd_dbt.mapper.status_reason import StatusReason


class DbtTestMapper:
    def __init__(self, context: DbtContext, generator: DbtGenerator) -> None:
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        data_entities = lmapcat(self.map_result, self._context.tests)

        return DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=data_entities,
        )

    def map_result(self, test_result: dict) -> Optional[tuple[DataEntity, DataEntity]]:
        test_id: str = test_result["unique_id"]
        invocation_id = self._context.run_results["metadata"]["invocation_id"]

        assert test_id is not None
        assert invocation_id is not None

        start_time_str: Optional[datetime] = get_in(
            test_result, ["timing", 0, "started_at"], None
        )
        end_time_str = get_in(test_result, ["timing", 1, "completed_at"])

        job = self.map_config(test_id)
        oddrn = self._generator.get_oddrn_by_path("runs", f"{test_id}.{invocation_id}")

        test_def = self._context.manifest["nodes"][test_id]
        test_status = test_result["status"]
        status, status_reason = parse_status(test_status, test_def)

        run = DataEntity(
            oddrn=oddrn,
            name=test_id,
            type=DataEntityType.JOB_RUN,
            owner=None,
            data_quality_test_run=DataQualityTestRun(
                data_quality_test_oddrn=job.oddrn,
                start_time=datetime_format(start_time_str) or datetime.now(),
                end_time=datetime_format(end_time_str),
                status=status,
                status_reason=status_reason,
            ),
        )

        return job, run

    def map_config(self, test_id: str) -> DataEntity:
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
            owner=None,
            name=f"{test_name}",
            type=DataEntityType.JOB,
            data_quality_test=dqt,
        )

    def _get_dataset_oddrn(self, config: dict, nodes: dict):
        data_source = DataSource(self._context.manifest["metadata"]["adapter_type"])

        model: str = config["test_metadata"]["kwargs"]["model"]
        model_name = re.search(r"'(.*)'", model)[1]

        model_id = f"model.{config['package_name']}.{model_name}"
        model_config = nodes[model_id]

        database = model_config["database"]
        schema = model_config["schema"]

        name = model_config["name"]
        if data_source == DataSource.SNOWFLAKE:
            name = name.upper()

        path = "tables"
        if model_config["config"]["materialized"] == "view":
            path = "views"

        generator = get_datasource_generator(
            data_source=data_source, database=database, profile=self._context.profile
        )
        generator.set_oddrn_paths(**{"schemas": schema})
        return generator.get_oddrn_by_path(path, name)


def datetime_format(date: Optional[str]) -> Optional[datetime]:
    if date:
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").astimezone()

    return None


def parse_status(
    test_status: str, test_def: dict
) -> tuple[QualityRunStatus, Optional[str]]:
    status = QualityRunStatus.SUCCESS
    status_reason = None

    if test_status == "fail":
        status = QualityRunStatus.FAILED
        status_reason = StatusReason(test_def).get_reason()

    return status, status_reason
