from datetime import datetime
from typing import Optional

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
from odd_dbt.models import Result


class DbtTestMapper:
    def __init__(self, context: DbtContext, generator: DbtGenerator) -> None:
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        data_entities = lmapcat(self.map_result, self._context.results)

        return DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=data_entities,
        )

    def map_result(self, result: Result) -> Optional[tuple[DataEntity, DataEntity]]:
        test_id: str = result.unique_id
        invocation_id: str = self._context.invocation_id

        assert test_id is not None
        assert invocation_id is not None

        start_time, end_time = result.execution_period
        test_node = self._context.manifest.nodes[test_id]

        job = self.map_config(test_id)
        oddrn = self._generator.get_oddrn_by_path("runs", f"{test_id}.{invocation_id}")
        status, status_reason = parse_status(result.status, test_node)

        run = DataEntity(
            oddrn=oddrn,
            name=test_id,
            type=DataEntityType.JOB_RUN,
            owner=None,
            data_quality_test_run=DataQualityTestRun(
                data_quality_test_oddrn=job.oddrn,
                start_time=datetime_format(start_time) or datetime.now(),
                end_time=datetime_format(end_time),
                status=status,
                status_reason=status_reason,
            ),
        )

        return job, run

    def map_config(self, test_id: str) -> DataEntity:
        nodes = self._context.manifest.nodes

        test_node = nodes[test_id]
        test_name = test_node["name"]
        original_type = test_node["alias"]
        database = test_node["database"]

        dataset_list = [self._get_dataset_oddrn(test_node, nodes)]

        self._generator.set_oddrn_paths(**{"databases": database, "tests": test_id})

        dqt = DataQualityTest(
            suite_name=test_name,
            dataset_list=dataset_list,
            expectation=DataQualityTestExpectation(type=original_type),
        )

        return DataEntity(
            oddrn=self._generator.get_oddrn_by_path("tests"),
            owner=None,
            name=f"{test_name}",
            type=DataEntityType.JOB,
            data_quality_test=dqt,
        )

    def _get_dataset_oddrn(self, config: dict, nodes: dict):
        data_source = DataSource(self._context.profile.type)

        model_id = config["depends_on"]["nodes"][
            0
        ]  # TODO: this is a hack, but it works for now
        model_node = nodes[
            model_id
        ]  # TODO: add error handling if model_id is not found

        database = model_node["database"]
        schema = model_node["schema"]
        name = model_node["name"]

        if data_source == DataSource.SNOWFLAKE:
            name = name.upper()

        generator = get_datasource_generator(
            data_source=data_source, database=database, profile=self._context.profile
        )

        path = "views" if model_node["config"]["materialized"] == "view" else "tables"
        generator.set_oddrn_paths(**{"schemas": schema})
        return generator.get_oddrn_by_path(path, name)


def datetime_format(date: Optional[str]) -> Optional[datetime]:
    if date:
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").astimezone()

    return None


def parse_status(
    test_status: str, test_node: dict
) -> tuple[QualityRunStatus, Optional[str]]:
    status = QualityRunStatus.SUCCESS
    status_reason = None

    if test_status == "fail":
        status = QualityRunStatus.FAILED
        status_reason = StatusReason(test_node).get_reason()

    return status, status_reason
