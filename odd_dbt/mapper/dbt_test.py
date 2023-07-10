from datetime import datetime
from typing import Iterable, Optional

import pytz
from dbt.contracts.graph.nodes import ParsedNode, TestNode
from funcy import lkeep, walk_values
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

from ..logger import logger


class DbtTestMapper:
    def __init__(self, context: DbtContext, generator: DbtGenerator) -> None:
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        data_entities = []
        all_nodes = walk_values(ParsedNode._deserialize, self._context.manifest.nodes)

        for result in self._context.results:
            try:
                data_entities.extend(self.map_result(result, all_nodes))
            except Exception as e:
                logger.warning(f"Can't map result {result.unique_id}: {e}")
                continue

        data_entities = DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=data_entities,
        )

        logger.debug(data_entities.json(exclude_none=True))
        return data_entities

    def map_result(
        self, result: Result, all_nodes
    ) -> Optional[tuple[DataEntity, DataEntity]]:
        test_id: str = result.unique_id
        invocation_id: str = self._context.invocation_id

        assert test_id is not None
        assert invocation_id is not None

        start_time, end_time = result.execution_period
        test_node: TestNode = all_nodes[test_id]

        if test_node.resource_type != "test":
            logger.warning(f"Node {test_id} is not a test node")
            raise ValueError(f"Node {test_id} is not a test node")

        job = self.map_config(test_node, all_nodes)

        oddrn = self._generator.get_oddrn_by_path("runs", f"{invocation_id}")
        status, status_reason = parse_status(result.status, test_node)

        run = DataEntity(
            oddrn=oddrn,
            name=test_id,
            type=DataEntityType.JOB_RUN,
            owner=None,
            data_quality_test_run=DataQualityTestRun(
                data_quality_test_oddrn=job.oddrn,
                start_time=datetime_format(start_time) or datetime.now(tz=pytz.UTC),
                end_time=datetime_format(end_time) or datetime.now(tz=pytz.UTC),
                status=status,
                status_reason=status_reason,
            ),
        )

        return job, run

    def map_config(
        self, test_node: TestNode, nodes: dict[str, ParsedNode]
    ) -> DataEntity:
        dataset_list = lkeep([*self.get_dataset_oddrn(test_node, nodes)])
        assert len(dataset_list) > 0

        self._generator.set_oddrn_paths(
            **{"databases": test_node.database, "tests": test_node.alias}
        )

        return DataEntity(
            oddrn=self._generator.get_oddrn_by_path("tests"),
            owner=None,
            name=f"{test_node.name}",
            type=DataEntityType.JOB,
            data_quality_test=DataQualityTest(
                suite_name=test_node.name,
                dataset_list=dataset_list,
                expectation=DataQualityTestExpectation(
                    type=test_node.test_metadata.name
                ),
            ),
        )

    def get_dataset_oddrn(
        self, test_node: TestNode, nodes: dict[str, ParsedNode]
    ) -> Iterable[Optional[str]]:
        data_source = DataSource(self._context.profile.type)

        if test_node.test_node_type == "generic":
            for model_id in test_node.depends_on_nodes:
                model = nodes[model_id]
                name = model.name

                if data_source == DataSource.SNOWFLAKE:
                    name = name.upper()

                path = "views" if model.config.materialized == "view" else "tables"
                generator = get_datasource_generator(
                    data_source=data_source,
                    database=model.database,
                    profile=self._context.profile,
                )
                generator.set_oddrn_paths(**{"schemas": model.schema})
                yield generator.get_oddrn_by_path(path, name)
        elif test_node.test_node_type == "singular":
            # We don't support it because it doesn't contains test_metadata
            raise NotImplementedError("Singular test nodes are not supported yet")
        else:
            raise NotImplementedError(
                f"Unknown test node type: {test_node.test_node_type}"
            )


def datetime_format(date: Optional[str]) -> Optional[datetime]:
    if date:
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(tz=pytz.utc)

    return None


def parse_status(
    test_status: str, test_node: TestNode
) -> tuple[QualityRunStatus, Optional[str]]:
    status = QualityRunStatus.SUCCESS
    status_reason = None

    if test_status in {"fail", "error"}:
        status = QualityRunStatus.FAILED
        status_reason = StatusReason().get_reason(test_node=test_node)

    return status, status_reason
