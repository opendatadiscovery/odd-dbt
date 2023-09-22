import traceback
from datetime import datetime
from typing import Iterable, Optional

import pytz
from dbt.contracts.graph.nodes import GenericTestNode, TestNode, SeedNode
from funcy import lkeep
from odd_dbt.domain import Result
from odd_dbt.domain.context import DbtContext
from odd_dbt.mapper.generator import create_generator
from odd_dbt.mapper.helpers import datetime_format
from odd_dbt.mapper.metadata import get_metadata
from odd_dbt.mapper.status_reason import StatusReason
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

from odd_dbt.logger import logger


class DbtTestMapper:
    def __init__(self, context: DbtContext, generator: DbtGenerator) -> None:
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        data_entities = []
        
        test_results = [res for res in self._context.results if res.unique_id.startswith("test.")]
        
        if not test_results:
            raise ValueError("run_results.json doesn't contain any test result. Was dbt test command executed?")

        for result in test_results:
            try:
                data_entities.extend(self.map_result(result, self._context.manifest.nodes))
            except Exception as e:
                logger.warning(f"Can't map result {result.unique_id}: {str(e)}")
                logger.debug(traceback.format_exc())
                continue
        
        if not data_entities:
            raise ValueError("No test results were mapped. Data will not be ingested")

        data_entities = DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=data_entities,
        )

        logger.debug(data_entities.json(exclude_none=True))
        return data_entities

    def map_result(
        self, result: Result, test_nodes: list[GenericTestNode]
    ) -> Optional[tuple[DataEntity, DataEntity]]:
        test_id: str = result.unique_id
        invocation_id: str = self._context.invocation_id

        assert test_id is not None
        assert invocation_id is not None

        start_time, end_time = result.execution_period
        test_node: TestNode = test_nodes.get(test_id)

        if not test_node:
            raise KeyError(f"Could not find test node with an id {test_id}")

        job = self.map_config(test_node)

        oddrn = self._generator.get_oddrn_by_path("runs", f"{invocation_id}")
        status, status_reason = parse_status(result, test_node)

        name = test_node.name
        if len(name) > 250:
            name = test_node.alias

        run = DataEntity(
            oddrn=oddrn,
            name=name,
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

    def map_config(self, test_node: TestNode) -> DataEntity:
        dataset_list = lkeep([*self.get_dataset_oddrn(test_node)])
        if len(dataset_list) < 1:
            raise ValueError(
                "Dataset list is empty. Each test should have at least one dataset"
            )

        name = test_node.name

        if len(name) > 120:
            name = test_node.alias

        self._generator.set_oddrn_paths(
            **{"databases": test_node.database, "tests": name}
        )

        return DataEntity(
            oddrn=self._generator.get_oddrn_by_path("tests"),
            owner=None,
            name=name,
            type=DataEntityType.JOB,
            metadata=[get_metadata(test_node)],
            data_quality_test=DataQualityTest(
                suite_name=test_node.name,
                dataset_list=dataset_list,
                expectation=DataQualityTestExpectation(
                    type=test_node.test_metadata.name
                ),
            ),
        )

    def get_dataset_oddrn(self, test_node: TestNode) -> Iterable[Optional[str]]:
        nodes = self._context.manifest.nodes
        sources = self._context.manifest.sources

        if test_node.test_node_type == "generic":
            dependencies = test_node.depends_on_nodes

            for model_id in dependencies:
                model = nodes.get(model_id)

                if node := nodes.get(model_id):
                    if model.config.materialized == "seed":
                        yield seed_node_oddrn(node, self._generator)
                    else:
                        yield create_generator(
                            adapter_type=self._context.adapter_type,
                            credentials=self._context.credentials,
                        ).get_oddrn_for(node)
                elif node := sources.get(model_id):
                    yield create_generator(
                        adapter_type=self._context.adapter_type,
                        credentials=self._context.credentials,
                    ).get_oddrn_for(node)
                else:
                    raise KeyError(f"Could not find model/source with an id {model_id}")
        elif test_node.test_node_type == "singular":
            # We don't support it because it doesn't contains test_metadata
            raise NotImplementedError("Singular test nodes are not supported yet")
        else:
            raise NotImplementedError(
                f"Unknown test node type: {test_node.test_node_type}"
            )


def seed_node_oddrn(node: SeedNode, generator: DbtGenerator) -> Optional[str]:
    return generator.get_data_source_oddrn() + f"/seeds/{node.unique_id}"


def parse_status(
    result: Result, test_node: TestNode
) -> tuple[QualityRunStatus, Optional[str]]:
    status = QualityRunStatus.SUCCESS
    status_reason = None

    if result.status in {"fail", "error"}:
        status = QualityRunStatus.FAILED

    if result.status == "error":
        status_reason = result.status_reason or "Error during test execution"

    if result.status == "fail":
        print(result.status_reason)
        status_reason = StatusReason().get_reason(test_node=test_node)

    return status, status_reason
