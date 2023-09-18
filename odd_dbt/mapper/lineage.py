import traceback
from typing import Optional

from dbt.contracts.graph.nodes import ModelNode
from funcy import compact, select_values, silent, walk_values
from odd_models.models import (
    DataEntity,
    DataEntityList,
    DataEntityType,
    DataTransformer,
)
from oddrn_generator import DbtGenerator

from odd_dbt import logger
from odd_dbt.domain.context import DbtContext
from odd_dbt.lib.dbt import is_a_model_node
from odd_dbt.mapper.generator import create_generator
from odd_dbt.mapper.metadata import get_model_metadata


class DbtLineageMapper:
    def __init__(self, context: DbtContext, generator: DbtGenerator) -> None:
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        model_nodes = select_values(is_a_model_node, self._context.nodes)
        mapped_nodes = compact(walk_values(silent(self.map_model_node), model_nodes))

        for model_id, data_entity in mapped_nodes.items():
            depends_on = model_nodes[model_id].depends_on.nodes

            for dep_id in depends_on:
                if node := mapped_nodes.get(dep_id):
                    data_entity.data_transformer.inputs.append(node.oddrn)
                else:
                    logger.warning(f"Can't find node {dep_id}")

        return DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=list(mapped_nodes.values()),
        )

    def map_model_node(self, model_node: ModelNode) -> Optional[DataEntity]:
        try:
            self._generator.set_oddrn_paths(models=model_node.unique_id)
            return DataEntity(
                name=model_node.unique_id,
                oddrn=self._generator.get_oddrn_by_path("models"),
                owner=None,
                type=DataEntityType.JOB,
                metadata=[get_model_metadata(model_node)],
                data_transformer=DataTransformer(
                    inputs=[],
                    outputs=[get_materialized_entity_oddrn(model_node, self._context)],
                ),
            )
        except Exception as e:
            logger.warning(f"Can't map model {model_node.unique_id}: {str(e)}")
            logger.debug(traceback.format_exc())
            return None


def get_materialized_entity_oddrn(model_node: ModelNode, context: DbtContext) -> str:
    return create_generator(
        adapter_type=context.adapter_type,
        credentials=context.credentials,
    ).get_oddrn_for(model_node)
