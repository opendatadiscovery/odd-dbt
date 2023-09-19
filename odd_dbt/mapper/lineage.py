import traceback
from typing import Optional

from dbt.contracts.graph.nodes import ModelNode
from funcy import compact, select_values, silent, walk_values
from odd_models.models import (
    DataEntityList,
    DataEntityType,
)
from oddrn_generator import DbtGenerator
from odd_dbt.domain.model import ModelEntity
from odd_dbt import logger
from odd_dbt.domain.context import DbtContext
from odd_dbt.libs.dbt import is_a_model_node
from odd_dbt.mapper.generator import create_generator
from odd_dbt.mapper.metadata import get_model_metadata


class DbtLineageMapper:
    def __init__(self, context: DbtContext, generator: DbtGenerator) -> None:
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        model_nodes = select_values(is_a_model_node, self._context.nodes)
        source_nodes = self._context.manifest.sources
        print(source_nodes)

        model_entities: dict[str, ModelEntity] = compact(
            walk_values(silent(self.map_model_node), model_nodes)
        )

        for model_id, entity in model_entities.items():
            depends_on = model_nodes[model_id].depends_on.nodes

            for dep_id in depends_on:
                if depend_on_entity := model_entities.get(dep_id):
                    entity.add_input(depend_on_entity.oddrn)
                    depend_on_entity.add_output(entity.oddrn)
                else:
                    logger.warning(f"Can't find node {dep_id}")

        return DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=list(model_entities.values()),
        )

    def map_model_node(self, model_node: ModelNode) -> Optional[ModelEntity]:
        try:
            self._generator.set_oddrn_paths(models=model_node.unique_id)
            model_entity = ModelEntity(
                name=model_node.unique_id,
                oddrn=self._generator.get_oddrn_by_path("models"),
                owner=None,
                type=DataEntityType.JOB,
                metadata=[get_model_metadata(model_node)],
            )
            model_entity.add_output(
                get_materialized_entity_oddrn(model_node, self._context)
            )
            return model_entity
        except Exception as e:
            logger.warning(f"Can't map model {model_node.unique_id}: {str(e)}")
            logger.debug(traceback.format_exc())
            return None


def get_materialized_entity_oddrn(model_node: ModelNode, context: DbtContext) -> str:
    return create_generator(
        adapter_type=context.adapter_type,
        credentials=context.credentials,
    ).get_oddrn_for(model_node)
