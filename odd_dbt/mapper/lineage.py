import traceback
from typing import Optional

from dbt.contracts.graph.nodes import ModelNode, SeedNode
from funcy import compact, select_values, silent, walk_values
from odd_models.models import (
    DataEntityList,
    DataEntityType,
)
from oddrn_generator import DbtGenerator

from odd_dbt import logger
from odd_dbt.domain.context import DbtContext
from odd_dbt.domain.model import ModelEntity, SeedEntity
from odd_dbt.libs.dbt import is_a_model_node, is_a_seed_node
from odd_dbt.mapper.generator import create_generator
from odd_dbt.mapper.metadata import get_model_metadata


class DbtLineageMapper:
    def __init__(self, context: DbtContext, generator: DbtGenerator) -> None:
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        model_nodes = select_values(is_a_model_node, self._context.nodes)
        seed_nodes = select_values(is_a_seed_node, self._context.nodes)

        model_entities: dict[str, ModelEntity] = compact(
            walk_values(silent(self.map_model), model_nodes)
        )

        seed_entities: dict[str, SeedEntity] = compact(
            walk_values(silent(self.map_seed), seed_nodes)
        )

        for model_id, entity in model_entities.items():
            depends_on = model_nodes[model_id].depends_on.nodes

            for dep_id in depends_on:
                if depend_on_entity := model_entities.get(dep_id):
                    entity.add_input(depend_on_entity.oddrn)
                    depend_on_entity.add_output(entity.oddrn)
                elif depend_on_entity := seed_entities.get(dep_id):
                    entity.add_input(depend_on_entity.oddrn)
                    # depend_on_entity.add_output(entity.oddrn)
                else:
                    logger.warning(f"Can't find node {dep_id}")

        return DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=list([*seed_entities.values(), *model_entities.values()]),
        )

    def map_seed(self, node: SeedNode) -> Optional[SeedEntity]:
        try:
            seed_entity = SeedEntity(
                name=node.unique_id,
                oddrn=f"{self._generator.get_data_source_oddrn()}/seeds/{node.unique_id}",
                owner=None,
                type=DataEntityType.FILE,
                metadata=[get_model_metadata(node)],
            )
            return seed_entity
        except Exception as e:
            logger.warning(f"Can't map seed {node.unique_id}: {str(e)}")
            logger.debug(traceback.format_exc())
            return None

    def map_model(self, node: ModelNode) -> Optional[ModelEntity]:
        try:
            self._generator.set_oddrn_paths(models=node.unique_id)
            model_entity = ModelEntity(
                name=node.unique_id,
                oddrn=self._generator.get_oddrn_by_path("models"),
                owner=None,
                type=DataEntityType.JOB,
                metadata=[get_model_metadata(node)],
            )
            model_entity.add_output(get_materialized_entity_oddrn(node, self._context))
            return model_entity
        except Exception as e:
            logger.warning(f"Can't map model {node.unique_id}: {str(e)}")
            logger.debug(traceback.format_exc())
            return None


def get_materialized_entity_oddrn(model_node: ModelNode, context: DbtContext) -> str:
    return create_generator(
        adapter_type=context.adapter_type,
        credentials=context.credentials,
    ).get_oddrn_for(model_node)
