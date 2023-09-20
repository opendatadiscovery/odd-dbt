import traceback
from typing import Optional, Union

from dbt.contracts.graph.nodes import ModelNode, SeedNode
from odd_models.models import (
    DataEntityList,
    DataEntityType,
)
from oddrn_generator import DbtGenerator

from odd_dbt import logger
from odd_dbt.domain.context import DbtContext
from odd_dbt.domain.model import ModelEntity, SeedEntity, NodeEntity
from odd_dbt.mapper.generator import create_generator
from odd_dbt.mapper.metadata import get_model_metadata


class DbtLineageMapper:
    _SUPPORTED_NODE_TYPES = (ModelNode, SeedNode)

    def __init__(self, context: DbtContext, generator: DbtGenerator) -> None:
        self._context = context
        self._generator = generator

    def map(self) -> DataEntityList:
        nodes: dict[str, Union[ModelNode, SeedNode]] = {
            uid: node
            for uid, node in self._context.manifest.nodes.items()
            if isinstance(node, self._SUPPORTED_NODE_TYPES)
        }
        node_entities = {}

        for uid, node in nodes.items():
            try:
                node_entities[uid] = self.map_node(node)
            except Exception as e:
                logger.warning(f"Can't map node {node.unique_id}: {str(e)}")
                logger.debug(traceback.format_exc())
                continue

        for node_id, entity in node_entities.items():
            upstream_ids = nodes[node_id].depends_on_nodes

            for upstream_id in upstream_ids:
                upstream_entity = node_entities.get(upstream_id)

                if not upstream_entity:
                    logger.warning(
                        f"For {node_id} Can't find node {upstream_id}. Upstream node must be model or seed"
                    )
                    continue

                entity.add_upstream(upstream_entity)

        return DataEntityList(
            data_source_oddrn=self._generator.get_data_source_oddrn(),
            items=list(node_entities.values()),
        )

    def map_node(self, node: Union[ModelNode, SeedNode]) -> Optional[NodeEntity]:
        if isinstance(node, ModelNode):
            return self.map_model(node)
        elif isinstance(node, SeedNode):
            return self.map_seed(node)

    def map_seed(self, node: SeedNode) -> SeedEntity:
        return SeedEntity(
            name=node.unique_id,
            oddrn=self._generator.get_oddrn_by_path("seeds", node.unique_id),
            owner=None,
            type=DataEntityType.FILE,
            metadata=[get_model_metadata(node)],
        )

    def map_model(self, node: ModelNode) -> ModelEntity:
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


def get_materialized_entity_oddrn(model_node: ModelNode, context: DbtContext) -> str:
    return create_generator(
        adapter_type=context.adapter_type,
        credentials=context.credentials,
    ).get_oddrn_for(model_node)
