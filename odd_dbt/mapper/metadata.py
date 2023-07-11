from dbt.contracts.graph.nodes import TestNode
from odd_models import MetadataExtension


def get_metadata(test_node: TestNode) -> MetadataExtension:
    metadata = {
        "name": test_node.name,
        "alias": test_node.alias,
        "build_path": test_node.build_path,
        "compiled": test_node.compiled,
        "compiled_code": test_node.compiled_code,
        "compiled_path": test_node.compiled_path,
        "created_at": test_node.created_at,
        "database": test_node.database,
        "deffered": test_node.deferred,
        "description": test_node.description,
        "extra_ctes_injected": test_node.extra_ctes_injected,
        "fqn": ", ".join(test_node.fqn),
        "language": test_node.language,
        "original_file_path": test_node.original_file_path,
        "package_name": test_node.package_name,
        "path": test_node.path,
    }

    schema_url = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/dbt.json#/definitions/DataQualityTestRun"
    return MetadataExtension(schema_url=schema_url, metadata=metadata)
