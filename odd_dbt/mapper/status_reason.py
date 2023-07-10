import re
from typing import Optional

from dbt.contracts.graph.nodes import (
    GenericTestNode,
    SingularTestNode,
    TestMetadata,
    TestNode,
)
from funcy import partial

from ..errors import CantParseReason
from ..logger import logger


def from_kwargs(test_metadata: TestMetadata, field: str, default=None):
    return test_metadata.kwargs.get(field, default)


get_column_name = partial(from_kwargs, field="column_name", default="Unknown column")
get_to = partial(from_kwargs, field="to")
get_field = partial(from_kwargs, field="field")
get_expression = partial(from_kwargs, field="expression")
get_compare_model = partial(from_kwargs, field="compare_model")
get_from_condition = partial(from_kwargs, field="from_condition")
get_to_condition = partial(from_kwargs, field="to_condition")
get_compare_column = partial(from_kwargs, field="compare_column", default=[])


def get_model_name(test_metadata: TestMetadata) -> Optional[str]:
    try:
        if model := from_kwargs(test_metadata, field="model"):
            return parse_model_name(model)
        else:
            return None
    except Exception as e:
        logger.warning(f"Can't get model name from {test_metadata=}. {e}")
        return None


def parse_model_name(model_name: Optional[str]) -> Optional[str]:
    if model_name is None:
        return None

    try:
        result = re.search(r"(\w+)", model_name)
        return model_name if result is None else result[1]
    except Exception as e:
        logger.warning(f"Can't parse model name {model_name=}. {e}")
        return model_name


class GenericTestReason:
    def get_reason(self, test_node: GenericTestNode) -> str:
        try:
            test_metadata = test_node.test_metadata

            return getattr(self, test_metadata.name)(test_node.test_metadata)
        except Exception as e:
            raise CantParseReason(f"Cant parse GenericTestReason. {e}") from e

    def unique(self, test_metadata: TestMetadata) -> str:
        column = get_column_name(test_metadata)
        model = get_model_name(test_metadata)

        return f"The {column=} in the {model=} must be unique"

    def not_null(self, test_metadata: TestMetadata) -> str:
        column = get_column_name(test_metadata)
        model = get_model_name(test_metadata)

        return f"The {column=} in the {model=} must not contain null values"

    def accepted_values(self, test_metadata: TestMetadata) -> str:
        column = get_column_name(test_metadata)
        model = get_model_name(test_metadata)
        acc_values = test_metadata.kwargs.get("values", "Unknown values")

        return f"The {column} in the {model=} must be one of {acc_values}"

    def relationships(self, test_metadata: TestMetadata) -> str:
        column = get_column_name(test_metadata)
        model = get_model_name(test_metadata)

        ref_model = parse_model_name(get_to(test_metadata))
        ref_field = get_field(test_metadata, "id")

        return f"Each value in the {column} in the {model} should exists as an {ref_field} in the {ref_model}"

    def expression_is_true(self, test_metadata: TestMetadata) -> str:
        expression = get_expression(test_metadata) or "Unknown expression"
        if len(expression) > 200:
            expression = f"{expression[:200]}..."
        return f"{expression=} is not true"

    def at_least_one(self, test_metadata: TestMetadata) -> str:
        return f"At least one row is expected for {get_column_name(test_metadata)}"

    def cardinality_equality(self, test_metadata: TestMetadata) -> str:
        return f"Cardinality of {get_column_name(test_metadata)} is not as expected"

    def equal_rowcount(self, test_metadata: TestMetadata) -> str:
        model = get_model_name(test_metadata)
        compare_model = parse_model_name(get_compare_model(test_metadata))
        return f"Rows count of {model=} and {compare_model=} are not equal"

    def equality(self, test_metadata: TestMetadata) -> str:
        model = get_model_name(test_metadata)
        compare_model = parse_model_name(get_compare_model(test_metadata))
        compare_column = get_compare_column(test_metadata) or []
        columns = ",".join(compare_column)
        if len(columns) > 200:
            columns = f"{columns[:200]}..."
        return f"{model} columns {columns} are not equal with {compare_model=}"

    def not_constant(self, test_metadata: TestMetadata) -> str:
        return f"{get_column_name(test_metadata)} is constant"

    def recency(self, test_metadata: TestMetadata) -> str:
        return f"{get_column_name(test_metadata)} is not recent enough"

    def relationships_where(self, test_metadata: TestMetadata) -> str:
        column = get_column_name(test_metadata)
        model = get_model_name(test_metadata)
        target_column = get_field(test_metadata, "id")
        target_model = parse_model_name(get_to(test_metadata))
        from_condition = get_from_condition(test_metadata)
        to_condition = get_to_condition(test_metadata)

        return f"{column=} in {model=} with {from_condition} is not related to {target_column=} in {target_model=} with {to_condition=}"

    def unique_combination_of_columns(self, test_metadata: TestMetadata) -> str:
        columns = test_metadata.kwargs.get("combination_of_columns", [])
        if len(columns) > 200:
            columns = f"{columns[:200]}..."
        return f"Combination of {columns} is not unique"


class SingularTestReason:
    def get_reason(self, test_node: SingularTestNode) -> str:
        raise CantParseReason("Reason for SingularTestNode not implemented yet.")


class StatusReason:
    def get_reason(self, test_node: TestNode) -> str:
        try:
            if isinstance(test_node, GenericTestNode):
                return GenericTestReason().get_reason(test_node)
            elif isinstance(test_node, SingularTestNode):
                return SingularTestReason().get_reason(test_node)
            else:
                return self.unknown_error()
        except Exception as e:
            logger.warning(f"Failed to get reason for test: {e}")
            logger.debug(f"Test definition: {test_node=}")
            return self.unknown_error()

    def unknown_error(self) -> str:
        return "Unknown reason"
