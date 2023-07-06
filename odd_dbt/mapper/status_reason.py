import re

from dbt.contracts.graph.nodes import GenericTestNode, SingularTestNode, TestNode

from ..logger import logger


class GenericTestReason:
    def get_reason(self, test_node: GenericTestNode) -> str:
        try:
            self.test_type = test_node.test_metadata.name
            self.metadata = test_node.test_metadata.kwargs
            self.model = re.search(r"'(.*)'", self.metadata["model"]).group(1)
            self.column = self.metadata.get("column_name")

            return getattr(self, self.test_type, self.default)()
        except Exception as e:
            logger.error(f"Failed to get reason for test {test_node.name}: {e}")
            logger.debug(f"Test definition: {test_node}")
            return self.default()

    def default(self) -> str:
        logger.warning(f"Reason for test {self.test_type} not implemented yet")
        return "Unknown reason"

    def unique(self) -> str:
        return f"The {self.column} column in the {self.model} model should be unique"

    def not_null(self) -> str:
        return f"the {self.column} column in the {self.model} model should not contain null values"

    def accepted_values(self) -> str:
        acc_values = self.metadata["values"]
        return f"The {self.column} column in the {self.model} should be one of {acc_values}"

    def relationships(self) -> str:
        ref_model = re.search(r"'(.*)'", self.metadata["to"]).group(1)
        ref_field = self.metadata["field"]
        return f"Each value in the {self.column} in the {self.model} should exists as an {ref_field} in the {ref_model}"


class SingularTestReason:
    def get_reason(self, test_node: SingularTestNode) -> str:
        logger.warning("Reason for SingularTestNode not implemented yet")
        logger.debug(f"Test definition: {test_node}")
        return "Unknown reason"


class StatusReason:
    def get_reason(self, test_node: TestNode) -> str:
        try:
            if isinstance(test_node, GenericTestNode):
                return GenericTestReason().get_reason(test_node)
            elif isinstance(test_node, SingularTestNode):
                return SingularTestReason().get_reason(test_node)
            else:
                return self.default()
        except Exception as e:
            logger.warning(f"Failed to get reason for test {test_node}: {e}")
            logger.debug(f"Test definition: {test_node}")
            return self.default()

    def default(self) -> str:
        return "Failed"
