import re


class StatusReason:
    def __init__(self, test_def: dict):
        self.test_type = test_def["test_metadata"]["name"]
        self.metadata = test_def["test_metadata"]["kwargs"]
        self.model = re.search(r"'(.*)'", self.metadata["model"]).group(1)
        self.column = self.metadata["column_name"]

    def get_reason(self) -> str:
        method = getattr(self, self.test_type, self.default)
        return method()

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

    def default(self) -> str:
        return f"Status reason for test {self.test_type} not implemented yet"
