from odd_models import DataEntity, DataTransformer, DataSet, DataInput, DataSetField
import abc


class NodeEntity(DataEntity, abc.ABC):
    @abc.abstractmethod
    def add_upstream(self, upstream_node: "NodeEntity") -> None:
        ...

    @abc.abstractmethod
    def add_input(self, oddrn: str) -> None:
        ...

    @abc.abstractmethod
    def add_output(self, oddrn: str) -> None:
        ...


class ModelEntity(NodeEntity):
    def __init__(self, **data: dict):
        super().__init__(**data)

        self.data_transformer = DataTransformer(
            inputs=[],
            outputs=[],
        )

    def add_upstream(self, upstream_node: "NodeEntity") -> None:
        self.add_input(upstream_node.oddrn)

    def add_input(self, oddrn: str) -> None:
        if oddrn not in self.data_transformer.inputs:
            self.data_transformer.inputs.append(oddrn)

    def add_output(self, oddrn: str) -> None:
        if oddrn not in self.data_transformer.outputs:
            self.data_transformer.outputs.append(oddrn)


class ColumnEntity(DataSetField):
    def __init__(self, **data: dict):
        super().__init__(**data)


class SeedEntity(NodeEntity):
    def __init__(self, **data: dict):
        super().__init__(**data)
        self.dataset = DataSet(field_list=[])
        self.data_input = DataInput(outputs=[])

    def add_upstream(self, upstream_node: "NodeEntity") -> None:
        pass

    def add_input(self, oddrn: str) -> None:
        pass

    def add_output(self, oddrn: str) -> None:
        if oddrn not in self.data_input.outputs:
            self.data_input.outputs.append(oddrn)

    def add_column(self, column: ColumnEntity) -> None:
        self.dataset.field_list.append(column)
