from odd_models import DataEntity, DataTransformer, DataSet


class ModelEntity(DataEntity):
    def __init__(self, **data: dict):
        super().__init__(**data)

        self.data_transformer = DataTransformer(
            inputs=[],
            outputs=[],
        )

    def add_input(self, oddrn: str) -> None:
        if oddrn not in self.data_transformer.inputs:
            self.data_transformer.inputs.append(oddrn)

    def add_output(self, oddrn: str) -> None:
        if oddrn not in self.data_transformer.outputs:
            self.data_transformer.outputs.append(oddrn)


class SeedEntity(DataEntity):
    def __init__(self, **data: dict):
        super().__init__(**data)
        self.dataset = DataSet(field_list=[])
