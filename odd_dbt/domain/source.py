import dataclasses


@dataclasses.dataclass
class Source:
    database: str
    schema: str
    name: str

    @classmethod
    def _deserialize(cls, data: dict) -> "Source":
        return cls(
            database=data["database"],
            schema=data["schema"],
            name=data["name"],
        )
