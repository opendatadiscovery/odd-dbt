from enum import Enum
from typing import Optional

from dbt.config.renderer import ProfileRenderer


class DataSourceType(Enum):
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    POSTGRES = "postgres"
    MSSQL = "mssql"


class Profile:
    def __init__(self, **config) -> None:
        del config["password"]
        self._config: dict[str, str] = ProfileRenderer().render_data(config)

    @classmethod
    def from_dict(cls, profile: dict, target: Optional[str] = None) -> "Profile":
        target = profile.get("target", target)
        if not target:
            raise ValueError("Target must be set")
        profile = profile.get("outputs", {}).get(target, {})
        return cls(**profile)

    @property
    def type(self) -> DataSourceType:
        return DataSourceType(self._config["type"])

    @property
    def host(self) -> str:
        return self._config["host"]

    @property
    def account(self) -> Optional[str]:
        return self._config.get("account")

    def get(self, key: str) -> Optional[str]:
        return self._config.get(key)
